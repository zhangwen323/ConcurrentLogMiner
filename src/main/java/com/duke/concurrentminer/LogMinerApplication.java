package com.duke.concurrentminer;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.duke.concurrentminer.core.LogReader;
import com.duke.concurrentminer.util.LogGenerator;

/**
 * ConcurrentLogMiner 系统启动主入口。
 *
 * 支持模式:
 *   --mode generate  生成测试日志数据
 *   --mode baseline  单线程串行扫描，建立性能基线
 *   --mode mine      多线程并发挖掘（后续 Phase 实现）
 *   --mode verify    单线程 vs 多线程对账（后续 Phase 实现）
 *
 * @author zqw
 */
public class LogMinerApplication {

    // 日志行解析正则: "Timestamp | Level | IP | Message"
    private static final Pattern LOG_PATTERN =
            Pattern.compile("^(.+?) \\| (.+?) \\| (.+?) \\| (.+)$");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printHelp();
            return;
        }

        String mode = "help";
        String dir = "testdata";
        int threads = 4;
        int count = 5_000_000;
        int files = 10;

        // 简易命令行参数解析
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mode":    mode = args[++i]; break;
                case "--dir":     dir = args[++i]; break;
                case "--threads": threads = Integer.parseInt(args[++i]); break;
                case "--count":   count = Integer.parseInt(args[++i]); break;
                case "--files":   files = Integer.parseInt(args[++i]); break;
                case "--help":
                case "-h":        printHelp(); return;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    printHelp();
                    return;
            }
        }

        switch (mode) {
            case "generate":
                LogGenerator.generate(count, dir, files);
                break;
            case "baseline":
                runBaseline(dir);
                break;
            case "mine":
                runMine(dir, threads);
                break;
            case "verify":
                System.out.println("[Verify] Not implemented yet — coming in Phase 7.");
                break;
            default:
                System.err.println("Unknown mode: " + mode);
                printHelp();
        }
    }

    // ============================================================
    // 单线程基线扫描
    // ============================================================

    /**
     * 单线程逐行扫描所有 .log 文件，统计各 Level 出现次数 + IP 分布，
     * 记录耗时与吞吐率，写入 baseline_result.txt。
     */
    static void runBaseline(String dirPath) throws IOException {
        File dir = new File(dirPath);
        File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (logFiles == null || logFiles.length == 0) {
            System.err.println("No .log files found in " + dir.getAbsolutePath());
            return;
        }

        System.out.println("=== Single-Thread Baseline Scan ===");
        System.out.printf("Directory: %s (%d file(s))%n", dir.getAbsolutePath(), logFiles.length);

        // 统计计数器（普通 HashMap，单线程用）
        Map<String, Long> levelCount = new LinkedHashMap<>();
        Map<String, Long> ipCount = new HashMap<>();
        for (String lv : new String[]{"INFO", "WARN", "ERROR", "DEBUG", "FATAL"}) {
            levelCount.put(lv, 0L);
        }
        long totalLines = 0;
        long parseErrors = 0;

        long startTime = System.currentTimeMillis();

        for (File file : logFiles) {
            System.out.printf("  Scanning %s ...%n", file.getName());
            try (BufferedReader reader = new BufferedReader(new FileReader(file), 256 * 1024)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    totalLines++;
                    Matcher m = LOG_PATTERN.matcher(line);
                    if (m.matches()) {
                        String level = m.group(2).trim();
                        String ip = m.group(3).trim();
                        levelCount.merge(level, 1L, Long::sum);
                        ipCount.merge(ip, 1L, Long::sum);
                    } else {
                        parseErrors++;
                    }
                }
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        double seconds = elapsed / 1000.0;
        double throughput = totalLines / seconds;

        // ── 输出结果 ──
        System.out.println();
        System.out.println("========== Baseline Results ==========");
        System.out.printf("Total lines:    %,d%n", totalLines);
        System.out.printf("Parse errors:   %,d%n", parseErrors);
        System.out.printf("Elapsed:        %,d ms (%.2f sec)%n", elapsed, seconds);
        System.out.printf("Throughput:     %,.0f lines/sec%n", throughput);
        System.out.println();
        System.out.println("--- Level Distribution ---");
        for (Map.Entry<String, Long> e : levelCount.entrySet()) {
            double pct = totalLines > 0 ? 100.0 * e.getValue() / totalLines : 0;
            System.out.printf("  %-5s: %,12d  (%5.2f%%)%n", e.getKey(), e.getValue(), pct);
        }

        // Top-10 Hot IPs
        System.out.println();
        System.out.println("--- Top 10 Hot IPs ---");
        ipCount.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(10)
                .forEach(e -> System.out.printf("  %-15s  %,12d%n", e.getKey(), e.getValue()));

        // ── 写入文件 ──
        try (PrintWriter pw = new PrintWriter(
                new FileWriter("baseline_result.txt"))) {
            pw.printf("=== ConcurrentLogMiner Baseline Results ===%n");
            pw.printf("Scan time: %s%n", new Date());
            pw.printf("Total lines: %,d%n", totalLines);
            pw.printf("Parse errors: %,d%n", parseErrors);
            pw.printf("Elapsed: %,d ms (%.2f sec)%n", elapsed, seconds);
            pw.printf("Throughput: %,.0f lines/sec%n", throughput);
            pw.println();
            pw.println("Level Distribution:");
            for (Map.Entry<String, Long> e : levelCount.entrySet()) {
                double pct = totalLines > 0 ? 100.0 * e.getValue() / totalLines : 0;
                pw.printf("  %-5s: %,12d  (%5.2f%%)%n", e.getKey(), e.getValue(), pct);
            }
        }
        System.out.println();
        System.out.println("[Baseline] Results written to baseline_result.txt");
    }

    // ============================================================
    // 多线程并发挖掘 (Phase 2: 生产者-消费者管道)
    // ============================================================

    /**
     * 启动生产者-消费者管道:
     *   生产者 = N 个 LogReader 线程 (每文件一个)
     *   消费者 = 1 个简单消费者线程 (Phase 3 将升级为 ThreadPoolExecutor)
     *   缓冲区  = ArrayBlockingQueue<String> (容量 10000)
     */
    static void runMine(String dirPath, int threads) throws Exception {
        File dir = new File(dirPath);
        File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (logFiles == null || logFiles.length == 0) {
            System.err.println("No .log files found in " + dir.getAbsolutePath());
            return;
        }

        int readerCount = logFiles.length;
        int consumerCount = threads; // 后续 Phase 3 使用，Phase 2 先用 1 个

        // 中央有界阻塞队列 —— 容量硬限制 10000，满了自动阻塞生产者
        ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10000);

        System.out.println("=== Multi-Thread Producer-Consumer Pipeline ===");
        System.out.printf("Files: %d | Queue Capacity: 10,000 | Consumers: %d (stub)%n",
                readerCount, consumerCount);

        long startTime = System.currentTimeMillis();

        // ── 启动消费者线程 ──
        // Phase 2: 简单单线程消费者，后续 Phase 3 升级为 ThreadPoolExecutor
        // 使用数组传引用，在线程内更新 totalConsumed
        final long[] totalConsumed = {0};

        Thread consumer = new Thread(() -> {
            String tName = Thread.currentThread().getName();
            long consumed = 0;
            long poisonReceived = 0;
            long lastReport = System.currentTimeMillis();

            try {
                while (poisonReceived < readerCount) {
                    String line = queue.take(); // 队列空时阻塞

                    if (LogReader.POISON_PILL.equals(line)) {
                        poisonReceived++;
                        continue;
                    }

                    consumed++;

                    // 每 1 秒输出队列状态
                    long now = System.currentTimeMillis();
                    if (now - lastReport >= 1000) {
                        System.out.printf("[%s] Consumed: %,d | Queue: %,d/10,000 | Poison: %d/%d%n",
                                tName, consumed, queue.size(),
                                poisonReceived, readerCount);
                        lastReport = now;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            totalConsumed[0] = consumed;
            System.out.printf("[%s] Consumer done: %,d lines consumed, %d poison pills received%n",
                    tName, consumed, poisonReceived);
        }, "Consumer-1");
        consumer.start();

        // ── 启动生产者线程 ──
        ExecutorService readerPool = Executors.newFixedThreadPool(readerCount);
        for (File file : logFiles) {
            readerPool.execute(new LogReader(file, queue, 1)); // 每个 Reader 投 1 枚毒丸
        }

        // ── 等待生产者全部完成 ──
        readerPool.shutdown();
        readerPool.awaitTermination(30, TimeUnit.MINUTES);

        // ── 等待消费者完成 ──
        consumer.join();

        long elapsed = System.currentTimeMillis() - startTime;
        long lines = totalConsumed[0];

        // Phase 2 报告 (先不打印 Level 统计，Phase 4 完善)
        System.out.println();
        System.out.println("========== Phase 2 Pipeline Results ==========");
        System.out.printf("Total lines:      %,d%n", lines);
        System.out.printf("Pipeline elapsed: %,d ms (%.2f sec)%n", elapsed, elapsed / 1000.0);
        System.out.printf("Queue remaining:  %,d lines (should be 0)%n", queue.size());
        System.out.printf("Throughput:       %,.0f lines/sec%n",
                lines * 1000.0 / Math.max(elapsed, 1));
    }

    // ============================================================
    // 工具方法
    // ============================================================

    static void printHelp() {
        System.out.println("ConcurrentLogMiner — 高性能多线程日志分析与检索系统");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  --mode MODE     Operation mode (required)");
        System.out.println("  --dir DIR       Log file directory (default: testdata)");
        System.out.println("  --count N       Total log lines to generate (default: 5000000)");
        System.out.println("  --files N       Number of log files (default: 10)");
        System.out.println("  --threads N     Number of worker threads (default: 4)");
        System.out.println();
        System.out.println("Modes:");
        System.out.println("  generate   Generate test log files");
        System.out.println("  baseline   Single-thread scan & performance baseline");
        System.out.println("  mine       Multi-thread concurrent mining (Phase 3+)");
        System.out.println("  verify     Single vs multi-thread reconciliation (Phase 7)");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar ... --mode generate --count 5000000 --files 10");
        System.out.println("  java -jar ... --mode baseline --dir testdata");
    }
}
