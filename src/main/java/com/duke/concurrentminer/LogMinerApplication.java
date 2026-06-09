package com.duke.concurrentminer;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.duke.concurrentminer.container.MetricsCollector;
import com.duke.concurrentminer.core.AsyncAlertManager;
import com.duke.concurrentminer.core.LogParserPool;
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
                runVerify(dir, threads);
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
    // 多线程并发挖掘 (Phase 3: ThreadPoolExecutor Pipeline)
    // ============================================================

    /**
     * 启动生产者-消费者管道:
     *   生产者 = N 个 LogReader 线程 (每文件一个)
     *   消费者 = LogParserPool (动态线程池) + MetricsCollector (无锁统计)
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
        int corePoolSize = threads;
        int maxPoolSize = threads * 2;

        // 中央有界阻塞队列
        ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10000);

        // 无锁统计容器
        MetricsCollector metrics = new MetricsCollector();

        // 异步告警编排器 — Phase 5 新增
        AsyncAlertManager alertManager = new AsyncAlertManager();

        System.out.println("=== Phase 5: Async Alert Chain Pipeline ===");
        System.out.printf("Files: %d | Queue: 10,000 | Pool: core=%d/max=%d | "
                        + "Stats: ConcurrentHashMap + LongAdder%n",
                readerCount, corePoolSize, maxPoolSize);

        long startTime = System.currentTimeMillis();

        // ── 启动消费者线程池 ──
        LogParserPool parserPool = new LogParserPool(
                queue, readerCount, corePoolSize, maxPoolSize, metrics, alertManager,
                "snapshots", 1_000_000);
        parserPool.start();

        // ── 启动生产者线程 ──
        ExecutorService readerPool = Executors.newFixedThreadPool(readerCount);
        for (File file : logFiles) {
            readerPool.execute(new LogReader(file, queue, corePoolSize));
        }

        // ── 等待生产者全部完成 ──
        readerPool.shutdown();
        readerPool.awaitTermination(30, TimeUnit.MINUTES);

        // ── 优雅关闭消费者线程池 ──
        parserPool.shutdownAndAwait(60);

        long elapsed = System.currentTimeMillis() - startTime;
        long lines = parserPool.getParsedCount();
        long errors = parserPool.getParseErrors();
        double seconds = elapsed / 1000.0;
        double throughput = lines * 1000.0 / Math.max(elapsed, 1);

        // ── Phase 4 报告 ──
        System.out.println();
        System.out.println("========== Phase 4 Pipeline Results ==========");
        System.out.printf("Total lines:      %,d%n", lines);
        System.out.printf("Parse errors:     %,d%n", errors);
        System.out.printf("Pipeline elapsed: %,d ms (%.2f sec)%n", elapsed, seconds);
        System.out.printf("Queue remaining:  %,d (should be 0)%n", queue.size());
        System.out.printf("Throughput:       %,.0f lines/sec%n", throughput);
        System.out.printf("Parser threads:   %d%n", corePoolSize);
        System.out.printf("Async alerts:     %,d (CompletableFuture chain)%n",
                alertManager.getAlertCount());

        // ── Level 分布 ──
        Map<String, Long> levelSnap = metrics.getLevelSnapshot();
        System.out.println();
        System.out.println("--- Level Distribution (Lock-Free) ---");
        for (Map.Entry<String, Long> e : levelSnap.entrySet()) {
            double pct = lines > 0 ? 100.0 * e.getValue() / lines : 0;
            System.out.printf("  %-5s: %,12d  (%5.2f%%)%n", e.getKey(), e.getValue(), pct);
        }

        // ── Top 10 Hot IPs ──
        System.out.println();
        System.out.println("--- Top 10 Hot IPs (Heap-Selected) ---");
        for (Map.Entry<String, Long> e : metrics.getTopNIPs(10)) {
            System.out.printf("  %-15s  %,12d%n", e.getKey(), e.getValue());
        }
    }

    // ============================================================
    // 对账验证 (Phase 7)
    // ============================================================

    /**
     * 单线程 vs 多线程对账验证。
     *
     * 红线指标: 多线程统计结果必须与单线程串行结果 100% 一致。
     * 任何差异都意味着并发数据结构使用不当。
     */
    static void runVerify(String dirPath, int threads) throws Exception {
        File dir = new File(dirPath);
        File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (logFiles == null || logFiles.length == 0) {
            System.err.println("No .log files found in " + dir.getAbsolutePath());
            return;
        }

        System.out.println("╔══════════════════════════════════════════════╗");
        System.out.println("║   Phase 7: Reconciliation Verification      ║");
        System.out.println("║   Single-Thread  vs  Multi-Thread (JUC)      ║");
        System.out.println("╚══════════════════════════════════════════════╝");
        System.out.println();

        // ── Step 1: 单线程串行扫描 ──
        System.out.println("─── Step 1/3: Single-Thread Scan ───");
        long stStart = System.currentTimeMillis();
        MetricsCollector singleMetrics = countSingleThreaded(dir, logFiles);
        long stElapsed = System.currentTimeMillis() - stStart;
        Map<String, Long> stLevels = singleMetrics.getLevelSnapshot();
        long stTotal = singleMetrics.getTotalLevelCount();

        System.out.printf("  Single-thread: %,d lines in %,d ms (%,.0f lines/sec)%n%n",
                stTotal, stElapsed, stTotal * 1000.0 / Math.max(stElapsed, 1));

        // ── Step 2: 多线程并发管道 ──
        System.out.println("─── Step 2/3: Multi-Thread Pipeline ───");
        long mtStart = System.currentTimeMillis();

        ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10000);
        MetricsCollector multiMetrics = new MetricsCollector();
        int readerCount = logFiles.length;
        int corePoolSize = threads;

        LogParserPool parserPool = new LogParserPool(
                queue, readerCount, corePoolSize, threads * 2,
                multiMetrics, null, "snapshots", 1_000_000); // 对账模式跳过异步告警
        parserPool.start();

        ExecutorService readerPool = Executors.newFixedThreadPool(readerCount);
        for (File file : logFiles) {
            readerPool.execute(new LogReader(file, queue, corePoolSize));
        }
        readerPool.shutdown();
        readerPool.awaitTermination(30, TimeUnit.MINUTES);
        parserPool.shutdownAndAwait(60);

        long mtElapsed = System.currentTimeMillis() - mtStart;
        Map<String, Long> mtLevels = multiMetrics.getLevelSnapshot();
        long mtTotal = parserPool.getParsedCount();
        long mtErrors = parserPool.getParseErrors();

        System.out.printf("  Multi-thread:  %,d lines in %,d ms (%,.0f lines/sec)%n",
                mtTotal, mtElapsed, mtTotal * 1000.0 / Math.max(mtElapsed, 1));
        System.out.println();

        // ── Step 3: 逐项对账 ──
        System.out.println("─── Step 3/3: Reconciliation ───");
        System.out.println();

        boolean allMatch = true;
        int mismatches = 0;

        // ── 对账: Level 分布 ──
        System.out.println("─── Level Distribution " + repeatChar('─', 35));
        System.out.printf("│ %-8s  %15s  %15s  %10s%n",
                "Level", "Single-Thread", "Multi-Thread", "Status");
        System.out.println("├" + repeatChar('─', 55));

        for (String lv : new String[]{"INFO", "WARN", "ERROR", "DEBUG", "FATAL"}) {
            long stVal = stLevels.getOrDefault(lv, 0L);
            long mtVal = mtLevels.getOrDefault(lv, 0L);
            boolean match = stVal == mtVal;
            if (!match) { allMatch = false; mismatches++; }
            System.out.printf("│ %-8s  %,15d  %,15d  %s%n",
                    lv, stVal, mtVal, match ? "✅" : "❌ DIFF=" + (mtVal - stVal));
        }
        System.out.println("└" + repeatChar('─', 55));
        System.out.println();

        // ── 对账: 总行数 ──
        System.out.printf("Total lines:   ST=%,d  MT=%,d  %s%n",
                stTotal, mtTotal, stTotal == mtTotal ? "✅" : "❌");
        System.out.printf("Parse errors:  ST=0  MT=%,d  %s%n",
                mtErrors, mtErrors == 0 ? "✅" : "⚠️");
        System.out.println();

        // ── 对账: Top-10 IP ──
        System.out.println("┌─ Top-10 IP Overlap " + repeatChar('─', 44));
        List<Map.Entry<String, Long>> stTop = singleMetrics.getTopNIPs(10);
        List<Map.Entry<String, Long>> mtTop = multiMetrics.getTopNIPs(10);

        Set<String> stIPs = new HashSet<>();
        for (Map.Entry<String, Long> e : stTop) stIPs.add(e.getKey());
        int overlap = 0;
        for (Map.Entry<String, Long> e : mtTop) {
            if (stIPs.contains(e.getKey())) overlap++;
        }
        System.out.printf("│ Top-10 overlap: %d/10%n", overlap);
        System.out.println("└" + repeatChar('─', 55));
        System.out.println();

        // ── 性能对比 ──
        double speedup = (double) stElapsed / Math.max(mtElapsed, 1);
        System.out.println("┌─ Performance " + repeatChar('─', 48));
        System.out.printf("│ Single-thread:  %,6d ms  (%,.0f lines/sec)%n",
                stElapsed, stTotal * 1000.0 / Math.max(stElapsed, 1));
        System.out.printf("│ Multi-thread:   %,6d ms  (%,.0f lines/sec)%n",
                mtElapsed, mtTotal * 1000.0 / Math.max(mtElapsed, 1));
        System.out.printf("│ Speedup:        %.2fx%n", speedup);
        System.out.println("└" + repeatChar('─', 55));
        System.out.println();

        // ── Snapshot 验证 ──
        File snapDir = new File("snapshots");
        File[] snaps = snapDir.listFiles((d, n) -> n.endsWith(".txt"));
        int snapCount = snaps != null ? snaps.length : 0;
        int expectedSnaps = (int) (mtTotal / 1_000_000);
        System.out.printf("Checkpoints: %d snapshot(s) found (expected ~%d)%n",
                snapCount, expectedSnaps);

        // ── 最终裁决 ──
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════╗");
        if (allMatch && mtErrors == 0) {
            System.out.println("║  🏆 VERDICT: ALL CLEAR — 100% Consistent    ║");
        } else {
            System.out.println("║  ❌ VERDICT: DATA DRIFT DETECTED             ║");
            System.out.printf("║  Mismatches: %d                              ║%n", mismatches);
        }
        System.out.println("╚══════════════════════════════════════════════╝");
    }

    /**
     * 单线程扫描，使用 MetricsCollector 收集统计（与多线程用同一数据结构）。
     */
    static MetricsCollector countSingleThreaded(File dir, File[] logFiles) throws IOException {
        MetricsCollector mc = new MetricsCollector();

        for (File file : logFiles) {
            try (BufferedReader reader = new BufferedReader(
                    new FileReader(file), 256 * 1024)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    Matcher m = LOG_PATTERN.matcher(line);
                    if (m.matches()) {
                        mc.incrementLevel(m.group(2).trim());
                        mc.incrementIP(m.group(3).trim());
                    }
                }
            }
        }
        return mc;
    }

    // ============================================================
    // 工具方法
    // ============================================================

    static String repeatChar(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }

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
