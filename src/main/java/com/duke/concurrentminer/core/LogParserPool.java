package com.duke.concurrentminer.core;

import com.duke.concurrentminer.container.MetricsCollector;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 日志解析线程池（消费者）。
 *
 * 负责从中央有界队列中取出原始日志行，进行解析、统计、告警等处理。
 *
 * 核心设计要点：
 *   1. 手动构造 ThreadPoolExecutor —— 深入理解 core/max/keepAlive/queue/拒绝策略
 *   2. 独立 Monitor 守护线程 —— 每 1000ms 输出线程池健康画像
 *   3. CallerRunsPolicy —— 过载时由调用者线程执行，形成限流回压
 *   4. 毒丸协议 —— 每个 Parser 收到 readerCount 枚毒丸后退出
 *
 * JUC 源码学习点：
 *   ThreadPoolExecutor.execute() → ctl (AtomicInteger) 的位运算：
 *     - 高 3 位: RUNNING(111) / SHUTDOWN(000) / STOP(001) / TIDYING(010) / TERMINATED(011)
 *     - 低 29 位: worker 数量
 *     - workerCountOf(c)  = c & CAPACITY
 *     - runStateOf(c)     = c & ~CAPACITY
 *
 * @author duke
 */
public class LogParserPool {

    /** 默认配置 */
    public static final int DEFAULT_CORE_POOL_SIZE = 4;
    public static final int DEFAULT_MAX_POOL_SIZE  = 8;
    public static final long DEFAULT_KEEP_ALIVE_SEC = 60;
    public static final int DEFAULT_POOL_QUEUE_CAP = 2000;

    /** 日志行解析正则: "Timestamp | Level | IP | Message" */
    private static final Pattern LOG_PATTERN =
            Pattern.compile("^(.+?) \\| (.+?) \\| (.+?) \\| (.+)$");

    private final ThreadPoolExecutor pool;
    private final ArrayBlockingQueue<String> sourceQueue;  // 中央日志队列
    private final int readerCount;                          // 生产者数量 (用于毒丸计数)
    private final MetricsCollector metrics;                  // 无锁统计容器
    private final AsyncAlertManager alertManager;            // 异步告警编排器

    /** 已解析行数（用于 Monitor 展示） */
    private final AtomicLong parsedCount = new AtomicLong(0);
    /** 解析失败行数 */
    private final AtomicLong parseErrors = new AtomicLong(0);

    private Thread monitorThread;

    /**
     * @param sourceQueue  中央有界阻塞队列 (LogReader → this)
     * @param readerCount  LogReader 线程数量
     * @param corePoolSize 核心线程数
     * @param maxPoolSize  最大线程数
     * @param metrics      无锁统计容器
     * @param alertManager 异步告警编排器 (null = 禁用告警)
     */
    public LogParserPool(ArrayBlockingQueue<String> sourceQueue,
                         int readerCount,
                         int corePoolSize,
                         int maxPoolSize,
                         MetricsCollector metrics,
                         AsyncAlertManager alertManager) {
        this.sourceQueue = sourceQueue;
        this.readerCount = readerCount;
        this.metrics = metrics;
        this.alertManager = alertManager;

        // 手动构造 ThreadPoolExecutor —— 学习重点！
        this.pool = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                DEFAULT_KEEP_ALIVE_SEC, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(DEFAULT_POOL_QUEUE_CAP),
                new ThreadPoolExecutor.CallerRunsPolicy()
                // ↑ 队列满 + 线程满时，由提交任务的线程直接执行 → 天然限流
        );
    }

    /**
     * 启动线程池：提交 corePoolSize 个 Parser 工作线程，启动 Monitor。
     */
    public void start() {
        int coreSize = pool.getCorePoolSize();

        // 预创建核心线程
        pool.prestartAllCoreThreads();

        // 提交 Parser 工作任务
        for (int i = 0; i < coreSize; i++) {
            pool.execute(new ParserWorker(i));
        }

        // 启动 Monitor 守护线程
        monitorThread = new Thread(new MonitorTask(), "Monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();

        System.out.printf("[LogParserPool] Started: core=%d, max=%d, keepAlive=%ds, "
                        + "poolQueueCap=%d, callerRunsPolicy=true%n",
                pool.getCorePoolSize(), pool.getMaximumPoolSize(),
                DEFAULT_KEEP_ALIVE_SEC, DEFAULT_POOL_QUEUE_CAP);
    }

    /**
     * 优雅关闭流程:
     *   1. shutdown() — 拒绝新任务，但现有 Parser 继续处理直至收到毒丸退出
     *   2. awaitTermination() — 等待所有 Parser 自然退出
     *   3. 超时 → shutdownNow() 强制中断
     *
     * 线程池终止后 Monitor 会在下一次循环检测到并自行退出。
     */
    public void shutdownAndAwait(long timeoutSec) {
        System.out.println("[LogParserPool] Initiating graceful shutdown...");

        pool.shutdown(); // 不再接收新任务
        try {
            // 等待所有 Parser 处理完毒丸并退出
            if (!pool.awaitTermination(timeoutSec, TimeUnit.SECONDS)) {
                System.err.println("[LogParserPool] Timeout after " + timeoutSec
                        + "s! Forcing shutdownNow...");
                pool.shutdownNow();
                // 再等一小段时间让强制中断生效
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("[LogParserPool] Pool did not terminate even after shutdownNow!");
                }
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.printf("[LogParserPool] Terminated. Final: parsed=%,d, completedTasks=%d%n",
                parsedCount.get(), pool.getCompletedTaskCount());

        // 关闭异步告警管理器
        if (alertManager != null) {
            alertManager.shutdown();
        }
    }

    /**
     * 获取已解析行数。
     */
    public long getParsedCount() {
        return parsedCount.get();
    }

    /**
     * 获取解析错误数。
     */
    public long getParseErrors() {
        return parseErrors.get();
    }

    /**
     * 获取统计容器引用（用于最终报告）。
     */
    public MetricsCollector getMetrics() {
        return metrics;
    }

    // ================================================================
    // Parser 工作线程
    // ================================================================

    /**
     * 每个 ParserWorker 是一个长期运行的任务，循环从中央队列 take() 日志行，
     * 直到收到足够数量的毒丸后退出。
     *
     * 每条日志进行正则解析，然后通过 MetricsCollector 无锁累加统计。
     *
     * 性能注意: regex 解析是 CPU 密集型操作，是当前管道的最大瓶颈。
     * ConcurrentHashMap + LongAdder 的组合确保了统计部分不会成为瓶颈。
     */
    class ParserWorker implements Runnable {
        private final int id;

        ParserWorker(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            long consumed = 0;
            long errors = 0;
            int poisonReceived = 0;

            try {
                while (poisonReceived < readerCount) {
                    String line = sourceQueue.take(); // 队列空时阻塞

                    if (LogReader.POISON_PILL.equals(line)) {
                        poisonReceived++;
                        continue;
                    }

                    // ── 正则解析 + 无锁统计 ──
                    Matcher m = LOG_PATTERN.matcher(line);
                    if (m.matches()) {
                        String level = m.group(2).trim();
                        String ip = m.group(3).trim();
                        String msg = m.group(4).trim();
                        // 无锁原子累加
                        metrics.incrementLevel(level);
                        metrics.incrementIP(ip);

                        // ERROR 级别 → 触发异步告警链 (不阻塞主解析)
                        if ("ERROR".equals(level) && alertManager != null) {
                            alertManager.handleError(line, ip, msg);
                        }
                    } else {
                        errors++;
                    }

                    consumed++;
                    parsedCount.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            parseErrors.addAndGet(errors);
            System.out.printf("[Parser-%d] Done: %,d lines consumed, "
                            + "%d errors, %d poison pills received%n",
                    id, consumed, errors, poisonReceived);
        }
    }

    // ================================================================
    // Monitor 守护线程
    // ================================================================

    /**
     * 每 1000ms 打印一次线程池健康画像，用于观察线程池动态行为。
     *
     * 关键监控指标：
     *   core    — 核心线程数
     *   active  — 当前活跃线程数（正在执行任务的线程）
     *   poolSz  — 当前池中线程总数（core + 临时扩展的）
     *   queue   — 线程池内部任务队列的排队情况
     *   srcQ    — 中央日志队列的当前大小
     *   parsed  — 累计已解析行数
     *   comp    — 累计已完成任务数
     */
    class MonitorTask implements Runnable {
        @Override
        public void run() {
            while (!pool.isTerminated()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }

                if (pool.isTerminated()) break;

                BlockingQueue<Runnable> poolQueue = pool.getQueue();
                System.out.printf(
                        "[Monitor] core=%d | active=%d | poolSz=%d | "
                                + "queue=%d/%d | srcQ=%d/%d | parsed=%,d | comp=%,d%n",
                        pool.getCorePoolSize(),
                        pool.getActiveCount(),
                        pool.getPoolSize(),
                        poolQueue.size(),
                        poolQueue.size() + poolQueue.remainingCapacity(),
                        sourceQueue.size(),
                        sourceQueue.size() + sourceQueue.remainingCapacity(),
                        parsedCount.get(),
                        pool.getCompletedTaskCount()
                );
                // 每秒额外打印 Level 分布概览
                Map<String, Long> snap = metrics.getLevelSnapshot();
                System.out.printf(
                        "[Monitor]   Levels: INFO=%,d WARN=%,d ERROR=%,d DEBUG=%,d FATAL=%,d | parseErr=%,d%n",
                        snap.getOrDefault("INFO", 0L),
                        snap.getOrDefault("WARN", 0L),
                        snap.getOrDefault("ERROR", 0L),
                        snap.getOrDefault("DEBUG", 0L),
                        snap.getOrDefault("FATAL", 0L),
                        parseErrors.get()
                );
            }
        }
    }
}
