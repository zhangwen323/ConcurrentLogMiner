package com.duke.concurrentminer.core;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ERROR 日志异步告警编排器。
 *
 * 当 Parser 检测到 ERROR 级别日志时，不阻塞主解析线程，
 * 而是通过 CompletableFuture 编排一条异步处理链：
 *
 *   ERROR Line
 *     │
 *     ▼ supplyAsync (CPU线程池)
 *   [Step 1] 提取异常堆栈特征码 (MD5)
 *     │
 *     ▼ thenApplyAsync (I/O线程池)
 *   [Step 2] IP 地理位置反查 (Thread.sleep(50ms) 模拟 I/O)
 *     │
 *     ▼ thenAcceptAsync (I/O线程池)
 *   [Step 3] 组装告警 → 控制台输出
 *
 * 线程池隔离策略:
 *   - CPU 池 (核心=2): 处理 MD5 计算等 CPU 密集操作
 *   - I/O 池 (核心=4): 处理"网络请求"等阻塞操作，避免阻塞 CPU 线程
 *
 * JUC 源码学习点:
 *   CompletableFuture 内部使用 Treiber Stack (无锁栈) 管理依赖任务链。
 *   postComplete() 在任务完成后遍历栈，触发后续依赖。
 *   所有 CAS 操作基于 Unsafe.compareAndSwapObject。
 *
 * @author duke
 */
public class AsyncAlertManager {

    /** CPU 密集型线程池 — 小核心，避免 CPU 竞争 */
    private final ExecutorService cpuPool;

    /** I/O 密集型线程池 — 大核心，适应阻塞等待 */
    private final ExecutorService ioPool;

    /** 已处理告警计数 */
    private final AtomicLong alertCount = new AtomicLong(0);

    /** 地理城市库（模拟） */
    private static final String[] CITIES = {
            "Beijing", "Shanghai", "Shenzhen", "Hangzhou", "Guangzhou",
            "Chengdu", "Nanjing", "Wuhan", "Xian", "Tokyo",
            "Seoul", "Singapore", "Sydney", "London", "Frankfurt",
            "New York", "San Francisco", "Seattle", "Toronto", "Mumbai"
    };

    public AsyncAlertManager() {
        // CPU 池: 核心数 = min(CPU核数, 4), 避免过度竞争
        int cpuCores = Math.min(4, Runtime.getRuntime().availableProcessors());
        this.cpuPool = new ThreadPoolExecutor(
                cpuCores, cpuCores,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(500),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // I/O 池: 核心数 = CPU核数 × 2, 适应阻塞等待
        int ioThreads = cpuCores * 2;
        this.ioPool = new ThreadPoolExecutor(
                ioThreads, ioThreads,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        System.out.printf("[AsyncAlert] Initialized: cpuPool=%d threads, ioPool=%d threads%n",
                cpuCores, ioThreads);
    }

    /**
     * 对一条 ERROR 日志启动异步告警链。
     *
     * @param line    原始日志行
     * @param ip      来源 IP
     * @param message 错误消息（已从日志行中解析出）
     * @return CompletableFuture<Void> 代表整个异步链的完成
     */
    public CompletableFuture<Void> handleError(String line, String ip, String message) {
        return CompletableFuture
                // Step 1: 提取堆栈特征码 (CPU 密集)
                .supplyAsync(() -> extractStackTraceHash(message), cpuPool)

                // Step 2: IP 反查地理位置 (I/O 密集，模拟)
                .thenApplyAsync(hash -> lookupGeoLocation(ip, hash), ioPool)

                // Step 3: 组装并发送告警 (最终消费)
                .thenAcceptAsync(alert -> sendAlert(alert), ioPool)

                // 异常兜底
                .exceptionally(ex -> {
                    System.err.printf("[AsyncAlert] Chain failed: %s%n", ex.getMessage());
                    return null;
                });
    }

    /**
     * Step 1 — CPU 密集: 对错误消息计算 MD5 作为"堆栈特征码"。
     *
     * 在真实系统中，这里会提取异常堆栈的顶层帧进行归一化，
     * 使得相同根因的告警被聚合。本项目用消息内容的 MD5 模拟。
     */
    String extractStackTraceHash(String message) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = md5.digest(message.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            return "HASH_ERROR";
        }
    }

    /**
     * Step 2 — I/O 密集: 模拟外部服务 IP 反查。
     *
     * Thread.sleep(50) 模拟网络往返延迟。
     * 返回包含城市信息的告警上下文。
     */
    AlertContext lookupGeoLocation(String ip, String hash) {
        // 模拟网络 I/O (5ms 延迟，演示异步非阻塞效果)
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 基于 IP 哈希伪随机选城市（使同一 IP 始终映射到同一城市）
        int idx = Math.abs(ip.hashCode()) % CITIES.length;
        String city = CITIES[idx];

        return new AlertContext(ip, hash, city);
    }

    /**
     * Step 3 — 最终消费: 格式化告警并输出到控制台。
     */
    void sendAlert(AlertContext ctx) {
        long count = alertCount.incrementAndGet();
        System.out.printf(
                "[ALERT #%d] IP=%s | City=%s | StackHash=%s | Time=%s%n",
                count, ctx.ip, ctx.city, ctx.hash.substring(0, 12) + "...",
                new java.util.Date()
        );
    }

    /**
     * 获取已发送告警总数。
     */
    public long getAlertCount() {
        return alertCount.get();
    }

    /**
     * 优雅关闭两个线程池。
     */
    public void shutdown() {
        System.out.printf("[AsyncAlert] Shutting down... (total alerts: %,d)%n",
                alertCount.get());

        shutdownPool(cpuPool, "cpuPool");
        shutdownPool(ioPool, "ioPool");

        System.out.println("[AsyncAlert] Shutdown complete.");
    }

    private void shutdownPool(ExecutorService pool, String name) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.printf("[AsyncAlert] %s timeout, forcing shutdownNow...%n", name);
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // ================================================================
    // 告警上下文 (不可变数据载体)
    // ================================================================

    static class AlertContext {
        final String ip;
        final String hash;
        final String city;

        AlertContext(String ip, String hash, String city) {
            this.ip = ip;
            this.hash = hash;
            this.city = city;
        }
    }
}
