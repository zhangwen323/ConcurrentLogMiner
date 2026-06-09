package com.duke.concurrentminer.container;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 无锁并发统计容器。
 *
 * 使用 ConcurrentHashMap + LongAdder 实现多线程下的多维指标累加，
 * 全程无显式锁，依赖 CAS + 分段思想保证高性能与线程安全。
 *
 * 统计维度:
 *   1. 各日志级别 (INFO/WARN/ERROR/DEBUG/FATAL) 的出现频次
 *   2. 来源 IP 的出现频次 → 支撑 Top-N 排行
 *
 * JUC 源码学习点:
 *   - ConcurrentHashMap.putVal() (JDK 8): 无冲突时 CAS 写入 bin,
 *     有冲突时 synchronized 锁住链表头节点 — 锁粒度细化到单个 hash bin
 *   - LongAdder.add(): 内部 Cell[] 数组, 每个 Cell 持有一个 long 值,
 *     线程通过 Thread.probe 哈希到不同 Cell, CAS 写入失败则换 Cell,
 *     最终 sum() 时累加所有 Cell — 用空间换热点分散
 *
 * @author duke
 */
public class MetricsCollector {

    /** 日志级别计数器: level → LongAdder */
    private final ConcurrentHashMap<String, LongAdder> levelCounters = new ConcurrentHashMap<>();

    /** IP 计数器: ip → LongAdder */
    private final ConcurrentHashMap<String, LongAdder> ipCounters = new ConcurrentHashMap<>();

    /** 预初始化常见 Level，避免第一次 computeIfAbsent 的竞争 */
    public MetricsCollector() {
        for (String lv : new String[]{"INFO", "WARN", "ERROR", "DEBUG", "FATAL"}) {
            levelCounters.put(lv, new LongAdder());
        }
    }

    /**
     * 原子递增指定 Level 的计数。
     *
     * computeIfAbsent 在 ConcurrentHashMap 中是原子的:
     *   1. 先检查 key 是否存在
     *   2. 不存在则 CAS 插入新的 bin 节点（只锁该 bin 的头节点）
     *   3. 返回已有的或新创建的 LongAdder
     * 然后 LongAdder.increment() 内部 CAS 写 Cell, 冲突时自动换 Cell。
     */
    public void incrementLevel(String level) {
        levelCounters.computeIfAbsent(level, k -> new LongAdder()).increment();
    }

    /**
     * 原子递增指定 IP 的计数。
     */
    public void incrementIP(String ip) {
        ipCounters.computeIfAbsent(ip, k -> new LongAdder()).increment();
    }

    /**
     * 获取 Level 计数的不可变快照。
     */
    public Map<String, Long> getLevelSnapshot() {
        Map<String, Long> snapshot = new LinkedHashMap<>();
        // 按预定顺序输出
        for (String lv : new String[]{"INFO", "WARN", "ERROR", "DEBUG", "FATAL"}) {
            LongAdder adder = levelCounters.get(lv);
            snapshot.put(lv, adder != null ? adder.sum() : 0L);
        }
        return snapshot;
    }

    /**
     * 获取 IP 计数的不可变快照。
     */
    public Map<String, Long> getIPSnapshot() {
        Map<String, Long> snapshot = new LinkedHashMap<>();
        for (Map.Entry<String, LongAdder> e : ipCounters.entrySet()) {
            snapshot.put(e.getKey(), e.getValue().sum());
        }
        return snapshot;
    }

    /**
     * 获取 Top-N 热点 IP。
     *
     * 使用小顶堆（size=N）高效筛选:
     *   遍历所有 IP → 堆不满时直接入堆 → 堆满后, 新元素比堆顶大则替换
     *   复杂度 O(M log N)，M = 总 IP 数，N = topK
     */
    public List<Map.Entry<String, Long>> getTopNIPs(int n) {
        // 小顶堆: 按 count 升序
        PriorityQueue<Map.Entry<String, Long>> minHeap = new PriorityQueue<>(
                n, Map.Entry.comparingByValue());

        for (Map.Entry<String, LongAdder> e : ipCounters.entrySet()) {
            long count = e.getValue().sum();
            if (count == 0) continue;

            if (minHeap.size() < n) {
                minHeap.offer(new AbstractMap.SimpleEntry<>(e.getKey(), count));
            } else if (count > minHeap.peek().getValue()) {
                minHeap.poll();
                minHeap.offer(new AbstractMap.SimpleEntry<>(e.getKey(), count));
            }
        }

        // 从堆中取出，按 count 降序排列
        List<Map.Entry<String, Long>> result = new ArrayList<>(minHeap);
        result.sort(Map.Entry.<String, Long>comparingByValue().reversed());
        return result;
    }

    /**
     * 获取所有计数器的总和（用于对账: 应等于总日志行数）。
     */
    public long getTotalLevelCount() {
        long total = 0;
        for (LongAdder adder : levelCounters.values()) {
            total += adder.sum();
        }
        return total;
    }

    /**
     * 将当前统计快照写入文件。
     *
     * 由 CyclicBarrier 的 barrier action 调用（最后一个到达屏障的线程执行）。
     * 读取 LongAdder.sum() 是线程安全的，但多个 LongAdder 之间不保证原子性 —
     * 这对 checkpoint 快照是可接受的（允许微小偏差）。
     */
    public void writeSnapshot(String outputDir, long checkpointAt) {
        java.io.File dir = new java.io.File(outputDir);
        if (!dir.exists()) dir.mkdirs();

        String filename = String.format("%s/snapshot_%07d.txt", outputDir, checkpointAt);
        try (java.io.PrintWriter pw = new java.io.PrintWriter(
                new java.io.FileWriter(filename))) {

            pw.printf("=== Snapshot at %,d lines ===%n", checkpointAt);
            pw.printf("Time: %s%n", new java.util.Date());
            pw.println();

            // Level 分布
            pw.println("Level Distribution:");
            Map<String, Long> levels = getLevelSnapshot();
            for (Map.Entry<String, Long> e : levels.entrySet()) {
                pw.printf("  %-5s: %,d%n", e.getKey(), e.getValue());
            }
            pw.println();

            // Top 10 IPs
            pw.println("Top 10 Hot IPs:");
            for (Map.Entry<String, Long> e : getTopNIPs(10)) {
                pw.printf("  %-15s  %,d%n", e.getKey(), e.getValue());
            }

        } catch (java.io.IOException e) {
            System.err.printf("[Snapshot] Failed to write %s: %s%n", filename, e.getMessage());
        }
    }

    /**
     * 重置所有计数器（用于多次测试）。
     */
    public void reset() {
        for (LongAdder adder : levelCounters.values()) {
            adder.reset();
        }
        for (LongAdder adder : ipCounters.values()) {
            adder.reset();
        }
    }
}
