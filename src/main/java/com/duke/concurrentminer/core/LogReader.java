package com.duke.concurrentminer.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 日志读取器（生产者）。
 *
 * 每个 .log 文件分配一个独立的 LogReader 线程进行串行读取，
 * 避免多线程竞争同一文件导致磁盘磁头反复寻道。
 *
 * 读取到的每一行原始日志通过 put() 投递到中央有界阻塞队列。
 * 当队列满时，put() 内部调用 Condition.await() 阻塞当前线程，
 * 形成对读取速度的天然背压（Backpressure）。
 *
 * 文件读完后投入一枚毒丸（POISON_PILL），通知消费者线程退出。
 *
 * JUC 源码学习点：
 *   ArrayBlockingQueue.put() → ReentrantLock.lockInterruptibly()
 *   → notFull.await() → 当队列满时生产者线程在 notFull 条件队列上等待
 *   → 消费者 take() 后调用 notFull.signal() 唤醒生产者
 *
 * @author duke
 */
public class LogReader implements Runnable {

    /** 毒丸标记 — 消费者收到此字符串后退出消费循环 */
    public static final String POISON_PILL = "__POISON__";

    private final File logFile;
    private final ArrayBlockingQueue<String> queue;
    private final int poisonPillCount; // 每个 Reader 投递的毒丸数量

    /**
     * @param logFile         要读取的日志文件
     * @param queue           共享有界阻塞队列
     * @param poisonPillCount 读完后投递的毒丸数（应等于消费者线程数）
     */
    public LogReader(File logFile, ArrayBlockingQueue<String> queue, int poisonPillCount) {
        this.logFile = logFile;
        this.queue = queue;
        this.poisonPillCount = poisonPillCount;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        long lineCount = 0;

        try (BufferedReader reader = new BufferedReader(
                new FileReader(logFile), 256 * 1024)) {  // 256KB 读缓冲区

            System.out.printf("[%s] Reading %s ...%n", threadName, logFile.getName());

            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    // put() 在队列满时阻塞 —— 核心学习点
                    queue.put(line);
                    lineCount++;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.printf("[%s] Interrupted during put, stopping.%n", threadName);
                    return;
                }
            }

        } catch (IOException e) {
            System.err.printf("[%s] Error reading %s: %s%n",
                    threadName, logFile.getName(), e.getMessage());
            return;
        }

        // 文件读完，投递毒丸通知消费者
        for (int i = 0; i < poisonPillCount; i++) {
            try {
                queue.put(POISON_PILL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        System.out.printf("[%s] Done: %,d lines read → %s (poison pills: %d)%n",
                threadName, lineCount, logFile.getName(), poisonPillCount);
    }
}
