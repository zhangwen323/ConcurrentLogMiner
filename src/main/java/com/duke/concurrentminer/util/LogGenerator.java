package com.duke.concurrentminer.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 离线日志生成工具 —— 用于生成海量测试日志文件。
 *
 * 日志格式: yyyy-MM-dd HH:mm:ss.SSS | LEVEL | IP_ADDR        | Message
 *
 * Level 分布 (模拟真实生产环境):
 *   INFO  80.0%  — 大部分是正常业务日志
 *   WARN  12.0%  — 非致命告警
 *   ERROR  6.0%  — 需要关注的错误
 *   DEBUG  1.5%  — 调试细节
 *   FATAL  0.5%  — 致命故障
 *
 * 使用方式:
 *   LogGenerator.generate(totalLines, outputDir, numFiles);
 *
 * @author zqw
 */
public class LogGenerator {

    /** 日志级别常量 */
    private static final String[] LEVELS = {"INFO ", "WARN ", "ERROR", "DEBUG", "FATAL"};

    /** 累积权重 — 用于 O(1) 按比例随机选 Level */
    private static final double[] LEVEL_CUM_WEIGHTS = {0.80, 0.92, 0.98, 0.995, 1.0};

    /** 日期格式化器（线程不安全，用 ThreadLocal 隔离） */
    private static final ThreadLocal<SimpleDateFormat> DATE_FMT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));

    // ================================================================
    // 各 Level 对应的消息模板库（随机选取，增加日志真实性）
    // ================================================================

    private static final String[] INFO_MSGS = {
            "User {user} login successfully from {ip}",
            "Order #{orderId} created, amount={amount}",
            "Payment processed: order=#{orderId}, channel=ALIPAY, cost={cost}ms",
            "API [/api/user/info] called, client={ip}, latency={latency}ms",
            "Scheduled task [CleanExpiredSession] finished, removed {count} sessions",
            "Configuration reloaded: key={key}, version={ver}",
            "Database connection pool status: active={active}, idle={idle}",
            "File upload completed: fileName={file}, size={size} bytes",
            "Message pushed to Kafka: topic={topic}, partition={part}, offset={offset}",
            "Health check passed: service={svc}, uptime={uptime}s",
            "Cache hit ratio: {ratio}%, keys={keys}",
            "Rate limiter token acquired: bucket={bucket}, remaining={remaining}",
            "Elasticsearch bulk index completed: docs={docs}, took={took}ms",
            "RPC call to {service} succeeded, traceId={traceId}, elapsed={elapsed}ms",
            "OAuth2 token refreshed for clientId={clientId}"
    };

    private static final String[] WARN_MSGS = {
            "Slow query detected: sql=[{sql}], elapsed={elapsed}ms, threshold=200ms",
            "Memory usage exceeds threshold: used={used}%, max={max}MB",
            "Thread pool queue almost full: pool={pool}, queued={queued}/{cap}",
            "Retrying failed operation: operation={op}, attempt={n}/{maxRetry}",
            "Circuit breaker half-open: service={svc}, failureRate={rate}%",
            "API rate limit approaching: endpoint={ep}, remaining={remaining}/{limit}",
            "SSL certificate will expire in {days} days: domain={domain}",
            "Disk usage warning: mount={mount}, used={used}%, avail={avail}GB",
            "Connection timeout (first attempt): host={host}, port={port}, timeout={timeout}ms",
            "Large response body: url={url}, size={size}KB",
            "Deprecated API used: {api}, caller={caller}, migrate to v{migrateVer}",
            "GC pause time above threshold: collector={gc}, pause={pause}ms"
    };

    private static final String[] ERROR_MSGS = {
            "NullPointerException at {class}.{method}({file}:{line})",
            "SQLException: connection refused to {host}:{port}, retries exhausted",
            "TimeoutException: downstream service {svc} did not respond in {timeout}ms",
            "IllegalArgumentException: invalid parameter [{param}={value}] in {class}.{method}",
            "IOException: failed to read file [{path}], errno={errno}",
            "RedisConnectionFailureException: unable to connect to {host}:{port}",
            "DuplicateKeyException: primary key violation, table={table}, id={id}",
            "OutOfMemoryError risk: heap usage={used}MB/{max}MB, pending={pending} tasks",
            "JsonParseException: malformed JSON at line {line}, col {col}, source={src}",
            "ClassCastException: cannot cast {from} to {to} in {class}.{method}",
            "ConcurrentModificationException detected in {class}, iterator over {collection}",
            "FileNotFoundException: {path} (Permission denied)",
            "HttpMessageNotReadableException: required request body is missing: {endpoint}",
            "StackOverflowError risk: recursive depth={depth} in {class}.{method}",
            "RejectedExecutionException: thread pool {pool} exhausted, task={task}"
    };

    private static final String[] DEBUG_MSGS = {
            "Entering method {class}.{method}(), args=[{args}]",
            "SQL parameters bound: [{params}]",
            "Request headers: {headers}",
            "Response body (truncated): {body}",
            "Session attributes: {attrs}",
            "Bean lifecycle: @PostConstruct on {bean}, scope={scope}",
            "AOP advice triggered: {advice} -> {method}",
            "Cache key resolved: {cacheName}:{key} -> {value}",
            "Feign request encoded: url={url}, body={body}",
            "Filter chain order: [{filters}]"
    };

    private static final String[] FATAL_MSGS = {
            "JVM crashed: signal={sig}, pid={pid}, hs_err_pid{pid}.log generated",
            "Disk failure detected: mount={mount}, I/O error on sector={sector}",
            "Cluster partition detected: node {node} isolated from quorum",
            "License expired: product={product}, expiry={date}, grace period exhausted"
    };

    /**
     * 在指定目录生成测试日志文件。
     *
     * @param totalLines 总行数（所有文件加起来）
     * @param outputDir  输出目录
     * @param numFiles   拆分为几个文件
     */
    public static void generate(int totalLines, String outputDir, int numFiles) throws IOException {
        File dir = new File(outputDir);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Cannot create output directory: " + outputDir);
        }

        int linesPerFile = totalLines / numFiles;
        int remainder = totalLines % numFiles;

        System.out.printf("[LogGenerator] Generating %,d lines across %d file(s) → %s%n",
                totalLines, numFiles, dir.getAbsolutePath());
        long start = System.currentTimeMillis();

        for (int f = 0; f < numFiles; f++) {
            int linesForThisFile = linesPerFile + (f < remainder ? 1 : 0);
            File file = new File(dir, String.format("access_%03d.log", f));
            writeOneFile(file, linesForThisFile);
        }

        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("[LogGenerator] Done. Elapsed: %,d ms (%.1f sec)%n",
                elapsed, elapsed / 1000.0);
    }

    /**
     * 写入单个日志文件。
     */
    private static void writeOneFile(File file, int lines) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file), 256 * 1024)) {
            // 时间基准：最近 7 天
            long now = System.currentTimeMillis();
            long sevenDaysAgo = now - 7L * 24 * 3600 * 1000;

            for (int i = 0; i < lines; i++) {
                String line = generateOneLine(sevenDaysAgo, now, i);
                writer.write(line);
                writer.newLine();

                // 进度日志：每 100 万行输出一次
                if (i > 0 && i % 1_000_000 == 0) {
                    System.out.printf("  [%s] ... %,d / %,d lines written%n",
                            file.getName(), i, lines);
                }
            }
        }
        System.out.printf("  [%s] %,d lines written.%n", file.getName(), lines);
    }

    /**
     * 生成一行日志。格式: Timestamp | LEVEL | IP | Message
     */
    static String generateOneLine(long timeFrom, long timeTo, int seq) {
        ThreadLocalRandom r = ThreadLocalRandom.current();

        // 1. 随机时间戳
        long ts = timeFrom + (long) (r.nextDouble() * (timeTo - timeFrom));
        String timestamp = DATE_FMT.get().format(new Date(ts));

        // 2. 按权重随机选 Level
        String level = pickLevel(r.nextDouble());

        // 3. 随机 IP
        String ip = randomIP(r);

        // 4. 根据 Level 选模板并填充假数据
        String msg = fillTemplate(level, r, seq);

        // 格式: Timestamp | Level | IP            | Message
        // Level 占 5 字符右补空格, IP 占 15 字符左补空格
        return String.format("%s | %-5s | %-15s | %s", timestamp, level.trim(), ip, msg);
    }

    /**
     * 按权重选 Level。O(1)，累积分布查找。
     */
    private static String pickLevel(double rand) {
        for (int i = 0; i < LEVEL_CUM_WEIGHTS.length; i++) {
            if (rand < LEVEL_CUM_WEIGHTS[i]) {
                return LEVELS[i];
            }
        }
        return LEVELS[LEVELS.length - 1]; // fallback
    }

    /**
     * 生成一个随机 IP（不区分内网/公网）。
     */
    private static String randomIP(ThreadLocalRandom r) {
        return r.nextInt(1, 256) + "." +
               r.nextInt(0, 256) + "." +
               r.nextInt(0, 256) + "." +
               r.nextInt(1, 256);
    }

    /**
     * 从模板库随机选取一条，用假数据替换占位符。
     */
    private static String fillTemplate(String level, ThreadLocalRandom r, int seq) {
        String[] pool;
        switch (level.trim()) {
            case "WARN":  pool = WARN_MSGS;  break;
            case "ERROR": pool = ERROR_MSGS; break;
            case "DEBUG": pool = DEBUG_MSGS; break;
            case "FATAL": pool = FATAL_MSGS; break;
            default:      pool = INFO_MSGS;  break;
        }

        String template = pool[r.nextInt(pool.length)];
        return resolvePlaceholders(template, r, seq);
    }

    /**
     * 替换模板中的 {placeholder} 为随机假值。
     *
     * 支持的占位符分类:
     *   网络类: {ip}, {host}, {port}, {domain}, {endpoint}, {url}, {ep}
     *   方法类: {class}, {method}, {file}, {line}, {service}, {svc}, {api}
     *   数值类: {orderId}, {latency}, {elapsed}, {timeout}, {cost}, {count}, {n}...
     *   其他: 见 switch-case
     */
    private static String resolvePlaceholders(String tmpl, ThreadLocalRandom r, int seq) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < tmpl.length()) {
            char c = tmpl.charAt(i);
            if (c == '{') {
                int end = tmpl.indexOf('}', i + 1);
                if (end == -1) { sb.append(c); i++; continue; }
                String key = tmpl.substring(i + 1, end);
                sb.append(resolveValue(key, r, seq));
                i = end + 1;
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }

    /**
     * 根据占位符 key 返回对应的随机假值。
     */
    private static String resolveValue(String key, ThreadLocalRandom r, int seq) {
        switch (key) {
            // ─── 网络相关 ───
            case "ip":       return randomIP(r);
            case "host":     return "db-" + r.nextInt(1, 20) + ".internal";
            case "port":     return String.valueOf(r.nextInt(1024, 65536));
            case "domain":   return "api" + r.nextInt(1, 10) + ".example.com";
            case "endpoint": case "ep":
                String[] eps = {"/api/user", "/api/order", "/api/pay", "/api/log", "/api/admin"};
                return eps[r.nextInt(eps.length)];
            case "url":
                return "/api/v" + r.nextInt(1, 4) + "/resource/" + r.nextInt(1000, 9999);

            // ─── 类/方法 ───
            case "class":
                String[] cls = {"UserService", "OrderHandler", "PaymentProcessor",
                        "LogParser", "CacheManager", "MessageQueue"};
                return "com.zqw.service." + cls[r.nextInt(cls.length)];
            case "method":
                String[] mtd = {"handle", "process", "execute", "query", "update", "validate"};
                return mtd[r.nextInt(mtd.length)];
            case "file":
                String[] files = {"UserService.java", "OrderController.java",
                        "PaymentGateway.java", "CacheUtil.java"};
                return files[r.nextInt(files.length)];
            case "line":     return String.valueOf(r.nextInt(20, 500));
            case "service": case "svc":
                String[] svcs = {"user-service", "order-service", "payment-service",
                        "inventory-service", "notification-service"};
                return svcs[r.nextInt(svcs.length)];

            // ─── ID / 追踪 ───
            case "orderId":  return String.valueOf(r.nextLong(10000, 999999));
            case "traceId":  return Long.toHexString(r.nextLong(0x1000000000000000L, Long.MAX_VALUE));
            case "clientId": return "client_" + r.nextInt(100, 999);
            case "id":       return String.valueOf(r.nextLong(1000, 99999));
            case "pid":      return String.valueOf(r.nextInt(1000, 99999));

            // ─── 时间/性能 ───
            case "latency": case "elapsed": case "cost": case "took":
                return String.valueOf(r.nextInt(1, 500));
            case "timeout":  return String.valueOf(r.nextInt(1000, 30000));
            case "uptime":   return String.valueOf(r.nextInt(3600, 86400 * 30));
            case "pause":    return String.valueOf(r.nextInt(50, 2000));
            case "days":     return String.valueOf(r.nextInt(1, 90));

            // ─── 计数/容量 ───
            case "count": case "docs": case "keys":
                return String.valueOf(r.nextInt(100, 10000));
            case "size":     return String.valueOf(r.nextInt(1024, 10 * 1024 * 1024));
            case "active":   return String.valueOf(r.nextInt(5, 50));
            case "idle":     return String.valueOf(r.nextInt(0, 20));
            case "cap": case "max": case "limit":
                return String.valueOf(r.nextInt(100, 2000));
            case "remaining":return String.valueOf(r.nextInt(0, 100));
            case "queued":   return String.valueOf(r.nextInt(100, 2000));
            case "pending":  return String.valueOf(r.nextInt(100, 10000));
            case "n":        return String.valueOf(r.nextInt(1, 5));
            case "maxRetry": return "3";
            case "attempt":  return String.valueOf(r.nextInt(1, 5));

            // ─── 百分比/比率 ───
            case "ratio": case "rate": case "used":
                return String.valueOf(r.nextInt(50, 99));
            case "remaining_pct": case "avail":
                return String.valueOf(r.nextInt(1, 50));

            // ─── 其他 ───
            case "user":     return "user_" + r.nextInt(1000, 9999);
            case "amount":   return String.format("%.2f", r.nextDouble(1.0, 9999.0));
            case "key":      return "cfg." + randomWord(r, 4, 10);
            case "ver": case "migrateVer": return r.nextInt(1, 5) + "." + r.nextInt(0, 10);
            case "topic":    return "topic-" + randomWord(r, 3, 8);
            case "part":     return String.valueOf(r.nextInt(0, 16));
            case "offset":   return String.valueOf(r.nextLong(100000, 9999999));
            case "sql":
                String[] sqls = {"SELECT * FROM orders WHERE id=?", "UPDATE users SET status=?",
                        "INSERT INTO logs VALUES (?,?,?)"};
                return sqls[r.nextInt(sqls.length)];
            case "pool":     return "parser-pool";
            case "op":       return "fetch_" + randomWord(r, 3, 6);
            case "bucket":   return "bucket_" + (char) ('A' + r.nextInt(8));
            case "mount":    return "/dev/sd" + (char) ('a' + r.nextInt(6));
            case "sector":   return String.valueOf(r.nextLong(1000000, 99999999));
            case "node":     return "node-" + r.nextInt(1, 10);
            case "product":  return "ConcurrentLogMiner";
            case "date":     return "2026-0" + r.nextInt(6, 10) + "-" + r.nextInt(10, 29);
            case "errno":    return "E" + r.nextInt(1, 50);
            case "table":    return r.nextBoolean() ? "orders" : "users";
            case "path":     return "/data/logs/app_" + r.nextInt(1, 20) + ".log";
            case "task":     return "parse-task-" + seq;
            case "param":    return "param_" + randomWord(r, 3, 6);
            case "value":    return randomWord(r, 4, 10);
            case "collection": return r.nextBoolean() ? "ArrayList" : "LinkedList";
            case "from":     return "java.lang.String";
            case "to":       return "java.lang.Integer";
            case "src":      return "kafka-message-" + seq;
            case "col":      return String.valueOf(r.nextInt(1, 120));
            case "depth":    return String.valueOf(r.nextInt(100, 10000));
            case "sig":      return "SIG" + (r.nextBoolean() ? "SEGV" : "BUS");
            case "headers":  return "{Content-Type=application/json, X-Trace-Id=" + randomWord(r, 8, 16) + "}";
            case "body":     return "{\"status\":\"ok\",\"data\":{...}}";
            case "attrs":    return "{username=user_" + r.nextInt(100, 999) + "}";
            case "bean":     return r.nextBoolean() ? "userController" : "orderService";
            case "scope":    return r.nextBoolean() ? "singleton" : "prototype";
            case "advice":   return r.nextBoolean() ? "@Around" : "@Before";
            case "cacheName": return r.nextBoolean() ? "userCache" : "orderCache";
            case "value_cache": return randomWord(r, 5, 15);
            case "filters":  return "AuthFilter, LogFilter, RateLimitFilter";
            case "args":     return "\"param1\", 123, true";
            case "params":   return "p1=val1, p2=456, p3=false";
            case "gc":
                return r.nextBoolean() ? "G1 Young Generation" : "G1 Mixed Generation";
            case "caller":
                String[] callers = {"LegacyService", "OldGateway", "DeprecatedClient"};
                return callers[r.nextInt(callers.length)];
            default:
                return "{" + key + "}"; // 未知占位符保持原样
        }
    }

    /**
     * 生成一个随机字母组成的"单词"。
     */
    private static String randomWord(ThreadLocalRandom r, int minLen, int maxLen) {
        int len = r.nextInt(minLen, maxLen + 1);
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) ('a' + r.nextInt(26));
        }
        return new String(chars);
    }

    // ================================================================
    // 命令行入口（也支持在 LogMinerApplication 中调用）
    // ================================================================

    public static void main(String[] args) throws IOException {
        int totalLines = 5_000_000;
        String outputDir = "testdata";
        int numFiles = 10;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--count": totalLines = Integer.parseInt(args[++i]); break;
                case "--dir":   outputDir = args[++i]; break;
                case "--files": numFiles = Integer.parseInt(args[++i]); break;
            }
        }

        generate(totalLines, outputDir, numFiles);
    }
}
