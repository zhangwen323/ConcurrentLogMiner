# ConcurrentLogMiner

多线程日志分析与检索系统 —— Java 并发编程（JUC）实战学习项目。

## 项目定位

在面对海量系统日志（TB 级网关日志、电商交易流水）时，单线程存在明显性能瓶颈。本项目基于 **Java SE 原生核心库**（不引入 Spring/Spring Boot），构建纯粹的多线程日志处理管道，将抽象的 JUC 源码原理转化为可观测的业务指标。

## 技术栈

| 类别 | 选型 |
|------|------|
| 语言 | Java 8 |
| 构建 | Maven |
| 并发核心 | `java.util.concurrent` (JUC) |
| 关键组件 | ThreadPoolExecutor, ArrayBlockingQueue, ConcurrentHashMap, LongAdder, CompletableFuture, CyclicBarrier |

## 系统架构

```
[日志文件 × N]
     │
     ▼ (LogReader 线程集群 — 生产者)
     │
     ▼ ArrayBlockingQueue<String> (cap=10,000, 有界缓冲区)
     │
     ▼ LogParserPool 线程池 — 消费者
     ├── ConcurrentHashMap + LongAdder → 无锁多维统计
     ├── CyclicBarrier → 每 100 万行阶段性对账落盘
     └── CompletableFuture → ERROR 日志异步告警链
          ├── MD5 堆栈特征提取 (CPU 密集)
          ├── IP 地理位置反查 (模拟 I/O)
          └── 告警通知
```

## 快速开始

```bash
# 前置条件: Java 8 + Maven
# 如使用 SDKMAN: source "$HOME/.sdkman/bin/sdkman-init.sh"

# 1. 编译
mvn compile

# 2. 生成 500 万行测试数据 (~538MB)
mvn exec:java -Dexec.args="--mode generate --count 5000000 --dir testdata --files 10"

# 3. 单线程基线扫描
mvn exec:java -Dexec.args="--mode baseline --dir testdata"

# 4. 多线程并发挖掘 (4 线程)
mvn exec:java -Dexec.args="--mode mine --dir testdata --threads 4"

# 5. 单线程 vs 多线程对账验证
mvn exec:java -Dexec.args="--mode verify --dir testdata --threads 4"
```

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--mode` | (必填) | generate / baseline / mine / verify |
| `--dir` | testdata | 日志文件目录 |
| `--count` | 5000000 | 生成日志的总行数 |
| `--files` | 10 | 拆分的文件数量 |
| `--threads` | 4 | 工作线程数 |

## 项目结构

```
src/main/java/com/duke/concurrentminer/
├── LogMinerApplication.java       # 主入口，模式路由
├── core/
│   ├── LogReader.java             # 生产者: 每文件一线程，有界队列阻塞写入
│   ├── LogParserPool.java         # 消费者: ThreadPoolExecutor + Monitor + CyclicBarrier
│   └── AsyncAlertManager.java     # CompletableFuture 异步告警编排 (CPU/IO 线程池隔离)
├── container/
│   └── MetricsCollector.java      # ConcurrentHashMap + LongAdder 无锁统计 + Top-N
└── util/
    └── LogGenerator.java          # 测试数据生成器 (权重分布 + 模板引擎)
```

## JUC 学习路线

| 组件 | 核心源码概念 | 对应 Phase |
|------|-------------|-----------|
| `ArrayBlockingQueue` | ReentrantLock + Condition (notEmpty/notFull) | Phase 2 |
| `ThreadPoolExecutor` | ctl 位运算状态机 (高3位=状态, 低29位=worker数) | Phase 3 |
| `ConcurrentHashMap` | CAS 无冲突写入 + synchronized 锁 bin 头节点 | Phase 4 |
| `LongAdder` | Cell[] 数组分散热点 + @Contended 消除伪共享 | Phase 4 |
| `CompletableFuture` | Treiber Stack 无锁栈 + postComplete() | Phase 5 |
| `CyclicBarrier` | AQS 共享模式 + Condition.await/signal | Phase 6 |

## 开发进度

- [x] Phase 1 — LogGenerator + 单线程基线
- [x] Phase 2 — LogReader + ArrayBlockingQueue
- [x] Phase 3 — ThreadPoolExecutor + Monitor 守护线程
- [x] Phase 4 — ConcurrentHashMap + LongAdder 无锁统计
- [x] Phase 5 — CompletableFuture 异步告警链
- [x] Phase 6 — CyclicBarrier 阶段性对账
- [x] Phase 7 — 全链路集成 + 对账验证

**对账结果**: 单线程 vs 多线程 **100% 一致** ✅
