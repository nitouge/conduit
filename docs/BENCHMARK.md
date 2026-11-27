# Conduit 性能基准（Benchmark）

> 对应 BACKLOG §7「性能验证」。目标是给出**可复现的方法**和**第一组真实数据**，用于后续优化前后对比，
> 而不是宣称绝对性能——本页数字来自一台 15W 低压笔记本，绝对值仅供参考，**同机横向对比才有意义**。

基准代码在 [`conduit-benchmark`](../conduit-benchmark/)，基于 [JMH](https://openjdk.org/projects/code-tools/jmh/)；
基准不在 `mvn test` 里执行（避免拖慢日常构建），需要显式打包运行。

---

## 1. 基准内容

| 基准 | 测什么 | 观测指标 |
| --- | --- | --- |
| `RpcCallBenchmark.callLatency` | 进程内真实 `RpcServer` + `RpcClientProxy` 的完整调用链（发现→负载均衡→容错→连接池→握手→编解码→业务线程池→反射调用） | 延迟分布 p50/p90/p95/p99/p99.9 |
| `RpcCallBenchmark.callThroughput` | 同上，4 线程并发（单连接多路复用） | QPS |
| `SerializationBenchmark` | 四种序列化（jdk/json/kryo/protostuff）× small(≈338–883B) / large(≈4.0–4.6KB) 报文 | 序列化/反序列化吞吐、编码后体积 |
| `ProtocolCodecBenchmark` | `RpcEncoder`/`RpcDecoder`：协议头 + Kryo + 可选 gzip | 编解码吞吐、压缩体积收益与 CPU 代价 |

---

## 2. 怎么跑

**务必带 `-am`**：基准模块依赖本仓库其它模块，只写 `-pl conduit-benchmark` 会用到本地仓库里可能过期的 SNAPSHOT
（曾因此出现 `NoClassDefFoundError: ProtocolOptions`）。

```bash
# 1) 打包出可执行的 benchmarks.jar（内部用 shade 合并所有依赖）
mvn -pl conduit-benchmark -am -DskipTests package

# 2) 跑全部基准（默认 3 轮预热 + 5 轮测量，每轮 1s）
java -jar conduit-benchmark/target/benchmarks.jar

# 3) 只跑某一类并缩短时间（快速冒烟）
java -jar conduit-benchmark/target/benchmarks.jar SerializationBenchmark -wi 1 -i 1 -w 1s -r 1s -f 1

# 4) 只跑某个参数组合
java -jar conduit-benchmark/target/benchmarks.jar SerializationBenchmark \
     -p serializerName=kryo -p payloadSize=large
```

常用参数：

| 参数 | 含义 |
| --- | --- |
| `-wi 3 -w 1s` | 预热 3 轮、每轮 1 秒 |
| `-i 5 -r 2s` | 测量 5 轮、每轮 2 秒 |
| `-f 1` | 每个基准 fork 1 个 JVM |
| `-t 4` | 覆盖线程数（`callThroughput` 默认 4） |
| `-p key=value` | 覆盖 `@Param` |
| `-l` | 列出所有可运行的基准 |

注意事项：

- 基准要的是**稳定环境**：关掉 IDE 索引、浏览器等吃 CPU 的进程，笔记本插电避免降频；同一台机器的数字才适合横向对比；
- `RpcCallBenchmark` 在同一个 JVM 内起服务端，走 127.0.0.1 回环，测的是**框架开销**
  （协议、连接池、线程切换、代理、集群、反射分发），不含真实网络延迟；
- 端口随机、注册中心用 `local`，不需要 Nacos/ZooKeeper/Etcd；
- 日志统一压到 WARN（模块内 `src/main/resources/logback.xml`），避免 DEBUG 日志污染测量。

---

## 3. 测试环境

| 项 | 值 |
| --- | --- |
| CPU | Intel Core i7-10710U（6 核 12 线程，基频 1.1GHz，15W 低压） |
| 内存 / 系统 | Windows 10（10.0.19045）开发机 |
| JDK | **JDK 17.0.9**（HotSpot 64-Bit Server VM） |
| JMH | 1.37，`-f 1`，Trial 内复用连接（setup 已排除建链/握手） |
| 负载 | **非独占**：IDE、浏览器、Nacos 等同时在跑；15W CPU 有功耗墙，持续满载会降频 |

> ⚠️ **测量稳定性**：同一配置在不同时间点跑，结果可差 2–5 倍（CPU 升频/降频、后台进程）。
> 下表取一次完整运行（`-wi 8 -i 5 -w 1s -r 2s`，2026-11-25），原始日志在
> `conduit-benchmark/target/jmh-final-*.txt`（不提交到仓库）。**看结论请看方向，不要抠绝对值。**

---

## 4. 结果

### 4.1 端到端 RPC（loopback，同 JMH fork）

| 序列化 | 吞吐 QPS（4线程） | p50 | p90 | p95 | p99 | p99.9 |
| --- | --- | --- | --- | --- | --- | --- |
| `jdk`（默认） | 1282 | 1040 µs | 1944 µs | 2343 µs | 3371 µs | 5885 µs |
| `kryo`（生产推荐） | 4331 | **774 µs** | **1480 µs** | **1898 µs** | **3244 µs** | 7903 µs |

观察：

- **kryo 的 p50 比 jdk 低约 25%**（774 vs 1040µs），p90 低约 24%；三次独立运行都复现了这个方向；
- 吞吐量一组噪声很大（同机不同轮次在 1.3k–6.6k QPS 之间波动），因为 4 个客户端线程 + 服务端业务
  线程池在这颗 15W CPU 上争抢核心，**吞吐结论以延迟分布为准**；
- p99 约为 p50 的 4 倍，p99.9 出现 6–8ms 的长尾，指向 GC / 线程调度抖动（见 §6）。

### 4.2 序列化（吞吐 ops/s，编码后字节数）

| 序列化 | small 编码 | small 序列化 | small 反序列化 | large 编码 | large 序列化 | large 反序列化 |
| --- | --- | --- | --- | --- | --- | --- |
| `jdk` | 883 B | 48 061 | **8 412** | 4 643 B | 13 165 | **4 245** |
| `json` | 394 B | 191 928 | 69 392 | 4 161 B | 11 422 | 18 080 |
| `kryo` | 338 B | 144 076 | 99 356 | 4 007 B | 22 501 | 22 719 |
| `protostuff` | 365 B | 184 319 | 68 845 | 4 068 B | **29 832** | **45 101** |

观察：

- **JDK 序列化最差**：small 报文只有 48k ops/s、反序列化 8.4k ops/s，large 反序列化仅 4.2k ops/s，
  比 kryo 慢 5 倍以上；体积也最大（883B vs 338B）。作为默认实现只是为了"零依赖开箱可用"，
  **生产应显式配置 `kryo`**；
- `kryo`/`protostuff` 体积最小（338–365B small），适合走网络 + gzip；
- large 报文的 JSON 序列化吞吐反而低于 small 的一半，说明 Jackson 在较大对象图上开销增长快。

### 4.3 协议编解码（Kryo + 4KB 报文，含 gzip 对比）

| 压缩 | 帧大小 | 编码 ops/s | 解码 ops/s |
| --- | --- | --- | --- |
| `none` | 4 025 B | 19 024 | 28 129 |
| `gzip` | **557 B**（体积 −86%，约 7.2×） | 7 202（≈ −62%） | 13 468（≈ −52%） |

观察：

- gzip 对 4KB 报文非常划算：体积降到 1/7，代价是编码吞吐下降到约 1/3；
- 默认压缩阈值 `compress-threshold: 1024` 的取舍是合理的：小报文（几十~几百字节）压缩后往往更大，
  而且白白付出 CPU；只有大报文才真正受益。

---

## 5. 结论（写进后续优化前的基线）

1. **序列化选择是第一优先级**：默认 JDK 在小报文上反序列化只有 kryo 的 1/12，
   端到端 p50 也高出约 25%。文档与示例已推荐 `kryo`，但默认值仍是 `jdk`——是否需要
   在 starter 里给"检测到 kryo 在 classpath 就默认用 kryo"的提示，值得讨论。
2. **压缩阈值策略成立**：4KB 报文压缩收益 7.2×、CPU 代价 2–3×，阈值 1024 的默认值无需调整。
3. **端到端延迟的真实瓶颈不在序列化**：jdk→kryo 只把 p50 从 1.04ms 降到 0.77ms，
   剩余 ~0.7ms 是框架自身的调度/线程切换开销（见 §6 第 1 条）。
4. 长尾（p99.9 ≈ 6–8ms）需要 GC / 线程调度层面的观测，不能靠改配置解决。

---

## 6. 后续优化方向（按预期收益排序）

| # | 方向 | 依据 | 预期 |
| --- | --- | --- | --- |
| 1 | 减少调用链上的线程切换：服务端对小任务改为 IO 线程内联执行（可配置），客户端同步等待改为可选的自旋+park 混合 | p50 仍有 ~0.7ms，而小报文序列化只要 ~7µs | 大 |
| 2 | 请求路径对象复用/池化（`RpcRequest`/`RpcMessage`/body `byte[]`） | 每次调用至少 3 次分配，GC 压力直接反映在 p99/p99.9 | 中 |
| 3 | 连接池调优：默认每地址 1 连接，4 线程时全部复用同一连接；实验 `max-connections-per-address=2/4` 对吞吐的影响 | 吞吐在 4 线程时波动大，疑似单连接写队列成为争用点 | 中 |
| 4 | 默认序列化策略：`jdk` → 检测到 kryo 则提示/自动选择 | §4.2 数据 | 中 |
| 5 | 异步调用基准：目前只测同步；`CompletableFuture` 路径应能绕开客户端阻塞等待 | 现有 `ProviderAsyncTest` 已覆盖功能，缺性能数据 | 中 |
| 6 | 引入 `-prof gc` / `-prof stack` 常态化：定位长尾来源 | p99.9 6–8ms | 待观测 |

> 基准模块本身也建议继续演进：加入"异步/批量/大报文"场景，以及在固定电源模式、独占机器的 CI 机器上
> 跑回归（当前这台开发机只能做同机对比）。

---

## 7. 如何对比与回归

```bash
# 只跑端到端延迟（约 1 分钟）
java -jar conduit-benchmark/target/benchmarks.jar RpcCallBenchmark.callLatency -wi 5 -i 5 -r 2s

# 只看 GC（定位长尾）
java -jar conduit-benchmark/target/benchmarks.jar RpcCallBenchmark.callLatency -prof gc

# 只跑序列化里的 kryo/大报文
java -jar conduit-benchmark/target/benchmarks.jar SerializationBenchmark \
     -p serializerName=kryo -p payloadSize=large
```

对比原则：

- 同一天、同一电源状态下跑前后两次，**对比变化方向**而不是绝对值；
- 关注 p50/p99 而不是平均值（平均值会掩盖长尾）；
- 改序列化/压缩相关代码时，同时看 §4.2/§4.3 的吞吐与体积两项指标。
