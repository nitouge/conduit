# Changelog

本文件记录 Conduit 各版本间的显著变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本 SemVer](https://semver.org/lang/zh-CN/)。

> 项目目前尚未打过正式 Git tag / GitHub Release，当前版本号为 `1.0.0-SNAPSHOT`（开发中）。下面的 `[Unreleased]` 汇总了主线 M1-M5 已完成的能力；第一次正式打 tag 时，把对应内容挪到一个具体版本号下即可。

## [Unreleased]

### Added

- **M1 直连调用**：自定义协议（定长头+变长体）、Netty 传输、JDK 序列化、JDK 动态代理，端到端同步调用打通。
- **M2 服务发现**：接入 Nacos 注册中心，消费者去掉写死地址。
- **M3 可替换 SPI**：`@SPI` + `ExtensionLoader` 机制；序列化扩展到 JDK / JSON(Jackson) / Kryo / Protostuff，换实现只改配置。
- **M4 异步与治理**：`CompletableFuture` 异步调用；负载均衡 Random(带权重) / RoundRobin / ConsistentHash；集群容错 FailFast / FailOver / FailSafe / FailBack；三态熔断器。
- **M5 Spring 集成**：`@EnableConduit` / `@RpcService` / `@RpcReference` 注解驱动，`application.yml` 自动配置。
- Filter 拦截链 SPI（消费侧 + 提供侧）与 `RpcRequest` 隐式附加参数 `attachments`（配合 `RpcContext`）。
- 真正的 Spring Boot Starter（`conduit-spring-boot-starter`）与跨进程 `conduit-starter-demo` 示例。
- Provider 端非阻塞异步调用支持。
- **性能基准（`conduit-benchmark`）**：新增 JMH 基准模块，覆盖端到端 QPS 与延迟分布
  （p50/p90/p95/p99/p99.9）、四种序列化的吞吐与体积、协议编解码（含 gzip 对比）；
  实测数据与优化方向见 [docs/BENCHMARK.md](docs/BENCHMARK.md)。
- **构建前置校验 JDK**：根 POM 增加 `maven-enforcer-plugin` 的 `requireJavaVersion [17,)`，
  Maven 跑在低版本 JDK 上时 `validate` 阶段直接给出明确提示（否则会在测试阶段抛
  `class file version 61.0 ... up to 52.0`，很难定位是 JVM 版本问题）。
- **JDK 17 启动脚本**：新增 `scripts/mvn17.cmd` / `mvn17.ps1` / `mvn17.sh`，多 JDK 机器上
  无需修改全局 `JAVA_HOME` 即可用 JDK 17 跑 Maven（按 `JAVA17_HOME` → 已是 17 的 `JAVA_HOME`
  → 常见安装目录查找）；新增 `scripts/test-one.cmd` / `test-one.ps1` 一键跑单个/多个测试类
  （自动补 `-pl/-am/-Dtest` 与两个 surefire 开关），以及 `scripts/use-jdk.cmd` 在当前 cmd 窗口
  切换 JDK（`call scripts\use-jdk.cmd 17|11|8`）。
- **测试基建（`conduit-testkit` / `conduit-it`）**：新增可复用测试夹具（真实 `TestServer`/`TestRpc`、
  注册中心替身、TLS 自签证书、条件等待、全局状态复位）与集成测试模块（多节点负载均衡/下线/容错/熔断、
  TLS 端到端、provider 运行在独立 JVM 的跨进程调用）；测试分层与约定见 [docs/TESTING.md](docs/TESTING.md)。
- **三阶段测试覆盖**：阶段一补齐请求-响应匹配（`PendingRequestsTest`）与序列化边界（空/null/数组/attachments/64KB）；
  阶段二新增 `MultiJvmRegistryIntegrationTest`（2 个 provider JVM + Nacos + consumer JVM，验证注册发现、
  跨进程调用、roundrobin、优雅下线收敛与重启恢复）；阶段三新增线性一致性检测器
  （`LinearizabilityChecker`，Wing &amp; Gong）并用于 `InMemoryRegistry`，以及 Chaos 三场景
  （进程崩溃/服务变慢/重启自愈）。

- **基础通信层加固（Protocol + Transport）**：
  - 协议版本协商：新增 `HANDSHAKE` / `HANDSHAKE_ACK` / `HANDSHAKE_REJECT` 控制帧；版本取双方支持区间的交集最高版（未来版本平滑降级、过旧版本明确拒绝），并在握手同时协商 codec 与压缩能力；握手超时、未握手业务帧拦截。
  - 数据压缩：`ProtocolOptions` 支持压缩算法与压缩阈值（默认 1024 字节，小报文不压缩），GZIP 走握手能力协商；解压炸弹防护。
  - 链路加密：新增 `Tls` 工具与 `sslContext` 配置项，服务端/客户端均可启用 TLS（JDK SslProvider），客户端默认开启 HTTPS 主机名校验，支持 PEM 与 keystore。
  - 连接池：新增 `ConnectionPool`，每地址默认单长连接，可配置多连接；按在途请求数选择连接、单飞建链、后台扩容到上限、闲置回收。
  - 心跳保活：客户端写空闲发 PING、服务端回 PONG，两端读空闲探活；心跳间隔/读空闲阈值可配置。
  - 断线自动重连：新增 `ReconnectManager`，指数退避后台重连（成功即停、达上限放弃），并保留按需建链兜底。
  - 客户端建链/发送改为完全异步，不再阻塞调用线程；请求超时清理改用独立调度线程。
  - 新增 `ClientOptions` / `ServerOptions` 与 `conduit.transport.*` 配置项。
  - 传输与协议设计（帧格式、握手、压缩、心跳、连接池、重连、TLS、不变量）见 [docs/TRANSPORT.md](docs/TRANSPORT.md)。

- **核心扩展机制（SPI + Cluster）**：
  - SPI 自适应扩展：新增 `@Adaptive` 与 `ExtensionLoader.getAdaptiveExtension()`，按 `Map` / `RpcRequest.attachments` 的 key 在每次调用时动态选择实现，未取到回退默认；
  - SPI 生命周期：新增 `Lifecycle`（init/destroy），ExtensionLoader 创建后自动 init，`destroyExtension/destroyAll/shutdownAll` 与 JVM 关闭钩子统一释放；
  - 集群可配置重试：新增 `ClusterOptions`（重试次数/退避/熔断参数），`@RpcReference(retries, retryBackoffMs)` 与 `conduit.cluster.*` 全局配置；FailOver 支持异步退避、熔断节点跳过不消耗预算；
  - 熔断器可配置：新增 `CircuitBreakerOptions` 与 `CircuitBreakerRegistry.configure/state/reset`；
  - Cluster/LoadBalancer 经自适应代理支持单次调用覆盖（`RpcContext` 的 `cluster` / `loadbalance` attachment）。
  - 扩展点与自适应加载见 [docs/SPI.md](docs/SPI.md)，负载均衡/容错策略/熔断状态机见 [docs/CLUSTER.md](docs/CLUSTER.md)。

- **服务治理（Registry + Observability）**：
  - 新增 `conduit-registry-zookeeper`：Curator 临时节点注册、`CuratorCache` 订阅、连接超时快速失败；使用 Curator `TestingServer` 内嵌测试。
  - 新增 `conduit-registry-etcd`：jetcd 租约注册、自动续租/失败重注册、前缀 watch；通过内存 `EtcdOperations` 假实现覆盖逻辑测试。
  - 新增 `TraceFilter`：自动生成/透传 traceId、MDC 绑定；`RpcContext` 增加 `getTraceId/setTraceId`；默认启用。
  - 扩展 `MetricsFilter`：QPS/成功率、平均/最大/最小 RT、延迟分桶与分位估算、全局汇总与重置。
  - 新增 `StructuredLogFilter`：一行 JSON 访问日志（`CONDUIT.ACCESS.JSON`），默认关闭。
  - 注册中心拆分：核心 `conduit-registry` 只保留抽象 + `local`；Nacos 迁到独立
    `conduit-registry-nacos`（Spring/starter 默认带上），ZooKeeper/Etcd 为独立模块；
    缺失适配模块时 `Registries` 给出明确的引入提示。
  - 真实环境验证：本地 Nacos 2.x 下 `NacosRegistryCallTest` 不再跳过、`conduit-starter-demo`
    两个真实进程跑通跨进程调用（provider/consumer pid 不同，Kryo + GZIP 按阈值协商生效）。
  - 注册中心抽象与各适配实现见 [docs/REGISTRY.md](docs/REGISTRY.md)，traceId/指标/结构化日志与开关见 [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)。

- **取消与 deadline（Invocation）+ 路由规则（Governance）**：
  - 新增 `TYPE_CANCEL` 控制帧（body 1 字节取消原因，不走序列化器）：客户端超时或调用方放弃时下发，
    服务端按「连接 + requestId」找到在途调用并中断正在执行业务的线程（`ServerCallRegistry`）；
    对端主动取消不再回写响应。
  - deadline 传递：`RpcAttachments.DEADLINE` 以**绝对时间**写进 attachments，逐跳取
    `min(上游deadline, now+本地超时)`；服务端到点自我中断并回一帧错误说明，已过期请求连业务线程都不占；
    客户端本地等待与每次重试的传输超时都改用剩余预算（`RpcAttachments.budgetMillis`）。
  - 新增 `Router` SPI 与 `DefaultRouter`：`discover → route → loadbalance`；version/group/tag 硬过滤、
    zone 软优先（同机房优先），硬过滤为空直接失败并列出候选实例的实际属性（不回退全量）。
  - `ServiceMeta` 增加 `version/group/tag/zone` 并随注册信息发布：Nacos 走实例 metadata，
    ZK/Etcd 走节点数据（新增 `ServiceMetaCodec` 统一编解码，兼容只有 weight 的老数据）；
    `@RpcService(version/group/tag/zone)`、`@RpcReference(version/group/tag)`、`ReferenceOptions` 全链路打通；
    单次调用可用 `RpcContext` 的 `conduit.version/group/tag/zone` attachment 覆盖（灰度切流）。
  - 文档同步：[TRANSPORT.md](docs/TRANSPORT.md) §6 取消与 deadline、[CLUSTER.md](docs/CLUSTER.md) §2 路由、
    [REGISTRY.md](docs/REGISTRY.md) 路由属性存储格式。

- **限流与幂等（Governance）**：
  - 新增 `RateLimitFilter`：提供侧令牌桶限流，方法级（`conduit.filter.ratelimit.qps`，可单方法覆盖）+
    全局限额（`...global-qps`），桶容量 `...burst` 默认等于 qps；超限立即 `RpcException` 快速失败。
    与 `ActiveLimitFilter`（信号量并发隔离）互补：一个挡瞬时尖峰、一个挡在途堆积。
  - 新增 `IdempotencyFilter`（消费侧）与 `IdempotentFilter`（提供侧）：消费侧给每次**逻辑调用**
    生成/保留幂等键（`RpcAttachments.IDEMPOTENCY_KEY`），failover 重放复用同一个键；提供侧按键去重
    ——已完成直接返回缓存结果、执行中挂到同一 future、失败不缓存，去重表按 TTL/条目数封顶。
    两者默认启用，`conduit.filter.idempotency.enabled` / `conduit.filter.idempotent.enabled=false` 可关闭。
  - `ConduitConfig` 增加 `getPositiveLong`；filter 开关表、SPI 扩展点清单与 [CLUSTER.md](docs/CLUSTER.md) 同步更新。

- **追踪 span 化 + 客户端写背压（Governance / Transport）**：
  - 追踪从"只有 traceId"升级为 **span 模型**：每跳新建 `spanId`、`parentSpanId` 取本线程正在服务的
    span（服务内部再调下游时父子关系自然延伸）；`conduit.trace.sample-rate` 按 traceId **确定性哈希**
    采样，结论（`conduit.trace.sampled`）沿链透传，避免半截链路；MDC 同步绑定三元组，
    文本访问日志与 JSON 访问日志都输出 `spanId`/`parentSpanId`。
  - `RpcRequestHandler` 把上下文还原提前到请求边界（Filter 链之前），提供侧 Filter 也能读到链路上下文。
  - 新增 `ContextSnapshot`：跨线程传播 `RpcContext` 与 MDC（`capture/wrap/run/call`）；
    消费侧异步调用的结果在发起线程的上下文快照里完成，调用方的 `thenApply/thenAccept` 不再丢 traceId。
  - 新增客户端**写背压**：写之前过闸——单连接在途请求上限（`max-in-flight-per-connection`，默认 1024）
    + channel 可写性（写缓冲高低水位，默认 64KB/32KB），超限立即 `RpcException("背压：...")` 快速失败，
    并计入 `NettyClient.getBackpressureRejections()`；此前服务端变慢只会让客户端无限堆积。
  - `RpcRequest` 增加 `removeAttachment`；`ConduitConfig` 增加 `getDouble`；
    文档同步 [TRANSPORT.md](docs/TRANSPORT.md) §7 写背压、[OBSERVABILITY.md](docs/OBSERVABILITY.md) §1。

### Docs

- 补齐开源基础文档：`LICENSE`（Apache-2.0）、`CONTRIBUTING.md`、`CHANGELOG.md`。
- 新增 [docs/TRANSPORT.md](docs/TRANSPORT.md)：传输层与协议设计（帧格式、握手、压缩、心跳、连接池、重连、TLS、不变量）。
- 新增 [docs/SPI.md](docs/SPI.md)：命名扩展、自适应加载、生命周期与扩展点清单。
- 新增 [docs/CLUSTER.md](docs/CLUSTER.md)：负载均衡、容错、重试配置与熔断器机制。
- 新增 [docs/REGISTRY.md](docs/REGISTRY.md)：注册中心抽象、Nacos/Local/ZooKeeper/Etcd 适配。
- 新增 [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)：traceId、指标、结构化日志与开关。
- 新增 [docs/BENCHMARK.md](docs/BENCHMARK.md)：JMH 基准方法、实测数据、瓶颈与优化方向（含基准怎么跑）。
- 新增 [docs/TESTING.md](docs/TESTING.md)：四层测试职责与三阶段（基本/分布式/工程质量）覆盖。
- 新增 [docs/LAYERS.md](docs/LAYERS.md)：Transport → Invocation → Governance 三层导读，
  知识点与代码位置的对应、覆盖状态与缺口清单。
- 整理 [docs/SPI.md](docs/SPI.md)、[docs/CLUSTER.md](docs/CLUSTER.md)、[docs/REGISTRY.md](docs/REGISTRY.md)：

### Fixed

- `conduit.filter.*` 开关在 Spring 应用里只认 JVM 系统属性：写进 `application.yml` 或用程序参数
  `--conduit.filter.metrics.enabled=true` 都不生效（把 `-D...` 填在 IDE 的 Program arguments 里同样无效）。
  新增 `ConduitConfig` 统一解析（系统属性 > Spring `Environment` > 环境变量），
  `ConduitAutoConfiguration` 在环境就绪时登记配置源并丢弃旧快照；`FilterLoader` / `ActiveLimitFilter`
  改走该入口，禁用日志同时打印具体配置键。
- `RpcDecoder` 遇到带 body 的心跳帧时未消费 body，会污染后续帧边界。
- 请求 body 反序列化失败时直接断开连接，客户端只能等到超时；现在回传可序列化的错误响应。
- 解码器未校验负数/超大 `length` 与业务帧协议版本。
- 半开连接无法被客户端感知（只有写空闲心跳、无读空闲探活），只能等请求超时。
- 超时清理任务挂在 EventLoop 上，EventLoop 关闭后无法调度，改为独立调度线程。
- 客户端 `close()` 未等待连接与线程真正释放。
- 服务端优雅停机未关闭已接入连接、未等待事件循环终止。

<!--
后续每次发版前，在此文件顶部新增一个版本块，格式例如：

## [1.0.0] - 2026-01-01

### Added
- ...

### Changed
- ...

### Fixed
- ...
-->
