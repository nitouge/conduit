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

- **基础通信层加固（Protocol + Transport）**：
  - 协议版本协商：新增 `HANDSHAKE` / `HANDSHAKE_ACK` / `HANDSHAKE_REJECT` 控制帧；版本取双方支持区间的交集最高版（未来版本平滑降级、过旧版本明确拒绝），并在握手同时协商 codec 与压缩能力；握手超时、未握手业务帧拦截。
  - 数据压缩：`ProtocolOptions` 支持压缩算法与压缩阈值（默认 1024 字节，小报文不压缩），GZIP 走握手能力协商；解压炸弹防护。
  - 链路加密：新增 `Tls` 工具与 `sslContext` 配置项，服务端/客户端均可启用 TLS（JDK SslProvider），客户端默认开启 HTTPS 主机名校验，支持 PEM 与 keystore。
  - 连接池：新增 `ConnectionPool`，每地址默认单长连接，可配置多连接；按在途请求数选择连接、单飞建链、后台扩容到上限、闲置回收。
  - 心跳保活：客户端写空闲发 PING、服务端回 PONG，两端读空闲探活；心跳间隔/读空闲阈值可配置。
  - 断线自动重连：新增 `ReconnectManager`，指数退避后台重连（成功即停、达上限放弃），并保留按需建链兜底。
  - 客户端建链/发送改为完全异步，不再阻塞调用线程；请求超时清理改用独立调度线程。
  - 新增 `ClientOptions` / `ServerOptions` 与 `conduit.transport.*` 配置项。

- **核心扩展机制（SPI + Cluster）**：
  - SPI 自适应扩展：新增 `@Adaptive` 与 `ExtensionLoader.getAdaptiveExtension()`，按 `Map` / `RpcRequest.attachments` 的 key 在每次调用时动态选择实现，未取到回退默认；
  - SPI 生命周期：新增 `Lifecycle`（init/destroy），ExtensionLoader 创建后自动 init，`destroyExtension/destroyAll/shutdownAll` 与 JVM 关闭钩子统一释放；
  - 集群可配置重试：新增 `ClusterOptions`（重试次数/退避/熔断参数），`@RpcReference(retries, retryBackoffMs)` 与 `conduit.cluster.*` 全局配置；FailOver 支持异步退避、熔断节点跳过不消耗预算；
  - 熔断器可配置：新增 `CircuitBreakerOptions` 与 `CircuitBreakerRegistry.configure/state/reset`；
  - Cluster/LoadBalancer 经自适应代理支持单次调用覆盖（`RpcContext` 的 `cluster` / `loadbalance` attachment）。

- **服务治理（Registry + Observability）**：
  - 新增 `conduit-registry-zookeeper`：Curator 临时节点注册、`CuratorCache` 订阅、连接超时快速失败；使用 Curator `TestingServer` 内嵌测试。
  - 新增 `conduit-registry-etcd`：jetcd 租约注册、自动续租/失败重注册、前缀 watch；通过内存 `EtcdOperations` 假实现覆盖逻辑测试。
  - 新增 `TraceFilter`：自动生成/透传 traceId、MDC 绑定；`RpcContext` 增加 `getTraceId/setTraceId`；默认启用。
  - 扩展 `MetricsFilter`：QPS/成功率、平均/最大/最小 RT、延迟分桶与分位估算、全局汇总与重置。
  - 新增 `StructuredLogFilter`：一行 JSON 访问日志（`CONDUIT.ACCESS.JSON`），默认关闭。
  - 新增 [docs/REGISTRY.md](docs/REGISTRY.md)、[docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)。
  - 注册中心拆分：核心 `conduit-registry` 只保留抽象 + `local`；Nacos 迁到独立
    `conduit-registry-nacos`（Spring/starter 默认带上），ZooKeeper/Etcd 为独立模块；
    缺失适配模块时 `Registries` 给出明确的引入提示。
  - 真实环境验证：本地 Nacos 2.x 下 `NacosRegistryCallTest` 不再跳过、`conduit-starter-demo`
    两个真实进程跑通跨进程调用（provider/consumer pid 不同，Kryo + GZIP 按阈值协商生效）。

### Docs

- 补齐开源基础文档：`LICENSE`（Apache-2.0）、`CONTRIBUTING.md`、`CHANGELOG.md`。
- 新增 [docs/TRANSPORT.md](docs/TRANSPORT.md)：传输层与协议设计（帧格式、握手、压缩、心跳、连接池、重连、TLS、不变量）。
- 新增 [docs/SPI.md](docs/SPI.md)：命名扩展、自适应加载、生命周期与扩展点清单。
- 新增 [docs/CLUSTER.md](docs/CLUSTER.md)：负载均衡、容错、重试配置与熔断器机制。
- 新增 [docs/REGISTRY.md](docs/REGISTRY.md)：注册中心抽象、Nacos/Local/ZooKeeper/Etcd 适配。
- 新增 [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)：traceId、指标、结构化日志与开关。

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
