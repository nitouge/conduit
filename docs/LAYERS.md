# Conduit 的三层模型：Transport → Invocation → Governance

RPC 框架的知识可以按"解决什么问题"分成三层，逐层递进：

```text
第三层  Governance   节点会变、网络会坏 → 让调用依然可控（发现/选路/容错/限流/追踪）
第二层  Invocation   远程怎么像本地     → 代理、requestId 匹配、同步/异步、超时、异常
第一层  Transport    字节怎么可靠送达   → TCP/NIO/EventLoop、长连接、粘包拆包、心跳、编解码
```

本页是[本仓库](../README.md)的**分层导读**：每个知识点对应哪个类、现在做到哪一步、还缺什么。
覆盖状态的图例：✅ 已实现并有测试 ｜ ⚠️ 部分实现/有限制 ｜ ❌ 尚未实现。

---

## 第一层：通信基础设施（Transport）

> 目标：把一帧请求可靠、高效地送到对端，并能感知连接的死活。

| 知识点 | 本仓库实现 | 关键设计 | 验证 |
| --- | --- | --- | --- |
| TCP / NIO / Reactor | `NettyServer` / `NettyClient`（Netty `NioEventLoopGroup`） | boss 1 线程只做 accept，worker 做 IO；**业务线程池与 IO 线程隔离** | `HeartbeatIntegrationTest`、`HandshakeIntegrationTest` |
| Netty EventLoop | `NettyServerHandler` / `NettyClientHandler` | IO 线程只做编解码与派发，绝不在 EventLoop 上跑业务；响应可从任意线程 `writeAndFlush`（Netty 保证线程安全） | `CompressionIntegrationTest` |
| 长连接与连接复用 | `ConnectionPool` | 每地址默认 1 条长连接，按在途请求数选连接；单飞建链、后台扩容到上限、闲置回收 | `ConnectionPoolTest`、`ConnectionPoolIntegrationTest` |
| 粘包 / 拆包 | `RpcDecoder` + `LengthFieldBasedFrameDecoder` | 18 字节定长头（magic/版本/type/codec/compress/requestId/length），按 `length` 切帧；非法长度/魔数直接断链 | `RpcCodecTest` |
| 心跳与连接管理 | `ClientHeartbeatHandler` / `ServerHeartbeatHandler`、`ReconnectManager` | 客户端写空闲发 PING、服务端回 PONG；两端读空闲探活（半开连接可被发现）；断线后指数退避后台重连 | `HeartbeatIntegrationTest`、`ReconnectIntegrationTest` |
| 编解码与序列化 | `RpcEncoder` / `RpcDecoder`、`Serializer` SPI、`Compressor` | codec/压缩算法写进协议头，收发两端各自按头解码；GZIP 按阈值触发（默认 1024B）；握手时协商版本/codec/压缩能力 | `SerializerSpiTest`、`RpcCodecTest`、`CompressorTest` |
| 链路加密 | `Tls` | 只依赖一个 `SslContext`，加密对上层透明；客户端默认开启 HTTPS 主机名校验 | `TlsIntegrationTest`、`TlsEndToEndIntegrationTest` |

**这一层的设计取舍**

- 头部固定 18 字节、字段顺序即二进制布局：解码零反射、可预测；
- 压缩"按阈值 + 协商"而不是"配置了就压"：小报文压缩往往更大；
- 握手独立成控制帧且不走序列化器：否则"还不知道用哪个序列化器"就成了死循环。

---

## 第二层：RPC 调用模型（Invocation）

> 目标：让远程调用看起来像本地方法调用，同时保留异步、超时、异常等真实语义。

| 知识点 | 本仓库实现 | 关键设计 | 验证 |
| --- | --- | --- | --- |
| 动态代理 | `RpcClientProxy`（JDK 动态代理）+ `RpcInvocationHandler` | 代理只做参数打包与线程上下文（`RpcContext`）转 attachments；不做业务逻辑 | `DirectCallTest` |
| 请求 ID 与响应匹配 | `PendingRequests` | 全局自增 `requestId` → `CompletableFuture`；响应按协议头 id 唤醒；**一个 id 只能完成一次**，未知 id 静默忽略 | `PendingRequestsTest` |
| 同步调用 | `RpcInvocationHandler` | 发起后 `future.get(timeout)`，对调用方是阻塞的"本地方法" | `DirectCallTest` |
| 异步调用 | `RpcInvocationHandler` 识别返回类型 `CompletableFuture` | 代理立即返回 future，不占调用线程；服务端 `RequestHandler` 也可返回 future，不占业务线程 | `ProviderAsyncTest`、`ClusterIntegrationTest.asyncCallReturnsFutureAndCompletes` |
| Future / Promise | `CompletableFuture<RpcResponse>` | 框架内部统一用 future 表达"结果尚未到达"，同步只是它的一个特例 | `ChaosIntegrationTest` |
| 超时 | `RpcInvocationHandler` 超时清理线程 + `PendingRequests.completeExceptionally` | 超时与连接断开都会清理在途表，避免内存泄漏 | `FixesTest.timeoutCleansPendingRequests`、`ChaosIntegrationTest.timeoutLeavesNoPendingRequests` |
| **取消** | ❌ 未实现 | 超时只是"客户端不等了"，服务端仍在执行；协议里没有 CANCEL 控制帧，也没有跨服务的 deadline 传递 | — |
| 服务端请求分发 | `NettyServerHandler` → `RpcRequestHandler` → `ServiceCache` | IO 线程派发到业务线程池，反射调用；**池满（有界队列 + AbortPolicy）立即回"繁忙"错误**，不让客户端干等超时 | `RobustnessTest`、`CompressionIntegrationTest` |
| 异常传播 | `RpcResponse.error` + `ExceptionFilter` + 客户端 `RpcInvocationHandler` | 业务异常/不可序列化结果都会转成可序列化的错误响应回传；客户端还原为 `RpcException` | `RobustnessTest.nonSerializableResultReturnsErrorNotTimeout`、`ReviewFixesTest` |
| 横切能力（Filter 链） | `FilterLoader` + `FilterChainBuilder` | 消费侧/提供侧各一条链，按 `order()` 排序；trace/accesslog/metrics/structuredlog/exception/activelimit 都是 Filter | `FilterChainTest`、`ObservabilityE2ETest` |

**这一层的设计取舍**

- `requestId` 放协议头而不是 body：关联逻辑不依赖序列化格式，换 codec 不影响匹配；
- 异常也用正常响应帧回传（`RpcResponse.hasException`）：客户端不必等超时才知道失败；
- 超时清理走独立调度线程而不是 EventLoop：EventLoop 关闭后仍能完成清理。

---

## 第三层：分布式治理（Governance）

> 目标：节点会上下线、网络会抖动，调用依然要可控：选对节点、失败能兜住、问题可观测。

| 知识点 | 本仓库实现 | 关键设计 | 验证 |
| --- | --- | --- | --- |
| 注册发现 | `Registry` SPI + `AbstractRegistry` + `NacosRegistry`/`ZookeeperRegistry`/`EtcdRegistry` | `AbstractRegistry` 统一做本地缓存、订阅推送去重、注册中心不可用时用缓存兜底 | `AbstractRegistryCacheTest`、`ZookeeperRegistryTest`、`EtcdRegistryTest`、`NacosRegistryCallTest` |
| 负载均衡 | `RandomLoadBalancer`（带权重）/`RoundRobinLoadBalancer`/`ConsistentHashLoadBalancer` | SPI + 自适应代理，支持单次调用用 `RpcContext` attachment 覆盖策略 | `LoadBalancerTest`、`MultiNodeClusterIntegrationTest` |
| 集群容错 | `Failover`/`Failfast`/`Failsafe`/`Failback` | 熔断节点快速跳过且**不消耗重试预算**；重试次数/退避可全局配置也可注解级覆盖 | `ClusterTest`、`ClusterRetryConfigTest` |
| **路由规则** | ❌ 未实现 | `RpcRequest.version/group`、`ServiceMeta` 有字段但未参与选路；没有 tag/灰度/同机房优先 | — |
| 熔断 | `CircuitBreaker` + `CircuitBreakerRegistry` | 节点级三态（CLOSED/OPEN/HALF_OPEN）、阈值/打开时长可配；`state()` 可观测 | `CircuitBreakerTest`、`MultiNodeClusterIntegrationTest.breakerOpensAndFailsFastAfterRepeatedFailures` |
| 限流 | ⚠️ 只有并发隔离 `ActiveLimitFilter`（信号量，按方法限在途数） | 超出即快速失败；**没有 QPS 令牌桶/热点参数限流** | `ActiveLimitFilter` 相关用例 |
| 重试与幂等 | 重试 ✅ / 幂等 ❌ | Failover 会重放请求，但框架不提供幂等键或服务端去重，**非幂等接口开重试可能重复执行** | `ClusterRetryConfigTest` |
| 链路追踪 | `TraceFilter` + `RpcContext` + MDC | traceId 自动生成、随 attachments 跨进程透传、写入 MDC；访问日志/JSON 日志带 traceId | `TraceFilterTest`、`ObservabilityE2ETest` |
| 指标 | `MetricsFilter` | 按 `interface#method` 统计 QPS/成功率/RT/延迟分桶与分位估算 | `MetricsFilterTest` |
| 结构化日志 | `StructuredLogFilter` | 一行 JSON（`CONDUIT.ACCESS.JSON`），便于采集 | `StructuredLogFilterTest` |

**这一层的设计取舍**

- 治理能力全部挂在 SPI 上（`Cluster`/`LoadBalancer`/`Registry`/`Filter`），换实现不改业务代码；
- 节点级熔断而不是接口级：一个坏节点不应该拖垮整个接口；
- traceId 放 `attachments` 而不是协议头：横切能力不污染协议，且天然跨 codec。

---

## 调用链是怎么穿起来的

一次 `helloService.sayHello("x")` 的完整路径（括号里是层）：

```text
JDK 动态代理                      [Invocation]  RpcClientProxy
 └ RpcInvocationHandler           [Invocation]  打包 RpcRequest + attachments(traceId)
   └ Cluster(failover)            [Governance]  选节点、重试、跳过熔断
     └ LoadBalancer               [Governance]  random / roundrobin / consistenthash
       └ ConnectionPool           [Transport]   复用长连接（按在途数选、单飞建链）
         └ 握手 + 编码             [Transport]   HANDSHAKE → RpcEncoder（Kryo + 可选 GZIP）
           └ TCP 发送 / 接收       [Transport]   Netty EventLoop
             └ 服务端解码          [Transport]   RpcDecoder 校验头 → 反序列化
               └ 业务线程池        [Invocation]  NettyServerHandler → RpcRequestHandler
                 └ 反射调用        [Invocation]  ServiceCache 取实现 → method.invoke
                   └ 响应编码回传  [Transport]  同一 requestId + 同一连接
       └ PendingRequests.complete  [Invocation]  按 requestId 唤醒 future
 └ 返回结果 / 抛异常
```

---

## 建议的阅读顺序（按层深入）

| # | 文件 | 读什么 |
| --- | --- | --- |
| 1 | `conduit-protocol/.../ProtocolConstants.java` | 18 字节头的字段与偏移，先建立"帧"的直觉 |
| 2 | `RpcEncoder` / `RpcDecoder` | 编解码如何校验、握手帧为何特殊、解码失败为何回错误响应 |
| 3 | `NettyServerHandler` / `NettyClientHandler` | IO 线程与业务线程的边界、响应如何写回 |
| 4 | `ConnectionPool` / `ReconnectManager` | 连接复用、单飞建链、指数退避重连 |
| 5 | `PendingRequests` | requestId ↔ future 的匹配与"恰好一次"语义 |
| 6 | `RpcClientProxy` / `RpcInvocationHandler` | 代理如何把本地调用翻译成请求帧、同步/异步/超时 |
| 7 | `RpcServer` / `RpcRequestHandler` | 暴露、注册、反射分发 |
| 8 | `ExtensionLoader` + `@SPI` | 为什么换序列化/注册中心/集群策略不用改业务代码 |
| 9 | `AbstractRegistry` / `LoadBalancer` / `Cluster` / `CircuitBreaker` | 治理能力的组合方式 |
| 10 | `FilterLoader` + 内置 Filter | 横切能力如何插进调用链 |

每层都可以单独验证：

```bash
mvn -pl conduit-remoting -am test                  # 第一层：协议/连接/心跳/压缩/TLS
mvn -pl conduit-remoting,conduit-client -am test    # 第二层：requestId 匹配/代理/超时
mvn -pl conduit-cluster,conduit-registry -am test   # 第三层：LB/容错/熔断/注册中心
mvn -pl conduit-it -am test                         # 跨层：多节点真实网络/跨进程/TLS/Chaos
mvn -pl conduit-it -am test -Dit.distributed=true   # 多JVM + Nacos 分布式验收
```

性能数据见 [BENCHMARK.md](BENCHMARK.md)，测试分层见 [TESTING.md](TESTING.md)。

---

## 三层视角下的缺口（下一步优先级）

| 层 | 缺口 | 影响 | 建议 |
| --- | --- | --- | --- |
| Invocation | 请求取消与 deadline 传递 | 上游超时后下游仍白跑，长尾放大 | 增加 CANCEL 控制帧 + 把 deadline 写进 attachments，服务端到点自我中断 |
| Governance | 路由规则 | 无法灰度/多版本/同机房优先 | 用 `version`/`group` 参与 `discover` 过滤，或加 tag 路由 Filter |
| Governance | QPS 限流 | 只有并发隔离，挡不住流量突刺 | 令牌桶 Filter（方法级 + 全局），配合既有快速失败语义 |
| Governance | 重试幂等 | 非幂等接口开 failover 可能重复下单 | 约定幂等键 attachment + 服务端去重表，或在文档中明确限制 |
| Governance | 追踪 span 化 | 只有 traceId，没有 span/父子关系/采样 | 引入 spanId + 采样率；异步线程上下文传播 |
| Transport | 客户端写背压 | 服务端变慢时客户端可能堆积在途请求 | 基于 channel writability 的水位控制 + 在途上限快速失败 |
