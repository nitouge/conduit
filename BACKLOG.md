# Conduit — Backlog（后续增强 + 硬化清单）

## 状态与工作量说明

- 待办: `- [ ]`
- 进行中: `- [~]`
- 完成: `- [x]`
- 工作量标注: `S` (小) / `M` (中) / `L` (大)

## 设计原则

- 面向接口: 所有核心模块均定义清晰的接口契约，隔离底层实现。
- SPI 机制: 核心组件通过 SPI 动态加载，支持开箱即用与自定义扩展。

---

## 1. Protocol

- [x] 协议版本协商 `S` — 握手帧（HANDSHAKE/ACK/REJECT）+ 版本取交集最高版 + 逐帧版本校验；未来版本平滑降级、能力（codec/压缩）协商
- [x] 数据压缩支持 `M` — GZIP + 压缩阈值（默认1024B）+ 握手能力协商 + 解压炸弹防护
- [x] 链路加密传输 `L` — TLS（JDK SslProvider）+ 默认HTTPS主机名校验 + PEM/keystore 支持

> 帧格式、握手协商与压缩细节见 [docs/TRANSPORT.md](docs/TRANSPORT.md)。

## 2. Transport

- [x] 连接池管理 `M` — 每地址连接池：按在途负载选连接、单飞建链、后台扩容到上限、闲置回收
- [x] 心跳保活机制 `S` — 客户端PING / 服务端PONG + 两端读空闲探活，半开连接可被及时发现
- [x] 断线自动重连 `M` — 指数退避后台重连（成功即停/达上限放弃），按需建链兜底；在途请求断链即快速失败

> 连接池、心跳、重连与 TLS 设计见 [docs/TRANSPORT.md](docs/TRANSPORT.md)。

## 3. SPI

- [x] 自适应扩展加载 `M` — `@Adaptive` + `getAdaptiveExtension()` 代理：按 Map/`RpcRequest.attachments` 的 key 每次调用动态选实现，未取到回退默认；Cluster/LoadBalancer 已接入，可用 `RpcContext` 的 `cluster`/`loadbalance` attachment 单次覆盖
- [x] 组件生命周期管理 `M` — `Lifecycle`（init/destroy）+ ExtensionLoader 自动初始化/销毁、`destroyExtension/destroyAll/shutdownAll` 与 JVM 关闭钩子

> 扩展点清单、自适应加载与生命周期见 [docs/SPI.md](docs/SPI.md)。

## 4. Cluster

- [x] 故障自动转移 `M` — Failover 失败自动切换其它节点、熔断节点快速跳过（不消耗重试预算），重试/退避按调用配置
- [x] 重试策略配置 `S` — `@RpcReference(retries/retryBackoffMs)` + `conduit.cluster.retries/retry-backoff-ms` 全局配置；`ClusterOptions` 随调用传入
- [x] 熔断器机制 `L` — 节点级三态熔断 + `CircuitBreakerOptions`（阈值/打开时长）可配置 + 注册表 `state/reset/configure`

> 负载均衡、容错策略与熔断状态机见 [docs/CLUSTER.md](docs/CLUSTER.md)。

## 5. Registry

- [x] Nacos 注册中心适配 `M` — 拆分为独立 `conduit-registry-nacos`（核心仅保留抽象+local）；Spring/starter默认带上
- [x] ZooKeeper 注册中心适配 `M` — 新增 `conduit-registry-zookeeper`：Curator临时节点(PersistentNode) + CuratorCache；内嵌 TestingServer 测试
- [x] Etcd 注册中心适配 `M` — 新增 `conduit-registry-etcd`：jetcd租约+前缀watch+自动续租；内存fake测试

> 注册中心抽象、缓存兜底与各适配实现见 [docs/REGISTRY.md](docs/REGISTRY.md)。

## 6. Observability

- [x] 分布式链路追踪 `L` — `TraceFilter` 自动生成/透传 traceId + MDC；`RpcContext.getTraceId/setTraceId`；默认开启
- [x] 核心指标采集 `M` — `MetricsFilter` 统计QPS/成功失败/平均/最大/最小/延迟分桶与分位估算；默认关闭、按需开启
- [x] 结构化日志输出 `S` — `StructuredLogFilter` 输出 JSON 访问日志（`CONDUIT.ACCESS.JSON`）；默认关闭

> traceId、指标、结构化日志与开关配置见 [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)。

## 7. Benchmark

- [x] QPS 吞吐量基准测试 `M` — 新增 `conduit-benchmark`（JMH）：端到端 4 线程并发吞吐（jdk/kryo 对比）
- [x] 延迟分布基准测试 `M` — 同一基准以 SampleTime 模式输出 p50/p90/p95/p99/p99.9/p100 分位
- [x] 序列化性能基准测试 `S` — 四种序列化 × small/large 报文的序列化/反序列化吞吐与编码体积；另含协议编解码（不压缩 vs gzip）

> 基准方法、实测数据与后续优化方向见 [docs/BENCHMARK.md](docs/BENCHMARK.md)。

## 8. 测试体系

- [x] `conduit-testkit` 测试夹具 `M` — `TestServer`/`TestRpc` 真实夹具、`TestRegistries` 替身、`TlsTestCerts` 自签证书、`Await`/`Ports`、`TestReset`
- [x] `conduit-it` 集成测试 `M` — 多节点负载均衡/下线/容错/熔断、TLS 端到端、provider 在独立 JVM 的跨进程调用
- [x] `conduit-benchmark` 性能基准 `M` — 见 §7 与 [docs/BENCHMARK.md](docs/BENCHMARK.md)
- [x] 阶段一 基本正确性 `M` — 协议编解码/序列化边界/请求响应匹配(`PendingRequestsTest`)/超时/连接管理/SPI 均有专项用例
- [x] 阶段二 分布式正确性 `L` — `MultiJvmRegistryIntegrationTest`：2 个 provider JVM + Nacos + consumer JVM，验证注册发现、跨进程调用、roundrobin、优雅下线收敛、重启恢复（`-Dit.distributed=true` 开启）
- [x] 阶段三 工程质量 `M` — JMH 基线；`LinearizabilityChecker`（Wing & Gong）+ `InMemoryRegistryLinearizabilityTest`；Chaos 三场景（进程崩溃/变慢/重启）

测试层次、三阶段覆盖与写法约定见 [docs/TESTING.md](docs/TESTING.md)。

## 9. 分层能力缺口（Transport / Invocation / Governance）

按 [docs/LAYERS.md](docs/LAYERS.md) 的三层模型盘点后，剩余缺口：

- [ ] 请求取消与 deadline 传递 `M` — 上游超时后下游仍在白跑；增加 CANCEL 控制帧，并把 deadline 放进 attachments，服务端到点自我中断
- [ ] 路由规则 `M` — `RpcRequest.version/group` 字段已有但未参与选路；按 version/group/tag 过滤节点，支撑灰度与多版本
- [ ] QPS 限流 `S` — 目前只有并发隔离（`ActiveLimitFilter` 信号量）；补令牌桶形态的方法级/全局限流
- [ ] 重试幂等 `M` — Failover 会重放请求，但无幂等键与服务端去重；非幂等接口开重试可能重复执行
- [ ] 追踪 span 化与采样 `M` — 只有 traceId，缺 spanId/父子关系/采样率，异步线程上下文也未传播
- [ ] 客户端写背压 `S` — 服务端变慢时缺少基于 channel writability 的在途水位控制与快速失败

---

## 建议落地顺序

1. 基础通信层: 优先完善 Transport 与 Protocol，确保底层通信稳定可靠。**（已完成，设计见 [docs/TRANSPORT.md](docs/TRANSPORT.md)）**
2. 核心扩展机制: 落地 SPI 与 Cluster 基础能力，支撑上层业务扩展与高可用。
3. 服务治理: 接入 Registry 与 Observability，实现服务发现与可观测性。
4. 性能验证: 最后执行 Benchmark，基于真实数据指导后续优化。**（已完成，方法、实测数据与优化方向见 [docs/BENCHMARK.md](docs/BENCHMARK.md)）**
5. 测试基建: 沉淀可复用夹具与真实网络/跨进程集成测试。**（已完成，见 [docs/TESTING.md](docs/TESTING.md)）**
