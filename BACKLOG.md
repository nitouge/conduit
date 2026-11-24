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

## 2. Transport

- [x] 连接池管理 `M` — 每地址连接池：按在途负载选连接、单飞建链、后台扩容到上限、闲置回收
- [x] 心跳保活机制 `S` — 客户端PING / 服务端PONG + 两端读空闲探活，半开连接可被及时发现
- [x] 断线自动重连 `M` — 指数退避后台重连（成功即停/达上限放弃），按需建链兜底；在途请求断链即快速失败

## 3. SPI

- [x] 自适应扩展加载 `M` — `@Adaptive` + `getAdaptiveExtension()` 代理：按 Map/`RpcRequest.attachments` 的 key 每次调用动态选实现，未取到回退默认；Cluster/LoadBalancer 已接入，可用 `RpcContext` 的 `cluster`/`loadbalance` attachment 单次覆盖
- [x] 组件生命周期管理 `M` — `Lifecycle`（init/destroy）+ ExtensionLoader 自动初始化/销毁、`destroyExtension/destroyAll/shutdownAll` 与 JVM 关闭钩子

## 4. Cluster

- [x] 故障自动转移 `M` — Failover 失败自动切换其它节点、熔断节点快速跳过（不消耗重试预算），重试/退避按调用配置
- [x] 重试策略配置 `S` — `@RpcReference(retries/retryBackoffMs)` + `conduit.cluster.retries/retry-backoff-ms` 全局配置；`ClusterOptions` 随调用传入
- [x] 熔断器机制 `L` — 节点级三态熔断 + `CircuitBreakerOptions`（阈值/打开时长）可配置 + 注册表 `state/reset/configure`

## 5. Registry

- [ ] Nacos 注册中心适配 `M`
- [ ] ZooKeeper 注册中心适配 `M`
- [ ] Etcd 注册中心适配 `M`

## 6. Observability

- [ ] 分布式链路追踪 `L`
- [ ] 核心指标采集 `M`
- [ ] 结构化日志输出 `S`

## 7. Benchmark

- [ ] QPS 吞吐量基准测试 `M`
- [ ] 延迟分布基准测试 `M`
- [ ] 序列化性能基准测试 `S`

---

## 建议落地顺序

1. 基础通信层: 优先完善 Transport 与 Protocol，确保底层通信稳定可靠。**（已完成，设计见 [docs/TRANSPORT.md](docs/TRANSPORT.md)）**
2. 核心扩展机制: 落地 SPI 与 Cluster 基础能力，支撑上层业务扩展与高可用。
3. 服务治理: 接入 Registry 与 Observability，实现服务发现与可观测性。
4. 性能验证: 最后执行 Benchmark，基于真实数据指导后续优化。
