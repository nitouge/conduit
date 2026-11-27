# Conduit 集群容错（Cluster）

`conduit-cluster` 回答四个问题：候选里哪些能去（`Router`）、选哪个节点（`LoadBalancer`）、
失败怎么办（`Cluster`）、不健康节点要不要先不打（`CircuitBreaker`）。

```text
RpcInvocationHandler
  └─ Router.route(...)                     # 按 version/group/tag 收窄候选，zone 软优先
       └─ Cluster（自适应代理，默认来自 @RpcReference / conduit.cluster.*）
            ├─ LoadBalancer.select(...)    # 自适应代理，支持单次覆盖
            └─ AbstractCluster.callWithBreaker # 节点级熔断闸门
                 └─ RemoteInvoker          # 真正的远程调用（由 conduit-client 注入）
```

**为什么路由在负载均衡之前**：负载均衡只能在"给定的候选"里挑，候选里混着 v1/v2、灰度/正式、
不同机房时，再公平地挑也是错的。所以顺序是 `discover → route → loadbalance`。

**模块边界**：`cluster` 只依赖 `api/common/registry`，**不依赖 `remoting`**——真正发请求的能力由
`conduit-client` 通过 `RemoteInvoker` 回调注入（`CompletableFuture<RpcResponse> invoke(ServiceMeta target, RpcRequest request)`），
这样下层保持纯净，也便于单测替换成假 invoker。

只有**传输层异常**（future 异常完成）才触发容错；业务异常包在正常完成的 `RpcResponse` 里返回，
不会被重试。

---

## 1. LoadBalancer：从多个节点里选一个

| 名称 | 策略 | 权重 | 是否粘住同一节点 | 适用场景 |
| --- | --- | --- | --- | --- |
| `random`（默认） | 加权随机 | 是（`ServiceMeta.weight`，默认 100） | 否 | 简单、长期均衡；机器配置不一可调权重 |
| `roundRobin` | 轮询 | 否 | 否 | 节点同构、希望严格平均 |
| `consistentHash` | 一致性哈希 | 否 | 是（同参数稳定落同一节点） | 需要会话/缓存亲和 |

实现要点：

- **加权随机**：求总权重后在 `[0, total)` 取随机落点，依次扣减各节点权重，落点归零即选中；
  权重全相同（或总和为 0）时退化为均匀随机；
- **轮询**：`AtomicInteger` 自增后 `& Integer.MAX_VALUE` 再取模，避免长期累加溢出成负数；
  `LoadBalancer` 是 SPI 单例，同一策略的计数天然全局共享；
- **一致性哈希**：每个物理节点复制成多个虚拟节点散布到哈希环，请求 key 取哈希后顺时针取第一个节点；
  **路由 key = 接口名 + 方法名 + 参数**，因此"同参数 → 同节点"。当前实现每次 `select` 重建哈希环
  （节点变动自动生效，代价是 O(n·虚拟节点数)），高频场景可按节点列表指纹缓存环。

---

## 2. Router：先收窄候选（版本/分组/标签/机房）

默认实现 `DefaultRouter`，SPI 名 `default`（可替换）。

| 条件 | 来源 | 语义 |
| --- | --- | --- |
| 版本 | `@RpcReference(version)` / `RpcContext` 的 `conduit.version` | **硬过滤**：实例的 `version` 必须相等 |
| 分组 | `@RpcReference(group)` / `conduit.group` | **硬过滤** |
| 标签 | `@RpcReference(tag)` / `conduit.tag` | **硬过滤**：实例 `@RpcService(tag)` 必须相等 |
| 机房 | `conduit.zone` / 配置 `conduit.router.zone` | **软优先**：同机房有实例就只去同机房，没有就跨机房 |

规则细节：

- 任何一项为 `null`/空串/`"*"` 都表示**不约束**——不声明版本的老调用行为完全不变；
- 硬过滤后为空 → **直接失败**（`没有匹配的路由实例: ...`，错误里带上约束值与候选实例的实际属性）。
  不回退到全量是刻意的：空结果几乎总是配置写错，静默回退会让流量悄悄打到错误的版本上；
- 路由发生在**集群容错之前**，所以路由失败不会被 failover 重试（换节点也没用）；
- 版本/分组/标签的取值来自注册信息：提供侧 `@RpcService(version/group/tag/zone)` → `ServiceMeta`
  → 各注册中心（Nacos 实例 metadata / ZK 节点数据 / Etcd value）→ 消费侧 `discover`。

**灰度切流**的典型用法（不改代码、不改引用）：

```java
// 把某个调用方实例整体导到灰度标签的提供者
RpcContext.getContext().setAttachment(RpcAttachments.TAG, "gray");
greetService.hello("x");
```

**同机房优先**：提供侧 `@RpcService(zone="sh-1")`；消费侧配置一次 `conduit.router.zone=sh-1`
（进程部署位置，天然是配置而不是每次调用的参数）。

---

## 3. Cluster：失败之后怎么办

| 策略 | 失败行为 | 返回 | 适用 |
| --- | --- | --- | --- |
| `failover`（默认） | 换其它节点重试，直到成功或预算用尽 | 成功值 / 抛最后一次异常 | 幂等读 |
| `failfast` | 立即抛异常，不重试 | 抛出 | **非幂等写**（下单、扣款） |
| `failsafe` | 吞掉异常并记日志 | `null` | 埋点/审计等可丢调用 |
| `failback` | 立即返回，失败任务丢后台定时重试 | `null`（首次立即返回） | 最终一致的通知类操作 |

`failover` 的细节：

- 总尝试次数 = `retries + 1`（默认 `retries=2`，即最多打 3 个不同节点）；
- 每轮从**未试过且未熔断**的候选里重选，避免重复打同一坏节点；
- 熔断打开的节点**零成本跳过**：既不消耗重试预算，也不退避；
- 真实失败后按 `retry-backoff-ms` **异步退避**再试，不阻塞调用线程；
- 每次尝试仍受单次调用 `timeout` 约束。

---

## 4. 超时与重试的总账（重要）

超时约束**单次**远程调用（`future.get(timeout)`），重试是它的**外层循环**。两条铁律：

1. **算总账**：`最坏总耗时 ≈ (retries + 1) × timeout + retries × retry-backoff-ms`。
   上游调用方的整体超时必须 ≥ 这个值，否则上游先超时、重试白跑；`retries` 也不宜过大，
   故障期间会把耗时和下游压力一起放大。
2. **只对幂等操作开重试**：`failover` 会重放请求，非幂等写操作请显式用 `failfast`
   （框架目前不提供幂等键/服务端去重，见 BACKLOG §9）。

---

## 5. 配置方式

### `@RpcReference`

```java
@RpcReference(
    cluster = ClusterPolicyEnum.FAIL_OVER,
    loadbalance = LoadbalancePolicyEnum.RANDOM,
    retries = 3,             // -1 = 用全局
    retryBackoffMs = 50      // -1 = 用全局
)
private OrderService orderService;
```

### `application.yml`

```yaml
conduit:
  cluster:
    retries: 2               # failover 额外重试次数，总尝试 = retries + 1
    retry-backoff-ms: 0      # 两次尝试之间的异步退避
    breaker:
      failure-threshold: 5   # 连续失败多少次打开
      open-duration-ms: 5000 # 打开多久后转半开探测
```

### 编程式

```java
ClusterOptions options = ClusterOptions.builder()
        .retries(3)
        .retryBackoffMs(50)
        .circuitBreaker(CircuitBreakerOptions.builder()
                .failureThreshold(5)
                .openDurationMs(5000)
                .build())
        .build();

HelloService proxy = RpcClientProxy.create(HelloService.class, registry, client, 3000,
        "failover", "random", options);
```

### 单次调用覆盖策略

`Cluster` / `LoadBalancer` 通过自适应扩展代理加载，默认名取注解/配置，单次调用可用
`RpcContext` 覆盖：

```java
RpcContext.getContext().setAttachment("cluster", "failfast");     // 本次调用不重试
RpcContext.getContext().setAttachment("loadbalance", "roundrobin");
orderService.query(...); // attachments 随请求透传，并用于本次策略选择
```

---

## 6. 熔断器

### 状态机

```text
                连续失败 >= failureThreshold
      +-----------------------------------------+
      |                                         v
  +---------+                              +---------+
  | CLOSED  |                              |  OPEN   |
  +---------+                              +---------+
      ^                                         |
      |  探测成功                                | openDuration 到期
      |                                         v
  +-------------------------------------> +-----------+
                                          | HALF_OPEN |
                                          +-----------+
                                                |
                                                +-- 探测失败 --> OPEN（重新计时）
```

- **CLOSED**：正常放行，累计**连续失败次数**，达到 `failure-threshold` 转 OPEN；
- **OPEN**：直接拒绝（快速失败，不再发起真实调用）；超过 `open-duration-ms` 后放行**一个**探测并转 HALF_OPEN；
- **HALF_OPEN**：只放行那一个探测，其余请求仍拒绝；探测成功回 CLOSED、失败回 OPEN。

> 最初设计的是"滑动窗口失败率阈值"，最终实现改为
> **连续失败计数**——状态更少、行为更可预测，也不需要窗口清理线程。

### 隐式契约（重要）

`allowRequest()` 在 HALF_OPEN 期放行探测后，调用方**必须恰好回报一次** `onSuccess()` 或 `onFailure()`，
否则 `probeInFlight` 不会复位、熔断器会永久卡在 HALF_OPEN 拒绝所有请求。
`AbstractCluster.callWithBreaker` 用 try/catch 保证"成功/异常各回报一次"。

### API

```java
CircuitBreakerRegistry.configure(CircuitBreakerOptions.builder()
        .failureThreshold(3)
        .openDurationMs(10_000)
        .build());

CircuitBreaker.State state = CircuitBreakerRegistry.state(node); // 观测
CircuitBreakerRegistry.reset(node);                              // 手动重置单节点
CircuitBreakerRegistry.options();                                // 当前默认参数
```

- 熔断器按节点（`host:port`）维度共享；已创建的熔断器保持创建时的参数，新节点使用新参数；
- `CircuitBreakerRegistry.clear()` 清空状态并恢复默认参数（测试/运维重置用）；
- Spring 场景下 `conduit.cluster.breaker.*` 在创建 `RpcClient` 时自动应用。

---

## 7. 测试与观测

| 关注点 | 用例 |
| --- | --- |
| 三种负载均衡的分流/权重/粘性 | `LoadBalancerTest` |
| 四种容错语义、重试预算与退避 | `ClusterTest`、`ClusterRetryConfigTest` |
| 熔断三态与"半开只放一个探测" | `CircuitBreakerTest`、`CircuitBreakerRegistryTest` |
| 多节点真实网络下的分流/下线/容错/熔断 | `conduit-it`：`MultiNodeClusterIntegrationTest` |
| 熔断状态观测 | `CircuitBreakerRegistry.state(node)`；`MetricsFilter` 记录调用成功率 |

设计取舍：节点级熔断而不是接口级——一个坏节点不该拖垮该接口的全部流量；重试与熔断都在 `AbstractCluster`
一处收口，四种策略只改"失败后的动作"，不重复实现选点与熔断闸门。
