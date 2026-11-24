# Conduit 集群容错（Cluster）

`conduit-cluster` 回答三个问题：选哪个节点（`LoadBalancer`）、失败怎么办（`Cluster`）、
不健康节点要不要先不打（`CircuitBreaker`）。本文件说明 v0.2 新增的**可配置重试/退避**与
**熔断器参数配置**。

---

## 1. 调用链位置

```
RpcInvocationHandler
  └─ Cluster（自适应代理，默认来自 @RpcReference / conduit.cluster.*）
       ├─ LoadBalancer.select(...)        # 自适应代理，支持单次覆盖
       └─ AbstractCluster.callWithBreaker # 节点级熔断闸门
            └─ RemoteInvoker             # 真正的远程调用
```

只有**传输层异常**（future 异常完成）才触发容错；业务异常包在正常完成的 `RpcResponse` 里返回。

---

## 2. 配置方式

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

---

## 3. FailOver 行为

- 总尝试次数 `retries + 1`；每次从**未试过**的候选里重选，避免重复打同一坏节点；
- 熔断打开的节点**零成本跳过**，不消耗重试预算、也不退避；
- 真实传输失败后可按 `retry-backoff-ms` 异步退避再试（不阻塞线程）；
- 最坏总耗时 ≈ `(retries + 1) × timeout + retries × backoff`，上游整体超时要覆盖它；
- 仅适合幂等操作；非幂等写操作请用 `failfast`。

---

## 4. 单次调用覆盖策略

`Cluster` / `LoadBalancer` 通过自适应扩展代理加载，默认名取注解/配置，单次调用可用
`RpcContext` 覆盖：

```java
RpcContext.getContext().setAttachment("cluster", "failfast");     // 本次调用不重试
RpcContext.getContext().setAttachment("loadbalance", "roundrobin");
orderService.query(...); // attachments 会随请求透传，并用于本次策略选择
```

---

## 5. 熔断器机制

三态：`CLOSED` →（连续失败达阈值）→ `OPEN` →（openDuration 后）→ `HALF_OPEN`，
只放一个探测：成功恢复 `CLOSED`，失败重新 `OPEN`。

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
- Spring 场景下，`conduit.cluster.breaker.*` 在创建 `RpcClient` 时自动应用。

---

## 6. 策略选择建议

| 策略 | 失败行为 | 适用 |
| --- | --- | --- |
| `failover`（默认） | 换节点重试到预算用尽 | 幂等读 |
| `failfast` | 立即失败 | 非幂等写 |
| `failsafe` | 忽略、返回空 | 埋点/审计等可丢调用 |
| `failback` | 先返回空、后台重试 | 最终一致的通知类操作 |
