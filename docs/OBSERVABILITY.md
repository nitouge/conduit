# Conduit 可观测性（Observability）

Conduit 的可观测性完全落在 **Filter 链**上，无需业务代码改动：

| 能力 | 实现 | 默认 | 开关（属性名） |
| --- | --- | --- | --- |
| 链路追踪（traceId/spanId） | `TraceFilter` | 开 | `conduit.filter.trace.enabled`；采样率 `conduit.trace.sample-rate`（默认 1.0） |
| 指标采集（QPS/RT/错误率/延迟分布） | `MetricsFilter` | 关 | `conduit.filter.metrics.enabled` |
| 结构化 JSON 访问日志 | `StructuredLogFilter` | 关 | `conduit.filter.structuredlog.enabled` |
| 文本访问日志 | `AccessLogFilter` | 开 | `conduit.filter.accesslog.enabled` |
| 提供侧并发隔离 | `ActiveLimitFilter` | 关 | `conduit.filter.activelimit.enabled`（`...permits` 设上限） |
| 提供侧 QPS 限流（令牌桶） | `RateLimitFilter` | 关 | `conduit.filter.ratelimit.enabled`（`...qps` / `...global-qps`） |
| 幂等键生成（消费侧） | `IdempotencyFilter` | 开 | `conduit.filter.idempotency.enabled` |
| 幂等去重（提供侧） | `IdempotentFilter` | 开 | `conduit.filter.idempotent.enabled`（`...ttl-ms` / `...max-entries`） |

---

## 0. 开关怎么配（三种方式，任选其一）

| 方式 | 写法 | 适用 |
| --- | --- | --- |
| JVM 系统属性 | `-Dconduit.filter.metrics.enabled=true` | 任何 Java 进程 |
| Spring 配置 | `application.yml` 里 `conduit.filter.metrics.enabled: true`，或程序参数 `--conduit.filter.metrics.enabled=true` | Spring Boot 应用（starter） |
| 环境变量 | `CONDUIT_FILTER_METRICS_ENABLED=true` | 容器/CI，不改启动命令 |

优先级：**系统属性 > Spring 配置 > 环境变量**；都没配则用 filter 自身默认（上表"默认"列）。
解析统一由 `ConduitConfig` 完成，Spring 环境下 `Environment` 由 `ConduitAutoConfiguration` 自动登记。

开关在**首次装载 filter 时**读取（启动早期），运行期改配置不生效，需要重启进程。

---

## 1. 链路追踪（traceId / spanId / 采样）

### 字段与模型

| 字段（attachment + MDC 同名） | 含义 |
| --- | --- |
| `traceId` | 一条调用链的唯一标识（32 位十六进制），跨进程、跨服务不变 |
| `spanId` | 本跳 span（16 位十六进制），每经过一次远程调用换新 |
| `parentSpanId` | 上游 span：发起这次调用的那个 span |
| `conduit.trace.sampled` | 采样结论 `"1"`/`"0"`，根调用决定一次后沿链透传 |

```text
服务A 处理请求(span=S0) ──调用──▶ 服务B 处理该调用(span=S1, parent=S0) ──调用──▶ 服务C(span=S2, parent=S1)
```

### 行为

- **消费侧**（`TraceFilter`）：保证请求带 traceId；每次发起调用新建 `spanId`，
  `parentSpanId` 取「本线程当前正在服务的 span」——所以服务内部再调下游时，父子关系自然延伸；
  调用期间把三元组绑定到 MDC，结束后恢复；
- **提供侧**（`RpcRequestHandler` 请求边界）：把 attachments 还原到 `RpcContext`，把 trace 三元组
  绑定到 MDC，因此业务方法与**提供侧 Filter** 都能读到链路上下文；
- **日志串联**：logback pattern 加 `%X{traceId}`（或 `%X{spanId}`）即可；文本访问日志与
  JSON 访问日志都已带上 `spanId`/`parentSpanId`；
- **采样**：`conduit.trace.sample-rate`（默认 `1.0` 全采）。根调用按 traceId 做**确定性哈希**采样，
  结论写进 `sampled` 沿链透传——同一 traceId 在任何服务上结论一致，不会出现"上游采、下游不采"的
  半截链路。采样只影响 span 日志/上报，traceId 始终透传。

### 主动指定 traceId

```java
RpcContext.getContext().setTraceId("order-20261123-0001");
orderService.create(request);
```

未主动设置时由过滤器自动生成；每次独立调用一条新链路。

### 读取

```java
RpcContext ctx = RpcContext.getContext();
String traceId = ctx.getTraceId();                                    // 便捷方法
String spanId = ctx.getAttachment(RpcAttachments.SPAN_ID);            // 本跳
String parent = ctx.getAttachment(RpcAttachments.PARENT_SPAN_ID);     // 上游
// 或任意线程的日志上下文
String fromMdc = MDC.get("spanId");
```

### 异步线程的上下文传播

`RpcContext` 是 `ThreadLocal`，跨线程要显式传播：

```java
ContextSnapshot snapshot = ContextSnapshot.capture();     // 在发起线程捕获（含traceId/spanId与MDC）
executor.execute(snapshot.wrap(() -> {
    // 这里能读到 traceId/spanId，下游调用也接着同一条链路
    orderService.create(...);
}));
```

- **消费侧异步调用**（方法返回 `CompletableFuture`）：框架已在发起线程的上下文快照里完成结果
  future，调用方随后的 `thenApply/thenAccept` 仍能看到 traceId/spanId；
- **provider 侧异步**：业务方法返回 future 后，continuation 跑在业务自己的线程上，框架不替你
  决定线程池语义——需要时用 `ContextSnapshot` 显式包一层。

---

## 2. 指标采集

开启：`conduit.filter.metrics.enabled=true`（三种配置方式见 [§0](#0-开关怎么配三种方式任选其一)）。指标按 `interface#method` 分桶：

```java
for (Map.Entry<String, MetricsFilter.Stats> e : MetricsFilter.snapshot().entrySet()) {
    MetricsFilter.Stats s = e.getValue();
    System.out.printf("%s total=%d success=%d error=%.2f%% avg=%.2fms min=%.2fms max=%.2fms p95~%.0fms%n",
        e.getKey(), s.total(), s.success(), s.errorRate() * 100,
        s.avgRtMs(), s.minRtMs(), s.maxRtMs(), s.estimatedPercentileMs(0.95));
}
```

- 延迟分布：固定桶 `1/5/10/50/100/500/1000ms` + `>1000ms`，`bucketCounts()` 可取原始桶；
  分位是桶上界的估算，不引入直方图库；
- 全局汇总：`MetricsFilter.totalCalls()` / `totalErrors()`；
- 重置：`MetricsFilter.reset()`；
- 无外部依赖（不耦合 Micrometer/Prometheus），导出器（Prometheus/OTLP）作为后续能力。

传输层已有可直接读取的计数器：`NettyClient#getHeartbeatsSent/getHeartbeatsReceived/
getResponsesReceived/totalActiveConnections`、`NettyServer#getHeartbeatsReceived/
getActiveConnectionCount`；熔断状态见 `CircuitBreakerRegistry.state(node)`。

---

## 3. 结构化日志

开启：`conduit.filter.structuredlog.enabled=true`（建议同时关闭文本版 accesslog，
避免重复日志：`conduit.filter.accesslog.enabled=false`）。

logger：`CONDUIT.ACCESS.JSON`，每次调用一行 JSON：

```json
{"timestamp":1763860000000,"traceId":"1d37611eb95974d8c32f052bb687dc2",
 "interface":"com.example.OrderService","method":"create","durationMs":3,"success":true}
```

失败时追加 `errorType` / `errorMessage`。输出为自拼接 JSON（字段转义、无额外依赖），
可直接被 Filebeat/Fluent Bit 等采集。

---

## 4. 配置示例

Spring Boot 应用写进 `application.yml`：

```yaml
conduit:
  filter:
    metrics:
      enabled: true          # 开启指标
    structuredlog:
      enabled: true          # 开启JSON访问日志
    accesslog:
      enabled: false         # 关闭文本访问日志，避免重复
```

非 Spring（或不想改配置文件）用 JVM 参数：

```bash
java -Dconduit.filter.metrics.enabled=true \
     -Dconduit.filter.structuredlog.enabled=true \
     -Dconduit.filter.accesslog.enabled=false \
     -jar app.jar
```

logback pattern 建议：

```xml
<pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%X{traceId}] %logger{36} - %msg%n</pattern>
```

> 指标目前只在进程内累计，用 `MetricsFilter.snapshot()` 程序化读取（见 §2）；
> 需要定期打点时，自己起个定时任务读快照并输出到日志/监控即可。
