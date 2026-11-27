# Conduit 可观测性（Observability）

Conduit 的可观测性完全落在 **Filter 链**上，无需业务代码改动：

| 能力 | 实现 | 默认 | 开关（属性名） |
| --- | --- | --- | --- |
| 链路追踪（traceId） | `TraceFilter` | 开 | `conduit.filter.trace.enabled` |
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

## 1. 链路追踪（traceId）

### 行为

- **消费侧**：`TraceFilter` 检查请求是否已带 `traceId`（来自 `RpcContext`），没有则生成一个
  32 位十六进制 id 写入 `RpcRequest.attachments`，随请求跨进程透传；
- **提供侧**：从请求里取出 `traceId`，写入当前线程 MDC（key=`traceId`），
  `RpcRequestHandler` 还会把它还原到 `RpcContext`，业务方法可直接读取；
- **日志串联**：logback pattern 加 `%X{traceId}` 即可让同一调用的客户端/服务端日志串起来；
- 调用结束 `TraceFilter` 恢复 MDC、`RpcInvocationHandler`/`RpcRequestHandler` 清理 `RpcContext`，
  不会泄漏到同线程的下一次调用。

### 主动指定 traceId

```java
RpcContext.getContext().setTraceId("order-20261123-0001");
orderService.create(request);
```

未主动设置时由过滤器自动生成；每次调用独立。

### 读取

```java
// 提供侧业务方法内
String traceId = RpcContext.getContext().getTraceId();
// 或任意线程的日志上下文
String fromMdc = MDC.get("traceId");
```

### 已知限制

异步 provider 方法的 continuation 运行在其它线程，MDC/ThreadLocal 不会自动传播；
需要时从 `RpcContext` 快照手动拷贝（与 `attachments` 的既有异步限制一致）。

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
