# 测试体系（Testing）

Conduit 的测试按"四层 + 三阶段"组织：四层是**跑在哪**，三阶段是**证明什么**。

| 层 | 位置 | 跑什么 | 何时跑 |
| --- | --- | --- | --- |
| 单元测试 | 各模块 `src/test` | 单个类/算法的行为 | 每次提交（`mvn test`） |
| testkit | `conduit-testkit` | 真实服务端/客户端夹具、注册中心替身、TLS证书、条件等待、全局状态复位、线性一致性检测器 | 被下层复用 |
| 集成测试 | `conduit-it` | 真实网络、多节点、跨进程、TLS、集群协作、Chaos | 每次提交（本机可跑，无外部依赖） |
| 性能基准 | `conduit-benchmark` | JMH 吞吐/延迟分布/序列化/编解码 | 手动（见 [BENCHMARK.md](BENCHMARK.md)） |

---

## 0. 三阶段目标与覆盖

### 阶段一：基本正确性（单进程内）

| 关注点 | 用例 |
| --- | --- |
| 协议编解码 | `conduit-protocol`：`RpcCodecTest`（请求/响应/心跳/握手往返、gzip 阈值、非法 magic/版本/codec/length、解码失败回错误响应）、`CompressorTest`、`ProtocolOptionsTest` |
| 序列化 | `conduit-serialization`：`SerializerSpiTest`（四种实现 SPI 装载、JSON 无 default typing、参数类型转型）、`SerializerEdgeCaseTest`（空参数/null 实参/数组/attachments/64KB 大报文） |
| 请求响应匹配 | `conduit-remoting`：`PendingRequestsTest`（唯一 id、恰好一次完成、未知 id 忽略、异常完成、并发完成只有一个赢家） |
| 超时 | `FixesTest.timeoutCleansPendingRequests`、`ChaosIntegrationTest.timeoutLeavesNoPendingRequests` |
| 连接管理 | `conduit-remoting`：`ConnectionPoolTest`、`ConnectionPoolIntegrationTest`（多连接/空闲回收/单飞建链） |
| SPI 扩展 | `conduit-common`：`ExtensionLoaderTest`、`AdaptiveExtensionTest`、`LifecycleExtensionTest` |

### 阶段二：分布式正确性（多进程/多节点）

| 关注点 | 用例 |
| --- | --- |
| 多节点负载均衡/下线/容错/熔断（同 JVM，真实Netty+注册中心推送） | `conduit-it`：`MultiNodeClusterIntegrationTest` |
| 跨进程直连（provider 在独立 JVM） | `conduit-it`：`CrossProcessJvmIntegrationTest` |
| **真实注册中心 + 多个真实 JVM**（注册发现/负载均衡/优雅下线/重启恢复） | `conduit-it`：`MultiJvmRegistryIntegrationTest`（需 Nacos，默认不跑，见下） |
| TLS 端到端 | `conduit-it`：`TlsEndToEndIntegrationTest` |

### 阶段三：工程质量

| 关注点 | 用例/文档 |
| --- | --- |
| 性能基线 | `conduit-benchmark` + [BENCHMARK.md](BENCHMARK.md) |
| 线性一致性 | `testkit.linearizability.LinearizabilityChecker` + `InMemoryRegistryLinearizabilityTest` |
| Chaos | `conduit-it`：`ChaosIntegrationTest`（进程崩溃/服务变慢/重启自愈） |

---

## 1. `conduit-testkit`：测试夹具

一个普通模块（不是 test-jar），测试代码直接依赖：

```xml
<dependency>
    <groupId>io.github.nitouge</groupId>
    <artifactId>conduit-testkit</artifactId>
    <scope>test</scope>
</dependency>
```

| 类型 | 说明 |
| --- | --- |
| `TestServer` | 起真实 `RpcServer`：`TestServer.start(iface, impl)`（随机端口）、`TestServer.of(registry)`（带注册）、`withOptions(ServerOptions)`（TLS等）、`onPort(port)`（重启场景）；`AutoCloseable` |
| `TestRpc` | 客户端夹具：`TestRpc.create("kryo")` + `proxy(...)` 多种重载（直连/注册中心/自定义容错与负载均衡）；`AutoCloseable` |
| `EchoService` / `EchoServiceImpl` | 共享测试契约：同步/异步/慢调用/异常/节点标识（`nodeId` 用于验证分流） |
| `TestRegistries` | 注册中心替身：`fixed(...)`（含死节点场景）、`empty()`、`mutable()`（可注册/推送）、`recording()`（记录注册轨迹） |
| `TlsTestCerts` | 测试期用 `keytool` 生成自签证书（SAN 含 127.0.0.1），给出服务端/客户端两份 `SslContext` |
| `Await` | 条件等待：`Await.until("描述", 超时, 条件)`，替代 `Thread.sleep` |
| `Ports` | `freePort()` / `connectable(port)` |
| `TestReset` | 复位进程级静态状态：`FilterLoader` 缓存、`ConduitConfig` 配置源、熔断器、指标累计 |
| `linearizability` | 线性一致性检测器（见 §3） |

约定：

- 测试**不要写死端口**，一律 `TestServer.random()`/`onPort(Ports.freePort())`；
- 断言"最终一致"用 `Await.until`，不要 `sleep`；
- 涉及熔断器/filter 开关/指标累计的用例，`@BeforeEach` 调 `TestReset.all()`。

---

## 2. `conduit-it`：集成测试

### 2.1 默认回归（无外部依赖，`mvn test` 会跑）

| 用例 | 验证点 |
| --- | --- |
| `MultiNodeClusterIntegrationTest` | 3 个真实节点 roundrobin 分流；节点下线后不再被选中；候选含死节点时 FailOver 切换；连续失败后熔断打开并快速失败 |
| `TlsEndToEndIntegrationTest` | 真实 TCP + TLS 握手 + 证书主机名校验；明文客户端打 TLS 服务端快速失败 |
| `CrossProcessJvmIntegrationTest` | provider 在**独立 JVM**，consumer 通过真实网络调用；节点标识带子进程 pid |
| `ChaosIntegrationTest` | 见 §4 |
| `InMemoryRegistryLinearizabilityTest` | 见 §3 |

### 2.2 分布式测试（需 Nacos，默认跳过）

`MultiJvmRegistryIntegrationTest` 会起 **5 个真实 JVM**（2 个 provider + 2 轮 consumer + 测试进程），
完整验证：注册发现 → roundrobin 跨进程分流 → 节点优雅下线后流量收敛 → 节点重启后恢复。
耗时约 80s，因此默认不跑，显式开启：

```bash
  mvn -pl conduit-it -am test -Dit.distributed=true
```

前提：本机 `127.0.0.1:8848` 有可用 Nacos；不可用时用例自动跳过（Assumptions）。

> 跨进程用例依赖 `conduit-it/pom.xml` 里的 surefire 配置 `<useManifestOnlyJar>false</useManifestOnlyJar>`，
> 否则 `java.class.path` 只有一个引导 jar，子 JVM 找不到测试类。

---

## 3. 线性一致性检测

`io.github.nitouge.conduit.testkit.linearizability` 提供了**寄存器模型**的线性一致性检测器
（Wing &amp; Gong 算法 + 状态记忆化）：

```java
List<HistoryOperation> history = List.of(
        HistoryOperation.write("t1", Boolean.TRUE, start, end),
        HistoryOperation.read("t2", Boolean.TRUE, start, end));
boolean ok = LinearizabilityChecker.isLinearizable(history, Boolean.FALSE); // 指定初值
```

判定标准：存在一个全局顺序，让每个读都返回"它之前最近一次写"的值，且真实时间上先结束的操作排在前面。

适用边界：

- 只支持 **null / Boolean** 的读写寄存器（如"某实例是否已注册"、"某开关是否打开"）；
- 操作数上限 62（位掩码 + 记忆化）；超出请缩小并发规模；
- 队列、锁、带超时的状态机不适用，需要各自的模型。

当前落地：`InMemoryRegistryLinearizabilityTest` 把 `InMemoryRegistry` 的
register(写true)/unregister(写false)/discover(读) 归约成布尔寄存器，4 线程随机混合操作后校验历史。
这类检测能抓到"注册后立刻发现不到""注销后仍被读到"等普通断言难以覆盖的竞态。

---

## 4. Chaos 测试（基础场景）

`ChaosIntegrationTest` 覆盖三类最常见故障：

| 场景 | 注入方式 | 断言 |
| --- | --- | --- |
| 进程崩溃 | provider 跑在独立 JVM，慢调用在途时 `destroyForcibly` | 在途请求 <2s 内失败（不等满超时）、`PendingRequests.pendingCount()==0` |
| 服务变慢 | provider `slow(2000)`，客户端超时 300ms | 调用抛超时异常、无 pending 残留 |
| 服务重启 | 同端口重建 `TestServer` | 客户端重连并在 20s 内恢复调用 |

> 注意区分：**优雅停机**允许在途请求完成（这是设计行为），只有真崩溃才会立即断链；
> 想验证前者见 `TestServer.close()`，后者见本用例。

后续可扩展：网络分区（丢包/延迟注入）、注册中心抖动、节点频繁上下线、磁盘/线程池耗尽。

---

## 5. 推荐写法

```java
class MyClusterTest {

    @BeforeEach
    void reset() {
        TestReset.all();
    }

    @Test
    void failoverSwitchesToLiveNode() {
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        try (TestServer n1 = TestServer.of(registry).export(EchoService.class, new EchoServiceImpl("n1")).start();
             TestServer n2 = TestServer.of(registry).export(EchoService.class, new EchoServiceImpl("n2")).start();
             TestRpc rpc = TestRpc.create("kryo")) {

            EchoService echo = rpc.proxy(EchoService.class, registry, 3_000, "failover", "roundrobin");
            assertEquals("ok:hi", echo.echo("hi"));

            n2.close();
            Await.until("注册中心应只剩1个节点", () -> registry.discover(EchoService.class.getName()).size() == 1);
            assertEquals("n1", echo.nodeId());
        }
    }
}
```

---

## 6. 回归口径

| 命令 | 覆盖 |
| --- | --- |
| `mvn test` | 单元 + testkit 自检 + 集成测试（不含分布式多JVM） |
| `mvn -pl conduit-it -am test -Dit.distributed=true` | 追加多JVM + 真实 Nacos 分布式测试 |
| `mvn -pl conduit-benchmark -am -DskipTests package` 后 `java -jar .../benchmarks.jar` | 性能基线（见 BENCHMARK.md） |

外部依赖（Nacos）不可用时相关用例自动跳过，不算失败；**发布前**应在有 Nacos 的环境跑一遍分布式用例。

### 只跑某几个用例

最省事：用仓内脚本（自动补 `-pl/-am/-Dtest` 和两个 surefire 开关，并用 JDK 17 执行）：

```bash
scripts\test-one.cmd conduit-serialization SerializerSpiTest
scripts\test-one.cmd conduit-serialization "SerializerSpiTest,SerializerEdgeCaseTest"   # 多个类
scripts\test-one.cmd conduit-it MultiNodeClusterIntegrationTest
```

手工写命令时，`-Dtest` 会应用到 **reactor 里所有模块**，而 `-am` 会把上游模块（api/common…）也拉进来，
它们没有匹配的测试类，surefire 默认会报
`No tests matching pattern "xxx" were executed`。加上两个开关即可让上游模块"没匹配就跳过"：

```bash
mvn -pl conduit-serialization -am test -Dtest=SerializerEdgeCaseTest \
    -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false
```

其它常用形态：

```bash
# 多个类（PowerShell 下整体加引号）
mvn -pl conduit-serialization -am test -Dtest='SerializerEdgeCaseTest,SerializerSpiTest' \
    -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false

# 已 install 过依赖时，可去掉 -am（更快，但要确认本地仓库的 SNAPSHOT 是最新的）
mvn -pl conduit-serialization test -Dtest=SerializerEdgeCaseTest

# 整个模块的测试（不带 -Dtest，无需额外开关）
mvn -pl conduit-serialization -am test
```

> 不要写 `mvn -pl xxx -am test -Dtest=Foo` 又不带上面两个开关——会在第一个"没匹配到"的上游模块直接失败。
> 项目没有在 `pom.xml` 里默认关闭 `failIfNoSpecifiedTests`：那样虽然省事，但测试类名拼错时也会"静默通过"
> （0 个用例），反而更危险。

