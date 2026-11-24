# Conduit 使用手册

一份从零设计并实现的 Java RPC 框架使用手册。本手册覆盖：如何引入、三种用法（Spring Boot / 编程式+注册中心 / 编程式直连）、
全部配置项、序列化与负载均衡与容错与异步的用法、示例工程运行方式以及常见问题。

- 目录
  - [1. 环境要求](#1-环境要求)
  - [2. 如何引入坐标](#2-如何引入坐标)
  - [3. 用法一：Spring Boot（推荐）](#3-用法一spring-boot推荐)
  - [4. 用法二：编程式 + 注册中心](#4-用法二编程式--注册中心)
  - [5. 用法三：编程式直连（无注册中心）](#5-用法三编程式直连无注册中心)
  - [6. 配置项参考](#6-配置项参考)
  - [7. 序列化切换](#7-序列化切换)
  - [8. 负载均衡 / 集群容错 / 熔断](#8-负载均衡--集群容错--熔断)
  - [9. 异步调用](#9-异步调用)
  - [10. 注册中心切换](#10-注册中心切换)
  - [11. 优雅停机](#11-优雅停机)
  - [12. 常见问题与注意事项](#12-常见问题与注意事项)
  - [13. 示例工程](#13-示例工程)
  - [14. 模块/依赖速查](#14-模块依赖速查)

---

## 1. 环境要求

- **JDK 17**（必须；Spring Boot 3.x 要求）。
- **Maven 3.8+**。
- **注册中心（可选）**：用 Nacos 模式需一个运行中的 **Nacos 2.x**（`127.0.0.1:8848`）；做测试/同进程可用内置 `local` 注册中心，无需任何外部组件。

---

## 2. 如何引入坐标

> Conduit 目前是本地多模块工程、版本 `1.0.0-SNAPSHOT`，**尚未发布到 Maven 中央仓库**。
> 要在别的工程按坐标引用，先把它装进本地仓库：

```bash
# 在 Conduit 项目根目录（用 JDK 17）
set JAVA_HOME=D:\path\to\jdk-17      # Windows；Linux/Mac: export JAVA_HOME=...
mvn clean install -DskipTests        # 安装全部模块到本地 ~/.m2
```

之后在你的工程里按需引入坐标（`groupId=io.github.nitouge`，`version=1.0.0-SNAPSHOT`）：

```xml
<!-- Spring Boot 应用：一个 starter 全搞定（自动装配） -->
<dependency>
    <groupId>io.github.nitouge</groupId>
    <artifactId>conduit-spring-boot-starter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

非 Spring 工程（编程式用法）：provider 引 `conduit-server`、consumer 引 `conduit-client`（都已传递带上注册中心与序列化实现）。

---

## 3. 用法一：Spring Boot（推荐）

最省事：引 starter，服务端加 `@RpcService`，消费端字段加 `@RpcReference`，配置写在 `application.yml`。**无需 `@EnableConduit`**——starter 会自动装配（`conduit.enabled=false` 可关闭）。

通常是三个工程：**共享 API**、**提供者**、**消费者**。

### 3.1 共享 API（普通 jar，不依赖 Conduit）

```java
package io.github.nitouge.conduit.starter.demo.api;

public interface GreetService {
    String greet(String name);
}
```

### 3.2 提供者应用

`pom.xml` 依赖 `conduit-spring-boot-starter` + 上面的 API。

```java
package io.github.nitouge.conduit.starter.demo.provider;

import io.github.nitouge.conduit.starter.demo.api.GreetService;
import io.github.nitouge.conduit.api.RpcService;
import org.springframework.stereotype.Component;

@Component          // 必须是 Spring Bean（starter 用 getBeansWithAnnotation 发现）
@RpcService         // 标记为要暴露的 RPC 服务（按其实现的业务接口注册）
public class GreetServiceImpl implements GreetService {
    @Override
    public String greet(String name) {
        return "Hello, " + name + " from Conduit!";
    }
}
```

```java
@org.springframework.boot.autoconfigure.SpringBootApplication
public class ProviderApplication {
    public static void main(String[] args) {
        org.springframework.boot.SpringApplication.run(ProviderApplication.class, args);
    }
}
```

`application.yml`（提供者）：

```yaml
conduit:
  application:
    name: greet-provider
  registry:
    type: nacos                 # nacos / local
    address: 127.0.0.1:8848
  protocol:
    host: 127.0.0.1
    port: 20880                 # 本服务端监听端口
```

启动后：`GreetServiceImpl` 被自动暴露、注册到 Nacos，Netty 监听 20880。

### 3.3 消费者应用

```java
package io.github.nitouge.conduit.starter.demo.consumer;

import io.github.nitouge.conduit.api.annotation.RpcReference;
import io.github.nitouge.conduit.api.enums.ClusterPolicyEnum;
import io.github.nitouge.conduit.api.enums.LoadbalancePolicyEnum;
import io.github.nitouge.conduit.starter.demo.api.GreetService;
import org.springframework.stereotype.Component;

@Component
public class GreetConsumer {

  @RpcReference(
          version = "1.0.0",
          group = "",
          timeout = 1000,
          cluster = ClusterPolicyEnum.FAIL_OVER,
          loadbalance = LoadbalancePolicyEnum.RANDOM
  )
  private GreetService greetService;

  public String greet(String name) {
    return greetService.greet(name);
  }
}
```

```java
package io.github.nitouge.conduit.starter.demo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GreetController {

  private static final Logger log = LoggerFactory.getLogger(GreetController.class);

  private final GreetConsumer consumer;

  public GreetController(GreetConsumer consumer) {
    this.consumer = consumer;
  }

  @GetMapping("/greet")
  public String greet(@RequestParam(name = "name", defaultValue = "World") String name) {
    long pid = ProcessHandle.current().pid();
    log.info("收到HTTP /greet?name={}，发起远程RPC ...（本consumer进程 pid={}）", name, pid);
    String remote = consumer.greet(name);
    return String.format("[consumer进程pid=%d] 远程返回 → %s%n", pid, remote);
  }
}
```

`application.yml`（消费者）：

```yaml
conduit:
  application:
    name: greet-consumer
  registry:
    type: nacos
    address: 127.0.0.1:8848
  protocol:
    serialization: kryo         # 出站序列化：jdk / json / kryo / protostuff
```

消费者通常不暴露服务，可不配 `protocol.port`。`serialization` 决定**本消费者发出的请求**用哪种序列化（服务端按请求里的标识自动用同一种回包）。

> **非 Spring 的普通 Spring 应用**（没有 Spring Boot 自动装配）：在配置类上加 `@io.github.nitouge.conduit.spring.EnableConduit` 显式开启即可，其余一致。

---

## 4. 用法二：编程式 + 注册中心

不依赖 Spring，直接用 API。provider 引 `conduit-server`、consumer 引 `conduit-client`。

**提供者：**

```java
import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.server.RpcServer;

Registry registry = Registries.getRegistry("nacos", "127.0.0.1:8848"); // 或 ("local", "x")
RpcServer server = new RpcServer("127.0.0.1", 20880, registry);
server.export(HelloService.class, new HelloServiceImpl());        // 暴露 + 注册
server.start();
// 优雅停机：Runtime.getRuntime().addShutdownHook(new Thread(server::close));
```

**消费者：**

```java
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;

Registry registry = Registries.getRegistry("nacos", "127.0.0.1:8848");
RpcClient client = new RpcClient("kryo");   // 出站序列化；无参 = jdk
HelloService svc = RpcClientProxy.create(
        HelloService.class, registry, client, 2000, "failover", "random"); // 接口, 注册中心, client, 超时ms, 容错, 负载均衡
System.out.println(svc.greet("Conduit"));
client.close();
```

---

## 5. 用法三：编程式直连（无注册中心）

适合点对点 / 调试。消费者直接给定 provider 地址（`ServiceMeta`）。

```java
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.server.RpcServer;

// 提供者（无 registry）
RpcServer server = new RpcServer("127.0.0.1", 20880);
server.export(HelloService.class, new HelloServiceImpl());
server.start();

// 消费者：直连固定地址
RpcClient client = new RpcClient();
ServiceMeta target = new ServiceMeta(HelloService.class.getName(), "127.0.0.1", 20880);
HelloService svc = RpcClientProxy.create(HelloService.class, target, client, 3000);
System.out.println(svc.greet("Conduit"));
client.close();
```

---

## 6. 配置项参考

`application.yml` 中 `conduit.*`（Spring Boot 用法）。编程式用法对应到构造参数。

| 配置项 | 默认 | 说明 |
|---|---|---|
| `conduit.enabled` | `true` | 设 `false` 关闭整个自动装配 |
| `conduit.application.name` | `conduit-app` | 应用名 |
| `conduit.registry.type` | `nacos` | 注册中心类型：`nacos` / `local` / `zookeeper` / `etcd` |
| `conduit.registry.address` | `127.0.0.1:8848` | 注册中心地址 |
| `conduit.protocol.host` | `127.0.0.1` | 服务端绑定主机 |
| `conduit.protocol.port` | `20880` | 服务端监听端口（消费者可不配） |
| `conduit.protocol.serialization` | `jdk` | 出站序列化：`jdk`/`json`/`kryo`/`protostuff` |

传输层（握手 / 压缩 / 心跳 / 连接池 / 重连 / TLS）：

| 配置项 | 默认 | 说明 |
|---|---|---|
| `conduit.protocol.compress` | `none` | 压缩算法：`none`/`gzip`（达到阈值才真正压缩） |
| `conduit.protocol.compress-threshold` | `1024` | 压缩阈值（字节），`0` 表示无条件压缩 |
| `conduit.transport.connect-timeout-ms` | `3000` | TCP 建链超时 |
| `conduit.transport.handshake-timeout-ms` | `3000` | 协议握手超时 |
| `conduit.transport.heartbeat-interval-ms` | `30000` | 心跳间隔，`0` 关闭心跳 |
| `conduit.transport.read-idle-timeout-ms` | `90000` | 客户端读空闲探活（须大于心跳间隔），`0` 关闭 |
| `conduit.transport.max-connections-per-address` | `1` | 每地址连接数上限（连接池容量） |
| `conduit.transport.idle-connection-timeout-ms` | `600000` | 闲置连接回收时长 |
| `conduit.transport.reconnect.enabled` | `true` | 断线是否后台自动重连 |
| `conduit.transport.reconnect.max-attempts` | `5` | 重连最大尝试次数 |
| `conduit.transport.reconnect.base-delay-ms` | `1000` | 首次退避时长（后按指数增长） |
| `conduit.transport.reconnect.max-delay-ms` | `30000` | 最大退避时长 |
| `conduit.transport.server.business-threads` | `200` | 服务端业务线程数 |
| `conduit.transport.server.queue-capacity` | `10000` | 服务端业务队列容量 |
| `conduit.transport.server.reader-idle-timeout-ms` | `60000` | 服务端读空闲清理 |
| `conduit.transport.server.shutdown-timeout-ms` | `10000` | 优雅停机等待时长 |
| `conduit.transport.tls.enabled` | `false` | 是否启用 TLS 链路加密 |
| `conduit.transport.tls.cert` / `key` | - | 服务端证书链/私钥（PEM，支持 `classpath:`） |
| `conduit.transport.tls.trust-cert` | - | 客户端信任的 CA/证书 PEM；留空用 JVM 默认信任库 |
| `conduit.transport.tls.insecure` | `false` | 客户端跳过证书校验（仅测试环境） |

> 传输层设计细节（握手时序、压缩策略、心跳语义、连接池与重连规则、TLS）见 [TRANSPORT.md](TRANSPORT.md)。

集群容错（`conduit.cluster.*`）：

| 配置项 | 默认 | 说明 |
|---|---|---|
| `conduit.cluster.retries` | `2` | failover 额外重试次数（总尝试 = retries + 1） |
| `conduit.cluster.retry-backoff-ms` | `0` | 两次重试之间的异步退避（毫秒） |
| `conduit.cluster.breaker.failure-threshold` | `5` | 节点连续失败多少次打开熔断 |
| `conduit.cluster.breaker.open-duration-ms` | `5000` | 打开后多久转半开探测 |

> 自适应扩展与生命周期见 [SPI.md](SPI.md)；集群容错与熔断细节见 [CLUSTER.md](CLUSTER.md)。

`@RpcReference` 字段级配置（覆盖默认）：

| 属性 | 默认 | 说明 |
|---|---|---|
| `timeout` | `3000` | 单次调用超时（ms） |
| `cluster` | `failover` | 容错：`failover`/`failfast`/`failsafe`/`failback` |
| `loadbalance` | `random` | 负载均衡：`random`/`roundrobin`/`consistenthash` |
| `retries` | `-1` | failover 额外重试次数；`-1` 表示用全局 `conduit.cluster.retries` |
| `retryBackoffMs` | `-1` | 重试退避（ms）；`-1` 表示用全局 `conduit.cluster.retry-backoff-ms` |
| `version` / `group` | `""` | 当前**声明但未参与路由**（serviceName 仅用接口名） |

`@RpcService`：`weight`（默认 100，用于带权负载均衡）、`version`/`group`（同上，暂未生效）。

---

## 7. 序列化切换

四种实现，经 SPI 加载，**换序列化只改配置、业务零改动**：

```yaml
conduit:
  protocol:
    serialization: kryo     # jdk(默认) / json / kryo(生产推荐) / protostuff
```

编程式：`new RpcClient("kryo")`。

- **jdk**：零依赖、最简，仅 Java、慢、体积大 —— 适合跑通/调试。
- **kryo**：快、体积小，**生产推荐**。
- **json**：可读、跨语言；已关闭 default typing（防反序列化 RCE），请求参数按 `paramTypes` 精确转型。**限制**：非 String 的返回值、异常对象在 JSON 下可能失真，这类场景用 jdk/kryo。
- **protostuff**：体积小、跨语言较好；最适合具体 POJO。

> 收发约定：消费者用哪种序列化由它的 `serialization` 决定，请求头带序列化标识，服务端按标识自动用同一种回包——两端无需手动对齐，但相应序列化实现都已随依赖打包可用。

---

## 8. 负载均衡 / 集群容错 / 熔断

- **负载均衡**（`loadbalance`）：`random`（加权随机，默认）、`roundrobin`（轮询）、`consistenthash`（相同参数稳定路由到同一节点）。
- **集群容错**（`cluster`）：
  - `failover`（默认）：失败自动重试其它节点，最多 `retries+1` 次（retries 可配，默认2）；避开已试节点，熔断节点零成本跳过。**仅适合幂等方法**。
  - `failfast`：失败立即抛，不重试。**写操作建议用它**。
  - `failsafe`：失败忽略、返回空（注意基本类型返回值见 BACKLOG H6）。
  - `failback`：失败先返回空 + 后台重试。
- **重试配置**：全局 `conduit.cluster.retries` / `retry-backoff-ms`，或字段级
  `@RpcReference(retries=..., retryBackoffMs=...)`（`-1` 表示沿用全局）。
- **单次覆盖**：调用前设置 `RpcContext.getContext().setAttachment("cluster", "failfast")`
  或 `"loadbalance"`，只影响本次调用（经 SPI 自适应代理生效）。
- **熔断**：自动按节点（host:port）熔断——连续失败达阈值打开、隔离一段时间后半开放一个探测、成功则恢复。坏节点会被 failover 自然避开；阈值/时长可配（见上表）。

示例：写操作用 failfast、读操作用 failover + roundrobin：

```java
@RpcReference(cluster = ClusterPolicyEnum.FAIL_FAST)                       // 写
private OrderService orderService;

@RpcReference(cluster = ClusterPolicyEnum.FAIL_FAST, loadbalance = LoadbalancePolicyEnum.ROUND_ROBIN)  // 读
private UserService userService;
```

---

## 9. 异步调用

把接口方法声明为返回 `CompletableFuture<T>`，调用方**不阻塞**：

```java
public interface GreetService {
    String greet(String name);                          // 同步
    java.util.concurrent.CompletableFuture<String> greetAsync(String name); // 异步
}
```

```java
CompletableFuture<String> f = greetService.greetAsync("Conduit");
f.thenAccept(System.out::println);   // 结果回来时回调，不阻塞当前线程
```

- **提供者侧也非阻塞**：服务实现可返回一个「稍后才完成」的 `CompletableFuture`（如 `supplyAsync`），服务端不会占住业务线程，响应在 future 完成时回写。
- 失败通过返回的 future 暴露（`exceptionally`/`handle`），不会在调用处同步抛。

---

## 10. 注册中心切换

```yaml
conduit:
  registry:
    type: nacos               # nacos / local / zookeeper / etcd
    address: 127.0.0.1:8848
```

- `nacos`：生产用，需 Nacos 2.x（默认）；由 `conduit-spring`/starter 默认带上。
- `local`：进程内注册中心，无需外部组件，适合测试 / 同 JVM（核心内置）。
- `zookeeper`：需额外引入 `conduit-registry-zookeeper`，地址如 `127.0.0.1:2181`。
- `etcd`：需额外引入 `conduit-registry-etcd`，地址如 `127.0.0.1:2379`。

```xml
<!-- 按需引入：核心 conduit-registry 只带 local，不含任何注册中心客户端 -->
<dependency>
  <groupId>io.github.nitouge</groupId>
  <artifactId>conduit-registry-nacos</artifactId>
</dependency>
<dependency>
  <groupId>io.github.nitouge</groupId>
  <artifactId>conduit-registry-zookeeper</artifactId>
</dependency>
<dependency>
  <groupId>io.github.nitouge</groupId>
  <artifactId>conduit-registry-etcd</artifactId>
</dependency>
```

> 只依赖 `conduit-client`/`conduit-server` 编程式使用时，默认的 nacos 需要显式引入
> `conduit-registry-nacos`；缺失时 `Registries` 会提示需要引入哪个模块。

注册中心宕机时，消费者会用**本地缓存**的最近一次发现结果兜底，仍可调用既有提供者。
存储模型与实现细节见 [REGISTRY.md](REGISTRY.md)。

---

## 11. 优雅停机

- **Spring**：容器关闭时自动从注册中心注销并关闭 Netty（`ServiceExporter` 负责）。
- **编程式**：自行 `server.close()`（先注销再关 Netty）、`client.close()`、必要时 `registry.destroy()`；建议挂 `addShutdownHook`。

---

## 12. 常见问题与注意事项

- **基于 JDK 17 构建/运行**。构建和运行环境需要使用 JDK 17 及以上版本。
- **未发布到中央仓库**：先 `mvn install` 到本地仓库，才能按坐标引用。
- **Nacos 版本**：依赖的是 `nacos-client 2.3.2`（gRPC 2.x 协议），服务端用 Nacos 2.x。
- **端口冲突**：`conduit.protocol.port` 默认 20880，多个 provider 在同机请用不同端口。
- **@RpcService 必须是 Spring Bean**：加 `@Component` 或以 `@Bean` 声明，否则不会被自动暴露。
- **@RpcReference 字段类型必须是接口**（动态代理要求），否则启动期报错。
- **序列化与安全**：jdk/json 反序列化网络字节存在反序列化攻击面，**当前定位为可信网络内使用**；对外暴露请加鉴权/TLS。
- **version/group 暂未参与路由**：多版本/分组隔离尚未生效。

---

## 13. 示例工程

示例工程位于 `conduit-example/`，该聚合模块设置了 `maven.deploy.skip=true`，只用于演示和验证，不参与发布。

### 13.1 `conduit-demo`：基础调用链示例

路径：`conduit-example/conduit-demo/`。该示例包含 `api`、`provider`、`consumer` 三组代码，覆盖：

- `ProviderMain` / `ConsumerMain`：编程式直连，不依赖注册中心；
- `NacosProviderMain` / `NacosConsumerMain`：通过 Nacos 注册发现；
- `DirectCallTest`、`NacosRegistryCallTest`：端到端测试；
- Spring 注解与异步、序列化、集群等能力的集成测试。

在项目根目录执行：

```bash
# 编译并运行该示例模块的测试
mvn -pl conduit-example/conduit-demo -am test
```

运行 Nacos 示例前，先启动 Nacos 2.x，并确保 `127.0.0.1:8848` 可访问；没有 Nacos 时，Nacos 端到端测试会自动跳过。

### 13.2 `conduit-starter-demo`：Spring Boot Starter 跨进程示例

路径：`conduit-example/conduit-starter-demo/`。该示例由三个子模块组成：

| 子模块 | 作用 | 端口 |
|---|---|---|
| `conduit-starter-demo-api` | provider 与 consumer 共享的 `GreetService` 接口 | - |
| `conduit-starter-demo-provider` | 只依赖 `conduit-spring-boot-starter`，自动暴露并注册服务 | RPC `20880` |
| `conduit-starter-demo-consumer` | 通过 `@RpcReference` 发现 provider，并提供 HTTP 接口 | HTTP `8080` |

provider 和 consumer 对 Conduit 的唯一直接依赖都是 `conduit-spring-boot-starter`，不需要显式添加 `@EnableConduit`。

1. 启动 Nacos：

```bash
docker run -d --name nacos -p 8848:8848 -p 9848:9848 -e MODE=standalone nacos/nacos-server:v2.3.2
```

Nacos 2.x 客户端使用 gRPC，必须同时映射 `9848` 端口。

2. 在项目根目录构建：

```bash
mvn -pl conduit-example/conduit-starter-demo -am package
```

3. 分别在两个终端启动 provider 和 consumer：

```bash
mvn -pl conduit-example/conduit-starter-demo/conduit-starter-demo-provider spring-boot:run
mvn -pl conduit-example/conduit-starter-demo/conduit-starter-demo-consumer spring-boot:run
```

4. 调用 consumer 的 HTTP 接口：

```bash
curl "http://127.0.0.1:8080/greet?name=Tom"
```

consumer 会经 Nacos 发现 provider，再通过 RPC 返回结果。停止 provider 后再次调用应失败，重新启动 provider 后恢复，以此验证这是跨进程远程调用。

预期输出（两个 pid 不同，即证明是跨进程调用，而不是同进程内的本地调用）：

```
$ curl "http://127.0.0.1:8080/greet?name=Tom"
[consumer进程pid=27852] 远程返回 → Hi, Tom! —— 来自provider进程(pid=38036, rpcPort=20880)
```

`spring-boot:run` 之外，也可以直接运行 fat jar（第 2 步的 `package` 已生成）：

```bash
java -jar conduit-example/conduit-starter-demo/conduit-starter-demo-provider/target/conduit-starter-demo-provider-1.0.0-SNAPSHOT.jar
java -jar conduit-example/conduit-starter-demo/conduit-starter-demo-consumer/target/conduit-starter-demo-consumer-1.0.0-SNAPSHOT.jar
```

一次成功的跨进程调用应在日志里留下这条链路的四个证据：

| 环节 | 日志证据 |
|---|---|
| 注册 | provider：`[REGISTER-SERVICE] ... GreetService with instance Instance{...port=20880...}` |
| 发现 | consumer：`[NacosRegistry] Nacos返回selectInstances ... 1个healthy实例`，随后 `[RandomLoadBalancer] ... -> 127.0.0.1:20880` |
| 握手协商 | 两端：`握手成功 ... Negotiated{version=1, codec=3, compress=1}`（codec=3 为 Kryo，compress=1 为 GZIP） |
| 可观测性 | 两端 `[AccessLog] traceId=...` 同一个 traceId；请求超过 `compress-threshold` 时 `RpcEncoder` 打印 `rawBytes=1565, wireBytes=96` |

不想启动 Nacos 时，可以运行 `StarterAutoConfigInProcessTest`。该测试使用 `local` 内存注册中心，在单进程内验证 starter 自动装配和真实 Netty 调用：

```bash
mvn -pl conduit-example/conduit-starter-demo/conduit-starter-demo-provider -am test
```

---

## 14. 模块/依赖速查

| 场景 | 引入坐标（io.github.nitouge） |
|---|---|
| Spring Boot 应用（推荐，自动装配） | `conduit-spring-boot-starter` |
| 普通 Spring 应用 | `conduit-spring`（配置类加 `@EnableConduit`） |
| 编程式 · 提供者 | `conduit-server` |
| 编程式 · 消费者 | `conduit-client` |
| 仅需注册中心抽象 | `conduit-registry`（仅抽象 + local 实现） |
| Nacos 注册中心 | `conduit-registry-nacos`（Spring/starter 默认带上） |
| ZooKeeper 注册中心 | `conduit-registry-zookeeper`（Curator） |
| Etcd 注册中心 | `conduit-registry-etcd`（jetcd） |

> 上述依赖都已传递带上其需要的下层模块（协议、传输、序列化实现、注册中心实现）。共享的**服务接口**由你自己的工程定义，不依赖 Conduit。

---

## 15. 可观测性

Filter 内置、按属性开关。属性可来自三种地方（优先级从高到低）：

1. JVM 系统属性：`-Dconduit.filter.metrics.enabled=true`（IntelliJ 里要填 **VM options**，不是 Program arguments）；
2. Spring Boot 配置：`application.yml` 的 `conduit.filter.*`，或程序参数 `--conduit.filter.metrics.enabled=true`；
3. 环境变量：`CONDUIT_FILTER_METRICS_ENABLED=true`。

| 能力 | 属性 | 默认 |
|---|---|---|
| traceId 生成/透传 + MDC | `conduit.filter.trace.enabled` | 开 |
| 指标（QPS/RT/错误率/延迟分布） | `conduit.filter.metrics.enabled` | 关 |
| JSON 结构化访问日志 | `conduit.filter.structuredlog.enabled` | 关 |
| 文本访问日志 | `conduit.filter.accesslog.enabled` | 开 |
| 提供侧并发隔离 | `conduit.filter.activelimit.enabled` / `...permits` | 关 / 200 |

开关在首次装载 filter 时读取，**改完要重启进程**。

```yaml
conduit:
  filter:
    metrics:
      enabled: true
    structuredlog:
      enabled: true
    accesslog:
      enabled: false
```

- **traceId**：默认自动生成并随 `RpcRequest.attachments` 透传；可用
  `RpcContext.getContext().setTraceId("...")` 主动指定，提供侧业务通过
  `RpcContext.getContext().getTraceId()` 读取；日志 pattern 加 `%X{traceId}` 即可串联。
- **指标**：`MetricsFilter.snapshot()` 读取每个 `interface#method` 的总量/成功/失败/平均/最大/
  最小/延迟分桶与分位估算，`MetricsFilter.reset()` 重置。
- **结构化日志**：logger `CONDUIT.ACCESS.JSON`，每调用一行 JSON（timestamp/traceId/interface/
  method/durationMs/success[/errorType/errorMessage]）。

> 详细行为、异步限制与配置示例见 [OBSERVABILITY.md](OBSERVABILITY.md)。
