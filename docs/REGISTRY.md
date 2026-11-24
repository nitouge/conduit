# Conduit 注册中心（Registry）

`conduit-registry` 定义统一抽象，具体后端通过 `RegistryFactory` SPI 按 scheme 加载：

```java
Registry registry = Registries.getRegistry("zookeeper", "127.0.0.1:2181");
Registry nacos = Registries.getRegistry("nacos", "127.0.0.1:8848");
```

| scheme | 实现模块 | 说明 |
| --- | --- | --- |
| `local` | `conduit-registry` | 进程内，测试/同 JVM 场景（核心内置，零依赖） |
| `nacos`（默认） | `conduit-registry-nacos` | Nacos 临时实例 + 订阅推送；`conduit-spring`/starter 默认带上 |
| `zookeeper` | `conduit-registry-zookeeper` | Curator 临时节点 + CuratorCache |
| `etcd` | `conduit-registry-etcd` | jetcd 租约 + 前缀 watch |

> 核心 `conduit-registry` 只保留抽象 + `local`，**不携带任何第三方注册中心客户端**；
> 使用 nacos/zookeeper/etcd 时按需引入对应适配模块（Spring starter 已默认引入 Nacos）。
> 缺失模块时 `Registries` 会给出明确的引入提示（而不是只抛"找不到扩展"）。

---

## 1. 抽象与缓存兜底

`Registry` 四个能力：`register` / `unregister` / `discover` / `subscribe`（+ `destroy`）。

`AbstractRegistry` 统一提供：

- **本地缓存兜底**：成功发现后缓存最近非空列表；注册中心不可用时 `discover` 退化为缓存；
- **订阅幂等 + 本地 fan-out**：同一 service 只向后端注册一个监听器，多个业务 listener 在本地分发；
- 订阅推送刷新缓存（只缓存非空，避免空事件毒化兜底）。

---

## 2. Nacos 适配

```xml
<dependency>
  <groupId>io.github.nitouge</groupId>
  <artifactId>conduit-registry-nacos</artifactId>
  <version>${conduit.version}</version>
</dependency>
```

- 默认注册中心；`conduit-spring` / `conduit-spring-boot-starter` 已默认携带该模块，开箱即用；
- 只用 ZK/Etcd 的项目可排除它，减小依赖；
- 编程式使用（只依赖 `conduit-client`/`conduit-server`）时需显式引入。

```yaml
conduit:
  registry:
    type: nacos
    address: 127.0.0.1:8848
```

实现：Nacos 临时实例注册、`selectInstances(healthy=true)` 发现、`subscribe` 订阅推送。

---

## 3. ZooKeeper 适配

存储模型：

```text
/conduit/services/{serviceName}/{host}:{port}   # EPHEMERAL，value = host|port|weight
```

- 注册：Curator `PersistentNode`（EPHEMERAL，会话断开自动删除，重连自动重建）；
- 发现：读子节点 + 节点数据；
- 订阅：`CuratorCache` 监听子节点增删改，变更后重新发现并 fan-out；
- 连接：构造时 `blockUntilConnected`，超时抛 `RpcException`。

```xml
<dependency>
  <groupId>io.github.nitouge</groupId>
  <artifactId>conduit-registry-zookeeper</artifactId>
  <version>${conduit.version}</version>
</dependency>
```

```yaml
conduit:
  registry:
    type: zookeeper
    address: 127.0.0.1:2181
```

测试：`ZookeeperRegistryTest` 使用 Curator `TestingServer` 在进程内启动真实 ZK，CI 无需外部服务。

---

## 4. Etcd 适配

存储模型：

```text
/conduit/services/{serviceName}/{host}:{port}   # value = weight，带TTL租约（默认30s）
```

- 注册：申请 lease + 带租约 put；后台每 10s `keepAliveOnce`，失败自动重新注册；
- 注销：删除 key + 撤销租约；
- 发现：按服务前缀 range；
- 订阅：前缀 watch（变更后重新发现）。

```xml
<dependency>
  <groupId>io.github.nitouge</groupId>
  <artifactId>conduit-registry-etcd</artifactId>
  <version>${conduit.version}</version>
</dependency>
```

```yaml
conduit:
  registry:
    type: etcd
    address: 127.0.0.1:2379
```

测试：注册中心逻辑通过内存 `EtcdOperations` 假实现覆盖（无需真实 Etcd）；
真实联调可本地起 etcd（`docker run -p 2379:2379 bitnami/etcd`）。

---

## 5. 使用建议

- 生产优先 Nacos（已长期验证）；ZK/Etcd 适配遵循同样语义，可按现有基础设施选择；
- 注册中心客户端连接/会话超时、ACL/TLS 认证等高级参数当前走默认值，作为后续增强；
- `Registry` 实例由 `RegistryFactory` 按地址缓存，同一地址全局复用，`destroy()` 由 Spring 容器关闭时调用；
- 消费者侧 `discover` 每次调用走注册中心（带缓存兜底）；服务端在 bind 端口成功后才注册，避免
  "可被发现但连不上" 的窗口。
