# Conduit 注册中心（Registry）

注册中心是可替换最典型的落点：上层（client/cluster）只认 `Registry` 接口，
后端可以是 Nacos、ZooKeeper 或 Etcd，业务与框架代码里不会出现 `new NacosRegistry()`。

---

## 1. 为什么需要注册中心

直连模式（M1）把提供者地址写死，在真实微服务里立刻失效：提供者是多台机器、会扩容缩容、会宕机、
容器会漂移。消费者真正需要的是一份**实时维护的"服务地址名录"**——注册中心就是这份电话簿。

顺着电话簿想，RPC 对注册中心的全部诉求只有四件事：

| 能力 | 谁来做 | 说人话 | 接口方法 |
| --- | --- | --- | --- |
| 注册 | 提供者 | 上线时登记"我提供 X 服务，地址是 host:port" | `register(ServiceMeta)` |
| 注销 | 提供者 | 优雅下线时把自己划掉 | `unregister(ServiceMeta)` |
| 发现 | 消费者 | 查一次：X 服务现在有哪些地址 | `discover(String)` |
| 监听 | 消费者 | 订阅 X 服务，地址一变就通知我 | `subscribe(String, NotifyListener)` |

`discover` 是"拉一次快照"，`subscribe` 是"以后每次变化都告诉我"，两者互补：启动时先拿初始列表，
之后靠订阅增量更新。

**这四问也是看懂三种中间件的最佳视角**：不管表面术语多不一样，只需回答——怎么注册、
怎么让死节点自动消失、怎么查、怎么订阅变化。

---

## 2. 抽象

核心 `conduit-registry` 只包含抽象 + `local` 实现，不携带任何第三方客户端：

```java
Registry registry = Registries.getRegistry("zookeeper", "127.0.0.1:2181");
Registry nacos    = Registries.getRegistry("nacos", "127.0.0.1:8848");
```

| scheme | 实现模块 | 说明 |
| --- | --- | --- |
| `local` | `conduit-registry` | 进程内，测试/同 JVM 场景（核心内置，零依赖） |
| `nacos`（默认） | `conduit-registry-nacos` | Nacos 临时实例 + 订阅推送；`conduit-spring`/starter 默认带上 |
| `zookeeper` | `conduit-registry-zookeeper` | Curator 临时节点 + `CuratorCache` |
| `etcd` | `conduit-registry-etcd` | jetcd 租约 + 前缀 watch |

缺失适配模块时 `Registries` 会给出明确的引入提示（而不是只抛"找不到扩展"）。

### 数据结构

- `ServiceMeta`：`serviceName + host + port + weight`，`host:port` 是实例的天然唯一标识；
  `equals/hashCode` 只比较 serviceName/host/port（节点身份），不含 weight；
- `NotifyListener.notify(List<ServiceMeta>)`：回调的是**全量最新列表**而非增量——
  消费者直接整体替换本地缓存，不用自己合并增量，不会出错；这也对齐三种注册中心查询接口的天然行为。

> `serviceName` 当前就是接口全限定名；`version`/`group` 字段已在 `RpcRequest`/`ServiceMeta` 中预留，
> 但**尚未参与服务名拼接与选路**（见 BACKLOG §9「路由规则」）。

---

## 3. 公共实现：`AbstractRegistry`

三种适配器都继承 `AbstractRegistry`，统一提供三件事：

1. **本地缓存兜底**：成功发现/收到推送后缓存最近一次**非空**列表；注册中心不可用时 `discover`
   退化为缓存，而不是抛异常或返回空。注册中心是"变更通知者"，不是调用路径上的强依赖；
2. **订阅幂等 + 本地 fan-out**：同一 service 只向后端注册**一个**监听器，多个业务 listener 在本地分发，
   避免每次调用都往 ZK/Nacos 挂一个 watcher；
3. **只缓存非空事件**：空列表不覆盖缓存，避免"注册中心抖动推了个空事件"把兜底数据毒化。

注册时序也是刻意设计的：服务端**先 bind 端口成功、再注册到注册中心**，消除"可被发现但连不上"的窗口。

---

## 4. 三种适配器：回答同样四个问题

| 问题 | Nacos | ZooKeeper | Etcd |
| --- | --- | --- | --- |
| 怎么注册 | `registerInstance`，临时实例 | 在 `services/<svc>/<host:port>` 创建 EPHEMERAL 节点 | `lease grant` + 带租约 `put` |
| 死节点怎么消失 | 客户端心跳，超时摘除 | 会话断开，临时节点自动删除 | TTL 到期（默认 30s），后台每 10s `keepAlive` |
| 怎么查 | `selectInstances(healthy=true)` | 读子节点 + 节点数据 | 按前缀 `range` |
| 怎么订阅 | `subscribe` 回调（UDP push + 长轮询兜底） | `CuratorCache` 监听子节点增删改（自动重注册） | 前缀 `watch`（MVCC revision 持续推） |

### 4.1 Nacos（默认）

```xml
<dependency>
  <groupId>io.github.nitouge</groupId>
  <artifactId>conduit-registry-nacos</artifactId>
</dependency>
```

```yaml
conduit:
  registry:
    type: nacos
    address: 127.0.0.1:8848
```

Nacos 自带"服务-实例"发现模型，与 `Registry` 四方法几乎一一对应，适配成本最低；
Spring/starter 已默认携带该模块，只用 ZK/Etcd 的项目可排除。

### 4.2 ZooKeeper

存储布局：

```text
/conduit/services/{serviceName}/{host}:{port}   # EPHEMERAL，value = host|port|weight
```

- 注册：Curator `PersistentNode`（EPHEMERAL，会话断开自动删除、重连自动重建）；
- 发现：读子节点 + 节点数据；
- 订阅：`CuratorCache` 监听子节点增删改，变更后重新发现并 fan-out（ZK 原生 watcher 一次性，
  Curator 负责自动重注册）；
- 连接：构造时 `blockUntilConnected`，超时抛 `RpcException`（快速失败而不是带病启动）。

```yaml
conduit:
  registry:
    type: zookeeper
    address: 127.0.0.1:2181
```

测试：`ZookeeperRegistryTest` 用 Curator `TestingServer` 在进程内起真实 ZK，CI 无需外部服务。

### 4.3 Etcd

存储布局：

```text
/conduit/services/{serviceName}/{host}:{port}   # value = weight，带 TTL 租约（默认 30s）
```

- 注册：申请 lease + 带租约 put；后台每 10s `keepAliveOnce`，失败自动重新注册；
- 注销：删除 key + 撤销租约；
- 发现：按服务前缀 range；
- 订阅：前缀 watch（变更后重新发现）。

```yaml
conduit:
  registry:
    type: etcd
    address: 127.0.0.1:2379
```

测试：逻辑用内存 `EtcdOperations` 假实现覆盖（无需真实 Etcd）；真实联调可本地起 etcd。

---

## 5. 三者对比与选型

| 维度 | ZooKeeper | Nacos | Etcd |
| --- | --- | --- | --- |
| 一致性模型 | CP（ZAB 强一致） | 默认 **AP**（也支持 CP 模式） | CP（Raft 强一致） |
| 死节点自动消失 | 临时节点（会话断开即删） | 临时实例 + 客户端心跳 | 租约 + keepAlive，TTL 到期删 |
| 通知机制 | Watcher 一次性，需重注册 | UDP push + 长轮询兜底 | MVCC revision 的 Watch 长流 |
| 数据模型 | 树形节点 | 服务-实例模型 | 扁平 KV + 前缀 |
| 分区时表现 | 少数派不可写，宁可不可用 | 仍可返回（可能略旧）列表 | 少数派不可写 |
| 运维复杂度 | 中（需自带 ZK 集群） | 低~中（控制台 + 配置中心一体） | 中（K8s 已有则顺手） |
| 额外能力 | 通用协调（锁、选主） | 服务发现 + 配置中心 | K8s 原生存储、强一致 KV |
| 何时选它 | 已有 ZK、需强一致、团队熟悉 | **新项目首选** | 已深度使用 K8s/Etcd |

选型结论：

- 服务发现场景 **AP 通常优于 CP**：分区时拿到"可能略旧但仍可用"的列表，好过拿不到列表导致整体不可用。
  这是默认 `nacos` 的原因；CP 注册中心（ZK/Etcd）分区时会牺牲可用性，用作纯服务发现要靠 §3 的缓存兜底弥补；
- 换注册中心不影响上层一行代码——这正是 `Registry` 抽象的价值。

---

## 6. 使用建议

- 生产优先 Nacos（已长期验证）；ZK/Etcd 遵循同样语义，按现有基础设施选择；
- 注册中心客户端连接/会话超时、ACL/TLS 认证等高级参数当前走默认值，作为后续增强；
- `Registry` 实例由 `RegistryFactory` 按地址缓存，同一地址全局复用，`destroy()` 由 Spring 容器关闭时调用；
- 消费者侧 `discover` 每次调用走注册中心（带缓存兜底）；提供者优雅下线时先注销再关连接。

## 7. 相关测试

| 关注点 | 用例 |
| --- | --- |
| 缓存兜底、订阅幂等/fan-out | `AbstractRegistryCacheTest` |
| ZK 临时节点/订阅/注销 | `ZookeeperRegistryTest`（Curator `TestingServer`） |
| Etcd 租约/续租/前缀 watch | `EtcdRegistryTest`（内存 `EtcdOperations`） |
| 真实 Nacos 注册发现 | `NacosRegistryCallTest`（无 Nacos 时跳过） |
| 多节点 + 多 JVM 真实注册发现 | `conduit-it`：`MultiJvmRegistryIntegrationTest`（`-Dit.distributed=true`） |
| 缺失适配模块的提示 | `RegistriesErrorMessageTest` |
