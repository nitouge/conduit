# Conduit SPI 扩展机制

SPI 是 Conduit 可替换目标的统一落地：上层只依赖接口，按名字加载实现，绝不 `new` 具体类。
本文件说明 `@SPI` / `ExtensionLoader` 的用法与内部结构，以及自适应加载、生命周期两项能力。

---

## 1. 为什么需要 SPI（以及为什么不用 JDK ServiceLoader）

RPC 框架里几乎每个关键能力都有多种实现：序列化（jdk/json/kryo/protostuff）、注册中心（nacos/zookeeper/etcd）、
负载均衡（random/roundRobin/consistentHash）、容错（failfast/failover/failsafe/failback）。
如果到处 `new KryoSerializer()`，"换实现"就变成满项目改代码。

我们需要一个统一入口：**给它一个接口和一个名字，它把对应实现给我**。JDK 自带的
`java.util.ServiceLoader` 能力不足：

| 需求 | JDK `ServiceLoader` | Conduit 需要 |
| --- | --- | --- |
| 按名字取某一个实现 | 不支持，只能遍历 | 必须支持，如 `getExtension("kryo")` |
| 指定默认实现 | 不支持 | 必须支持，如序列化默认 jdk、注册中心默认 nacos |
| 加载时机 | 一次性实例化所有实现 | 按需实例化 |
| 实例缓存 | 每次迭代新建 | 缓存单例、全局复用、线程安全 |
| 配置形式 | 一行一个实现类，无名字 | `name=实现类全限定名` |

因此自己实现一套极简 SPI：「命名扩展 + 默认值 + 缓存单例，但保持简单、不做代码生成与注入。

---

## 2. 基本用法

扩展点接口标注 `@SPI("默认名")`，实现类在 `META-INF/rpc/<接口全限定名>` 中登记（`#` 注释、空行均可）：

```properties
# META-INF/rpc/io.github.nitouge.conduit.serialization.Serializer
jdk=io.github.nitouge.conduit.serialization.support.JdkSerializer
kryo=io.github.nitouge.conduit.serialization.support.KryoSerializer
```

```java
Serializer serializer = ExtensionLoader.getExtensionLoader(Serializer.class).getExtension("kryo");
Serializer defaultOne = ExtensionLoader.getExtensionLoader(Serializer.class).getDefaultExtension();
```

文件用 `ClassLoader.getResources` 扫描 classpath 上**所有**同名文件并合并，因此每个模块/JAR
可以各自提供自己的扩展（例如注册中心适配器就是独立模块带一行配置）。

---

## 3. ExtensionLoader 内部结构

三层缓存 + 一次配置解析，全部用并发容器：

| 缓存 | 内容 | 说明 |
| --- | --- | --- |
| `LOADERS` | 接口 → Loader | 每个 `@SPI` 接口一个 Loader，全局共享 |
| `nameToClass` | 扩展名 → 实现类 | 配置文件只解析一次（`configLoaded` 标记），之后查表 |
| `instances` | 扩展名 → 单例 | `computeIfAbsent` 保证并发下只实例化一次 |
| `adaptiveExtensions` | 默认名 → 自适应代理 | 代理无状态，按默认名缓存复用 |

对外 API：

| 方法 | 说明 |
| --- | --- |
| `getExtension(name)` | 按名取单例（按需实例化），名字不存在直接抛 `RpcException`，**不静默降级** |
| `getDefaultExtension()` | 取 `@SPI` 指定的默认扩展 |
| `getExtensionInfos()` | 全部扩展（名字 → 实例），`FilterLoader` 用它加载整条 Filter 链 |
| `getAdaptiveExtension(...)` | 取自适应代理（见 §4），可指定默认名 |
| `destroyExtension(name)` / `destroyAll()` / `shutdownAll()` | 触发 `Lifecycle.destroy()`（见 §5） |

---

## 4. 自适应扩展加载（@Adaptive）

标注在 SPI 接口方法上，声明"本次调用用哪个实现"：

```java
@SPI("random")
public interface LoadBalancer {
    @Adaptive({"loadbalance"})
    ServiceMeta select(List<ServiceMeta> services, RpcRequest request);
}
```

`getAdaptiveExtension()` 返回动态代理，调用时按 key 从参数里取值：

| 参数类型 | 取值方式 |
| --- | --- |
| `Map` | `map.get(key)` |
| `RpcRequest` | `request.getAttachment(key)` |
| 取不到 / 值为 `default` | 回退：代理创建时传入的默认名 → `@SPI` 默认名 |

```java
LoadBalancer lb = ExtensionLoader.getExtensionLoader(LoadBalancer.class)
        .getAdaptiveExtension("random");       // 默认 random

RpcContext.getContext().setAttachment("loadbalance", "roundrobin"); // 单次调用覆盖
```

规则：

- 取到的扩展名不存在会直接抛 `RpcException`，不静默降级；
- 代理按默认名缓存（无状态、可复用）；未标 `@Adaptive` 的方法总走默认扩展；
- `Cluster` 与 `LoadBalancer` 已接入：默认名来自 `@RpcReference`/配置，支持用
  `RpcContext` 的 `cluster` / `loadbalance` attachment 做**单次调用覆盖**。

---

## 5. 组件生命周期（Lifecycle）

扩展实现可同时实现 `Lifecycle`：

```java
public class MyRegistry implements RegistryFactory, Lifecycle {
    @Override public void init() { /* 建连接、起线程池 */ }
    @Override public void destroy() { /* 释放资源 */ }
}
```

- `init()`：实例创建后、对外返回前调用一次；抛异常会导致创建失败，实例不会进入缓存；
- `destroy()`：`destroyExtension(name)` / `destroyAll()` / 静态 `shutdownAll()` 与 JVM 关闭钩子中
  调用一次；异常只记日志，不影响其它扩展释放；
- 再次 `getExtension(name)` 会重建实例并重新 `init()`；
- init 内不要回调同一 `ExtensionLoader` 的按名加载，避免重复初始化。

---

## 6. 当前扩展点

| 扩展点接口 | 所在模块 | `@SPI` 默认 | 内置实现 |
| --- | --- | --- | --- |
| `Serializer` | `conduit-serialization` | `jdk` | jdk / json / kryo / protostuff |
| `RegistryFactory` | `conduit-registry` + 各适配模块 | `nacos` | local（核心）/ nacos / zookeeper / etcd |
| `LoadBalancer` | `conduit-cluster` | `random` | random / roundRobin / consistentHash |
| `Router` | `conduit-cluster` | `default` | default（version/group/tag 硬过滤 + zone 软优先） |
| `Cluster` | `conduit-cluster` | `failover` | failfast / failover / failsafe / failback |
| `Filter` | `conduit-common` | 无默认（显式注册） | accesslog / trace / structuredlog / idempotency / metrics / idempotent / ratelimit / exception / activelimit |

> 注意 `Registry` 的实现是按**模块**拆分的：核心只带 `local`，nacos/zookeeper/etcd 各自一个模块带一行
> `META-INF/rpc/...RegistryFactory` 配置，引入依赖即生效（`Registries` 在缺失时会提示该引哪个模块）。

### 新增一个扩展点（三步）

1. 接口加 `@SPI("默认名")`；
2. 写实现（无参构造）；
3. 在 `META-INF/rpc/<接口全限定名>` 加一行 `name=实现类`。

上层依旧只通过 `getExtension(name)` 拿实现，业务代码零改动。

---

## 7. 明确不做（可选后续）

为控制规模、保持可读，以下能力**暂不实现**（取舍）：

- **`@Adaptive` 代码生成**：已实现轻量版自适应代理（反射 + 动态代理），不做运行时字节码生成；
- **`@Activate` 自动激活**：不做条件自动激活；Filter 链通过 SPI 配置 + `order()` 显式排序，
  每个 Filter 还可用 `conduit.filter.<name>.enabled` 单独开关；
- **扩展点 IoC / Wrapper 自动包装**：扩展实现无参构造、自给自足，需要协作时由上层显式组合。

---

## 8. 约束与常见坑

1. 扩展实现必须有无参构造；单例被全局复用，需自行保证线程安全。
2. 扩展点接口必须标注 `@SPI`，否则 `getExtensionLoader` 直接拒绝。
3. 自适应扩展只支持从 `Map` 与 `RpcRequest.attachments` 取值；其它来源需要扩展取值逻辑。
4. 生命周期是"进程内单例"语义，不要用它替代业务级连接池的精细回收。
5. 扩展名写错要立刻失败（抛 `RpcException`），不要依赖"回退到默认实现"掩盖配置错误。
