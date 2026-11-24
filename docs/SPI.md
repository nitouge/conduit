# Conduit SPI 扩展机制

SPI 是 Conduit「可替换」目标的统一落地：上层只依赖接口，按名字加载实现，绝不 `new` 具体类。
本文件补充 v0.2 新增的**自适应扩展**与**生命周期**能力。

---

## 1. 基本用法

扩展点接口标注 `@SPI("默认名")`，实现类在 `META-INF/rpc/<接口全限定名>` 中登记：

```properties
# META-INF/rpc/io.github.nitouge.conduit.serialization.Serializer
jdk=io.github.nitouge.conduit.serialization.support.JdkSerializer
kryo=io.github.nitouge.conduit.serialization.support.KryoSerializer
```

```java
Serializer serializer = ExtensionLoader.getExtensionLoader(Serializer.class)
        .getExtension("kryo");
Serializer defaultOne = ExtensionLoader.getExtensionLoader(Serializer.class).getDefaultExtension();
```

特点：按名加载、默认扩展、单例缓存、线程安全、按需实例化；配置文件可跨 JAR 合并。

---

## 2. 自适应扩展加载（@Adaptive）

标注在 SPI 接口方法上，声明"本次调用用哪个实现"：

```java
@SPI("random")
public interface LoadBalancer {
    @Adaptive({"loadbalance"})
    ServiceMeta select(List<ServiceMeta> services, RpcRequest request);
}
```

`ExtensionLoader.getAdaptiveExtension()` 返回动态代理，调用方法时按 key 从参数里取值：

| 参数类型 | 取值方式 |
| --- | --- |
| `Map` | `map.get(key)` |
| `RpcRequest` | `request.getAttachment(key)` |
| 取不到 / 值为 `default` | 回退：代理创建时传入的默认名 → `@SPI` 默认名 |

```java
LoadBalancer lb = ExtensionLoader.getExtensionLoader(LoadBalancer.class)
        .getAdaptiveExtension("random");   // 默认 random

// 单次调用临时切到 roundrobin（RpcContext 会在发起调用时拷入 request.attachments）
RpcContext.getContext().setAttachment("loadbalance", "roundrobin");
```

规则：

- 取到的扩展名不存在会直接抛 `RpcException`，不静默降级；
- `getAdaptiveExtension()` 按默认名缓存代理（无状态、可复用）；
- 未标 `@Adaptive` 的方法总是走默认扩展；
- `Cluster` 与 `LoadBalancer` 已接入：默认名来自 `@RpcReference`/配置，支持用
  `RpcContext` 的 `cluster` / `loadbalance` attachment 做**单次调用覆盖**。

---

## 3. 组件生命周期（Lifecycle）

扩展实现可同时实现 `Lifecycle`：

```java
public class MyRegistry implements Registry, Lifecycle {
    @Override public void init() { /* 建连接、起线程池 */ }
    @Override public void destroy() { /* 释放资源 */ }
}
```

- `init()`：实例创建后、对外返回前调用一次；抛异常会导致创建失败，实例不会进入缓存；
- `destroy()`：`destroyExtension(name)` / `destroyAll()` / 静态 `shutdownAll()` 与 JVM
  关闭钩子中调用一次；异常只记日志，不影响其它扩展释放；
- 再次 `getExtension(name)` 会重建实例并重新 `init()`；
- init 内不要回调同一 `ExtensionLoader` 的按名加载，避免重复初始化。

---

## 4. 当前扩展点

| 扩展点 | 默认 | 内置实现 |
| --- | --- | --- |
| `Serializer` | `jdk` | jdk / json / kryo / protostuff |
| `RegistryFactory` | `nacos` | nacos / local |
| `LoadBalancer` | `random` | random / roundRobin / consistentHash |
| `Cluster` | `failover` | failfast / failover / failsafe / failback |
| `Filter` | 无默认 | accesslog / activelimit / metrics / exception 等 |

新增扩展三步：接口加 `@SPI("默认名")` → 写实现（无参构造）→ 在
`META-INF/rpc/<接口全限定名>` 加一行 `name=实现类`。

---

## 5. 约束

1. 扩展实现必须有无参构造；单例被全局复用，需自行保证线程安全。
2. 扩展点接口必须标注 `@SPI`，否则 `getExtensionLoader` 直接拒绝。
3. 自适应扩展只支持从 `Map` 与 `RpcRequest.attachments` 取值；其它动态参数类型需要扩展
   `ExtensionLoader` 的取值逻辑。
4. 生命周期回调是"进程内单例"语义，不要用它替代业务级连接池的精细回收。
