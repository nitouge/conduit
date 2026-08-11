package io.github.nitouge.conduit.registry;

import io.github.nitouge.conduit.api.ServiceMeta;

import java.util.List;

/**
 * 注册中心抽象——一个RPC框架真正需要它的四件事：注册、注销、发现、监听。
 *
 * <p>无论Nacos/ZooKeeper/Etcd，本质都是register/unregister/lookup(discover)/watch(subscribe)。
 * 上层（server/client）只面向本接口编程，<b>绝不new具体实现</b>。SPI扩展点在
 * {@link RegistryFactory}（按scheme选实现），经 {@link Registries} 取用。</p>
 */
public interface Registry {

    /** 注册一个服务实例（提供者启动时调用） */
    void register(ServiceMeta service);

    /** 注销一个服务实例（提供者优雅停机时调用） */
    void unregister(ServiceMeta service);

    /** 发现某服务的全部可用实例（消费者调用前查询，带本地缓存兜底） */
    List<ServiceMeta> discover(String serviceName);

    /** 订阅某服务的实例变更，变更时回调 {@link NotifyListener} */
    void subscribe(String serviceName, NotifyListener listener);

    /** 关闭注册中心客户端，释放连接 */
    void destroy();
}
