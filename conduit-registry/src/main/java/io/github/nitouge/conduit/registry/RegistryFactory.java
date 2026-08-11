package io.github.nitouge.conduit.registry;

import io.github.nitouge.conduit.common.spi.ExtensionLoader;
import io.github.nitouge.conduit.common.spi.SPI;

/**
 * 注册中心工厂（@SPI扩展点）：按注册中心类型（nacos/zookeeper/etcd）创建 {@link Registry}。
 *
 * <p>这是注册中心可替换的SPI入口——与 {@code  io.github.nitouge.conduit.serialization.Serializer} 一样，
 * 由同一个{@link ExtensionLoader} 按名加载。换注册中心只改scheme配置 + 加一个实现依赖，业务零改动。
 * 通过 {@link Registries} 取用。</p>
 */
@SPI("nacos")
public interface RegistryFactory {

    /** 按地址取/建一个注册中心实例（同地址应复用） */
    Registry getRegistry(String address);
}
