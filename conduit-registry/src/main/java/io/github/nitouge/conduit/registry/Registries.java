package io.github.nitouge.conduit.registry;

import io.github.nitouge.conduit.common.spi.ExtensionLoader;

/**
 * 注册中心入口：经SPI（{@link ExtensionLoader}）按scheme选 {@link RegistryFactory} 再取 {@link Registry}。
 *
 * <p>上层只调这里，绝不new具体注册中心：{@code Registries.getRegistry("127.0.0.1:8848")}（默认nacos）
 * 或 {@code Registries.getRegistry("zookeeper", "127.0.0.1:2181")}。</p>
 */
public final class Registries {

    private Registries() {
    }

    /** 用默认注册中心类型（@SPI 默认nacos）取注册中心 */
    public static Registry getRegistry(String address) {
        return ExtensionLoader.getExtensionLoader(RegistryFactory.class)
                .getDefaultExtension()
                .getRegistry(address);
    }

    /** 按scheme（nacos/zookeeper/etcd）取注册中心 */
    public static Registry getRegistry(String scheme, String address) {
        return ExtensionLoader.getExtensionLoader(RegistryFactory.class)
                .getExtension(scheme)
                .getRegistry(address);
    }
}
