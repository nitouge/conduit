package io.github.nitouge.conduit.registry;

import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.spi.ExtensionLoader;

/**
 * 注册中心入口：经SPI（{@link ExtensionLoader}）按scheme选 {@link RegistryFactory} 再取 {@link Registry}。
 *
 * <p>上层只调这里，绝不new具体注册中心：{@code Registries.getRegistry("127.0.0.1:8848")}（默认nacos）
 * 或 {@code Registries.getRegistry("zookeeper", "127.0.0.1:2181")}。</p>
 *
 * <p>核心只带 {@code local} 实现；使用 nacos/zookeeper/etcd 需引入对应适配模块
 * （{@code conduit-registry-nacos} / {@code conduit-registry-zookeeper} / {@code conduit-registry-etcd}）。
 * 缺失模块时这里会给出明确的引入提示，而不是只抛“找不到扩展”。</p>
 */
public final class Registries {

    private Registries() {
    }

    /**
     * 用默认注册中心类型（@SPI 默认nacos）取注册中心
     */
    public static Registry getRegistry(String address) {
        RegistryFactory factory;
        try {
            factory = ExtensionLoader.getExtensionLoader(RegistryFactory.class).getDefaultExtension();
        } catch (RpcException e) {
            throw new RpcException("默认注册中心(nacos)未加载："
                    + "请引入conduit-registry-nacos（Spring starter已默认包含），"
                    + "或显式指定conduit.registry.type=local/zookeeper/etcd", e);
        }
        return factory.getRegistry(address);
    }

    /**
     * 按scheme（nacos/zookeeper/etcd/local）取注册中心
     */
    public static Registry getRegistry(String scheme, String address) {
        RegistryFactory factory;
        try {
            factory = ExtensionLoader.getExtensionLoader(RegistryFactory.class).getExtension(scheme);
        } catch (RpcException e) {
            throw new RpcException("注册中心[" + scheme + "]未加载：" + moduleHint(scheme), e);
        }
        return factory.getRegistry(address);
    }

    private static String moduleHint(String scheme) {
        String normalized = scheme == null ? "" : scheme.trim().toLowerCase();
        return switch (normalized) {
            case "nacos" -> "请引入conduit-registry-nacos（Spring starter已默认包含）";
            case "zookeeper" -> "请引入conduit-registry-zookeeper";
            case "etcd" -> "请引入conduit-registry-etcd";
            case "local" -> "local内置于conduit-registry，请检查依赖是否被排除";
            default -> "请引入提供该RegistryFactory的适配模块";
        };
    }
}
