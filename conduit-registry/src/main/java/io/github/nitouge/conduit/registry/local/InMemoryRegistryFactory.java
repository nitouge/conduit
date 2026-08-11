package io.github.nitouge.conduit.registry.local;

import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.registry.RegistryFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内注册中心工厂：同一地址复用同一个 {@link InMemoryRegistry} 实例，
 * 使同进程内的provider注册与consumer发现命中同一份内存数据。
 */
public class InMemoryRegistryFactory implements RegistryFactory {

    private final Map<String, Registry> registries = new ConcurrentHashMap<>();

    @Override
    public Registry getRegistry(String address) {
        return registries.computeIfAbsent(address == null ? "" : address, a -> new InMemoryRegistry());
    }
}
