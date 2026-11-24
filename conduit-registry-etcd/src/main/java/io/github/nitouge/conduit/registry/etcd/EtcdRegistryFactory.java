package io.github.nitouge.conduit.registry.etcd;

import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.registry.RegistryFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Etcd注册中心工厂：按地址缓存并复用 {@link EtcdRegistry}。
 */
public class EtcdRegistryFactory implements RegistryFactory {

    private final Map<String, Registry> registries = new ConcurrentHashMap<>();

    @Override
    public Registry getRegistry(String address) {
        if (StringUtils.isBlank(address)) {
            throw new RpcException("Etcd注册中心地址不能为空");
        }
        return registries.computeIfAbsent(address, EtcdRegistry::new);
    }
}
