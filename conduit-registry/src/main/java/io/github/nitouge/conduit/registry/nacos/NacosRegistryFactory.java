package io.github.nitouge.conduit.registry.nacos;

import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.spi.ExtensionLoader;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.registry.RegistryFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nacos注册中心工厂：按地址缓存并复用 {@link NacosRegistry}。
 *
 * <p>由 {@link ExtensionLoader} 经无参构造创建为单例（每个ExtensionLoader一个）。</p>
 */
public class NacosRegistryFactory implements RegistryFactory {

    private final Map<String, Registry> registries = new ConcurrentHashMap<>();

    @Override
    public Registry getRegistry(String address) {
        // 显式校验：空地址对Nacos毫无意义（不像InMemory可用默认值兜底），直接抛带信息异常而非裸NPE
        if (StringUtils.isBlank(address)) {
            throw new RpcException("Nacos注册中心地址不能为空");
        }
        return registries.computeIfAbsent(address, NacosRegistry::new);
    }
}
