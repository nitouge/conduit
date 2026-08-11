package io.github.nitouge.conduit.server.support;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务缓存：key = 服务名（接口全名，必要时拼接version/group），value = 真实实现对象。
 *
 * <p>服务端不需要代理、不需要复杂容器，持有真实实现即可，请求进来O(1)命中后反射调用。</p>
 */
public class ServiceCache {

    private final Map<String, Object> cache = new ConcurrentHashMap<>();

    /**
     * 暴露/覆盖一个服务实现。serviceName与impl均不可为null；同名重复注册会<b>覆盖</b>前者
     * （同一接口的多次暴露以最后一次为准）。
     */
    public void register(String serviceName, Object impl) {
        Objects.requireNonNull(serviceName, "serviceName不能为空");
        Objects.requireNonNull(impl, "服务实现不能为空");
        cache.put(serviceName, impl);
    }

    /** 按服务名取实现对象；未注册返回null（由调用方决定如何处理未知服务）。 */
    public Object get(String serviceName) {
        return cache.get(serviceName);
    }
}
