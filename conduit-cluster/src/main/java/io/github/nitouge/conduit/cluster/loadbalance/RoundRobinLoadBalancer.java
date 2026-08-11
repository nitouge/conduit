package io.github.nitouge.conduit.cluster.loadbalance;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.LoadBalancer;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 轮询：按接口维度各持一个自增计数器，依次轮流选中各实例。
 *
 * <p>计数器按interfaceName区分，避免不同服务相互干扰；为简单起见不做带权平滑轮询。</p>
 */
public class RoundRobinLoadBalancer implements LoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(RoundRobinLoadBalancer.class);

    private final Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();

    @Override
    public ServiceMeta select(List<ServiceMeta> services, RpcRequest request) {
        if (CollectionUtils.isEmpty(services)) {
            log.warn("[RoundRobinLoadBalancer] 候选实例为空");
            return null;
        }
        String key = request == null ? "" : String.valueOf(request.getInterfaceName());
        AtomicInteger counter = counters.computeIfAbsent(key, k -> new AtomicInteger());
        int index = (counter.getAndIncrement() & Integer.MAX_VALUE) % services.size();
        ServiceMeta serviceMeta = services.get(index);
        log.debug("[RoundRobinLoadBalancer] select index={}/{} service={} -> {}:{}",
                index, services.size(),
                serviceMeta.getServiceName(),
                serviceMeta.getHost(), serviceMeta.getPort());
        return serviceMeta;
    }
}
