package io.github.nitouge.conduit.cluster.loadbalance;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.LoadBalancer;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 加权随机：按weight占比随机选一个实例。weight越大被选中概率越高。
 */
public class RandomLoadBalancer implements LoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(RandomLoadBalancer.class);

    @Override
    public ServiceMeta select(List<ServiceMeta> services, RpcRequest request) {
        if (CollectionUtils.isEmpty(services)) {
            log.warn("[RandomLoadBalancer] 候选实例为空");
            return null;
        }

        final ServiceMeta selected;
        if (services.size() == 1) {
            selected = services.get(0);
        } else {
            // 用long累加权重：避免实例多/单权重大时int溢出成负数，导致nextInt抛IllegalArgumentException
            long total = 0;
            for (ServiceMeta s : services) {
                total += weight(s);
            }
            long r = ThreadLocalRandom.current().nextLong(total);
            ServiceMeta hit = null;
            for (ServiceMeta s : services) {
                r -= weight(s);
                if (r < 0) {
                    hit = s;
                    break;
                }
            }
            selected = hit != null ? hit : services.get(services.size() - 1);
        }
        log.debug("[RandomLoadBalancer] select service={} weight={} -> {}:{}",
                selected.getServiceName(), selected.getWeight(),
                selected.getHost(), selected.getPort());
        return selected;
    }

    private static int weight(ServiceMeta s) {
        return Math.max(1, s.getWeight());
    }
}
