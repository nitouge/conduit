package io.github.nitouge.conduit.cluster.loadbalance;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.LoadBalancer;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 一致性哈希：相同请求参数总路由到同一实例（适合有状态/缓存亲和场景）。
 *
 * <p>每个实例放 {@value #VIRTUAL_NODES} 个虚拟节点到哈希环以均衡分布；按
 * hash(接口+方法+参数) 在环上顺时针找第一个实例。为简单起见每次调用重建环（实例少时开销可忽略；
 * 优化点：按实例列表缓存环，留待后续）。</p>
 */
public class ConsistentHashLoadBalancer implements LoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(ConsistentHashLoadBalancer.class);

    private static final int VIRTUAL_NODES = 160;

    @Override
    public ServiceMeta select(List<ServiceMeta> services, RpcRequest request) {
        if (CollectionUtils.isEmpty(services)) {
            log.warn("[ConsistentHashLoadBalancer] 候选实例为空");
            return null;
        }
        SortedMap<Long, ServiceMeta> ring = new TreeMap<>();
        for (ServiceMeta s : services) {
            String node = s.getHost() + ":" + s.getPort();
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                ring.put(hash(node + "#" + i), s);
            }
        }
        long key = hash(routingKey(request));
        SortedMap<Long, ServiceMeta> tail = ring.tailMap(key);
        Long target = tail.isEmpty() ? ring.firstKey() : tail.firstKey();
        ServiceMeta serviceMeta = ring.get(target);
        log.debug("[ConsistentHashLoadBalancer] select key={} service={} -> {}:{}",
                key, serviceMeta.getServiceName(),
                serviceMeta.getHost(), serviceMeta.getPort());
        return serviceMeta;
    }

    /** 路由键：接口 + 方法 + 参数，保证相同调用稳定路由 */
    private static String routingKey(RpcRequest request) {
        if (request == null) {
            return "";
        }
        // deepToString：嵌套数组参数也能得到稳定字符串，保证相同调用稳定路由（Arrays.toString对数组元素只打印地址）
        return request.getInterfaceName() + "#" + request.getMethodName()
                + "#" + Arrays.deepToString(request.getArgs());
    }

    /** 取MD5前8字节作为环上的long位置 */
    private static long hash(String key) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = md5.digest(key.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h & 0x7FFFFFFFFFFFFFFFL; // 非负
        } catch (NoSuchAlgorithmException e) {
            throw new RpcException("MD5不可用", e);
        }
    }
}
