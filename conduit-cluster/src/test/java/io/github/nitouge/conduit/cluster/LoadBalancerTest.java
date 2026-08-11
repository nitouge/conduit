package io.github.nitouge.conduit.cluster;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.loadbalance.ConsistentHashLoadBalancer;
import io.github.nitouge.conduit.cluster.loadbalance.RandomLoadBalancer;
import io.github.nitouge.conduit.cluster.loadbalance.RoundRobinLoadBalancer;
import io.github.nitouge.conduit.common.spi.ExtensionLoader;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoadBalancerTest {

    private final ServiceMeta a = new ServiceMeta("svc", "10.0.0.1", 1);
    private final ServiceMeta b = new ServiceMeta("svc", "10.0.0.2", 2);

    private RpcRequest request(String iface, Object... args) {
        RpcRequest r = new RpcRequest();
        r.setInterfaceName(iface);
        r.setMethodName("m");
        r.setArgs(args);
        return r;
    }

    @Test
    void loadBalancersLoadViaSpi() {
        ExtensionLoader<LoadBalancer> loader = ExtensionLoader.getExtensionLoader(LoadBalancer.class);
        assertInstanceOf(RandomLoadBalancer.class, loader.getDefaultExtension()); // @SPI("random")
        assertInstanceOf(RoundRobinLoadBalancer.class, loader.getExtension("roundrobin"));
        assertInstanceOf(ConsistentHashLoadBalancer.class, loader.getExtension("consistenthash"));
    }

    @Test
    void roundRobinCyclesThroughAllNodes() {
        LoadBalancer rr = ExtensionLoader.getExtensionLoader(LoadBalancer.class).getExtension("roundrobin");
        List<ServiceMeta> list = Arrays.asList(a, b);
        RpcRequest req = request("rr.test.Svc"); // 独立接口名，计数器从0起

        ServiceMeta first = rr.select(list, req);
        ServiceMeta second = rr.select(list, req);
        ServiceMeta third = rr.select(list, req);

        assertNotEquals(first, second);   // 连续两次轮到不同节点
        assertEquals(first, third);       // 第三次回到第一个
    }

    @Test
    void consistentHashStableForSameArgs() {
        LoadBalancer ch = ExtensionLoader.getExtensionLoader(LoadBalancer.class).getExtension("consistenthash");
        List<ServiceMeta> list = Arrays.asList(a, b);

        ServiceMeta s1 = ch.select(list, request("ch.Svc", "key-1"));
        ServiceMeta s2 = ch.select(list, request("ch.Svc", "key-1"));
        assertEquals(s1, s2); // 相同参数稳定路由到同一节点
    }

    @Test
    void randomAlwaysSelectsFromList() {
        LoadBalancer random = new RandomLoadBalancer();
        List<ServiceMeta> list = Arrays.asList(a, b);
        RpcRequest req = request("rnd.Svc");
        for (int i = 0; i < 100; i++) {
            assertTrue(list.contains(random.select(list, req)));
        }
    }
}
