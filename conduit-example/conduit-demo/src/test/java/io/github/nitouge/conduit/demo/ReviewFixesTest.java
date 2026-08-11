package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.cluster.LoadBalancer;
import io.github.nitouge.conduit.cluster.RemoteInvoker;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreaker;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreakerRegistry;
import io.github.nitouge.conduit.cluster.loadbalance.RandomLoadBalancer;
import io.github.nitouge.conduit.cluster.support.FailoverCluster;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.registry.NotifyListener;
import io.github.nitouge.conduit.registry.support.AbstractRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 全面Review确认bug的回归测试。每个用例对应一处已修复的问题。
 */
class ReviewFixesTest {

    @BeforeEach
    void resetBreakers() {
        CircuitBreakerRegistry.clear();
    }

    private RpcRequest req() {
        RpcRequest r = new RpcRequest();
        r.setInterfaceName("svc");
        r.setMethodName("m");
        return r;
    }

    /** 代理的Object方法（equals/hashCode/toString）按代理身份处理，equals自反性成立。 */
    @Test
    void proxyObjectMethodsUseProxyIdentity() {
        RpcClient client = new RpcClient();
        try {
            ServiceMeta target = new ServiceMeta(HelloService.class.getName(), "127.0.0.1", 1);
            HelloService proxy = RpcClientProxy.create(HelloService.class, target, client, 1000);

            assertTrue(proxy.equals(proxy), "proxy.equals(proxy) 必须为true（自反性）");
            assertFalse(proxy.equals("not-a-proxy"));
            assertEquals(proxy.hashCode(), proxy.hashCode(), "hashCode必须稳定");
            assertNotNull(proxy.toString());
            assertTrue(proxy.toString().contains(HelloService.class.getName()));
        } finally {
            client.close();
        }
    }

    /** ServiceMeta.equals/hashCode只看节点身份，忽略weight：改权重仍是同一实例。 */
    @Test
    void serviceMetaEqualsIgnoresWeight() {
        ServiceMeta a = new ServiceMeta("svc", "10.0.0.1", 20880, 100);
        ServiceMeta b = new ServiceMeta("svc", "10.0.0.1", 20880, 999);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    /** 加权随机用long累加权重，权重很大时不会int溢出成负数导致nextInt抛异常。 */
    @Test
    void randomLoadBalancerNoOverflowWithLargeWeights() {
        LoadBalancer lb = new RandomLoadBalancer();
        List<ServiceMeta> services = Arrays.asList(
                new ServiceMeta("svc", "10.0.0.1", 1, Integer.MAX_VALUE - 1),
                new ServiceMeta("svc", "10.0.0.2", 2, Integer.MAX_VALUE - 1)); // 两者之和会让int溢出
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 100; i++) {
                assertNotNull(lb.select(services, req()));
            }
        });
    }

    /**
     * 熔断器OPEN的节点被快速跳过，但<b>不消耗</b> FailOver的有限重试预算。
     * 构造3个已熔断节点 + 1个健康节点，retries=2（预算3）。旧逻辑会被3次熔断跳过耗尽预算而失败；
     * 修复后跳过不计预算，最终仍能切到健康节点成功。
     */
    @Test
    void failoverSkipsOpenBreakersWithoutConsumingRetryBudget() throws Exception {
        ServiceMeta n1 = new ServiceMeta("svc", "10.0.0.1", 1);
        ServiceMeta n2 = new ServiceMeta("svc", "10.0.0.2", 2);
        ServiceMeta n3 = new ServiceMeta("svc", "10.0.0.3", 3);
        ServiceMeta healthy = new ServiceMeta("svc", "10.0.0.9", 9);

        // 打开3个坏节点的熔断器（连续失败达默认阈值5）
        for (ServiceMeta bad : Arrays.asList(n1, n2, n3)) {
            CircuitBreaker breaker = CircuitBreakerRegistry.of(bad);
            for (int i = 0; i < 5; i++) {
                breaker.onFailure();
            }
        }

        RemoteInvoker ri = (node, request) -> {
            if (node.equals(healthy)) {
                return CompletableFuture.completedFuture(RpcResponse.success("ok-from-healthy"));
            }
            CompletableFuture<RpcResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("已熔断节点不应被真正调用: " + node.getHost()));
            return failed;
        };

        FailoverCluster failover = new FailoverCluster(); // retries=2 -> 预算 3
        List<ServiceMeta> all = new ArrayList<>(Arrays.asList(n1, n2, n3, healthy));
        RpcResponse resp = failover.invoke(all, req(), new RandomLoadBalancer(), ri).get(1, TimeUnit.SECONDS);
        assertEquals("ok-from-healthy", resp.getData());
    }

    /**
     * 同一serviceName多次subscribe只向后端注册一个监听器，
     * 多个业务listener在本地fan-out（一次推送两个都收到）。
     */
    @Test
    void subscribeIsIdempotentPerService() {
        AtomicInteger backendSubscribeCount = new AtomicInteger();
        AtomicReference<NotifyListener> backendListener = new AtomicReference<>();

        AbstractRegistry reg = new AbstractRegistry() {
            @Override
            protected void doRegister(ServiceMeta service) {
            }

            @Override
            protected void doUnregister(ServiceMeta service) {
            }

            @Override
            protected List<ServiceMeta> doDiscover(String serviceName) {
                return Collections.emptyList();
            }

            @Override
            protected void doSubscribe(String serviceName, NotifyListener listener) {
                backendSubscribeCount.incrementAndGet();
                backendListener.set(listener);
            }

            @Override
            public void destroy() {
            }
        };

        AtomicReference<List<ServiceMeta>> got1 = new AtomicReference<>();
        AtomicReference<List<ServiceMeta>> got2 = new AtomicReference<>();
        reg.subscribe("svc", got1::set);
        reg.subscribe("svc", got2::set);

        assertEquals(1, backendSubscribeCount.get(), "同一服务只应向后端注册一个监听器");

        // 模拟后端推送一次变更，两个业务listener都应收到
        List<ServiceMeta> push = Collections.singletonList(new ServiceMeta("svc", "10.0.0.1", 20880));
        backendListener.get().notify(push);
        assertSame(push, got1.get());
        assertSame(push, got2.get());
    }
}
