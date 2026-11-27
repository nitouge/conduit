package io.github.nitouge.conduit.registry.etcd;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Etcd注册中心逻辑测试：用内存 {@link EtcdOperations} 假实现覆盖注册/发现/注销/订阅与路径解析。
 */
class EtcdRegistryTest {

    private InMemoryEtcdOperations ops;
    private EtcdRegistry registry;

    @BeforeEach
    void setUp() {
        ops = new InMemoryEtcdOperations();
        registry = new EtcdRegistry(ops);
    }

    @AfterEach
    void tearDown() {
        registry.destroy();
    }

    @Test
    void registerDiscoverAndUnregister() {
        ServiceMeta service = new ServiceMeta("svc.Etcd", "10.0.0.1", 20880, 42);

        registry.register(service);

        List<ServiceMeta> found = registry.discover("svc.Etcd");
        assertEquals(1, found.size());
        assertEquals("10.0.0.1", found.get(0).getHost());
        assertEquals(20880, found.get(0).getPort());
        assertEquals(42, found.get(0).getWeight());

        registry.unregister(service);
        assertTrue(registry.discover("svc.Etcd").isEmpty());
    }

    @Test
    void servicesAreIsolatedByPrefix() {
        registry.register(new ServiceMeta("svc.A", "10.0.0.1", 1, 100));
        registry.register(new ServiceMeta("svc.B", "10.0.0.2", 2, 100));

        assertEquals(1, registry.discover("svc.A").size());
        assertEquals(1, registry.discover("svc.B").size());
        assertEquals("10.0.0.2", registry.discover("svc.B").get(0).getHost());
        assertTrue(registry.discover("svc").isEmpty(), "不应跨服务前缀匹配");
    }

    @Test
    void subscribeReceivesRegisterAndUnregisterEvents() throws Exception {
        CountDownLatch initial = new CountDownLatch(1);
        AtomicReference<List<ServiceMeta>> latest = new AtomicReference<>();
        registry.subscribe("svc.Sub", services -> {
            latest.set(services);
            initial.countDown();
        });
        assertTrue(initial.await(2, TimeUnit.SECONDS), "订阅后应立即回放当前列表");

        ServiceMeta service = new ServiceMeta("svc.Sub", "10.0.0.3", 20882, 100);
        registry.register(service);
        awaitTrue("注册变化应推送给订阅者", 2_000,
                () -> latest.get() != null && latest.get().size() == 1);

        registry.unregister(service);
        awaitTrue("注销变化应推送给订阅者", 2_000,
                () -> latest.get() != null && latest.get().isEmpty());
    }

    @Test
    void routingAttributesSurviveRegistration() {
        ServiceMeta service = new ServiceMeta("svc.Route", "10.0.0.4", 20883, 60)
                .routeAttributes("2.0.0", "pay", "gray", "sh-1");

        registry.register(service);

        List<ServiceMeta> found = registry.discover("svc.Route");
        assertEquals(1, found.size());
        assertEquals("2.0.0", found.get(0).getVersion(), "value里应带上版本等路由属性");
        assertEquals("pay", found.get(0).getGroup());
        assertEquals("gray", found.get(0).getTag());
        assertEquals("sh-1", found.get(0).getZone());
        assertEquals(60, found.get(0).getWeight());
    }

    @Test
    void legacyWeightOnlyValueStillParses() {
        // 升级前写入的实例 value 只有 weight，key 里是 host:port：升级后仍必须能发现
        ops.kv.put(EtcdRegistry.BASE_PATH + "/svc.Legacy/10.0.0.7:20884", "77");

        List<ServiceMeta> found = registry.discover("svc.Legacy");
        assertEquals(1, found.size());
        assertEquals("10.0.0.7", found.get(0).getHost());
        assertEquals(20884, found.get(0).getPort());
        assertEquals(77, found.get(0).getWeight());
        assertNull(found.get(0).getVersion());
    }

    @Test
    void malformedKeysAreIgnored() {
        ops.kv.put(EtcdRegistry.BASE_PATH + "/svc.Bad/not-a-host", "100");
        ops.kv.put(EtcdRegistry.BASE_PATH + "/svc.Bad/10.0.0.1:notaport", "100");
        ops.kv.put(EtcdRegistry.BASE_PATH + "/svc.Bad/nested/host:1", "100");

        assertTrue(registry.discover("svc.Bad").isEmpty());
    }

    @Test
    void factoryRejectsBlankAddress() {
        assertThrows(RpcException.class, () -> new EtcdRegistryFactory().getRegistry(" "));
    }

    private static void awaitTrue(String description, long timeoutMillis, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("等待被中断", e);
            }
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("等待超时(" + timeoutMillis + "ms): " + description);
        }
    }
}
