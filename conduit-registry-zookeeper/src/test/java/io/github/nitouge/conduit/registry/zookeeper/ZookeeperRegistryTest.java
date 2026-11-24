package io.github.nitouge.conduit.registry.zookeeper;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.registry.Registry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ZooKeeper注册中心集成测试：使用Curator {@link TestingServer} 在进程内启动真实ZK，无需外部服务。
 */
class ZookeeperRegistryTest {

    private static TestingServer server;
    private final List<ZookeeperRegistry> registries = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        if (server == null) {
            server = new TestingServer(true);
        }
        registries.clear();
    }

    @AfterEach
    void tearDown() {
        for (ZookeeperRegistry registry : registries) {
            try {
                registry.destroy();
            } catch (Exception ignored) {
                // 测试清理尽力而为
            }
        }
        registries.clear();
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    private ZookeeperRegistry newRegistry() {
        ZookeeperRegistry registry = new ZookeeperRegistry(server.getConnectString(), 5_000, 10_000);
        registries.add(registry);
        return registry;
    }

    private static String uniqueService() {
        return "test.Svc" + UUID.randomUUID().toString().replace("-", "");
    }

    @Test
    void registerDiscoverAndUnregister() {
        ZookeeperRegistry registry = newRegistry();
        String serviceName = uniqueService();
        ServiceMeta service = new ServiceMeta(serviceName, "10.0.0.1", 20880, 42);

        registry.register(service);
        awaitTrue("注册后应能发现实例", 5_000,
                () -> registry.discover(serviceName).size() == 1);

        ServiceMeta found = registry.discover(serviceName).get(0);
        assertEquals(serviceName, found.getServiceName());
        assertEquals("10.0.0.1", found.getHost());
        assertEquals(20880, found.getPort());
        assertEquals(42, found.getWeight());

        registry.unregister(service);
        awaitTrue("注销后应不再发现实例", 5_000,
                () -> registry.discover(serviceName).isEmpty());
    }

    @Test
    void reRegisterUpdatesWeight() {
        ZookeeperRegistry registry = newRegistry();
        String serviceName = uniqueService();

        registry.register(new ServiceMeta(serviceName, "10.0.0.2", 20881, 100));
        registry.register(new ServiceMeta(serviceName, "10.0.0.2", 20881, 250));

        awaitTrue("重复注册应只剩一个实例且权重更新", 5_000, () -> {
            List<ServiceMeta> found = registry.discover(serviceName);
            return found.size() == 1 && found.get(0).getWeight() == 250;
        });
    }

    @Test
    void subscribeNotifiesOnRegisterAndUnregister() throws Exception {
        ZookeeperRegistry registry = newRegistry();
        String serviceName = uniqueService();
        CountDownLatch initial = new CountDownLatch(1);
        AtomicReference<List<ServiceMeta>> latest = new AtomicReference<>();

        registry.subscribe(serviceName, services -> {
            latest.set(services);
            initial.countDown();
        });
        assertTrue(initial.await(5, TimeUnit.SECONDS), "订阅后应立即回放当前列表");

        ServiceMeta service = new ServiceMeta(serviceName, "10.0.0.3", 20882, 100);
        registry.register(service);
        awaitTrue("注册变化应推送给订阅者", 5_000,
                () -> latest.get() != null && latest.get().size() == 1);

        registry.unregister(service);
        awaitTrue("注销变化应推送给订阅者", 5_000,
                () -> latest.get() != null && latest.get().isEmpty());
    }

    @Test
    void factoryReusesRegistryPerAddress() {
        ZookeeperRegistryFactory factory = new ZookeeperRegistryFactory();
        Registry first = factory.getRegistry(server.getConnectString());
        Registry second = factory.getRegistry(server.getConnectString());
        try {
            assertSame(first, second, "同一地址应复用同一注册中心实例");
        } finally {
            if (first instanceof ZookeeperRegistry zookeeperRegistry) {
                zookeeperRegistry.destroy();
            }
        }
    }

    @Test
    void invalidAddressFailsFast() {
        assertThrows(RpcException.class,
                () -> new ZookeeperRegistry("127.0.0.1:1", 300, 300));
    }

    private static void awaitTrue(String description, long timeoutMillis, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
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
