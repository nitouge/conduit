package io.github.nitouge.conduit.testkit;

import io.github.nitouge.conduit.api.ServiceMeta;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * testkit自检测：夹具本身要可靠，否则所有依赖它的集成测试都会"假绿"。
 */
class TestkitSmokeTest {

    @Test
    void serverAndClientFixturesPerformRealCall() {
        try (TestServer server = TestServer.start(EchoService.class, new EchoServiceImpl("self"));
             TestRpc rpc = TestRpc.create()) {

            EchoService echo = rpc.proxy(EchoService.class, server.meta(EchoService.class), 2_000);
            assertEquals("ok:hi", echo.echo("hi"));
            assertEquals("self", echo.nodeId());
        }
    }

    @Test
    void mutableRegistryNotifiesAndDiscovers() {
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        ServiceMeta meta = new ServiceMeta(EchoService.class.getName(), "127.0.0.1", 1);

        AtomicBoolean pushed = new AtomicBoolean();
        registry.subscribe(EchoService.class.getName(), instances -> pushed.set(!instances.isEmpty()));
        registry.register(meta);

        assertTrue(pushed.get(), "注册后应推送给订阅者");
        assertEquals(List.of(meta), registry.discover(EchoService.class.getName()));

        registry.unregister(meta);
        assertEquals(List.of(), registry.discover(EchoService.class.getName()));
    }

    @Test
    void fixedRegistryIgnoresRegistrations() {
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        registry.register(new ServiceMeta(EchoService.class.getName(), "127.0.0.1", 1));
        assertEquals(1, registry.discover(EchoService.class.getName()).size());

        var fixed = TestRegistries.fixed(List.of());
        fixed.register(new ServiceMeta(EchoService.class.getName(), "127.0.0.1", 2));
        assertEquals(0, fixed.discover(EchoService.class.getName()).size(), "fixed注册中心不接收注册");
    }

    @Test
    void awaitTimesOutWithReadableMessage() {
        AssertionError error = assertThrows(AssertionError.class,
                () -> Await.until("永不成立的条件", 100, () -> false));
        assertTrue(error.getMessage().contains("永不成立的条件"), error.getMessage());
    }

    @Test
    void freePortIsUsable() {
        int port = Ports.freePort();
        assertTrue(port > 0);
        assertTrue(!Ports.connectable(port), "刚释放的空闲端口不应有人监听");
    }
}
