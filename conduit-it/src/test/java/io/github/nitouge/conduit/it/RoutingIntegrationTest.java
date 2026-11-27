package io.github.nitouge.conduit.it;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcContext;
import io.github.nitouge.conduit.client.ReferenceOptions;
import io.github.nitouge.conduit.cluster.ClusterOptions;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.testkit.EchoService;
import io.github.nitouge.conduit.testkit.EchoServiceImpl;
import io.github.nitouge.conduit.testkit.TestRegistries;
import io.github.nitouge.conduit.testkit.TestReset;
import io.github.nitouge.conduit.testkit.TestRpc;
import io.github.nitouge.conduit.testkit.TestServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 路由规则的端到端验证（真实Netty服务端 + 真实客户端 + 进程内注册中心）：
 *
 * <ul>
 *   <li>多版本共存：指定 version 后只落在该版本的实例上；</li>
 *   <li>灰度标签：按 tag 切流，且支持单次调用用 {@code RpcContext} 覆盖；</li>
 *   <li>同机房优先：同机房有实例就只去同机房，没有才跨机房；</li>
 *   <li>版本写错：明确报错并带上候选实例的实际属性，而不是悄悄打到别的版本上。</li>
 * </ul>
 */
class RoutingIntegrationTest {

    private static final long TIMEOUT_MILLIS = 3_000;

    @BeforeEach
    void reset() {
        TestReset.all();
    }

    @AfterEach
    void clearContext() {
        RpcContext.removeContext();
    }

    @Test
    void versionRoutesOnlyToMatchingInstances() {
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        try (TestServer v1 = TestServer.of(registry)
                .export(EchoService.class, new EchoServiceImpl("v1"), "1.0.0", null, null, null).start();
             TestServer v2 = TestServer.of(registry)
                     .export(EchoService.class, new EchoServiceImpl("v2"), "2.0.0", null, null, null).start();
             TestRpc rpc = TestRpc.create()) {

            EchoService legacy = rpc.proxy(EchoService.class, registry, TIMEOUT_MILLIS,
                    new ReferenceOptions("1.0.0", null, null));
            for (int i = 0; i < 6; i++) {
                assertEquals("v1", legacy.nodeId(), "声明1.0.0时不应打到2.0.0实例");
            }

            EchoService next = rpc.proxy(EchoService.class, registry, TIMEOUT_MILLIS,
                    new ReferenceOptions("2.0.0", null, null));
            for (int i = 0; i < 6; i++) {
                assertEquals("v2", next.nodeId(), "声明2.0.0时不应打到1.0.0实例");
            }
        }
    }

    @Test
    void versionWithMultipleInstancesStillUsesOnlyThatVersion() {
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        try (TestServer v1a = TestServer.of(registry)
                .export(EchoService.class, new EchoServiceImpl("v1a"), "1.0.0", null, null, null).start();
             TestServer v1b = TestServer.of(registry)
                     .export(EchoService.class, new EchoServiceImpl("v1b"), "1.0.0", null, null, null).start();
             TestServer v2 = TestServer.of(registry)
                     .export(EchoService.class, new EchoServiceImpl("v2"), "2.0.0", null, null, null).start();
             TestRpc rpc = TestRpc.create()) {

            EchoService echo = rpc.proxy(EchoService.class, registry, TIMEOUT_MILLIS, "failover", "roundrobin",
                    ClusterOptions.defaults(), new ReferenceOptions("1.0.0", null, null));

            Set<String> visited = new HashSet<>();
            for (int i = 0; i < 8; i++) {
                visited.add(echo.nodeId());
            }
            assertEquals(Set.of("v1a", "v1b"), visited, "1.0.0 的同版本多实例应被轮询覆盖，2.0.0 不参与");
        }
    }

    @Test
    void tagRoutesToGrayInstanceAndCanBeOverriddenPerCall() {
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        try (TestServer stable = TestServer.of(registry)
                .export(EchoService.class, new EchoServiceImpl("stable"), "1.0.0", null, null, null).start();
             TestServer gray = TestServer.of(registry)
                     .export(EchoService.class, new EchoServiceImpl("gray"), "1.0.0", null, "gray", null).start();
             TestRpc rpc = TestRpc.create()) {

            EchoService canary = rpc.proxy(EchoService.class, registry, TIMEOUT_MILLIS,
                    new ReferenceOptions("1.0.0", null, "gray"));
            for (int i = 0; i < 5; i++) {
                assertEquals("gray", canary.nodeId(), "带gray标签的引用应只打灰度实例");
            }

            // 单次调用覆盖：同一个代理，用RpcContext临时改标签
            EchoService plain = rpc.proxy(EchoService.class, registry, TIMEOUT_MILLIS,
                    new ReferenceOptions("1.0.0", null, null));
            RpcContext.getContext().setAttachment(RpcAttachments.TAG, "gray");
            assertEquals("gray", plain.nodeId(), "RpcContext的tag应覆盖引用上的默认值");
        }
    }

    @Test
    void sameZoneIsPreferredWhenAvailable() {
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        // 消费方机房是"这个进程部署在哪"，天然是配置而非常量上下文：用配置注入，避免每次调用都要重设
        System.setProperty("conduit.router.zone", "sh-1");
        try (TestServer bj = TestServer.of(registry)
                .export(EchoService.class, new EchoServiceImpl("bj"), "1.0.0", null, null, "bj-1").start();
             TestServer sh = TestServer.of(registry)
                     .export(EchoService.class, new EchoServiceImpl("sh"), "1.0.0", null, null, "sh-1").start();
             TestRpc rpc = TestRpc.create()) {

            EchoService echo = rpc.proxy(EchoService.class, registry, TIMEOUT_MILLIS,
                    new ReferenceOptions("1.0.0", null, null));

            for (int i = 0; i < 6; i++) {
                assertEquals("sh", echo.nodeId(), "同机房优先：调用方在sh-1时应只选sh-1的实例");
            }
        } finally {
            System.clearProperty("conduit.router.zone");
        }
    }

    @Test
    void unknownVersionFailsFastWithActionableMessage() {
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        try (TestServer v1 = TestServer.of(registry)
                .export(EchoService.class, new EchoServiceImpl("v1"), "1.0.0", null, null, null).start();
             TestRpc rpc = TestRpc.create()) {

            EchoService echo = rpc.proxy(EchoService.class, registry, TIMEOUT_MILLIS,
                    new ReferenceOptions("9.9.9", null, null));

            RpcException error = assertThrows(RpcException.class, echo::nodeId);
            assertTrue(error.getMessage().contains("没有匹配的路由实例"), error.getMessage());
            assertTrue(error.getMessage().contains("version=9.9.9"), "错误应带上写错的约束: " + error.getMessage());
            assertTrue(error.getMessage().contains("1.0.0"), "错误应带上候选的实际版本: " + error.getMessage());
            assertEquals(1, registry.discover(EchoService.class.getName()).size(),
                    "路由只应过滤候选，不改动注册中心内容");
        }
    }

    @Test
    void untaggedReferenceStillSeesEveryVersion() {
        // 未声明任何条件的老用法必须保持原样：所有实例都是候选
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        try (TestServer v1 = TestServer.of(registry)
                .export(EchoService.class, new EchoServiceImpl("v1"), "1.0.0", null, null, null).start();
             TestServer v2 = TestServer.of(registry)
                     .export(EchoService.class, new EchoServiceImpl("v2"), "2.0.0", null, null, null).start();
             TestRpc rpc = TestRpc.create()) {

            EchoService echo = rpc.proxy(EchoService.class, registry, TIMEOUT_MILLIS, "failover", "roundrobin");

            Set<String> visited = new HashSet<>();
            for (int i = 0; i < 8; i++) {
                visited.add(echo.nodeId());
            }
            assertEquals(Set.of("v1", "v2"), visited, "不声明版本时不参与路由过滤");
        }
    }
}
