package io.github.nitouge.conduit.it;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreaker;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreakerRegistry;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.testkit.Await;
import io.github.nitouge.conduit.testkit.EchoService;
import io.github.nitouge.conduit.testkit.EchoServiceImpl;
import io.github.nitouge.conduit.testkit.Ports;
import io.github.nitouge.conduit.testkit.TestRegistries;
import io.github.nitouge.conduit.testkit.TestReset;
import io.github.nitouge.conduit.testkit.TestRpc;
import io.github.nitouge.conduit.testkit.TestServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多节点集群协作（真实Netty监听 + 真实注册中心推送）：
 * 负载均衡分流、节点下线后不再被选中、选中死节点时FailOver切活节点、连续失败后熔断快速失败。
 *
 * <p>注册中心用testkit的进程内实现（支持注册/注销/订阅推送），但<b>服务端与客户端都是真实网络栈</b>。</p>
 */
class MultiNodeClusterIntegrationTest {

    private static final long TIMEOUT_MILLIS = 3_000;

    @BeforeEach
    void reset() {
        TestReset.all();
    }

    /** roundrobin：9次调用应覆盖全部3个节点，证明真实负载分流 */
    @Test
    void roundRobinSpreadsAcrossThreeRealNodes() {
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        try (TestServer n1 = TestServer.of(registry).export(EchoService.class, new EchoServiceImpl("n1")).start();
             TestServer n2 = TestServer.of(registry).export(EchoService.class, new EchoServiceImpl("n2")).start();
             TestServer n3 = TestServer.of(registry).export(EchoService.class, new EchoServiceImpl("n3")).start();
             TestRpc rpc = TestRpc.create()) {

            EchoService echo = rpc.proxy(EchoService.class, registry, TIMEOUT_MILLIS, "failover", "roundrobin");

            Set<String> visited = new HashSet<>();
            for (int i = 0; i < 9; i++) {
                visited.add(echo.nodeId());
            }
            assertEquals(Set.of("n1", "n2", "n3"), visited, "roundrobin应覆盖全部节点");
            assertEquals("ok:hi", echo.echo("hi"));
        }
    }

    /** 节点下线：注销后订阅推送更新，后续调用只落在存活节点 */
    @Test
    void consumerStopsUsingNodeAfterItLeaves() {
        TestRegistries.MutableRegistry registry = TestRegistries.mutable();
        try (TestServer n1 = TestServer.of(registry).export(EchoService.class, new EchoServiceImpl("n1")).start();
             TestRpc rpc = TestRpc.create()) {
            TestServer n2 = TestServer.of(registry).export(EchoService.class, new EchoServiceImpl("n2")).start();
            EchoService echo = rpc.proxy(EchoService.class, registry, TIMEOUT_MILLIS, "failover", "roundrobin");

            Set<String> before = new HashSet<>();
            for (int i = 0; i < 4; i++) {
                before.add(echo.nodeId());
            }
            assertEquals(Set.of("n1", "n2"), before);

            n2.close(); // 先注销、再关监听
            Await.until("n2下线后注册中心应只剩1个节点",
                    () -> registry.discover(EchoService.class.getName()).size() == 1);

            for (int i = 0; i < 6; i++) {
                assertEquals("n1", echo.nodeId(), "下线节点不应再被选中");
            }
        }
    }

    /** 容错：候选里含一个死节点，roundrobin首选它 → FailOver切到活节点并成功 */
    @Test
    void failoverFromDeadNodeToLiveNode() {
        try (TestServer live = TestServer.start(EchoService.class, new EchoServiceImpl("live")); TestRpc rpc = TestRpc.create()) {
            ServiceMeta dead = new ServiceMeta(EchoService.class.getName(), "127.0.0.1", Ports.freePort());
            Registry registry = TestRegistries.fixed(List.of(dead, live.meta(EchoService.class)));

            // roundrobin index0 = dead；failover 自动切到 live
            EchoService echo = rpc.proxy(EchoService.class, registry, 1_000, "failover", "roundrobin");
            assertEquals("live", echo.nodeId());
        }
    }

    /** 熔断：同一死节点连续失败达到阈值后打开，后续调用快速失败（不再等连接超时） */
    @Test
    void breakerOpensAndFailsFastAfterRepeatedFailures() {
        ServiceMeta dead = new ServiceMeta(EchoService.class.getName(), "127.0.0.1", Ports.freePort());
        Registry registry = TestRegistries.fixed(List.of(dead));

        try (TestRpc rpc = TestRpc.create()) {
            EchoService echo = rpc.proxy(EchoService.class, registry, 800, "failfast", "random");

            for (int i = 0; i < CircuitBreakerRegistry.options().getFailureThreshold(); i++) {
                assertThrows(RuntimeException.class, echo::nodeId, "目标端口没人监听，调用应失败");
            }
            assertEquals(CircuitBreaker.State.OPEN, CircuitBreakerRegistry.state(dead), "达到阈值后应处于打开状态");

            long start = System.nanoTime();
            assertThrows(RuntimeException.class, echo::nodeId);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMillis < 200, "熔断打开后应快速失败，实际耗时 " + elapsedMillis + "ms");
        }
    }
}
