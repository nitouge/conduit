package io.github.nitouge.conduit.it;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcContext;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.testkit.TestReset;
import io.github.nitouge.conduit.testkit.TestRpc;
import io.github.nitouge.conduit.testkit.TestServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 治理类Filter的端到端验证（真实Netty + 真实序列化，attachment要真的跨进程）：
 *
 * <ul>
 *   <li>幂等键：同键的重复请求在提供侧只执行一次，第二次直接拿第一次的结果；</li>
 *   <li>QPS限流：超过每秒令牌数立即快速失败，不会排到超时。</li>
 * </ul>
 */
class GovernanceFiltersIntegrationTest {

    private static final long TIMEOUT_MILLIS = 3_000;
    private static final String RATE_LIMIT_ENABLED = "conduit.filter.ratelimit.enabled";

    @BeforeEach
    void reset() {
        TestReset.all();
    }

    @AfterEach
    void clearConfig() {
        System.clearProperty(RATE_LIMIT_ENABLED);
        TestReset.filters();
        RpcContext.removeContext();
    }

    @Test
    void sameIdempotencyKeyExecutesOnceAcrossRepeatedRequests() {
        CounterImpl impl = new CounterImpl();
        try (TestServer server = TestServer.start(Counter.class, impl);
             TestRpc rpc = TestRpc.create()) {

            Counter counter = rpc.proxy(Counter.class, server.meta(Counter.class), TIMEOUT_MILLIS);

            // 模拟 failover 重放：同一个幂等键被发了两次
            RpcContext.getContext().setAttachment(RpcAttachments.IDEMPOTENCY_KEY, "order-42");
            String first = counter.create("42");
            RpcContext.getContext().setAttachment(RpcAttachments.IDEMPOTENCY_KEY, "order-42");
            String second = counter.create("42");

            assertEquals("created:1:42", first);
            assertEquals(first, second, "重复请求应拿到第一次的结果");
            assertEquals(1, impl.executions(), "同一幂等键在提供侧只能真正执行一次");
        }
    }

    @Test
    void differentKeysStillExecuteSeparately() {
        CounterImpl impl = new CounterImpl();
        try (TestServer server = TestServer.start(Counter.class, impl);
             TestRpc rpc = TestRpc.create()) {

            Counter counter = rpc.proxy(Counter.class, server.meta(Counter.class), TIMEOUT_MILLIS);

            assertEquals("created:1:a", counter.create("a"));
            assertEquals("created:2:b", counter.create("b"));
            assertEquals(2, impl.executions());
        }
    }

    @Test
    void qpsLimitRejectsExcessRequestsFast() {
        // 限流默认关闭，这里显式开启；provider 在创建时读取filter开关，所以先改配置再起服务
        System.setProperty(RATE_LIMIT_ENABLED, "true");
        System.setProperty("conduit.filter.ratelimit." + LimitService.class.getName() + "#ping.qps", "2");
        TestReset.filters();

        LimitServiceImpl impl = new LimitServiceImpl();
        try (TestServer server = TestServer.start(LimitService.class, impl);
             TestRpc rpc = TestRpc.create()) {

            LimitService service = rpc.proxy(LimitService.class, server.meta(LimitService.class), TIMEOUT_MILLIS);

            assertEquals("pong", service.ping());
            assertEquals("pong", service.ping());

            RpcException error = assertThrows(RpcException.class, service::ping);
            assertTrue(error.getMessage().contains("RateLimit"),
                    "超限应立即失败并说明原因: " + error.getMessage());
            assertEquals(2, impl.executions(), "被限流的请求不应进入业务方法");
        } finally {
            System.clearProperty("conduit.filter.ratelimit." + LimitService.class.getName() + "#ping.qps");
        }
    }

    /** 幂等测试用服务：返回"created:第几次:id" */
    public interface Counter {

        String create(String id);
    }

    /** 记录真正进入业务方法的次数 */
    public static final class CounterImpl implements Counter {

        private final AtomicInteger executions = new AtomicInteger();

        @Override
        public String create(String id) {
            return "created:" + executions.incrementAndGet() + ":" + id;
        }

        public int executions() {
            return executions.get();
        }
    }

    /** 限流测试用服务 */
    public interface LimitService {

        String ping();
    }

    public static final class LimitServiceImpl implements LimitService {

        private final AtomicInteger executions = new AtomicInteger();

        @Override
        public String ping() {
            executions.incrementAndGet();
            return "pong";
        }

        public int executions() {
            return executions.get();
        }
    }
}
