package io.github.nitouge.conduit.it;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcContext;
import io.github.nitouge.conduit.testkit.TestReset;
import io.github.nitouge.conduit.testkit.TestRpc;
import io.github.nitouge.conduit.testkit.TestServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 链路追踪的端到端验证：span 父子关系能跨服务延伸，异步回调仍能读到上下文。
 */
class TracePropagationIntegrationTest {

    private static final long TIMEOUT_MILLIS = 3_000;

    @BeforeEach
    void reset() {
        TestReset.all();
    }

    @AfterEach
    void clear() {
        RpcContext.removeContext();
    }

    /** 三跳链路：消费方 → B → C，C 回传它看到的 trace/span/parent */
    @Test
    void spanParentChainExtendsAcrossServices() {
        try (TestServer serverC = TestServer.start(TraceService.class, new TraceServiceImpl());
             TestRpc rpcB = TestRpc.create()) {

            TraceService c = rpcB.proxy(TraceService.class, serverC.meta(TraceService.class), TIMEOUT_MILLIS);

            try (TestServer serverB = TestServer.start(RelayService.class, new RelayServiceImpl(c));
                 TestRpc rpcA = TestRpc.create()) {

                RelayService b = rpcA.proxy(RelayService.class, serverB.meta(RelayService.class), TIMEOUT_MILLIS);

                RpcContext.getContext().setAttachment(RpcAttachments.TRACE_ID, "trace-chain-1");
                String ids = b.relay();

                String[] parts = ids.split("\\|", -1);
                assertEquals("trace-chain-1", parts[0], "traceId应贯穿整条链路");
                assertNotNull(parts[1], "C应收到一个spanId");
                assertNotEquals("null", parts[2], "C的span应该有父节点（B发起调用时新建的那个span）");
                assertNotEquals(parts[1], parts[2], "spanId与父spanId不能是同一个");
            }
        }
    }

    /** 异步调用：回调线程上仍能读到发起线程的 traceId */
    @Test
    void asyncCallbackKeepsTraceContext() throws Exception {
        try (TestServer server = TestServer.start(AsyncService.class, new AsyncServiceImpl());
             TestRpc rpc = TestRpc.create()) {

            AsyncService service = rpc.proxy(AsyncService.class, server.meta(AsyncService.class), TIMEOUT_MILLIS);

            RpcContext.getContext().setAttachment(RpcAttachments.TRACE_ID, "trace-async-1");

            AtomicReference<String> traceInCallback = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            service.later().thenApply(value -> {
                traceInCallback.set(RpcContext.getContext().getTraceId());
                done.countDown();
                return value;
            });

            assertTrue(done.await(5, TimeUnit.SECONDS), "异步回调应在期限内完成");
            assertEquals("trace-async-1", traceInCallback.get(),
                    "回调跑在回调线程池上，仍应看到发起线程的traceId");
        }
    }

    /** 回传它看到的追踪字段，格式 traceId|spanId|parentSpanId */
    public interface TraceService {

        String ids();
    }

    public static final class TraceServiceImpl implements TraceService {

        @Override
        public String ids() {
            RpcContext context = RpcContext.getContext();
            return context.getAttachment(RpcAttachments.TRACE_ID) + "|"
                    + context.getAttachment(RpcAttachments.SPAN_ID) + "|"
                    + context.getAttachment(RpcAttachments.PARENT_SPAN_ID);
        }
    }

    /** 收到请求后再调下游一次，用来验证父span在服务内部继续传递 */
    public interface RelayService {

        String relay();
    }

    public static final class RelayServiceImpl implements RelayService {

        private final TraceService downstream;

        private RelayServiceImpl(TraceService downstream) {
            this.downstream = downstream;
        }

        @Override
        public String relay() {
            return downstream.ids();
        }
    }

    /** 提供侧异步返回，确保消费方回调注册发生在结果到达之前 */
    public interface AsyncService {

        CompletableFuture<String> later();
    }

    public static final class AsyncServiceImpl implements AsyncService {

        @Override
        public CompletableFuture<String> later() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "done";
            });
        }
    }
}
