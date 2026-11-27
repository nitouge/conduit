package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.RequestHandler;
import io.github.nitouge.conduit.remoting.ServerCallRegistry;
import io.github.nitouge.conduit.remoting.TestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 取消与 deadline 的端到端测试（真实 Netty 收发）：
 *
 * <ul>
 *   <li>客户端超时 -> 下发 CANCEL -> 服务端中断正在执行业务的线程；</li>
 *   <li>请求携带的 deadline 到点 -> 服务端<b>自己</b>中断，并回一帧说明原因；</li>
 *   <li>deadline 在到达时就已过期 -> 业务方法根本不执行；</li>
 *   <li>正常完成的请求不会在在途表里留下残留。</li>
 * </ul>
 */
class CancelDeadlineIntegrationTest {

    @AfterEach
    void clearRegistry() {
        ServerCallRegistry.reset();
    }

    @Test
    void clientTimeoutCancelsServerSideExecution() throws Exception {
        int port = TestSupport.freePort();
        WorkerProbe probe = new WorkerProbe();
        NettyServer server = new NettyServer(TestSupport.HOST, port, probe.handler(5_000));
        server.start();
        NettyClient client = new NettyClient(ClientOptions.builder().reconnectEnabled(false).build());
        try {
            long timeoutMillis = 300;
            Throwable cause = sendFailing(client, port, TestSupport.request("slow"), timeoutMillis);

            assertEquals(TimeoutException.class, cause.getClass(),
                    "客户端应因本地超时失败，实际: " + cause);            TestSupport.awaitTrue("服务端业务线程应被 CANCEL 中断", 3_000, probe.interrupted::get);
            TestSupport.awaitTrue("在途表应清空", 3_000, () -> ServerCallRegistry.activeCount() == 0);
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void expiredDeadlineNeverReachesBusinessMethod() throws Exception {
        int port = TestSupport.freePort();
        WorkerProbe probe = new WorkerProbe();
        NettyServer server = new NettyServer(TestSupport.HOST, port, probe.handler(5_000));
        server.start();
        NettyClient client = new NettyClient(ClientOptions.builder().reconnectEnabled(false).build());
        try {
            RpcRequest request = TestSupport.request("slow");
            RpcAttachments.setDeadline(request, System.currentTimeMillis() - 50);

            RpcResponse response = client.send(request, TestSupport.target(port), 3_000)
                    .get(3_000, TimeUnit.MILLISECONDS);

            assertTrue(response.hasException(), "已过期的deadline应直接回错误");
            assertTrue(String.valueOf(response.getException()).contains("deadline"),
                    "错误原因应说明是deadline: " + response.getException());
            assertEquals(0, probe.executions.get(), "业务方法不应被执行");
            assertEquals(0, ServerCallRegistry.activeCount(), "不应在在途表留下残留");
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void serverSelfInterruptsWhenDeadlineReached() throws Exception {
        int port = TestSupport.freePort();
        WorkerProbe probe = new WorkerProbe();
        NettyServer server = new NettyServer(TestSupport.HOST, port, probe.handler(10_000));
        server.start();
        NettyClient client = new NettyClient(ClientOptions.builder().reconnectEnabled(false).build());
        try {
            RpcRequest request = TestSupport.request("slow");
            // 客户端愿意等10s，但deadline只给300ms：服务端必须到点自己停下来，而不是跑满10s
            RpcAttachments.setDeadline(request, System.currentTimeMillis() + 300);

            // 先做一次快速调用把连接建好：否则建连+握手可能就吃掉这300ms，请求到达时deadline已过，
            // 走的会是"过期直接拒绝"分支，测不到"到点自我中断"
            assertEquals("ok:ping", TestSupport.call(client, port, "ping").getData());

            RpcResponse response = client.send(request, TestSupport.target(port), 10_000)
                    .get(5_000, TimeUnit.MILLISECONDS);

            assertTrue(response.hasException(), "deadline中止应体现为错误响应");
            assertTrue(probe.started.await(2, TimeUnit.SECONDS), "业务方法应已开始执行");
            assertTrue(probe.interrupted.get(), "到点后服务端应中断业务线程");
            assertEquals(0, ServerCallRegistry.activeCount(), "在途表应清空");
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void normalCallLeavesNoInFlightEntry() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler());
        server.start();
        NettyClient client = new NettyClient(ClientOptions.builder().reconnectEnabled(false).build());
        try {
            assertEquals("ok:ping", TestSupport.call(client, port, "ping").getData());
            assertEquals(0, ServerCallRegistry.activeCount(), "正常请求结束后不应残留");
        } finally {
            client.close();
            server.close();
        }
    }

    /** 让客户端以指定超时发起调用，返回真正的失败原因（ExecutionException 的 cause 或超时本身） */
    private static Throwable sendFailing(NettyClient client, int port, RpcRequest request, long timeoutMillis) {
        try {
            RpcResponse response = client.send(request, TestSupport.target(port), timeoutMillis)
                    .get(TestSupport.CALL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            throw new AssertionError("期望调用失败，但成功了: " + response);
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
            return e.getCause();
        } catch (TimeoutException e) {
            return e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("调用被中断", e);
        }
    }

    /**
     * 执行业务方法的探针：记录是否开始执行、执行了几次、是否被中断。
     *
     * <p>业务方法用 {@code sleep} 模拟慢调用——它是可中断的，正是「服务端到点自我中断」要验证的
     * 那个行为；不响应中断的业务代码框架也救不了，不在断言范围内。</p>
     */
    private static final class WorkerProbe {

        private final AtomicInteger executions = new AtomicInteger();
        private final AtomicBoolean interrupted = new AtomicBoolean(false);
        private final CountDownLatch started = new CountDownLatch(1);

        private RequestHandler handler(long sleepMillis) {
            return request -> {
                // ping 走快路径：测试用它预热连接，不占据业务线程
                if ("ping".equals(request.getMethodName())) {
                    return java.util.concurrent.CompletableFuture.completedFuture(
                            RpcResponse.success("ok:ping"));
                }
                executions.incrementAndGet();
                started.countDown();
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    Thread.currentThread().interrupt();
                }
                return java.util.concurrent.CompletableFuture.completedFuture(
                        RpcResponse.success("done:" + request.getMethodName()));
            };
        }
    }
}
