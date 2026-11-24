package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.RpcContext;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.demo.filter.CountingFilter;
import io.github.nitouge.conduit.server.RpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * RpcRequest.attachments + RpcContext端到端验收。
 *
 * <p>验证点：</p>
 * <ul>
 *   <li>消费侧在调用线程把traceId写入 {@link RpcContext}，调用后自动清空。</li>
 *   <li>提供侧在业务方法体内从 {@link RpcContext} 读到同一个traceId。</li>
 *   <li>调用结束后消费侧RpcContext已清空（一次性语义，不泄漏到同线程下次调用）。</li>
 * </ul>
 */
class AttachmentTest {

    private static final String HOST = "127.0.0.1";

    private static final int PORT = 20910;

    /** 提供侧在业务方法体里把读到的traceId写回（用于断言）。key区分调用便于并发。 */
    private static final ConcurrentMap<String, String> RECEIVED = new ConcurrentHashMap<>();

    /** 哨兵：表示提供侧读不到traceId（REQUEST真的没带attachments） */
    private static final String NONE = "<none>";

    /** provider实现：把traceId从RpcContext写入RECEIVED，并把请求体回传给客户端 */
    private static HelloService recorder() {
        return new HelloService() {
            @Override
            public String sayHello(String name) {
                String traceId = RpcContext.getContext().getAttachment("traceId");
                // ConcurrentHashMap不允许null值：用NONE哨兵表示提供侧未读到traceId
                RECEIVED.put(name, traceId == null ? NONE : traceId);
                return "Hello, " + name + " from Conduit!";
            }

            @Override
            public CompletableFuture<String> sayHelloAsync(String name) {
                return CompletableFuture.completedFuture(sayHello(name));
            }
        };
    }

    private RpcServer server;
    private RpcClient client;
    private HelloService proxy;

    @BeforeEach
    void setUp() {
        CountingFilter.reset(); // 顺便清掉其它测试残留
        RECEIVED.clear();
        server = new RpcServer(HOST, PORT);
        server.export(HelloService.class, recorder());
        server.start();

        client = new RpcClient();
        ServiceMeta target = new ServiceMeta(HelloService.class.getName(), HOST, PORT);
        proxy = RpcClientProxy.create(HelloService.class, target, client, 3000);
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void attachmentsPropagateThroughRpcRequestAndRpcContext() {
        // 调用前写入traceId到当前线程RpcContext
        RpcContext.getContext().setAttachment("traceId", "abc-123");

        String result = proxy.sayHello("attach");

        assertEquals("Hello, attach from Conduit!", result);
        // 提供侧业务方法体里读到了同一个traceId
        assertEquals("abc-123", RECEIVED.get("attach"), "提供侧应能从RpcContext读到消费侧写入的traceId");

        // 调用边界清空：消费侧RpcContext.removeContext()已在代理内调用，
        // 之后任何setAttachment都是新一次写入——验证attachments不残留到下一次调用。
        assertNull(RpcContext.getContext().getAttachment("traceId"), "调用结束后当前线程RpcContext已清空，traceId不应残留");
    }

    @Test
    void emptyContextCallStillGetsGeneratedTraceId() {
        // 没有手动设置traceId也能正常调用；TraceFilter会自动生成traceId并透传
        String result = proxy.sayHello("noAttach");
        assertEquals("Hello, noAttach from Conduit!", result);

        String received = RECEIVED.get("noAttach");
        assertNotNull(received, "TraceFilter应为未携带traceId的调用自动生成一个");
        assertEquals(32, received.length(), "自动生成的traceId应为32位十六进制");
        assertNull(RpcContext.getContext().getTraceId(), "调用结束后消费侧RpcContext已清空");
    }
}