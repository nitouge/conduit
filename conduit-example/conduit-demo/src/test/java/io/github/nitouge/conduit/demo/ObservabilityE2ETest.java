package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.RpcContext;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.server.RpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 可观测性端到端：TraceFilter自动生成/透传traceId，提供侧业务可从RpcContext读取。
 */
class ObservabilityE2ETest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 20912;

    /** 返回提供侧当前traceId的探针服务 */
    public interface TraceEchoService {
        String traceId();
    }

    public static class TraceEchoServiceImpl implements TraceEchoService {
        @Override
        public String traceId() {
            return RpcContext.getContext().getTraceId();
        }
    }

    private RpcServer server;
    private RpcClient client;
    private TraceEchoService proxy;

    @BeforeEach
    void setUp() {
        server = new RpcServer(HOST, PORT);
        server.export(TraceEchoService.class, new TraceEchoServiceImpl());
        server.start();

        client = new RpcClient();
        ServiceMeta target = new ServiceMeta(TraceEchoService.class.getName(), HOST, PORT);
        proxy = RpcClientProxy.create(TraceEchoService.class, target, client, 3000);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void userTraceIdIsPropagatedToProvider() {
        RpcContext.getContext().setTraceId("trace-e2e-1");

        assertEquals("trace-e2e-1", proxy.traceId());
        assertNull(RpcContext.getContext().getTraceId(), "消费侧上下文应在调用边界后清空");
    }

    @Test
    void missingTraceIdIsGeneratedAndPropagated() {
        String traceId = proxy.traceId();

        assertNotNull(traceId, "未设置traceId时TraceFilter应自动生成");
        assertEquals(32, traceId.length());
    }

    @Test
    void eachCallGetsDistinctGeneratedTraceId() {
        String first = proxy.traceId();
        String second = proxy.traceId();

        assertNotNull(first);
        assertNotNull(second);
        assertNotEquals(first, second, "未显式设置时每次调用应生成独立traceId");
    }
}
