package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.server.RpcServer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 失败路径鲁棒性验收（对抗性审查后补充）。
 */
class RobustnessTest {

    private static final String HOST = "127.0.0.1";

    /**
     * provider未启动时连接失败，调用方应拿到框架自有的 {@link RpcException}（RuntimeException），
     * 而不是JDK动态代理包装出的 {@code UndeclaredThrowableException} 或裸的受检异常。
     */
    @Test
    void connectionRefusedThrowsRpcException() {
        RpcClient client = new RpcClient();
        try {
            ServiceMeta dead = new ServiceMeta(HelloService.class.getName(), HOST, 20999); // 无人监听
            HelloService hello = RpcClientProxy.create(HelloService.class, dead, client, 1000);

            RpcException ex = assertThrows(RpcException.class, () -> hello.sayHello("x"));
            assertNotNull(ex.getMessage());
        } finally {
            client.close();
        }
    }

    /**
     * 服务端返回不可序列化对象，响应编码在I/O线程失败时应回传可序列化的兜底错误，
     * 调用方拿到 {@link RpcException} 而不是一直等到超时。
     */
    @Test
    void nonSerializableResultReturnsErrorNotTimeout() {
        RpcServer server = new RpcServer(HOST, 20991);
        server.export(BadService.class, new BadServiceImpl());
        server.start();

        RpcClient client = new RpcClient();
        try {
            ServiceMeta target = new ServiceMeta(BadService.class.getName(), HOST, 20991);
            BadService bad = RpcClientProxy.create(BadService.class, target, client, 3000);

            assertThrows(RpcException.class, bad::badReturn);
        } finally {
            client.close();
            server.close();
        }
    }

    /** 返回不可序列化对象的测试服务 */
    public interface BadService {
        Object badReturn();
    }

    public static class BadServiceImpl implements BadService {
        @Override
        public Object badReturn() {
            return new Object(); // 不实现Serializable
        }
    }
}
