package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.demo.provider.HelloServiceImpl;
import io.github.nitouge.conduit.server.RpcServer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 端到端验收：在同一个JVM内启动provider + consumer，跑通一次真实的直连远程调用。
 *
 * <p>这是退出标准：start provider, call HelloService.sayHello, assert返回值。</p>
 */
class DirectCallTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 20890; // 与demo main的20880错开，避免本地占用冲突

    @Test
    void directRemoteCallReturnsExpectedResult() throws Exception {
        RpcServer server = new RpcServer(HOST, PORT);
        server.export(HelloService.class, new HelloServiceImpl());
        server.start();

        RpcClient client = new RpcClient();
        try {
            ServiceMeta target = new ServiceMeta(HelloService.class.getName(), HOST, PORT);
            HelloService hello = RpcClientProxy.create(HelloService.class, target, client, 3000);

            String result = hello.sayHello("Conduit");

            assertEquals("Hello, Conduit from Conduit!", result);
        } finally {
            client.close();
            server.close();
        }
    }
}
