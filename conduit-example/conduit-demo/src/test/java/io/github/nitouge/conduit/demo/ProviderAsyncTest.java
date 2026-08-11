package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.server.RpcServer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * provider端非阻塞异步：服务方法返回一个稍后才完成的CompletableFuture（在另一线程延迟完成），
 * 服务端不再get()阻塞业务线程，而是在该future完成时回写响应。
 *
 * <p>验证延迟完成的provider future能被正确等待并回传，且多个并发异步调用都能完成。</p>
 */
class ProviderAsyncTest {

    private static final String HOST = "127.0.0.1";

    /** 真异步实现：sayHelloAsync在另一线程延迟200ms后才完成（非completedFuture） */
    private static HelloService delayedProvider() {
        return new HelloService() {
            @Override
            public String sayHello(String name) {
                return "sync-" + name;
            }

            @Override
            public CompletableFuture<String> sayHelloAsync(String name) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "async-" + name;
                });
            }
        };
    }

    @Test
    void delayedProviderFutureCompletesAndConcurrentCallsAllReturn() throws Exception {
        RpcServer server = new RpcServer(HOST, 20907);
        server.export(HelloService.class, delayedProvider());
        server.start();

        RpcClient client = new RpcClient();
        try {
            ServiceMeta target = new ServiceMeta(HelloService.class.getName(), HOST, 20907);
            HelloService hello = RpcClientProxy.create(HelloService.class, target, client, 5000);

            // 两个并发异步调用，provider端各自延迟完成
            CompletableFuture<String> f1 = hello.sayHelloAsync("A");
            CompletableFuture<String> f2 = hello.sayHelloAsync("B");

            assertEquals("async-A", f1.get(5, TimeUnit.SECONDS));
            assertEquals("async-B", f2.get(5, TimeUnit.SECONDS));
        } finally {
            client.close();
            server.close();
        }
    }
}
