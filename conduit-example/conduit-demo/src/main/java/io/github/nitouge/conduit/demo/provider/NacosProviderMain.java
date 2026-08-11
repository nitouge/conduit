package io.github.nitouge.conduit.demo.provider;

import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.server.RpcServer;

/**
 * M2示例 · 提供者：暴露 {@link HelloService} 并注册到Nacos（127.0.0.1:8848）。
 *
 * <p>需先启动Nacos。运行后保持存活，等待消费者经服务发现调用。</p>
 */
public class NacosProviderMain {

    public static void main(String[] args) {
        Registry registry = Registries.getRegistry("127.0.0.1:8848");
        RpcServer server = new RpcServer("127.0.0.1", 20880, registry);
        server.export(HelloService.class, new HelloServiceImpl());
        server.start();
        System.out.println("Provider(M2) 已启动并注册到Nacos，监听127.0.0.1:20880");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();      // 先 Nacos注销
            registry.destroy();  // 再关闭Nacos客户端
        }));
    }
}
