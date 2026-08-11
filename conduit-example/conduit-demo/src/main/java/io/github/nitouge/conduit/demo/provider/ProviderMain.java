package io.github.nitouge.conduit.demo.provider;

import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.demo.consumer.ConsumerMain;
import io.github.nitouge.conduit.server.RpcServer;

/**
 * M1直连示例 · 提供者：暴露 {@link HelloService} 并监听20880端口。
 *
 * <p>运行后保持存活（Netty工作线程为非守护线程）。先启动它，再运行 {@link ConsumerMain}。</p>
 */
public class ProviderMain {

    public static void main(String[] args) {
        RpcServer server = new RpcServer("127.0.0.1", 20880);
        server.export(HelloService.class, new HelloServiceImpl());
        server.start();
        System.out.println("Provider已启动，监听127.0.0.1:20880，等待调用...");

        // 优雅停机：Ctrl+C时关闭
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    }
}
