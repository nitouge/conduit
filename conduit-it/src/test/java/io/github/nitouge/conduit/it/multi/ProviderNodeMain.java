package io.github.nitouge.conduit.it.multi;

import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.server.RpcServer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * 多JVM用例的provider进程：真实 {@link RpcServer} + 真实Nacos注册。
 *
 * <p>参数：{@code <port> <nodeId> <nacosAddr>}。启动完成后打印 {@code READY}；
 * 从stdin收到 {@code STOP} 时<b>优雅下线</b>（先注销再关监听），让测试能观察实例数变化。</p>
 */
public final class ProviderNodeMain {

    private ProviderNodeMain() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        String nodeId = args[1];
        String nacosAddress = args[2];

        Registry registry = Registries.getRegistry("nacos", nacosAddress);
        RpcServer server = new RpcServer("127.0.0.1", port, registry);
        server.export(NodeService.class, new NodeServiceImpl(nodeId));
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            registry.destroy();
        }, "it-provider-shutdown"));

        System.out.println("READY " + nodeId + " port=" + port);
        System.out.flush();

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = stdin.readLine()) != null) {
            if ("STOP".equalsIgnoreCase(line.trim())) {
                server.close();       // 注销 + 关闭监听
                registry.destroy();
                System.out.println("STOPPED " + nodeId);
                System.out.flush();
                System.exit(0);
            }
        }
    }
}
