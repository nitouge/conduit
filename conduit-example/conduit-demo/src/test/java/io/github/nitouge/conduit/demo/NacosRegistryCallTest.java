package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.demo.provider.HelloServiceImpl;
import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.server.RpcServer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 端到端验收：provider注册到Nacos、consumer经服务发现调用（不写死地址）。
 *
 * <p>需要一个运行中的Nacos（127.0.0.1:8848）。若Nacos不可用，则通过
 * {@link Assumptions#abort} 跳过（保证无Nacos的CI不会因此失败）。</p>
 */
class NacosRegistryCallTest {

    private static final String NACOS = "127.0.0.1:8848";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 20882;

    @Test
    void registryBasedCallWorks() throws Exception {
        Registry registry;
        try {
            registry = Registries.getRegistry(NACOS);
            // 探活：对一个不存在的服务做一次发现，确认Nacos真正可达（不可用则跳过）
            registry.discover("__conduit_probe__");
        } catch (Throwable t) {
            Assumptions.abort("Nacos不可用(" + NACOS + ")，跳过M2端到端测试: " + t);
            return; // 实际不可达
        }

        RpcServer server = new RpcServer(HOST, PORT, registry);
        RpcClient client = new RpcClient();
        try {
            server.export(HelloService.class, new HelloServiceImpl());
            server.start();

            HelloService hello = RpcClientProxy.create(HelloService.class, registry, client, 3000);

            // 容忍注册 -> 可发现的传播延迟：重试若干次
            String result = null;
            RpcException last = null;
            for (int i = 0; i < 20; i++) {
                try {
                    result = hello.sayHello("Nacos");
                    break;
                } catch (RpcException e) {
                    last = e; // 通常是无可用服务提供者，等待注册生效后重试
                    Thread.sleep(300);
                }
            }
            if (result == null && last != null) {
                throw last;
            }
            assertEquals("Hello, Nacos from Conduit!", result);
        } finally {
            client.close();
            server.close();
            registry.destroy();
        }
    }
}
