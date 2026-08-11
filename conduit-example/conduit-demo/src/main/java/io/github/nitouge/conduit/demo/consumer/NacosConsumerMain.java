package io.github.nitouge.conduit.demo.consumer;

import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.demo.provider.NacosProviderMain;
import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;

/**
 * M2示例 · 消费者：从Nacos发现 {@link HelloService} 提供者并调用——<b>不写死地址</b>。
 *
 * <p>需先启动Nacos与 {@link NacosProviderMain}。</p>
 */
public class NacosConsumerMain {

    public static void main(String[] args) {
        Registry registry = Registries.getRegistry("127.0.0.1:8848");
        RpcClient client = new RpcClient();
        try {
            HelloService helloService = RpcClientProxy.create(HelloService.class, registry, client, 3000);
            String result = helloService.sayHello("Nacos");
            System.out.println("收到远程返回: " + result);
        } finally {
            client.close();
            registry.destroy();
        }
    }
}
