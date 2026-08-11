package io.github.nitouge.conduit.demo.consumer;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.demo.provider.ProviderMain;

/**
 * M1直连示例 · 消费者：连接provider，像调本地方法一样调用远程服务。
 *
 * <p>需先运行 {@link ProviderMain}。</p>
 */
public class ConsumerMain {

    public static void main(String[] args) {
        // System.setProperty("jdk.proxy.ProxyGenerator.saveGeneratedFiles", "true");

        RpcClient client = new RpcClient();
        try {
            // M1直连：直接给定目标地址
            ServiceMeta target = new ServiceMeta(HelloService.class.getName(), "127.0.0.1", 20880);
            HelloService helloService = RpcClientProxy.create(HelloService.class, target, client, 3000);

            String result = helloService.sayHello("Conduit");
            System.out.println("收到远程返回: " + result);
        } finally {
            client.close();
        }
    }
}
