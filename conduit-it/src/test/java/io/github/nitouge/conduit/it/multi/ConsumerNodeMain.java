package io.github.nitouge.conduit.it.multi;

import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;

/**
 * 多JVM用例的consumer进程：从Nacos发现provider，按roundrobin发若干次调用，
 * 每次打印命中的节点；便于测试进程断言"分流是否覆盖多个进程"。
 *
 * <p>参数：{@code <nacosAddr> <calls> <cluster> <loadbalance>}，输出 {@code CALL <i> <nodeId>}。</p>
 */
public final class ConsumerNodeMain {

    private ConsumerNodeMain() {
    }

    public static void main(String[] args) {
        String nacosAddress = args[0];
        int calls = Integer.parseInt(args[1]);
        String cluster = args[2];
        String loadBalance = args[3];

        Registry registry = Registries.getRegistry("nacos", nacosAddress);
        RpcClient client = new RpcClient("kryo");
        try {
            NodeService service = RpcClientProxy.create(
                    NodeService.class, registry, client, 3_000, cluster, loadBalance);
            for (int i = 0; i < calls; i++) {
                try {
                    System.out.println("CALL " + i + " " + service.whoAmI());
                } catch (RuntimeException e) {
                    System.out.println("CALL " + i + " ERROR " + e.getClass().getSimpleName());
                }
                System.out.flush();
            }
            System.out.println("DONE");
            System.out.flush();
        } finally {
            client.close();
            registry.destroy();
        }
    }
}
