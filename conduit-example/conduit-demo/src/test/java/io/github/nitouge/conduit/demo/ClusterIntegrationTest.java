package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreakerRegistry;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.demo.provider.HelloServiceImpl;
import io.github.nitouge.conduit.registry.NotifyListener;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.server.RpcServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 端到端：异步调用、集群容错（杀节点FailOver成功）、负载均衡分流——均走真实Netty。
 */
class ClusterIntegrationTest {

    private static final String HOST = "127.0.0.1";

    @BeforeEach
    void resetBreakers() {
        CircuitBreakerRegistry.clear();
    }

    /** 内存注册中心：discover固定返回给定节点列表 */
    private static Registry inMemoryRegistry(List<ServiceMeta> nodes) {
        return new Registry() {
            @Override
            public void register(ServiceMeta service) {
            }

            @Override
            public void unregister(ServiceMeta service) {
            }

            @Override
            public List<ServiceMeta> discover(String serviceName) {
                return nodes;
            }

            @Override
            public void subscribe(String serviceName, NotifyListener listener) {
            }

            @Override
            public void destroy() {
            }
        };
    }

    /** 异步：方法返回CompletableFuture，调用立即返回future，不阻塞。 */
    @Test
    void asyncCallReturnsFutureAndCompletes() throws Exception {
        RpcServer server = new RpcServer(HOST, 20897);
        server.export(HelloService.class, new HelloServiceImpl());
        server.start();
        RpcClient client = new RpcClient();
        try {
            ServiceMeta target = new ServiceMeta(HelloService.class.getName(), HOST, 20897);
            HelloService hello = RpcClientProxy.create(HelloService.class, target, client, 3000);

            CompletableFuture<String> future = hello.sayHelloAsync("Conduit");
            assertNotNull(future);
            assertEquals("Hello, Conduit from Conduit!", future.get(3, TimeUnit.SECONDS));
        } finally {
            client.close();
            server.close();
        }
    }

    /** 容错：候选含一个死节点；roundrobin首选死节点，FailOver确定性切到活节点并成功。 */
    @Test
    void failoverFromDeadNodeToLiveNode() {
        RpcServer live = new RpcServer(HOST, 20898);
        live.export(HelloService.class, new HelloServiceImpl());
        live.start();
        RpcClient client = new RpcClient();
        try {
            ServiceMeta dead = new ServiceMeta(HelloService.class.getName(), HOST, 20899); // 无人监听
            ServiceMeta liveMeta = new ServiceMeta(HelloService.class.getName(), HOST, 20898);
            Registry registry = inMemoryRegistry(Arrays.asList(dead, liveMeta));

            // roundrobin 首次选index0=dead → 调用失败 → FailOver切到live → 成功
            HelloService hello = RpcClientProxy.create(
                    HelloService.class, registry, client, 2000, "failover", "roundrobin");

            assertEquals("Hello, Conduit from Conduit!", hello.sayHello("Conduit"));
        } finally {
            client.close();
            live.close();
        }
    }

    /** 负载均衡：两个活节点 + roundrobin，连续两次调用分流到不同节点。 */
    @Test
    void roundRobinDistributesAcrossNodes() {
        RpcServer s1 = new RpcServer(HOST, 20901);
        s1.export(HelloService.class, named("S1"));
        s1.start();
        RpcServer s2 = new RpcServer(HOST, 20902);
        s2.export(HelloService.class, named("S2"));
        s2.start();
        RpcClient client = new RpcClient();
        try {
            ServiceMeta m1 = new ServiceMeta(HelloService.class.getName(), HOST, 20901);
            ServiceMeta m2 = new ServiceMeta(HelloService.class.getName(), HOST, 20902);
            Registry registry = inMemoryRegistry(Arrays.asList(m1, m2));

            HelloService hello = RpcClientProxy.create(
                    HelloService.class,
                    registry,
                    client,
                    2000,
                    "failover",
                    "roundrobin"
            );

            String r1 = hello.sayHello("x");
            String r2 = hello.sayHello("x");
            assertNotEquals(r1, r2); // 两次轮询到不同节点（S1/S2）
        } finally {
            client.close();
            s1.close();
            s2.close();
        }
    }

    /** 修复：异步方法失败（如无可用提供者）应通过异常完成的future暴露，而不是在调用处同步抛出。 */
    @Test
    void asyncFailureCompletesFutureExceptionallyNotSyncThrow() {
        RpcClient client = new RpcClient();
        try {
            Registry empty = inMemoryRegistry(Collections.emptyList());
            HelloService hello = RpcClientProxy.create(HelloService.class, empty, client, 1000);

            // 调用本身不应抛（返回future）；失败体现在future上
            CompletableFuture<String> f = hello.sayHelloAsync("x");
            assertNotNull(f);
            ExecutionException ex = assertThrows(ExecutionException.class, () -> f.get(2, TimeUnit.SECONDS));
            assertNotNull(ex.getCause());
        } finally {
            client.close();
        }
    }

    /** 返回带节点标识的实现，便于验证负载均衡分流 */
    private static HelloService named(String id) {
        return new HelloService() {
            @Override
            public String sayHello(String name) {
                return id + ":" + name;
            }

            @Override
            public CompletableFuture<String> sayHelloAsync(String name) {
                return CompletableFuture.completedFuture(sayHello(name));
            }
        };
    }
}
