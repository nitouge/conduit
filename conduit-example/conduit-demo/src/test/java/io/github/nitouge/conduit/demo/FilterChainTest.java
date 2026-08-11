package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.demo.filter.CountingFilter;
import io.github.nitouge.conduit.demo.provider.HelloServiceImpl;
import io.github.nitouge.conduit.server.RpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Filter责任链端到端验收。
 *
 * <p>验证点：</p>
 * <ul>
 *   <li>同步调用经过消费侧与提供侧filter（各before+after触发一次）。</li>
 *   <li>异步调用也经过filter链（after在future完成时触发）。</li>
 *   <li>before/after顺序：外层先进后出（责任链嵌套语义）。</li>
 * </ul>
 */
class FilterChainTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 20909;

    private RpcServer server;
    private RpcClient client;
    private HelloService proxy;

    @BeforeEach
    void setUp() {
        CountingFilter.reset();
        server = new RpcServer(HOST, PORT);
        server.export(HelloService.class, new HelloServiceImpl());
        server.start();

        client = new RpcClient();
        ServiceMeta target = new ServiceMeta(HelloService.class.getName(), HOST, PORT);
        proxy = RpcClientProxy.create(HelloService.class, target, client, 3000);
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void syncCallPassesThroughConsumerAndProviderFilters() {
        String result = proxy.sayHello("Filter");

        assertEquals("Hello, Filter from Conduit!", result);
        // CountingFilter单例被消费侧与提供侧各加载一次（同一实例），因此一次端到端调用中：
        // before +2（consumer前置 + provider前置），after +2（consumer后置 + provider后置）
        assertEquals(2, CountingFilter.BEFORE.get(), "before应触发2次（消费+提供）");
        assertEquals(2, CountingFilter.AFTER.get(), "after应触发2次（消费+提供）");
        // 顺序：before,before,after,after（外层先进后出——消费侧在最外、提供侧在次外）
        assertEquals(4, CountingFilter.ORDER.size());
        assertEquals("before", CountingFilter.ORDER.get(0));
        assertEquals("before", CountingFilter.ORDER.get(1));
        assertEquals("after", CountingFilter.ORDER.get(2));
        assertEquals("after", CountingFilter.ORDER.get(3));
    }

    @Test
    void asyncCallPassesThroughFiltersAndAfterTriggersOnCompletion() throws Exception {
        CompletableFuture<String> future = proxy.sayHelloAsync("AsyncFilter");

        String result = future.get(5, TimeUnit.SECONDS);
        assertEquals("Hello, AsyncFilter from Conduit!", result);
        // 异步路径也经过 filter：before 同步触发，after 在 future 完成时触发
        assertEquals(2, CountingFilter.BEFORE.get(), "异步before应触发2次");
        assertEquals(2, CountingFilter.AFTER.get(), "异步after应在future完成时触发2次");
    }
}
