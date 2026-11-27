package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QPS 令牌桶限流测试：突发打满、超限快速失败、按速率匀速回填、全局桶与方法桶叠加。
 */
class RateLimitFilterTest {

    private static final String QPS_KEY = "conduit.filter.ratelimit.qps";
    private static final String GLOBAL_KEY = "conduit.filter.ratelimit.global-qps";

    @AfterEach
    void clearConfig() {
        System.clearProperty(QPS_KEY);
        System.clearProperty(GLOBAL_KEY);
    }

    @Test
    void burstWithinCapacityPassesThenFailsFast() {
        System.setProperty(QPS_KEY, "3");
        RateLimitFilter filter = new RateLimitFilter();
        FilterChain chain = okChain();

        for (int i = 0; i < 3; i++) {
            assertEquals("ok", filter.invoke(request("m1"), chain).join().getData());
        }

        // 超限必须立刻失败，而不是排队等待令牌
        ExecutionException error = assertThrows(ExecutionException.class,
                () -> filter.invoke(request("m1"), chain).get());
        assertTrue(String.valueOf(error.getCause()).contains("RateLimit"),
                "错误原因应说明是限流: " + error.getCause());
    }

    @Test
    void tokensRefillOverTime() throws Exception {
        System.setProperty(QPS_KEY, "100");
        RateLimitFilter filter = new RateLimitFilter();
        FilterChain chain = okChain();

        for (int i = 0; i < 100; i++) {
            filter.invoke(request("m1"), chain);
        }
        assertThrows(ExecutionException.class, () -> filter.invoke(request("m1"), chain).get());

        // 100 QPS 意味着 10ms 回填 1 个令牌，睡 60ms 足够再放行
        Thread.sleep(60);
        assertEquals("ok", filter.invoke(request("m1"), chain).join().getData());
    }

    @Test
    void bucketsAreIsolatedPerMethod() {
        System.setProperty(QPS_KEY, "1");
        RateLimitFilter filter = new RateLimitFilter();
        FilterChain chain = okChain();

        assertEquals("ok", filter.invoke(request("m1"), chain).join().getData());
        // m1 的令牌已用完，但 m2 有自己的桶
        assertEquals("ok", filter.invoke(request("m2"), chain).join().getData());
        assertThrows(ExecutionException.class, () -> filter.invoke(request("m1"), chain).get());
    }

    @Test
    void globalBucketCapsAllMethodsTogether() {
        System.setProperty(QPS_KEY, "1000");
        System.setProperty(GLOBAL_KEY, "2");
        RateLimitFilter filter = new RateLimitFilter();
        FilterChain chain = okChain();

        assertEquals("ok", filter.invoke(request("m1"), chain).join().getData());
        assertEquals("ok", filter.invoke(request("m2"), chain).join().getData());

        ExecutionException error = assertThrows(ExecutionException.class,
                () -> filter.invoke(request("m3"), chain).get());
        assertTrue(String.valueOf(error.getCause()).contains("全局限流"),
                "全局限流应与方法限流区分开: " + error.getCause());
    }

    private static RpcRequest request(String method) {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("com.example.DemoService");
        request.setMethodName(method);
        return request;
    }

    private static FilterChain okChain() {
        return request -> CompletableFuture.completedFuture(RpcResponse.success("ok"));
    }
}
