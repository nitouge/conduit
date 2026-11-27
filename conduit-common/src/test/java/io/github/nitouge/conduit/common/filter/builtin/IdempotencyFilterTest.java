package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.FilterChain;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 幂等键生成Filter测试：消费侧的键必须"一次逻辑调用一个、重放时不变"。
 */
class IdempotencyFilterTest {

    private final IdempotencyFilter filter = new IdempotencyFilter();

    @Test
    void generatesKeyWhenAbsent() {
        RpcRequest request = request();
        filter.invoke(request, okChain()).join();

        String key = request.getAttachment(RpcAttachments.IDEMPOTENCY_KEY);
        assertNotNull(key, "消费侧应自动生成幂等键");
        assertEquals(36, key.length(), "应为UUID形态: " + key);
    }

    @Test
    void keepsCallerProvidedKey() {
        RpcRequest request = request();
        request.setAttachment(RpcAttachments.IDEMPOTENCY_KEY, "order-20260101-001");

        filter.invoke(request, okChain()).join();

        assertEquals("order-20260101-001", request.getAttachment(RpcAttachments.IDEMPOTENCY_KEY),
                "调用方指定的键（如订单号）必须原样保留");
    }

    @Test
    void sameRequestReuseKeepsTheSameKeyAcrossReplays() {
        // failover 重放的是同一个请求对象：多次 invoke 必须沿用同一个键，否则服务端无法去重
        RpcRequest request = request();
        filter.invoke(request, okChain()).join();
        String first = request.getAttachment(RpcAttachments.IDEMPOTENCY_KEY);

        filter.invoke(request, okChain()).join();
        String second = request.getAttachment(RpcAttachments.IDEMPOTENCY_KEY);

        assertEquals(first, second);
    }

    @Test
    void differentCallsGetDifferentKeys() {
        RpcRequest first = request();
        RpcRequest second = request();
        filter.invoke(first, okChain()).join();
        filter.invoke(second, okChain()).join();

        assertNotEquals(first.getAttachment(RpcAttachments.IDEMPOTENCY_KEY),
                second.getAttachment(RpcAttachments.IDEMPOTENCY_KEY),
                "两次独立业务调用不能被误判成重复");
    }

    private static RpcRequest request() {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("com.example.OrderService");
        request.setMethodName("create");
        return request;
    }

    private static FilterChain okChain() {
        return request -> CompletableFuture.completedFuture(RpcResponse.success("created"));
    }
}
