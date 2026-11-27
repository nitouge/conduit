package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.filter.Side;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * 幂等键生成Filter（消费侧）：给每次<b>逻辑调用</b>发一个稳定键，供服务端去重。
 *
 * <p>为什么放在框架里而不是业务代码里：failover 重放请求时，业务代码根本没有感知——重放发生在
 * 集群容错层，是同一个 {@link RpcRequest} 对象被再次发出。只有把键打在请求上（而不是每次尝试
 * 现生成），重放的两次才共享同一个键，服务端才能识别出「这是同一次调用」。</p>
 *
 * <ul>
 *   <li>请求已带 {@code conduit.idempotency-key}（调用方自己指定，如订单号）→ 原样保留；</li>
 *   <li>没带 → 生成一个 UUID 写入；</li>
 *   <li>两次独立的业务调用各拿一个新键，不会被误判为重复。</li>
 * </ul>
 *
 * <p>默认启用（{@code conduit.filter.idempotency.enabled=false} 关闭）：生成一个 UUID 的成本
 * 可以忽略，而它让「开了重试的非幂等接口」在配合服务端 {@link IdempotentFilter} 时变成安全的。
 * 服务端没有去重时，这个 attachment 只是被忽略，不改变任何行为。</p>
 */
public class IdempotencyFilter implements Filter {

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        if (isBlank(request.getAttachment(RpcAttachments.IDEMPOTENCY_KEY))) {
            request.setAttachment(RpcAttachments.IDEMPOTENCY_KEY, UUID.randomUUID().toString());
        }
        return chain.invoke(request);
    }

    @Override
    public int order() {
        // 在 Trace(-200)/AccessLog(-100) 之后、Metrics(0) 之前：让访问日志能带上幂等键
        return -50;
    }

    @Override
    public boolean appliesTo(Side side) {
        return side == Side.CONSUMER;
    }

    @Override
    public String name() {
        return "idempotency";
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
