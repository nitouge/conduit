package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.trace.TraceContext;

import java.util.concurrent.CompletableFuture;

/**
 * 链路追踪Filter：保证每次RPC调用都携带traceId并透传到提供侧。
 *
 * <ul>
 *   <li>消费侧：请求已有traceId（来自 {@code RpcContext}）则沿用，否则生成一个写入
 *       {@link RpcRequest#getAttachments()}；</li>
 *   <li>提供侧：从请求中取出traceId，写入当前线程MDC，业务方法可通过
 *       {@code RpcContext.getContext().getTraceId()} 读取；</li>
 *   <li>双侧都会在调用期间把traceId放入MDC，使日志中的 {@code %X{traceId}} 可直接串联。</li>
 * </ul>
 *
 * <p>默认启用；用 {@code -Dconduit.filter.trace.enabled=false} 关闭。
 * 注意：异步provider的continuation跑在其它线程，MDC不会自动跨线程；需要时自行从
 * {@code RpcContext} 拷贝。</p>
 */
public class TraceFilter implements Filter {

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        String traceId = TraceContext.ensureTraceId(request);
        String previous = TraceContext.putMdc(traceId);
        try {
            return chain.invoke(request);
        } finally {
            // 同步前置/终端已执行完；异步回调不依赖MDC（AccessLog/StructuredLog直接读request）
            TraceContext.restoreMdc(previous);
        }
    }

    @Override
    public int order() {
        return -200; // 最外层：先于AccessLog等生成/绑定traceId
    }

    @Override
    public String name() {
        return "trace";
    }
}
