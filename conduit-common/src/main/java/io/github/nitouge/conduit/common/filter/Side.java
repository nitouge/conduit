package io.github.nitouge.conduit.common.filter;

/**
 * Filter作用方：消费侧（{@code CONSUMER}）或提供侧（{@code PROVIDER}）。
 *
 * <p>配合 {@link Filter#appliesTo(Side)} 让某些filter只在一侧生效（如只有消费侧需要的限流、只有提供侧需要的鉴权）。</p>
 */
public enum Side {

    /** 消费侧：客户端发起调用前/后 */
    CONSUMER,

    /** 提供侧：服务端接收请求后、反射调用业务方法前/后 */
    PROVIDER
}
