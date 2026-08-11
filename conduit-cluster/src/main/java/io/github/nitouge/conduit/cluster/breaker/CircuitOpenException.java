package io.github.nitouge.conduit.cluster.breaker;

import io.github.nitouge.conduit.common.exception.RpcException;

import java.io.Serial;

/**
 * 熔断器处于OPEN（或半开已放出探测）而<b>主动拒绝</b>本次调用时抛出。
 *
 * <p>它与真正发起远程调用后失败在语义上不同：熔断拒绝是<b>零成本的快速跳过</b>，
 * 不应计入FailOver的有限重试预算（否则坏节点的熔断反而会挤掉对健康节点的尝试）。
 * {@link io.github.nitouge.conduit.cluster.support.FailoverCluster} 据此把它与普通调用异常区别对待。</p>
 */
public class CircuitOpenException extends RpcException {

    @Serial
    private static final long serialVersionUID = 1L;

    public CircuitOpenException(String message) {
        super(message);
    }
}
