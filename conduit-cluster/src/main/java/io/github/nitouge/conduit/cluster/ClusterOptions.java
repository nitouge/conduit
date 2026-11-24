package io.github.nitouge.conduit.cluster;

/**
 * 集群容错的可配置项：重试次数、重试退避与熔断器参数。
 *
 * <p>由上层（client/Spring）按 {@code @RpcReference} 与全局 {@code conduit.cluster.*} 配置构造，
 * 再随每次调用传给 {@link Cluster#invoke(java.util.List, io.github.nitouge.conduit.api.RpcRequest,
 * LoadBalancer, RemoteInvoker, ClusterOptions)}。默认值为重试2次、不退避、熔断阈值5/5s。</p>
 */
public final class ClusterOptions {

    /** 默认额外重试次数：2（即最多尝试3个节点） */
    public static final int DEFAULT_RETRIES = 2;

    /** 默认重试退避：0（失败立即换节点） */
    public static final long DEFAULT_RETRY_BACKOFF_MS = 0;

    private final int retries;
    private final long retryBackoffMs;
    private final CircuitBreakerOptions circuitBreaker;

    private ClusterOptions(Builder builder) {
        this.retries = builder.retries;
        this.retryBackoffMs = builder.retryBackoffMs;
        this.circuitBreaker = builder.circuitBreaker;
    }

    public static ClusterOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder()
                .retries(retries)
                .retryBackoffMs(retryBackoffMs)
                .circuitBreaker(circuitBreaker);
    }

    /**
     * 额外重试次数（不含首次调用）。总尝试次数 = retries + 1。
     */
    public int getRetries() {
        return retries;
    }

    /** 两次尝试之间的退避时长（毫秒）；0表示立即重试 */
    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    /** 节点级熔断器参数 */
    public CircuitBreakerOptions getCircuitBreaker() {
        return circuitBreaker;
    }

    @Override
    public String toString() {
        return "ClusterOptions{" +
                "retries=" + retries +
                ", retryBackoffMs=" + retryBackoffMs +
                ", circuitBreaker=" + circuitBreaker +
                '}';
    }

    /** {@link ClusterOptions} 构建器 */
    public static final class Builder {

        private int retries = DEFAULT_RETRIES;
        private long retryBackoffMs = DEFAULT_RETRY_BACKOFF_MS;
        private CircuitBreakerOptions circuitBreaker = CircuitBreakerOptions.defaults();

        private Builder() {
        }

        public Builder retries(int retries) {
            if (retries < 0) {
                throw new IllegalArgumentException("retries不能为负数: " + retries);
            }
            this.retries = retries;
            return this;
        }

        public Builder retryBackoffMs(long retryBackoffMs) {
            if (retryBackoffMs < 0) {
                throw new IllegalArgumentException("retryBackoffMs不能为负数: " + retryBackoffMs);
            }
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder circuitBreaker(CircuitBreakerOptions circuitBreaker) {
            this.circuitBreaker = circuitBreaker == null ? CircuitBreakerOptions.defaults() : circuitBreaker;
            return this;
        }

        public ClusterOptions build() {
            return new ClusterOptions(this);
        }
    }
}
