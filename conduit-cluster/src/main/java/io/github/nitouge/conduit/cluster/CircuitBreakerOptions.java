package io.github.nitouge.conduit.cluster;

/**
 * 熔断器参数（节点级健康闸门）。
 *
 * <p>失败阈值采用"连续失败次数"语义：连续 {@code failureThreshold} 次传输失败后打开，
 * 打开 {@code openDurationMs} 毫秒后进入半开并放行一次探测；探测成功恢复、失败重新打开。</p>
 *
 * <p>通过 {@code CircuitBreakerRegistry.configure(...)} 生效，Spring场景对应
 * {@code conduit.cluster.breaker.*} 配置。</p>
 */
public final class CircuitBreakerOptions {

    /** 默认连续失败阈值：5次失败打开 */
    public static final int DEFAULT_FAILURE_THRESHOLD = 5;

    /** 默认打开时长：5秒后转半开探测 */
    public static final long DEFAULT_OPEN_DURATION_MS = 5_000;

    private final int failureThreshold;
    private final long openDurationMs;

    private CircuitBreakerOptions(Builder builder) {
        this.failureThreshold = builder.failureThreshold;
        this.openDurationMs = builder.openDurationMs;
    }

    public static CircuitBreakerOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getFailureThreshold() {
        return failureThreshold;
    }

    public long getOpenDurationMs() {
        return openDurationMs;
    }

    @Override
    public String toString() {
        return "CircuitBreakerOptions{" +
                "failureThreshold=" + failureThreshold +
                ", openDurationMs=" + openDurationMs +
                '}';
    }

    /** {@link CircuitBreakerOptions} 构建器 */
    public static final class Builder {

        private int failureThreshold = DEFAULT_FAILURE_THRESHOLD;
        private long openDurationMs = DEFAULT_OPEN_DURATION_MS;

        private Builder() {
        }

        /** 连续失败多少次打开熔断器（>=1） */
        public Builder failureThreshold(int failureThreshold) {
            if (failureThreshold < 1) {
                throw new IllegalArgumentException("failureThreshold必须>=1: " + failureThreshold);
            }
            this.failureThreshold = failureThreshold;
            return this;
        }

        /** 打开后多久转半开探测（毫秒，>=0） */
        public Builder openDurationMs(long openDurationMs) {
            if (openDurationMs < 0) {
                throw new IllegalArgumentException("openDurationMs不能为负数: " + openDurationMs);
            }
            this.openDurationMs = openDurationMs;
            return this;
        }

        public CircuitBreakerOptions build() {
            return new CircuitBreakerOptions(this);
        }
    }
}
