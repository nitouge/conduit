package io.github.nitouge.conduit.testkit;

import io.github.nitouge.conduit.cluster.breaker.CircuitBreakerRegistry;
import io.github.nitouge.conduit.common.config.ConduitConfig;
import io.github.nitouge.conduit.common.filter.FilterLoader;
import io.github.nitouge.conduit.common.filter.builtin.MetricsFilter;

/**
 * 全局静态状态复位：框架里有若干"进程级"状态（filter快照、熔断器、配置源、指标累计），
 * 测试之间必须显式复位，否则用例顺序会互相影响（尤其并发执行时）。
 *
 * <pre>{@code
 * @BeforeEach
 * void reset() { TestReset.all(); }
 * }</pre>
 */
public final class TestReset {

    private TestReset() {
    }

    /**
     * 复位所有已知的进程级状态
     */
    public static void all() {
        FilterLoader.reset();
        ConduitConfig.clearSource();
        CircuitBreakerRegistry.clear();
        MetricsFilter.reset();
    }

    /**
     * 只复位熔断器（指定节点/全部）
     */
    public static void breakers() {
        CircuitBreakerRegistry.clear();
    }

    /**
     * 只复位filter链缓存（改过filter开关配置后调用）
     */
    public static void filters() {
        FilterLoader.reset();
        ConduitConfig.clearSource();
    }
}
