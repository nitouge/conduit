package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.cluster.ClusterOptions;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreakerRegistry;
import io.github.nitouge.conduit.spring.ConduitAutoConfiguration;
import io.github.nitouge.conduit.spring.ConduitProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 集群容错配置绑定验收：{@code conduit.cluster.*} 绑定到 {@link ClusterOptions}，
 * 并在自动装配启动时应用到全局熔断器注册表。
 */
class ClusterConfigBindingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConduitAutoConfiguration.class))
            .withPropertyValues(
                    "conduit.registry.type=local",
                    "conduit.registry.address=cluster-binding-test",
                    "conduit.cluster.retries=5",
                    "conduit.cluster.retry-backoff-ms=123",
                    "conduit.cluster.breaker.failure-threshold=2",
                    "conduit.cluster.breaker.open-duration-ms=456");

    @AfterEach
    void resetBreakers() {
        // 熔断器是进程级全局状态：测试结束恢复默认，避免影响其它用例
        CircuitBreakerRegistry.clear();
    }

    @Test
    void clusterPropertiesBindToClusterOptions() {
        runner.run(context -> {
            if (context.getStartupFailure() != null) {
                throw new AssertionError("上下文启动失败: " + context.getStartupFailure(),
                        context.getStartupFailure());
            }
            ConduitProperties props = context.getBean(ConduitProperties.class);
            ClusterOptions options = props.toClusterOptions();

            assertEquals(5, options.getRetries());
            assertEquals(123, options.getRetryBackoffMs());
            assertEquals(2, options.getCircuitBreaker().getFailureThreshold());
            assertEquals(456, options.getCircuitBreaker().getOpenDurationMs());

            // 自动装配在创建RpcClient时已把参数应用到全局注册表
            assertEquals(2, CircuitBreakerRegistry.options().getFailureThreshold());
            assertEquals(456, CircuitBreakerRegistry.options().getOpenDurationMs());
        });
    }

    @Test
    void invalidRetriesFailFastAtStartup() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ConduitAutoConfiguration.class))
                .withPropertyValues(
                        "conduit.registry.type=local",
                        "conduit.cluster.retries=-1")
                .run(context -> {
                    Throwable failure = context.getStartupFailure();
                    assertNotNull(failure, "非法retries应在启动时快速失败");
                    assertTrue(hasMessage(failure, "retries"), "失败原因应指出retries问题: " + failure);
                });
    }

    private static boolean hasMessage(Throwable failure, String expected) {
        Throwable current = failure;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(expected)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
