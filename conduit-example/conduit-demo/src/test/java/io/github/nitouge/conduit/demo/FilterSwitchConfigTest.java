package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.common.config.ConduitConfig;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterLoader;
import io.github.nitouge.conduit.common.filter.Side;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code conduit.filter.*} 开关在Spring应用里真正生效（而不是只认JVM参数 {@code -D...}）。
 *
 * <p>这里把开关放进Spring配置（等价于写进 {@code application.yml} 或用
 * {@code --conduit.filter.metrics.enabled=true} 程序参数）：Spring {@code Environment}
 * 被自动配置登记为配置源后，{@link FilterLoader} 装载filter时应读到这些值。</p>
 */
@SpringBootTest(
        classes = FilterSwitchConfigTest.App.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "conduit.registry.type=local",
                "conduit.registry.address=filter-switch-jvm",
                "conduit.filter.metrics.enabled=true",
                "conduit.filter.structuredlog.enabled=true",
                "conduit.filter.accesslog.enabled=false"
        })
class FilterSwitchConfigTest {

    @Test
    void springPropertiesControlFilterSwitches() {
        FilterLoader.reset(); // 丢弃任何更早装载的快照，按当前配置装载

        Set<String> provider = names(FilterLoader.load(Side.PROVIDER));
        assertTrue(provider.contains("metrics"), "metrics=true应启用: " + provider);
        assertTrue(provider.contains("structuredlog"), "structuredlog=true应启用: " + provider);
        assertFalse(provider.contains("accesslog"), "accesslog=false应禁用: " + provider);
        assertTrue(provider.contains("trace"), "未配置的filter沿用自身默认: " + provider);
        assertTrue(provider.contains("exception"), "提供侧异常filter默认仍启用: " + provider);

        Set<String> consumer = names(FilterLoader.load(Side.CONSUMER));
        assertTrue(consumer.contains("metrics"), "消费侧同样读到配置: " + consumer);
        assertFalse(consumer.contains("accesslog"), "消费侧同样读到配置: " + consumer);
        assertFalse(consumer.contains("exception"), "exception只作用于提供侧: " + consumer);
    }

    @AfterAll
    static void restoreGlobalState() {
        // FilterLoader缓存与配置源都是进程级静态状态，测试结束复位，避免影响同JVM的其他测试
        ConduitConfig.clearSource();
        FilterLoader.reset();
    }

    private static Set<String> names(List<Filter> filters) {
        return filters.stream().map(Filter::name).collect(Collectors.toSet());
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class App {
    }
}
