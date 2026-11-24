package io.github.nitouge.conduit.common.filter;

import io.github.nitouge.conduit.common.config.ConduitConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * filter开关来自 {@link ConduitConfig}（而非只认系统属性）：
 * 安装一个Spring Environment风格的配置源，验证默认关闭的filter被启用、默认开启的被关闭。
 */
class FilterLoaderConfigTest {

    @AfterEach
    void cleanup() {
        ConduitConfig.clearSource();
        FilterLoader.reset();
    }

    @Test
    void installedConfigSourceControlsFilterSwitches() {
        ConduitConfig.installSource(withProperties(Map.of(
                "conduit.filter.metrics.enabled", "true",
                "conduit.filter.structuredlog.enabled", "true",
                "conduit.filter.accesslog.enabled", "false")));

        FilterLoader.reset();
        Set<String> provider = names(FilterLoader.load(Side.PROVIDER));

        assertTrue(provider.contains("metrics"), "配置源里metrics=true应启用: " + provider);
        assertTrue(provider.contains("structuredlog"), "配置源里structuredlog=true应启用: " + provider);
        assertFalse(provider.contains("accesslog"), "配置源里accesslog=false应禁用: " + provider);
        assertTrue(provider.contains("trace"), "未配置的filter沿用自身默认（开）: " + provider);
        assertTrue(provider.contains("exception"), "未配置的filter沿用自身默认（开）: " + provider);
        assertFalse(provider.contains("activelimit"), "未配置的filter沿用自身默认（关）: " + provider);
    }

    @Test
    void removingSourceRestoresDefaults() {
        ConduitConfig.installSource(withProperties(Map.of("conduit.filter.metrics.enabled", "true")));
        FilterLoader.reset();
        assertTrue(names(FilterLoader.load(Side.CONSUMER)).contains("metrics"));

        ConduitConfig.clearSource();
        FilterLoader.reset();
        Set<String> consumer = names(FilterLoader.load(Side.CONSUMER));

        assertFalse(consumer.contains("metrics"), "配置源移除后应回到默认（关）: " + consumer);
        assertTrue(consumer.contains("accesslog"), "默认开启的filter应回来: " + consumer);
    }

    private static ConduitConfig.Source withProperties(Map<String, String> properties) {
        return properties::get;
    }

    private static Set<String> names(List<Filter> filters) {
        return filters.stream().map(Filter::name).collect(Collectors.toSet());
    }
}
