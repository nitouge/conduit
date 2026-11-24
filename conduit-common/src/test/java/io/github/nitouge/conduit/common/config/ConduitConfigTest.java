package io.github.nitouge.conduit.common.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ConduitConfig} 配置解析：系统属性 > 安装的配置源 > 环境变量。
 */
class ConduitConfigTest {

    private static final String KEY = "conduit.test.switch.enabled";

    @AfterEach
    void cleanup() {
        ConduitConfig.clearSource();
        System.clearProperty(KEY);
    }

    @Test
    void installedSourceIsConsultedWhenNoSystemProperty() {
        ConduitConfig.installSource(key -> "conduit.test.switch.enabled".equals(key) ? "true" : null);

        assertEquals("true", ConduitConfig.get(KEY));
        assertTrue(ConduitConfig.getBoolean(KEY, false));
    }

    @Test
    void systemPropertyWinsOverInstalledSource() {
        ConduitConfig.installSource(key -> "false");
        System.setProperty(KEY, "true");

        assertTrue(ConduitConfig.getBoolean(KEY, false), "系统属性优先级最高");
    }

    @Test
    void reinstallReplacesPreviousSource() {
        ConduitConfig.installSource(key -> "true");
        ConduitConfig.installSource(key -> null);

        assertNull(ConduitConfig.get(KEY), "后安装的配置源整体覆盖前一个，不会串");
        assertTrue(ConduitConfig.getBoolean(KEY, true));
    }

    @Test
    void missingKeyFallsBackToDefault() {
        assertNull(ConduitConfig.get(KEY));
        assertFalse(ConduitConfig.getBoolean(KEY, false));
        assertTrue(ConduitConfig.getBoolean(KEY, true));
    }

    @Test
    void invalidBooleanFallsBackToDefault() {
        ConduitConfig.installSource(key -> "yes-please");

        // 只认 true/false（忽略大小写与空白），其余一律回退默认值，避免拼错导致开关"薛定谔"
        assertTrue(ConduitConfig.getBoolean(KEY, true));
        assertFalse(ConduitConfig.getBoolean(KEY, false));
    }

    @Test
    void booleanIgnoresCaseAndWhitespace() {
        ConduitConfig.installSource(key -> "  TRUE  ");

        assertTrue(ConduitConfig.getBoolean(KEY, false));
    }

    @Test
    void positiveIntParsing() {
        ConduitConfig.installSource(new ConduitConfig.Source() {
            @Override
            public String get(String key) {
                return switch (key) {
                    case "conduit.test.a" -> "200";
                    case "conduit.test.b" -> "0";
                    case "conduit.test.c" -> "abc";
                    default -> null;
                };
            }
        });

        assertEquals(200, ConduitConfig.getPositiveInt("conduit.test.a", 50));
        assertEquals(50, ConduitConfig.getPositiveInt("conduit.test.b", 50), "非正数回退默认");
        assertEquals(50, ConduitConfig.getPositiveInt("conduit.test.c", 50), "非法数字回退默认");
        assertEquals(50, ConduitConfig.getPositiveInt("conduit.test.missing", 50));
    }

    @Test
    void envKeyFollowsSpringRelaxedNaming() {
        assertEquals("CONDUIT_FILTER_METRICS_ENABLED",
                ConduitConfig.envKey("conduit.filter.metrics.enabled"));
        assertEquals("CONDUIT_FILTER_ACTIVELIMIT_PERMITS",
                ConduitConfig.envKey("conduit.filter.activelimit.permits"));
        // 键里带短横线同样映射为下划线（与Spring relaxed binding一致）
        assertEquals("CONDUIT_TRANSPORT_READ_IDLE_TIMEOUT_MS",
                ConduitConfig.envKey("conduit.transport.read-idle-timeout-ms"));
    }

    @Test
    void sourceMayReturnNullForUnknownKeys() {
        ConduitConfig.installSource(new ConduitConfig.Source() {
            private final Map<String, String> values = Map.of("conduit.test.switch.enabled", "false");

            @Override
            public String get(String key) {
                return values.get(key);
            }
        });

        assertFalse(ConduitConfig.getBoolean(KEY, true));
        assertNull(ConduitConfig.get("conduit.test.other"));
    }
}
