package io.github.nitouge.conduit.common.config;

import java.util.Locale;

/**
 * 框架运行期配置的统一读取入口（如 {@code conduit.filter.metrics.enabled}）。
 *
 * <p>框架自身不依赖Spring，最直接的配置来源是JVM系统属性
 * （{@code -Dconduit.filter.metrics.enabled=true}）。但在Spring Boot应用里，用户更自然地把开关写进
 * {@code application.yml}、或用 {@code --conduit.filter.metrics.enabled=true} 程序参数、或用环境变量——
 * 这些都不在{@link System#getProperty}里。于是由 {@code conduit-spring} 的自动配置把
 * Spring {@code Environment} 注册为一个配置源，框架内部统一经本类读取，两条路径就都生效了。</p>
 *
 * <p><b>解析顺序（先命中先返回）：</b></p>
 * <ol>
 *   <li>JVM系统属性 {@link System#getProperty}</li>
 *   <li>已安装的配置源（{@link #installSource(Source)}，Spring应用即Spring {@code Environment}）</li>
 *   <li>环境变量，按Spring的宽松命名转换：{@code conduit.filter.metrics.enabled}
 *       → {@code CONDUIT_FILTER_METRICS_ENABLED}</li>
 * </ol>
 *
 * <p>注意配置源是"覆盖式"安装：同一JVM里启动多个Spring上下文时以最新一个为准
 * （测试里反复建上下文不会互相串味）。非Spring用法无需安装任何配置源，行为与只用系统属性一致。</p>
 */
public final class ConduitConfig {

    /**
     * 配置来源；返回{@code null}表示该源没有这个配置项
     */
    @FunctionalInterface
    public interface Source {
        String get(String key);
    }

    /**
     * 外部安装的配置源（Spring Environment）；覆盖式，保持最新
     */
    private static volatile Source source;

    private ConduitConfig() {
    }

    /**
     * 安装外部配置源（如Spring {@code Environment}），覆盖之前安装的那个。
     *
     * @param source 配置源；传 {@code null} 等价于 {@link #clearSource()}
     */
    public static void installSource(Source source) {
        ConduitConfig.source = source;
    }

    /**
     * 卸载外部配置源（测试用；生产一般不需要）
     */
    public static void clearSource() {
        ConduitConfig.source = null;
    }

    /**
     * 读取字符串配置，未配置返回 {@code null}。
     */
    public static String get(String key) {
        String value = System.getProperty(key);
        if (value != null) {
            return value;
        }
        Source installed = source;
        if (installed != null) {
            value = installed.get(key);
            if (value != null) {
                return value;
            }
        }
        return getenv(key);
    }

    /**
     * 读取布尔配置；未配置或值非法时返回 {@code defaultValue}
     */
    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        String trimmed = value.trim();
        if ("true".equalsIgnoreCase(trimmed)) {
            return true;
        }
        if ("false".equalsIgnoreCase(trimmed)) {
            return false;
        }
        return defaultValue;
    }

    /**
     * 读取整数配置；未配置、非法或非正数时返回 {@code defaultValue}
     */
    public static int getPositiveInt(String key, int defaultValue) {
        String value = get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 配置项名 → 环境变量名：小写点分键转大写下划线，例如 {@code a.b-c} → {@code A_B_C}
     */
    static String envKey(String key) {
        return key.replace('.', '_').replace('-', '_').toUpperCase(Locale.ROOT);
    }

    private static String getenv(String key) {
        String value = System.getenv(envKey(key));
        return (value == null || value.isBlank()) ? null : value;
    }
}
