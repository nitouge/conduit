package io.github.nitouge.conduit.common.filter;

import io.github.nitouge.conduit.common.config.ConduitConfig;
import io.github.nitouge.conduit.common.spi.ExtensionLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按 {@link Side} 加载并缓存filter列表。
 *
 * <p>经 {@link ExtensionLoader#getExtensionInfos()} 取全部已注册 {@link Filter}（携带SPI名），
 * 按 {@link Filter#enabled()} 读配置决定是否装载、按 {@link Filter#appliesTo(Side)} 过滤本侧适用者，
 * 再按 {@link Filter#order()} 从小到大稳定排序（值小的靠外层、先执行）。
 * 结果<b>每侧缓存一份</b>——filter是无状态单例，加载一次即可复用。</p>
 *
 * <p>开关读的是 {@link ConduitConfig}，因此系统属性、Spring配置（yml/程序参数/环境变量）都生效。</p>
 *
 * <p>无任何注册的filter时返回空列表；调用方（消费/提供侧接线）据此直通，零开销。</p>
 */
public final class FilterLoader {

    private static final Logger log = LoggerFactory.getLogger(FilterLoader.class);

    /** 每个Side一份不可变快照 */
    private static final Map<Side, List<Filter>> CACHE = new ConcurrentHashMap<>();

    private FilterLoader() {
    }

    /** 取某一侧适用、已按order排序的filter列表（缓存）。 */
    public static List<Filter> load(Side side) {
        return CACHE.computeIfAbsent(side, FilterLoader::doLoad);
    }

    /**
     * 清空按侧缓存的filter快照，下次 {@link #load(Side)} 按当前配置重新装载。
     *
     * <p>配置源在启动过程中就绪（如Spring自动配置安装{@code Environment}）时会自动调用；
     * 已有调用方手里持有的filter链不受影响——缓存只决定"之后新装载"的链。</p>
     */
    public static void reset() {
        CACHE.clear();
    }

    private static List<Filter> doLoad(Side side) {
        Map<String, Filter> allWithNames = ExtensionLoader.getExtensionLoader(Filter.class).getExtensionInfos();
        List<Filter> applicable = new ArrayList<>();

        for (Map.Entry<String, Filter> entry : allWithNames.entrySet()) {
            String spiName = entry.getKey();
            Filter f = entry.getValue();

            // 1.按enabled()读配置决定是否装载
            if (!isEnabled(f)) {
                log.info("Filter [{}] 已按配置禁用（conduit.filter.{}.enabled=false），跳过装载", spiName, f.name());
                continue;
            }

            // 2.按appliesTo过滤侧
            if (!f.appliesTo(side)) {
                continue;
            }

            applicable.add(f);
        }

        // 稳定排序：order小的靠外层先执行；同order保持加载顺序
        applicable.sort(Comparator.comparingInt(Filter::order));
        log.info("FilterLoader为{}加载了{}个filter: {}",
                side, applicable.size(),
                applicable.stream().map(Filter::name).toList());
        return List.copyOf(applicable);
    }

    /**
     * 判断某个filter是否启用：配置 {@code conduit.filter.<key>.enabled} 覆盖filter自身默认。
     *
     * <p>解析规则：</p>
     * <ol>
     *   <li>查配置 {@code conduit.filter.<name小写>.enabled}，再退化到类全限定名小写
     *       （经 {@link ConduitConfig}：系统属性 → Spring配置 → 环境变量）；</li>
     *   <li>属性<b>显式设置</b>（true/false）→ 以它为准（可开可关）；</li>
     *   <li>属性<b>未设置</b> → 回退到 {@link Filter#enabled()} 的返回值（filter自己的默认，
     *       如AccessLog/Exception默认true、Metrics/ActiveLimit默认false）。</li>
     * </ol>
     */
    private static boolean isEnabled(Filter filter) {
        String shortKey = filter.name().toLowerCase();
        String fqcnKey = filter.getClass().getName().toLowerCase();

        // 显式设置以属性为准；未设置回退到filter自身默认（enabled()）
        return ConduitConfig.getBoolean("conduit.filter." + shortKey + ".enabled",
                ConduitConfig.getBoolean("conduit.filter." + fqcnKey + ".enabled", filter.enabled()));
    }
}
