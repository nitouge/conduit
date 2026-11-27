package io.github.nitouge.conduit.client;

/**
 * 一个 {@code @RpcReference} 的静态路由偏好：版本、分组、默认标签。
 *
 * <p>它们是「这个消费方要找哪一类提供者」的声明，不是单次调用的参数：单次覆盖走
 * {@code RpcContext} 的 attachment（{@code conduit.version}/{@code conduit.group}/{@code conduit.tag}），
 * 那条路优先级更高，用于灰度切流这类临时动作。</p>
 *
 * <p>空串与 {@code null} 等价（都表示「不约束」），构造时统一规整成 {@code null}，避免调用方
 * 到处判空。</p>
 */
public final class ReferenceOptions {

    /** 不约束任何路由条件 */
    private static final ReferenceOptions EMPTY = new ReferenceOptions(null, null, null);

    private final String version;
    private final String group;
    private final String tag;

    public ReferenceOptions(String version, String group, String tag) {
        this.version = trimToNull(version);
        this.group = trimToNull(group);
        this.tag = trimToNull(tag);
    }

    /** 无路由约束（等价于只用注册中心里该接口的全部实例） */
    public static ReferenceOptions empty() {
        return EMPTY;
    }

    /** 期望的服务版本；{@code null} 表示不限版本 */
    public String getVersion() {
        return version;
    }

    /** 期望的服务分组；{@code null} 表示不限分组 */
    public String getGroup() {
        return group;
    }

    /** 默认路由标签（灰度/压测等）；{@code null} 表示不带标签 */
    public String getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return "ReferenceOptions{version='" + version + "', group='" + group + "', tag='" + tag + "'}";
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
