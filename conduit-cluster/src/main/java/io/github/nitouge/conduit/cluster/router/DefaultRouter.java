package io.github.nitouge.conduit.cluster.router;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.config.ConduitConfig;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * 默认路由器：按 <b>版本 → 分组 → 标签</b> 做硬过滤，最后按 <b>机房</b> 做软优先。
 *
 * <p>硬/软的区别来自业务语义：版本/分组/标签是「我只能用这一批」，不满足就不能用，所以为空即报错；
 * 机房是「优先同机房，没有就跨机房」，跨机房仍能成功，所以为空时退回上一级结果。</p>
 *
 * <p>约束来源：</p>
 * <ul>
 *   <li>版本/分组：{@link RpcRequest#getVersion()}/{@link RpcRequest#getGroup()}（消费侧由
 *       {@code @RpcReference} 或 {@code RpcContext} 的 {@code conduit.version}/{@code conduit.group} 写入）；</li>
 *   <li>标签：attachment {@code conduit.tag}，对应实现类上的 {@code @RpcService(tag="gray")}；</li>
 *   <li>机房：attachment {@code conduit.zone}，未设置时读配置 {@code conduit.router.zone}。</li>
 * </ul>
 *
 * <p>任何一项为 {@code null}/空串/{@code "*"} 都表示「不约束」，此时完全不参与过滤——这也是
 * 既有调用（不声明版本）行为不变的原因。</p>
 */
public class DefaultRouter implements Router {

    private static final Logger log = LoggerFactory.getLogger(DefaultRouter.class);

    /** 通配：显式声明「不限」 */
    static final String WILDCARD = "*";

    /** 调用方所在机房（attachment 未指定时使用） */
    static final String ZONE_CONFIG_KEY = "conduit.router.zone";

    @Override
    public List<ServiceMeta> route(List<ServiceMeta> providers, RpcRequest request) {
        if (CollectionUtils.isEmpty(providers) || request == null) {
            return providers == null ? List.of() : providers;
        }

        String version = constraint(request.getAttachment(RpcAttachments.VERSION), request.getVersion());
        String group = constraint(request.getAttachment(RpcAttachments.GROUP), request.getGroup());
        String tag = constraint(request.getAttachment(RpcAttachments.TAG));
        String zone = constraint(request.getAttachment(RpcAttachments.ZONE), ConduitConfig.get(ZONE_CONFIG_KEY));

        List<ServiceMeta> matched = providers;
        if (version != null) {
            matched = filter(matched, meta -> version.equals(meta.getVersion()));
        }
        if (group != null) {
            matched = filter(matched, meta -> group.equals(meta.getGroup()));
        }
        if (tag != null) {
            matched = filter(matched, meta -> tag.equals(meta.getTag()));
        }

        if (matched.isEmpty()) {
            // 硬约束落空 = 配置错，明确失败比"悄悄回退到全量"安全得多
            throw new RpcException(noMatchMessage(request, providers, version, group, tag));
        }

        if (zone != null && matched.size() > 1) {
            List<ServiceMeta> sameZone = filter(matched, meta -> zone.equals(meta.getZone()));
            if (!sameZone.isEmpty() && sameZone.size() < matched.size()) {
                log.debug("[DefaultRouter] 同机房优先 {}: 命中{}/{}个实例",
                        request.getInterfaceName(), sameZone.size(), matched.size());
                return sameZone;
            }
        }
        if (matched.size() != providers.size()) {
            log.debug("[DefaultRouter] 路由收窄 {}: {} -> {}个实例 (version={}, group={}, tag={})",
                    request.getInterfaceName(), providers.size(), matched.size(), version, group, tag);
        }
        return matched;
    }

    private static List<ServiceMeta> filter(List<ServiceMeta> providers, Predicate<ServiceMeta> predicate) {
        List<ServiceMeta> result = new ArrayList<>(providers.size());
        for (ServiceMeta meta : providers) {
            if (meta != null && predicate.test(meta)) {
                result.add(meta);
            }
        }
        return result;
    }

    /**
     * 规整一个约束值：空串/{@code null}/{@code "*"} 都表示不约束。
     * 多个候选来源中取第一个有效的（attachment 覆盖静态配置）。
     */
    private static String constraint(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) {
                String trimmed = candidate.trim();
                if (!WILDCARD.equals(trimmed)) {
                    return trimmed;
                }
                return null; // 显式通配：立刻停止，不再看后面更弱的来源
            }
        }
        return null;
    }

    private static String noMatchMessage(RpcRequest request, List<ServiceMeta> candidates,
                                         String version, String group, String tag) {
        StringBuilder sb = new StringBuilder("没有匹配的路由实例: ").append(request.getInterfaceName());
        if (version != null) {
            sb.append(", version=").append(version);
        }
        if (group != null) {
            sb.append(", group=").append(group);
        }
        if (tag != null) {
            sb.append(", tag=").append(tag);
        }
        sb.append(", 候选").append(candidates.size()).append("个=[");
        int limit = Math.min(candidates.size(), 5);
        for (int i = 0; i < limit; i++) {
            ServiceMeta meta = candidates.get(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(meta.getHost()).append(':').append(meta.getPort())
                    .append("(version=").append(meta.getVersion())
                    .append(", group=").append(meta.getGroup())
                    .append(", tag=").append(meta.getTag()).append(')');
        }
        if (candidates.size() > limit) {
            sb.append(", ...");
        }
        sb.append(']');
        return sb.toString();
    }
}
