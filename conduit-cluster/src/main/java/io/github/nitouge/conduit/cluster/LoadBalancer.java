package io.github.nitouge.conduit.cluster;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.spi.Adaptive;
import io.github.nitouge.conduit.common.spi.SPI;

import java.util.List;

/**
 * 负载均衡：从一批候选实例里为本次调用选一个。@SPI扩展点，默认random。
 *
 * <p>方法标注 {@link Adaptive}：配合 {@code ExtensionLoader.getAdaptiveExtension()}，可按
 * {@code RpcContext} 的 {@code loadbalance} attachment对单次调用临时切换策略（未设置则用默认）。</p>
 */
@SPI("random")
public interface LoadBalancer {

    /**
     * @param services 候选实例（非空）
     * @param request  本次请求（ConsistentHash等会用到方法/参数做路由）
     * @return 选中的实例；services为空时返回null
     */
    @Adaptive({"loadbalance"})
    ServiceMeta select(List<ServiceMeta> services, RpcRequest request);
}
