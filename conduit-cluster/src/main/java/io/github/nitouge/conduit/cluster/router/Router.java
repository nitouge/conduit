package io.github.nitouge.conduit.cluster.router;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.spi.SPI;

import java.util.List;

/**
 * 路由：在负载均衡之前，先把「全部提供者」收窄成「本次调用真正该去的那些实例」。
 *
 * <p>它解决的是负载均衡解决不了的问题：负载均衡只会在给定候选里挑一个，而候选里可能混着
 * v1/v2 两个版本、灰度与正式两组标签、不同机房的机器——先选错集合，再公平地挑也是错的。
 * 所以顺序是 <b>discover → route → loadbalance</b>。</p>
 *
 * <p><b>空结果怎么办：</b>默认实现选择「报错」而不是「回退到全量」。因为空结果几乎总是配置
 * 写错（版本号拼错、灰度标签没打上），静默回退会让流量悄悄打到错误的版本上——那比快速失败
 * 危险得多。错误消息里带上约束条件与候选实例，照日志就能定位。</p>
 *
 * <p>路由是<b>软失败不重试</b>的：它发生在集群容错之前，抛出的异常直接成为本次调用的失败原因。</p>
 */
@SPI("default")
public interface Router {

    /**
     * 过滤候选实例。
     *
     * @param providers 注册中心发现的全部实例（可能为 null/空）
     * @param request   本次请求（版本/分组在字段上，标签/机房在 attachments 上）
     * @return 过滤后的候选实例；<b>不应返回 null</b>
     */
    List<ServiceMeta> route(List<ServiceMeta> providers, RpcRequest request);
}
