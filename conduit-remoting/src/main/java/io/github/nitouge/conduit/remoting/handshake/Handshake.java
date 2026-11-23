package io.github.nitouge.conduit.remoting.handshake;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.concurrent.CompletableFuture;

/**
 * 握手在Channel上的挂载点：客户端用 {@link #FUTURE} 等待握手结果（连接只有握手成功才可用），
 * 两端都用 {@link #NEGOTIATED} 记录协商结果（服务端据此判断"是否已完成握手"）。
 */
public final class Handshake {

    private Handshake() {
    }

    /** 客户端：握手结果future（在handlerAdded时创建，channelActive时发起握手） */
    public static final AttributeKey<CompletableFuture<Negotiated>> FUTURE =
            AttributeKey.valueOf("conduit.handshake.future");

    /** 两端：协商结果（null表示尚未完成握手） */
    public static final AttributeKey<Negotiated> NEGOTIATED =
            AttributeKey.valueOf("conduit.handshake.negotiated");

    /** 取该连接协商结果；未完成握手返回null */
    public static Negotiated negotiated(Channel channel) {
        return channel.attr(NEGOTIATED).get();
    }

    /** 该连接是否已完成握手 */
    public static boolean isCompleted(Channel channel) {
        return negotiated(channel) != null;
    }
}
