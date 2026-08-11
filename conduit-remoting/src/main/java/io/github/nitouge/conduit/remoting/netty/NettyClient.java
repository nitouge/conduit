package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.protocol.codec.RpcDecoder;
import io.github.nitouge.conduit.protocol.codec.RpcEncoder;
import io.github.nitouge.conduit.remoting.PendingRequests;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 客户端传输底座：连接provider、发送请求、关联响应。
 *
 * <p>长连接复用：每个目标地址缓存一条TCP连接；请求-响应关联交给 {@link PendingRequests}。</p>
 */
public class NettyClient {

    private static final Logger log = LoggerFactory.getLogger(NettyClient.class);

    private final EventLoopGroup group = new NioEventLoopGroup();
    private final Bootstrap bootstrap = new Bootstrap();

    /**
     * 连接缓存：每地址复用一条长连接。值用 {@code CompletableFuture<Channel>} 占位符而非Channel本身，
     * 这样建链（可能阻塞数秒的TCP connect）发生在map锁<b>之外</b>——map上只做O(1)的
     * get/putIfAbsent/remove，不违反ConcurrentHashMap映射函数应短小、不得执行耗时操作」的约定。
     */
    private final Map<InetSocketAddress, CompletableFuture<Channel>> channels = new ConcurrentHashMap<>();

    /** 出站请求使用的序列化codec（M3起可配置，默认jdk）。响应由服务端复用请求的codec。 */
    private final byte codec;

    /** 每条连接已写出、未收到响应的requestId集合；连接断开时据此统一失败在途请求 */
    public static final AttributeKey<Set<Long>> IN_FLIGHT = AttributeKey.valueOf("conduit.client.inflight");

    /** 取（或惰性初始化）某连接的在途requestId集合 */
    public static Set<Long> inFlight(Channel channel) {
        Attribute<Set<Long>> attr = channel.attr(IN_FLIGHT);
        Set<Long> set = attr.get();
        if (set == null) {
            set = ConcurrentHashMap.newKeySet();
            Set<Long> prev = attr.setIfAbsent(set);
            if (prev != null) {
                set = prev;
            }
        }
        return set;
    }

    public NettyClient() {
        this(ProtocolConstants.CODEC_JDK);
    }

    public NettyClient(byte codec) {
        this.codec = codec;
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(
                                                ProtocolConstants.MAX_FRAME_LENGTH,
                                                ProtocolConstants.LENGTH_FIELD_OFFSET,
                                                ProtocolConstants.LENGTH_FIELD_LENGTH,
                                                0,
                                                0
                                        )
                                )
                                .addLast(new RpcDecoder())
                                .addLast(new IdleStateHandler(0, 30, 0, TimeUnit.SECONDS)) // 写空闲30s发心跳
                                .addLast(new NettyClientHandler())
                                .addLast(new RpcEncoder());
                    }
                });
    }

    /**
     * 异步发送：生成requestId、登记future、写出请求帧，立即返回future。
     *
     * <p>同时按timeoutMillis在该连接的EventLoop上挂一个超时清理任务：到点若仍未完成，则失败该
     * 请求并从PENDING/inFlight移除——这是超时这条清理路径（避免慢/不回包的请求永久泄漏）。
     * 请求一旦完成（响应/写失败/连接断开/超时）即取消该定时任务，避免定时器堆积。</p>
     */
    public CompletableFuture<RpcResponse> send(RpcRequest request, ServiceMeta target, long timeoutMillis) {
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        long requestId = PendingRequests.register(future);
        log.debug("[NettyClient] 发送请求 requestId={}, {}#{}, target={}:{}, timeout={}ms",
                requestId, request.getInterfaceName(), request.getMethodName(),
                target.getHost(), target.getPort(), timeoutMillis);
        try {
            InetSocketAddress addr = new InetSocketAddress(target.getHost(), target.getPort());
            Channel channel = getChannel(addr);
            inFlight(channel).add(requestId); // 登记到该连接的在途集合，连接断开时统一失败

            // 超时清理任务：到点未完成则失败并清表，杜绝超时请求在PENDING/inFlight中泄漏
            ScheduledFuture<?> timeoutTask = channel.eventLoop().schedule(() -> {
                if (!future.isDone()) {
                    inFlight(channel).remove(requestId);
                    log.debug("[NettyClient] 请求超时清理 requestId={}, timeout={}ms", requestId, timeoutMillis);
                    PendingRequests.completeExceptionally(requestId,
                            new TimeoutException("调用超时(" + timeoutMillis + "ms)"));
                }
            }, timeoutMillis, TimeUnit.MILLISECONDS);
            // 完成后取消定时任务，避免已完成请求仍占着定时器
            future.whenComplete((resp, err) -> timeoutTask.cancel(false));

            RpcMessage message = RpcMessage.request(requestId, codec, request);
            channel.writeAndFlush(message).addListener(f -> {
                if (!f.isSuccess()) {
                    inFlight(channel).remove(requestId);
                    log.warn("[NettyClient] 请求写出失败 requestId={}, target={}:{}: {}",
                            requestId, target.getHost(), target.getPort(), String.valueOf(f.cause()));
                    PendingRequests.completeExceptionally(requestId, f.cause());
                }
            });
        } catch (Throwable t) {
            log.warn("[NettyClient] 发送异常 requestId={}, target={}:{}: {}",
                    requestId, target.getHost(), target.getPort(), t.toString());
            PendingRequests.completeExceptionally(requestId, t);
        }
        return future;
    }

    /**
     * 取/建到目标地址的长连接（阻塞直到建链完成）；连接关闭时自动从缓存移除。
     *
     * <p>并发约定：同一地址只会建链一次——首个线程用 {@code putIfAbsent} 抢到占位符后在<b>锁外</b>建链，
     * 其余线程 {@code join()} 同一占位符等待其完成；建链失败或连接失效则移除占位符后重试。</p>
     */
    public Channel getChannel(InetSocketAddress address) {
        while (true) {
            CompletableFuture<Channel> holder = channels.get(address);
            if (holder != null) {
                Channel ch;
                try {
                    ch = holder.join();          // 在map锁之外等待建链结果
                } catch (Throwable t) {
                    channels.remove(address, holder); // 建链失败的占位符：移除后重试
                    continue;
                }
                if (ch.isActive()) {
                    return ch;
                }
                channels.remove(address, holder); // 连接已失效：移除后重建
                continue;
            }
            CompletableFuture<Channel> created = new CompletableFuture<>();
            CompletableFuture<Channel> prev = channels.putIfAbsent(address, created); // O(1)占位
            if (prev != null) {
                continue; // 别的线程正在建链，下一轮走它的占位符
            }
            try {
                Channel newChannel = bootstrap.connect(address).syncUninterruptibly().channel(); // 锁外建链
                newChannel.closeFuture().addListener(f -> channels.remove(address, created));
                created.complete(newChannel);
                return newChannel;
            } catch (Throwable t) {
                channels.remove(address, created); // 回滚占位符，唤醒等待者去重试
                created.completeExceptionally(t);
                throw t;
            }
        }
    }

    /** 关闭客户端：断开所有连接并释放线程组 */
    public void close() {
        channels.values().forEach(holder -> {
            Channel ch = holder.getNow(null); // 仅关闭已建成的连接，未完成的占位符忽略
            if (ch != null) {
                ch.close();
            }
        });
        channels.clear();
        group.shutdownGracefully();
    }
}
