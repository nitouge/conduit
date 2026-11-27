package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.protocol.codec.RpcDecoder;
import io.github.nitouge.conduit.protocol.codec.RpcEncoder;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.PendingRequests;
import io.github.nitouge.conduit.remoting.handshake.ClientHandshakeHandler;
import io.github.nitouge.conduit.remoting.handshake.Handshake;
import io.github.nitouge.conduit.remoting.handshake.Negotiated;
import io.github.nitouge.conduit.remoting.pool.ConnectionPool;
import io.github.nitouge.conduit.remoting.reconnect.ReconnectManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * 客户端传输底座：建连（TCP + 协议握手）→ 选连接（池）→ 发送请求 → 关联响应，并负责保活与断线重连。
 *
 * <p>一条请求的完整生命周期：</p>
 * <ol>
 *   <li>生成全局requestId，登记到 {@link PendingRequests} 并挂上超时清理任务；</li>
 *   <li>向 {@link ConnectionPool} 要一条该地址的<b>已握手</b>连接（没有就建，建链单飞）；</li>
 *   <li>把requestId登记到该连接的在途集合，用协商出的version/codec/compress编码请求帧写出；
 *   <b>写之前先过背压阀</b>（在途上限 + channel 可写性），服务端变慢时快速失败而不是无限堆积；</li>
 *   <li>响应到达时由 {@link NettyClientHandler} 按requestId唤醒future；</li>
 *   <li>超时或调用方放弃时，在清理在途表的同时给对端发一帧 CANCEL（让服务端别白跑）；</li>
 *   <li>连接断开时在途请求统一失败并触发后台重连。</li>
 * </ol>
 *
 * <p>整个发送过程不阻塞调用线程（连接建立也是异步的）：同步调用由上层在future上等待。</p>
 */
public class NettyClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(NettyClient.class);

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

    private final ClientOptions options;
    private final EventLoopGroup group;
    private final ScheduledExecutorService timeoutScheduler;
    private final ConnectionPool pool;
    private final ReconnectManager reconnectManager;
    /** 本客户端发出的、尚未完成的请求id（关闭时统一失败，避免调用方干等） */
    private final Set<Long> pendingRequestIds = ConcurrentHashMap.newKeySet();
    private final LongAdder heartbeatsSent = new LongAdder();
    private final LongAdder heartbeatsReceived = new LongAdder();
    private final LongAdder responsesReceived = new LongAdder();
    /** 因背压（在途上限/写缓冲水位）被拒绝的请求数：持续增长说明客户端正在被压垮 */
    private final LongAdder backpressureRejections = new LongAdder();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** 默认配置（jdk序列化、无压缩、单连接、心跳30s、断线重连开启） */
    public NettyClient() {
        this(ClientOptions.defaults());
    }

    /** 只指定默认序列化codec */
    public NettyClient(byte codec) {
        this(ClientOptions.of(codec));
    }

    public NettyClient(ClientOptions options) {
        this.options = options;
        // 客户端线程设为daemon：应用退出时不因客户端未close而卡住JVM
        this.group = new NioEventLoopGroup(0,
                new DefaultThreadFactory("conduit-client", true)
        );
        this.timeoutScheduler = Executors.newSingleThreadScheduledExecutor(
                daemonThreadFactory("conduit-client-timeout")
        );
        this.pool = new ConnectionPool(this::connect,
                channel -> inFlight(channel).size(),
                options.getMaxConnectionsPerAddress(),
                0,
                options.getIdleConnectionTimeoutMillis()
        );
        this.reconnectManager = options.isReconnectEnabled() && options.getReconnectMaxAttempts() > 0
                ? new ReconnectManager(pool::ensureConnection, options.getReconnectMaxAttempts(),
                options.getReconnectBaseDelayMillis(), options.getReconnectMaxDelayMillis())
                : null;
        if (reconnectManager != null) {
            pool.setConnectionListener(reconnectManager::onDisconnected);
        }
        log.debug("[NettyClient] 初始化完成, {}", options);
    }

    /**
     * 为一次建链创建Bootstrap：TLS场景必须把<b>对端主机名</b>交给SslHandler（newHandler带peerHost/peerPort），
     * 否则JDK SSLEngine不知道peerHost，HTTPS主机名校验会直接失败（"Hostname or IP address is undefined"）。
     * Bootstrap本身很轻，每个目标地址建链时创建一次即可。
     */
    private Bootstrap newBootstrap(String peerHost, int peerPort) {
        return new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        (int) Math.min(Integer.MAX_VALUE, options.getConnectTimeoutMillis()))
                // 写缓冲水位：超过高水位后 channel 变不可写，客户端据此拒绝新请求（写背压）
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                        options.getWriteBufferLowWaterMark(), options.getWriteBufferHighWaterMark()))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        initPipeline(ch, peerHost, peerPort);
                    }
                });
    }

    /** 装配客户端pipeline：TLS(可选) -> 切帧 -> 解义 -> 空闲检测 -> 心跳 -> 握手 -> 业务 -> 编码 */
    private void initPipeline(SocketChannel ch, String peerHost, int peerPort) {
        ChannelPipeline pipeline = ch.pipeline();
        if (options.getSslContext() != null) {
            SslHandler sslHandler = options.getSslContext().newHandler(ch.alloc(), peerHost, peerPort);
            sslHandler.setHandshakeTimeoutMillis(options.getHandshakeTimeoutMillis());
            pipeline.addLast("ssl", sslHandler);
        }
        pipeline.addLast(new LengthFieldBasedFrameDecoder(
                ProtocolConstants.MAX_FRAME_LENGTH,
                ProtocolConstants.LENGTH_FIELD_OFFSET,
                ProtocolConstants.LENGTH_FIELD_LENGTH,
                0,
                0));
        pipeline.addLast(new RpcDecoder());
        if (options.getHeartbeatIntervalMillis() > 0 || options.getReadIdleTimeoutMillis() > 0) {
            pipeline.addLast(new IdleStateHandler(
                    options.getReadIdleTimeoutMillis(),
                    options.getHeartbeatIntervalMillis(),
                    0,
                    TimeUnit.MILLISECONDS));
        }
        pipeline.addLast(new ClientHeartbeatHandler(options.getReadIdleTimeoutMillis(),
                heartbeatsSent, heartbeatsReceived));
        pipeline.addLast(new ClientHandshakeHandler(ProtocolConstants.MAX_SUPPORTED_VERSION,
                options.getCodec(), options.getCompress(), options.getHandshakeTimeoutMillis()));
        pipeline.addLast(new NettyClientHandler(responsesReceived));
        pipeline.addLast(new RpcEncoder(options.protocolOptions()));
    }

    /**
     * 异步发送：生成requestId、登记future、取连接并写出请求帧，立即返回future。
     *
     * <p>超时清理任务在独立调度线程上执行（不依赖EventLoop生命周期），到点若仍未完成则失败该请求
     * 并从PENDING/inFlight移除，杜绝超时请求泄漏。请求一旦完成（响应/写失败/连接断开/超时/关闭）
     * 即取消该任务，避免定时器堆积。</p>
     */
    public CompletableFuture<RpcResponse> send(RpcRequest request, ServiceMeta target, long timeoutMillis) {
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        if (closed.get()) {
            future.completeExceptionally(new RpcException("客户端已关闭，无法发送请求"));
            return future;
        }
        if (timeoutMillis <= 0) {
            future.completeExceptionally(new RpcException("超时时间必须为正数: " + timeoutMillis + "ms"));
            return future;
        }

        final long requestId = PendingRequests.register(future);
        pendingRequestIds.add(requestId);

        final AtomicReference<Channel> channelRef = new AtomicReference<>();
        final ScheduledFuture<?> timeoutTask;
        try {
            timeoutTask = timeoutScheduler.schedule(() -> {
                Channel channel = channelRef.get();
                if (channel != null) {
                    inFlight(channel).remove(requestId);
                    // 告诉服务端"我不等了"：否则服务端线程仍会把这次调用跑完（长尾放大的根源）
                    sendCancel(channel, requestId, ProtocolConstants.CANCEL_CLIENT_TIMEOUT);
                }
                PendingRequests.completeExceptionally(requestId,
                        new TimeoutException("调用超时(" + timeoutMillis + "ms)"));
            }, timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // 客户端正在关闭（调度器已停止）：立即失败，不能把请求挂在关联表里
            pendingRequestIds.remove(requestId);
            PendingRequests.completeExceptionally(requestId, new RpcException("客户端已关闭，无法发送请求", e));
            return future;
        }
        future.whenComplete((response, error) -> {
            timeoutTask.cancel(false);
            pendingRequestIds.remove(requestId);
            // 调用方主动取消（future.cancel/CompletableFuture 链上放弃）：同样把取消送达到对端
            if (future.isCancelled()) {
                Channel channel = channelRef.get();
                if (channel != null) {
                    inFlight(channel).remove(requestId);
                    sendCancel(channel, requestId, ProtocolConstants.CANCEL_CALLER_ABANDONED);
                }
            }
        });
        // 与close竞态：调度成功后客户端可能已关闭（关闭逻辑已清空在途请求），这里再兜底一次
        if (closed.get()) {
            pendingRequestIds.remove(requestId);
            PendingRequests.completeExceptionally(requestId, new RpcException("客户端已关闭，无法发送请求"));
            return future;
        }

        final InetSocketAddress address = new InetSocketAddress(target.getHost(), target.getPort());
        log.debug("[NettyClient] 发送请求 requestId={}, {}#{}, target={}:{}, timeout={}ms",
                requestId, request.getInterfaceName(), request.getMethodName(),
                target.getHost(), target.getPort(), timeoutMillis);

        pool.acquire(address).whenComplete((channel, acquireError) -> {
            if (acquireError != null) {
                log.warn("[NettyClient] 获取连接失败 requestId={}, target={}:{}: {}",
                        requestId, target.getHost(), target.getPort(), String.valueOf(acquireError));
                PendingRequests.completeExceptionally(requestId, unwrap(acquireError));
                return;
            }
            // 取连接期间已超时/被取消：不再写出，避免"超时之后仍然发出请求"
            if (future.isDone()) {
                inFlight(channel).remove(requestId);
                return;
            }
            // 背压阀：服务端变慢时，请求会先堆写缓冲、再堆在途表，最后把客户端内存打满。
            // 与其无限堆积，不如立刻拒绝——调用方还能立刻决策（降级/重试/熔断），而不是等着超时。
            String rejection = backpressureReason(channel);
            if (rejection != null) {
                backpressureRejections.increment();
                log.warn("[NettyClient] 背压拒绝 requestId={}, target={}:{}: {}",
                        requestId, target.getHost(), target.getPort(), rejection);
                PendingRequests.completeExceptionally(requestId, new RpcException(rejection));
                return;
            }
            channelRef.set(channel);
            inFlight(channel).add(requestId);
            // 缩小时序窗口：登记在途后再次确认请求是否已被超时清理，避免留下"僵尸在途条目"
            if (future.isDone()) {
                inFlight(channel).remove(requestId);
                return;
            }

            // 使用握手的协商结果编码：版本/codec/压缩算法两端必须一致
            Negotiated negotiated = Handshake.negotiated(channel);
            byte version = negotiated != null ? negotiated.getVersion() : ProtocolConstants.VERSION;
            byte codec = negotiated != null ? negotiated.getCodec() : options.getCodec();
            byte compress = negotiated != null ? negotiated.getCompress() : options.getCompress();

            RpcMessage message = RpcMessage.request(requestId, codec, request);
            message.setVersion(version);
            message.setCompress(compress);
            channel.writeAndFlush(message).addListener(writeFuture -> {
                if (!writeFuture.isSuccess()) {
                    inFlight(channel).remove(requestId);
                    log.warn("[NettyClient] 请求写出失败 requestId={}, target={}:{}: {}",
                            requestId, target.getHost(), target.getPort(), String.valueOf(writeFuture.cause()));
                    PendingRequests.completeExceptionally(requestId, unwrap(writeFuture.cause()));
                }
            });
        });
        return future;
    }

    /**
     * 判断这条连接现在还能不能接新请求（写背压）。
     *
     * <p>两个水位指标：</p>
     * <ul>
     *   <li><b>在途请求数</b>：服务端处理慢时，在途表只增不减，这是最直接的堆积来源；</li>
     *   <li><b>channel 可写性</b>：Netty 出站缓冲超过高水位后 {@code isWritable()} 为 false，
     *       说明对端读得慢/TCP 发送窗口打满，再写只会继续占内存。</li>
     * </ul>
     *
     * @return 拒绝原因；{@code null} 表示可以发送
     */
    private String backpressureReason(Channel channel) {
        int inFlight = inFlight(channel).size();
        int limit = options.getMaxInFlightPerConnection();
        if (inFlight >= limit) {
            return "背压：单连接在途请求已达上限(" + inFlight + "/" + limit + ")，快速失败";
        }
        if (!channel.isWritable()) {
            return "背压：写缓冲超过高水位(" + options.getWriteBufferHighWaterMark()
                    + "字节)，对端消费变慢，快速失败";
        }
        return null;
    }

    /**
     * 尽力而为地给对端发一帧 CANCEL：告诉服务端这次调用不必再跑。
     *
     * <p>失败只记 debug——调用方此刻已经拿到结果（超时/取消），取消帧发不出去不应改变任何语义；
     * 连接已断时也没必要发（对端要么已断开、要么稍后会因连接断开自行收尾）。</p>
     */
    private void sendCancel(Channel channel, long requestId, byte reason) {
        if (channel == null || !channel.isActive()) {
            return;
        }
        Negotiated negotiated = Handshake.negotiated(channel);
        byte version = negotiated != null ? negotiated.getVersion() : ProtocolConstants.VERSION;
        byte codec = negotiated != null ? negotiated.getCodec() : options.getCodec();
        RpcMessage cancel = RpcMessage.cancel(requestId, codec, version, reason);
        channel.writeAndFlush(cancel).addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                log.debug("[NettyClient] CANCEL发送失败 requestId={}, reason={}: {}",
                        requestId, ProtocolConstants.cancelReasonText(reason),
                        String.valueOf(writeFuture.cause()));
            }
        });
    }

    /**
     * 阻塞取一条到目标地址的已握手连接（保留给需要直接操作连接的调用方）。
     *
     * @throws RpcException 建连失败/超时
     */
    public Channel getChannel(InetSocketAddress address) {
        try {
            long waitMillis = options.getConnectTimeoutMillis() + options.getHandshakeTimeoutMillis();
            return pool.acquire(address).get(waitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException("等待连接被中断: " + address, e);
        } catch (ExecutionException e) {
            throw new RpcException("建连失败: " + address, unwrap(e.getCause() != null ? e.getCause() : e));
        } catch (TimeoutException e) {
            throw new RpcException("建连超时: " + address, e);
        }
    }

    /** 建连（TCP + 协议握手），连接池的Connector实现 */
    private CompletableFuture<Channel> connect(InetSocketAddress address) {
        CompletableFuture<Channel> result = new CompletableFuture<>();
        try {
            newBootstrap(address.getHostString(), address.getPort()).connect(address)
                    .addListener((ChannelFutureListener) connectFuture -> {
                if (!connectFuture.isSuccess()) {
                    result.completeExceptionally(connectFuture.cause());
                    return;
                }
                Channel channel = connectFuture.channel();
                CompletableFuture<Negotiated> handshakeFuture = ClientHandshakeHandler.channelFuture(channel);
                handshakeFuture.whenComplete((negotiated, error) -> {
                    if (error != null) {
                        channel.close();
                        result.completeExceptionally(unwrap(error));
                    } else {
                        result.complete(channel);
                    }
                });
            });
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
        return result;
    }

    /** 当前到目标地址的活跃连接数（连接池视角） */
    public int activeConnections(InetSocketAddress address) {
        return pool.activeCount(address);
    }

    /** 当前到目标地址的活跃连接数 */
    public int activeConnections(String host, int port) {
        return pool.activeCount(new InetSocketAddress(host, port));
    }

    /** 全客户端活跃连接数 */
    public int totalActiveConnections() {
        return pool.totalActiveCount();
    }

    /** 本客户端尚未完成的请求数 */
    public int pendingRequestCount() {
        return pendingRequestIds.size();
    }

    public long getHeartbeatsSent() {
        return heartbeatsSent.sum();
    }

    public long getHeartbeatsReceived() {
        return heartbeatsReceived.sum();
    }

    public long getResponsesReceived() {
        return responsesReceived.sum();
    }

    /** 因背压（在途上限/写缓冲水位）被拒绝的请求数 */
    public long getBackpressureRejections() {
        return backpressureRejections.sum();
    }

    public ClientOptions getOptions() {
        return options;
    }

    public boolean isClosed() {
        return closed.get();
    }

    /** 关闭客户端：失败在途请求 → 停止重连 → 关闭连接池 → 释放线程组 */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("正在关闭Conduit NettyClient...");
        RpcException cause = new RpcException("客户端已关闭");
        for (Long requestId : pendingRequestIds) {
            PendingRequests.completeExceptionally(requestId, cause);
        }
        pendingRequestIds.clear();

        if (reconnectManager != null) {
            reconnectManager.close();
        }
        pool.close();
        timeoutScheduler.shutdownNow();

        Future<?> shutdown = group.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        if (!inEventLoop()) {
            shutdown.syncUninterruptibly();
        }
        log.info("Conduit NettyClient已关闭");
    }

    private boolean inEventLoop() {
        for (EventExecutor executor : group) {
            if (executor.inEventLoop()) {
                return true;
            }
        }
        return false;
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof java.util.concurrent.CompletionException && t.getCause() != null)
                ? t.getCause() : t;
    }

    private static ThreadFactory daemonThreadFactory(String prefix) {
        AtomicInteger index = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + index.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
