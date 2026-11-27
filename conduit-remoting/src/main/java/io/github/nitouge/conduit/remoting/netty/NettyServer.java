package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.codec.RpcDecoder;
import io.github.nitouge.conduit.protocol.codec.RpcEncoder;
import io.github.nitouge.conduit.remoting.RequestHandler;
import io.github.nitouge.conduit.remoting.ServerOptions;
import io.github.nitouge.conduit.remoting.handshake.ServerHandshakeHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * 服务端传输底座：监听端口、完成协议握手、接收请求帧、把业务处理派发到业务线程池，并负责优雅停机。
 *
 * <p>pipeline：TLS(可选) -> FrameDecoder（切帧）-> RpcDecoder（解义）-> IdleStateHandler（读空闲探活）
 * -> ServerHeartbeatHandler（PING/PONG）-> ServerHandshakeHandler（握手/拒绝未握手业务帧）
 * -> NettyServerHandler（业务派发）-> RpcEncoder（出站编码）。</p>
 */
public class NettyServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    private final String host;
    private final int port;
    private final RequestHandler handler;
    private final ServerOptions options;

    private EventLoopGroup boss;
    private EventLoopGroup worker;
    private ExecutorService businessPool;
    private Channel serverChannel;
    /**
     * 所有已接入的连接（用于状态观测与关闭时统一断开）
     */
    private final ChannelGroup childChannels = new DefaultChannelGroup(
            "conduit-server-children", GlobalEventExecutor.INSTANCE);
    private final LongAdder heartbeatsReceived = new LongAdder();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public NettyServer(String host, int port, RequestHandler handler) {
        this(host, port, handler, ServerOptions.defaults());
    }

    public NettyServer(String host, int port, RequestHandler handler, int businessThreads) {
        this(host, port, handler, ServerOptions.builder().businessThreads(businessThreads).build());
    }

    public NettyServer(String host, int port, RequestHandler handler, int businessThreads, int queueCapacity) {
        this(host, port, handler, ServerOptions.builder()
                .businessThreads(businessThreads)
                .queueCapacity(queueCapacity)
                .build());
    }

    public NettyServer(String host, int port, RequestHandler handler, ServerOptions options) {
        this.host = host;
        this.port = port;
        this.handler = handler;
        this.options = options;
    }

    /**
     * 启动并绑定端口（阻塞直到绑定完成）
     */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("NettyServer已关闭，不能再次启动: " + host + ":" + port);
        }
        if (serverChannel != null && serverChannel.isActive()) {
            throw new IllegalStateException("NettyServer已启动: " + host + ":" + port);
        }
        boss = new NioEventLoopGroup(1);
        worker = new NioEventLoopGroup();
        // 有界队列 + AbortPolicy：过载时拒绝而非无限堆积；被拒的请求由NettyServerHandler捕获并
        // 回写服务繁忙错误响应，让客户端快速失败，而不是无界增长直到OOM或干等到超时。
        businessPool = new ThreadPoolExecutor(
                options.getBusinessThreads(),
                options.getBusinessThreads(),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(options.getQueueCapacity()),
                namedFactory("conduit-biz"),
                new ThreadPoolExecutor.AbortPolicy()
        );

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        initPipeline(ch);
                    }
                });

        serverChannel = bootstrap.bind(host, port).syncUninterruptibly().channel();
        log.info("Conduit NettyServer已启动，监听{}:{}（{})", host, port, options);
    }

    /**
     * 装配服务端pipeline
     */
    private void initPipeline(SocketChannel ch) {
        childChannels.add(ch);
        ChannelPipeline pipeline = ch.pipeline();
        if (options.getSslContext() != null) {
            SslHandler sslHandler = options.getSslContext().newHandler(ch.alloc());
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
        if (options.getReaderIdleTimeoutMillis() > 0) {
            pipeline.addLast(new IdleStateHandler(
                    options.getReaderIdleTimeoutMillis(), 0, 0, TimeUnit.MILLISECONDS));
        }
        pipeline.addLast(new ServerHeartbeatHandler(options.getReaderIdleTimeoutMillis(), heartbeatsReceived));
        pipeline.addLast(new ServerHandshakeHandler(options.getHandshakeTimeoutMillis()));
        pipeline.addLast(new NettyServerHandler(handler, businessPool));
        pipeline.addLast(new RpcEncoder(options.protocolOptions()));
    }

    /**
     * 优雅停机（顺序很重要）：先停止接收新连接（关serverChannel），再排空业务线程池，
     * 之后关闭所有已接入连接，最后才关闭worker/boss事件循环。
     *
     * <p>先排空业务线程池，provider异步方法在途的CompletableFuture完成后仍有存活的worker EventLoop
     * 能把响应回写出去，避免在途响应被丢弃、客户端干等到超时。</p>
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        // 1.先排空业务线程池（阻塞等待在途业务执行完），worker此时仍存活可回写响应
        if (businessPool != null) {
            businessPool.shutdown();
            try {
                if (!businessPool.awaitTermination(options.getShutdownTimeoutMillis(), TimeUnit.MILLISECONDS)) {
                    businessPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                businessPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        // 2.关闭已接入的连接（此时业务已排空，响应已写出）
        childChannels.close().awaitUninterruptibly(options.getShutdownTimeoutMillis(), TimeUnit.MILLISECONDS);
        // 3.再关闭事件循环
        Future<?> workerShutdown = null;
        Future<?> bossShutdown = null;
        if (worker != null) {
            workerShutdown = worker.shutdownGracefully(0,
                    options.getShutdownTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        if (boss != null) {
            bossShutdown = boss.shutdownGracefully(0,
                    options.getShutdownTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        if (!inEventLoop()) {
            if (workerShutdown != null) {
                workerShutdown.syncUninterruptibly();
            }
            if (bossShutdown != null) {
                bossShutdown.syncUninterruptibly();
            }
        }
        log.info("Conduit NettyServer已关闭");
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public ServerOptions getOptions() {
        return options;
    }

    /**
     * 当前已接入的连接数
     */
    public int getActiveConnectionCount() {
        return childChannels.size();
    }

    /**
     * 已接收的PING数（保活观测）
     */
    public long getHeartbeatsReceived() {
        return heartbeatsReceived.sum();
    }

    public boolean isStarted() {
        return serverChannel != null && serverChannel.isActive();
    }

    private boolean inEventLoop() {
        EventLoopGroup group = worker;
        if (group == null) {
            return false;
        }
        for (EventExecutor executor : group) {
            if (executor.inEventLoop()) {
                return true;
            }
        }
        return false;
    }

    private static ThreadFactory namedFactory(String prefix) {
        AtomicInteger idx = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + idx.incrementAndGet());
            t.setDaemon(false);
            return t;
        };
    }
}
