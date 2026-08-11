package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.codec.RpcDecoder;
import io.github.nitouge.conduit.protocol.codec.RpcEncoder;
import io.github.nitouge.conduit.remoting.RequestHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务端传输底座：监听端口、接收请求帧、把业务处理派发到业务线程池。
 *
 * <p>pipeline：FrameDecoder（切帧）-> RpcDecoder（解义）-> IdleStateHandler（读空闲探活）-> NettyServerHandler（业务派发）-> RpcEncoder（出站编码）。</p>
 */
public class NettyServer {

    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    /** 业务线程池有界队列默认容量：限制堆积上限，超过则触发拒绝（背压），避免无界队列OOM */
    private static final int DEFAULT_QUEUE_CAPACITY = 10_000;

    private final String host;
    private final int port;
    private final RequestHandler handler;
    private final int businessThreads;
    private final int queueCapacity;

    private EventLoopGroup boss;
    private EventLoopGroup worker;
    private ExecutorService businessPool;
    private Channel serverChannel;

    public NettyServer(String host, int port, RequestHandler handler) {
        this(host, port, handler, 200, DEFAULT_QUEUE_CAPACITY);
    }

    public NettyServer(String host, int port, RequestHandler handler, int businessThreads) {
        this(host, port, handler, businessThreads, DEFAULT_QUEUE_CAPACITY);
    }

    public NettyServer(String host, int port, RequestHandler handler, int businessThreads, int queueCapacity) {
        this.host = host;
        this.port = port;
        this.handler = handler;
        this.businessThreads = businessThreads;
        this.queueCapacity = queueCapacity;
    }

    /** 启动并绑定端口（阻塞直到绑定完成） */
    public void start() {
        boss = new NioEventLoopGroup(1);
        worker = new NioEventLoopGroup();
        // 有界队列 + AbortPolicy：过载时拒绝而非无限堆积；被拒的请求由NettyServerHandler捕获并
        // 回写服务繁忙错误响应，让客户端快速失败，而不是无界增长直到OOM或干等到超时。
        businessPool = new ThreadPoolExecutor(
                businessThreads,
                businessThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                namedFactory("conduit-biz"),
                new ThreadPoolExecutor.AbortPolicy()
        );

        ServerBootstrap b = new ServerBootstrap();
        b.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
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
                                .addLast(new IdleStateHandler(
                                                60,
                                                0,
                                                0,
                                                TimeUnit.SECONDS
                                        )
                                ) // 读空闲60s
                                .addLast(new NettyServerHandler(handler, businessPool))
                                .addLast(new RpcEncoder());
                    }
                });

        serverChannel = b.bind(host, port).syncUninterruptibly().channel();
        log.info("Conduit NettyServer已启动，监听{}:{}", host, port);
    }

    /**
     * 优雅停机（顺序很重要）：先停止接收新连接（关serverChannel），再排空业务线程池，
     * 最后才关闭worker/boss事件循环。这样provider异步方法在途的CompletableFuture完成后，
     * 仍有存活的worker EventLoop能把响应回写出去，避免在途响应被丢弃、客户端干等到超时。
     */
    public void close() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        // 1.先排空业务线程池（阻塞等待在途业务执行完），worker此时仍存活可回写响应
        if (businessPool != null) {
            businessPool.shutdown();
            try {
                if (!businessPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    businessPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                businessPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        // 2.再关闭事件循环
        if (worker != null) {
            worker.shutdownGracefully();
        }
        if (boss != null) {
            boss.shutdownGracefully();
        }
        log.info("Conduit NettyServer已关闭");
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
