package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.protocol.codec.RpcDecoder;
import io.github.nitouge.conduit.protocol.codec.RpcEncoder;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.ServerOptions;
import io.github.nitouge.conduit.remoting.TestSupport;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 心跳保活集成测试：PING/PONG 双向保活、客户端读空闲探活、服务端读空闲清理。
 */
class HeartbeatIntegrationTest {

    @Test
    void pingPongKeepsConnectionAlive() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler());
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .heartbeatIntervalMillis(100)
                .readIdleTimeoutMillis(500)
                .build());
        try {
            assertEquals("ok:ping", TestSupport.call(client, port, "ping").getData());

            TestSupport.awaitTrue("客户端应发出PING",
                    2_000, () -> client.getHeartbeatsSent() >= 1);
            TestSupport.awaitTrue("服务端应收到PING",
                    2_000, () -> server.getHeartbeatsReceived() >= 1);
            TestSupport.awaitTrue("客户端应收到PONG",
                    2_000, () -> client.getHeartbeatsReceived() >= 1);

            Channel channel = client.getChannel(new InetSocketAddress(TestSupport.HOST, port));
            assertTrue(channel.isActive(), "PONG应刷新读空闲计时，连接保持存活");
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void clientDetectsDeadPeerAndClosesConnection() throws Exception {
        int port = TestSupport.freePort();
        // 假死服务端：正常握手但不回PONG
        try (FakeServer fakeServer = FakeServer.start(port, false)) {
            NettyClient client = new NettyClient(ClientOptions.builder()
                    .heartbeatIntervalMillis(100)
                    .readIdleTimeoutMillis(300)
                    .reconnectEnabled(false)
                    .build());
            try {
                assertTrue(client.getChannel(new InetSocketAddress(TestSupport.HOST, port)).isActive());

                TestSupport.awaitTrue("读空闲超时应判定对端失联并关闭连接",
                        3_000, () -> client.activeConnections(TestSupport.HOST, port) == 0);
                assertTrue(client.getHeartbeatsSent() >= 1, "关闭前应至少发过一次PING");
            } finally {
                client.close();
            }
        }
    }

    @Test
    void serverDetectsSilentClientAndReleasesConnection() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler(),
                ServerOptions.builder().readerIdleTimeoutMillis(200).build());
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .heartbeatIntervalMillis(0)
                .readIdleTimeoutMillis(0)
                .reconnectEnabled(false)
                .build());
        try {
            assertEquals("ok:ping", TestSupport.call(client, port, "ping").getData());
            assertEquals(1, server.getActiveConnectionCount());

            TestSupport.awaitTrue("服务端应在读空闲后释放失联连接",
                    3_000, () -> server.getActiveConnectionCount() == 0);
        } finally {
            client.close();
            server.close();
        }
    }

    /**
     * 假死服务端：完成协议握手，但（可选）不回PONG、不处理业务请求，用于验证客户端读空闲探活。
     */
    private static final class FakeServer implements AutoCloseable {

        private final EventLoopGroup boss;
        private final EventLoopGroup worker;
        private final Channel channel;

        private FakeServer(EventLoopGroup boss, EventLoopGroup worker, Channel channel) {
            this.boss = boss;
            this.worker = worker;
            this.channel = channel;
        }

        private static FakeServer start(int port, boolean echoHeartbeat) {
            EventLoopGroup boss = new NioEventLoopGroup(1);
            EventLoopGroup worker = new NioEventLoopGroup();
            try {
                ServerBootstrap bootstrap = new ServerBootstrap()
                        .group(boss, worker)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline()
                                        .addLast(new LengthFieldBasedFrameDecoder(
                                                ProtocolConstants.MAX_FRAME_LENGTH,
                                                ProtocolConstants.LENGTH_FIELD_OFFSET,
                                                ProtocolConstants.LENGTH_FIELD_LENGTH,
                                                0,
                                                0))
                                        .addLast(new RpcDecoder())
                                        .addLast(new SimpleChannelInboundHandler<RpcMessage>() {
                                            @Override
                                            protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) {
                                                if (msg.isHandshakeRequest()) {
                                                    ctx.channel().writeAndFlush(RpcMessage.handshakeAck(
                                                            ProtocolConstants.MAX_SUPPORTED_VERSION,
                                                            msg.getCodec(), msg.getCompress()));
                                                    return;
                                                }
                                                if (msg.isHeartbeat() && echoHeartbeat) {
                                                    RpcMessage pong = RpcMessage.heartbeat(
                                                            msg.getRequestId(), msg.getCodec());
                                                    pong.setVersion(msg.getVersion());
                                                    ctx.channel().writeAndFlush(pong);
                                                }
                                                // 其它帧一律忽略：模拟服务端假死
                                            }
                                        })
                                        .addLast(new RpcEncoder());
                            }
                        });
                Channel channel = bootstrap.bind(TestSupport.HOST, port).syncUninterruptibly().channel();
                return new FakeServer(boss, worker, channel);
            } catch (RuntimeException e) {
                boss.shutdownGracefully(0, 2, TimeUnit.SECONDS);
                worker.shutdownGracefully(0, 2, TimeUnit.SECONDS);
                throw e;
            }
        }

        @Override
        public void close() {
            channel.close().syncUninterruptibly();
            worker.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
            boss.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}
