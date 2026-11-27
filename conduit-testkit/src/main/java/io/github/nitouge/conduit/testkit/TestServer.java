package io.github.nitouge.conduit.testkit;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.server.RpcServer;
import io.github.nitouge.conduit.remoting.ServerOptions;

/**
 * 真实服务端夹具：在随机/指定端口上启动一个真实的 {@link RpcServer}（真实Netty监听），
 * 用try-with-resources或 {@link #close()} 保证释放。
 *
 * <pre>{@code
 * try (TestServer server = TestServer.start(EchoService.class, new EchoServiceImpl("n1"))) {
 *     EchoService echo = TestRpc.create().proxy(EchoService.class, server.meta(EchoService.class), 3000);
 *     assertEquals("ok:hi", echo.echo("hi"));
 * }
 * }</pre>
 *
 * <p>典型用法：{@code TestServer.random()} 申请随机端口 → {@code export(...)} → {@code start()}；
 * 需要注册中心的场景用 {@code TestServer.of(registry)}。</p>
 */
public final class TestServer implements AutoCloseable {

    /** 测试统一使用回环地址 */
    public static final String HOST = "127.0.0.1";

    private final RpcServer server;
    private final String host;
    private final int port;
    private boolean started;

    private TestServer(String host, int port, Registry registry, ServerOptions options) {
        this.host = host;
        this.port = port;
        this.server = new RpcServer(host, port, registry,
                options == null ? ServerOptions.defaults() : options);
    }

    /** 随机端口、不带注册中心 */
    public static TestServer random() {
        return new TestServer(HOST, Ports.freePort(), null, null);
    }

    /** 随机端口 + 指定注册中心（服务端start时自动注册） */
    public static TestServer of(Registry registry) {
        return new TestServer(HOST, Ports.freePort(), registry, null);
    }

    /** 随机端口 + 自定义服务端选项（TLS/线程数等） */
    public static TestServer withOptions(ServerOptions options) {
        return new TestServer(HOST, Ports.freePort(), null, options);
    }

    /** 随机端口 + 注册中心 + 自定义选项 */
    public static TestServer of(Registry registry, ServerOptions options) {
        return new TestServer(HOST, Ports.freePort(), registry, options);
    }

    /** 指定端口（调试时便于用固定端口抓包） */
    public static TestServer onPort(int port) {
        return new TestServer(HOST, port, null, null);
    }

    /** 一步到位：随机端口 + 暴露一个实现 + 启动 */
    public static TestServer start(Class<?> interfaceClass, Object impl) {
        return random().export(interfaceClass, impl).start();
    }

    /** 暴露服务（必须在 {@link #start()} 之前调用） */
    public TestServer export(Class<?> interfaceClass, Object impl) {
        server.export(interfaceClass, impl);
        return this;
    }

    /** 启动监听；带注册中心时同时完成注册 */
    public TestServer start() {
        server.start();
        this.started = true;
        return this;
    }

    /** 该服务端的服务元数据（直连调用时作为target） */
    public ServiceMeta meta(Class<?> interfaceClass) {
        return new ServiceMeta(interfaceClass.getName(), host, port);
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public void close() {
        if (started) {
            started = false;
            server.close();
        }
    }
}
