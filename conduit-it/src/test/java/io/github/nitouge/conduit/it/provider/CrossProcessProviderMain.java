package io.github.nitouge.conduit.it.provider;

import io.github.nitouge.conduit.server.RpcServer;
import io.github.nitouge.conduit.testkit.EchoService;
import io.github.nitouge.conduit.testkit.EchoServiceImpl;

/**
 * 跨进程用例的被调方：在独立JVM里起一个真实的 {@link RpcServer}，节点标识带上本进程pid，
 * 让测试能证明"调用确实跨了进程"。
 *
 * <p>启动参数：{@code <port>}。</p>
 */
public final class CrossProcessProviderMain {

    private CrossProcessProviderMain() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        long pid = ProcessHandle.current().pid();

        RpcServer server = new RpcServer("127.0.0.1", port);
        server.export(EchoService.class, new EchoServiceImpl("child-" + pid));
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "it-provider-shutdown"));

        System.out.println("READY " + port + " pid=" + pid);
        System.out.flush();

        // 常驻，等待测试进程结束它；Netty线程本身也是非守护线程
        Thread.currentThread().join();
    }
}
