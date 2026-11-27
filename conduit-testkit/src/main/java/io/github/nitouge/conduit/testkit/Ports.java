package io.github.nitouge.conduit.testkit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 端口工具：申请一个当前空闲的本地端口。
 *
 * <p>实现为"绑定0号端口拿到系统分配值后立即释放"，因此在<b>紧接</b>着被使用时才有意义；
 * 适合测试里`TestServer.random()`这种"申请后马上listen"的场景，不要提前申请隔很久再用。</p>
 */
public final class Ports {

    private Ports() {
    }

    /** 申请一个空闲端口（IPv4/IPv6通配绑定，随后释放） */
    public static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException("申请空闲端口失败", e);
        }
    }

    /** 端口当前是否可连接（用于等待服务端真正listen就绪） */
    public static boolean connectable(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
