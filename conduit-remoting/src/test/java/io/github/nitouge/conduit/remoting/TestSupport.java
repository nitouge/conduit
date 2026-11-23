package io.github.nitouge.conduit.remoting;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.remoting.netty.NettyClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

/**
 * 传输层测试通用支撑：端口分配、请求构造、处理Handler与条件等待。
 */
public final class TestSupport {

    public static final String HOST = "127.0.0.1";
    public static final long CALL_TIMEOUT_MILLIS = 5_000;

    private TestSupport() {
    }

    /** 申请一个空闲端口（bind 0 后立即释放，紧接使用） */
    public static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static RpcRequest request(String methodName, Object... args) {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("io.github.nitouge.conduit.test.EchoService");
        request.setMethodName(methodName);
        request.setParamTypes(new Class<?>[0]);
        request.setArgs(args);
        return request;
    }

    public static ServiceMeta target(int port) {
        return new ServiceMeta("io.github.nitouge.conduit.test.EchoService", HOST, port);
    }

    /** 回声Handler：返回 "ok:方法名"，用于验证通路 */
    public static RequestHandler echoHandler() {
        return request -> CompletableFuture.completedFuture(RpcResponse.success("ok:" + request.getMethodName()));
    }

    /** 原样回传第一个参数，用于验证压缩/大报文 */
    public static RequestHandler echoArgumentHandler() {
        return request -> {
            Object arg = request.getArgs() == null || request.getArgs().length == 0 ? null : request.getArgs()[0];
            return CompletableFuture.completedFuture(RpcResponse.success(arg));
        };
    }

    /** 慢Handler：business线程立即返回future，响应在millis后完成（模拟异步provider） */
    public static RequestHandler slowHandler(long millis) {
        return request -> CompletableFuture.supplyAsync(() -> {
            sleepQuietly(millis);
            return RpcResponse.success("slow:" + request.getMethodName());
        });
    }

    public static RpcResponse call(NettyClient client, int port, String method, Object... args) throws Exception {
        return client.send(request(method, args), target(port), CALL_TIMEOUT_MILLIS).get(CALL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    /** 断言异步异常：返回真正的cause便于精确断言 */
    public static Throwable callFailing(NettyClient client, int port, String method, Object... args) {
        try {
            RpcResponse response = client.send(request(method, args), target(port), CALL_TIMEOUT_MILLIS)
                    .get(CALL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            throw new AssertionError("期望调用失败，但成功了: " + response);
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (TimeoutException e) {
            return e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("调用被中断", e);
        }
    }

    public static void awaitTrue(String description, long timeoutMillis, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleepQuietly(20);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("等待超时(" + timeoutMillis + "ms): " + description);
        }
    }

    public static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
