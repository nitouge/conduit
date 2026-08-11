package io.github.nitouge.conduit.client;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.remoting.netty.NettyClient;
import io.github.nitouge.conduit.serialization.SerializerType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 客户端调用入口：业务层与传输层之间的薄封装。
 *
 * <p>它把RpcRequest + 目标地址交给 {@link NettyClient}，拿回 {@code CompletableFuture<RpcResponse>}，
 * 再决定同步等待还是异步返回。自身不生成requestId、不管理关联表——那是传输层的职责。</p>
 */
public class RpcClient {

    private final NettyClient nettyClient;

    /** 默认使用jdk序列化 */
    public RpcClient() {
        this.nettyClient = new NettyClient();
    }

    /** 指定出站序列化方式（jdk/json/kryo/protostuff）。M5起由application.yml配置注入。 */
    public RpcClient(String serializationName) {
        this.nettyClient = new NettyClient(SerializerType.idOf(serializationName));
    }

    /** 同步调用：阻塞等待响应。超时抛 {@link TimeoutException}；传输失败抛 {@link ExecutionException}（cause为真正原因） */
    public RpcResponse send(RpcRequest request, ServiceMeta target, long timeoutMillis)
            throws InterruptedException, ExecutionException, TimeoutException {
        return sendAsync(request, target, timeoutMillis).get(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /** 异步调用：立即返回future（传输层会按timeout自动清理超时的在途请求） */
    public CompletableFuture<RpcResponse> sendAsync(RpcRequest request, ServiceMeta target, long timeoutMillis) {
        return nettyClient.send(request, target, timeoutMillis);
    }

    /** 关闭客户端，释放连接与线程组 */
    public void close() {
        nettyClient.close();
    }
}
