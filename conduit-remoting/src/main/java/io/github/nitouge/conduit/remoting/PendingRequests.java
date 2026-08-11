package io.github.nitouge.conduit.remoting;

import io.github.nitouge.conduit.api.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * requestId -> CompletableFuture关联表：整个异步调用的物理基础。
 *
 * <p>发请求时生成全局自增requestId并登记future；收响应时按协议头requestId取出future
 * 并完成它。同步调用只是拿到future后立即get(timeout)的特例。</p>
 */
public final class PendingRequests {

    private static final Logger log = LoggerFactory.getLogger(PendingRequests.class);

    private PendingRequests() {
    }

    private static final AtomicLong ID_GEN = new AtomicLong(0);

    private static final Map<Long, CompletableFuture<RpcResponse>> PENDING = new ConcurrentHashMap<>();

    /** 发送前：生成requestId并登记future */
    public static long register(CompletableFuture<RpcResponse> future) {
        long id = ID_GEN.incrementAndGet();
        PENDING.put(id, future);
        log.debug("[PendingRequests] register requestId={}, pending={}", id, PENDING.size());
        return id;
    }

    /** 收到响应：根据协议头requestId唤醒等待者 */
    public static void complete(long requestId, RpcResponse response) {
        CompletableFuture<RpcResponse> future = PENDING.remove(requestId);
        if (future != null) {
            log.debug("[PendingRequests] complete requestId={}, pending={}", requestId, PENDING.size());
            future.complete(response);
        } else {
            // 通常是超时/连接断开已先清理，或responseId对应的future已被异常完成
            log.debug("[PendingRequests] complete 未找到 requestId={}（可能已超时/异常）", requestId);
        }
    }

    /** 超时/连接断开时清理，避免内存泄漏 */
    public static void completeExceptionally(long requestId, Throwable cause) {
        CompletableFuture<RpcResponse> future = PENDING.remove(requestId);
        if (future != null) {
            log.debug("[PendingRequests] completeExceptionally requestId={}, cause={}, pending={}",
                    requestId, cause.toString(), PENDING.size());
            future.completeExceptionally(cause);
        }
    }

    /** 当前在途请求数（用于监控/测试断言泄漏） */
    public static int pendingCount() {
        return PENDING.size();
    }
}
