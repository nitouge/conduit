package io.github.nitouge.conduit.remoting.reconnect;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 断线自动重连：连接断开后按<b>指数退避</b>在后台重建连接，直到成功或达到上限。
 *
 * <p>与"按需建链"（下一次调用时再连）互补：provider重启/网络闪断后，消费者无需等下一次调用
 * 才恢复，避免恢复瞬间的首调用延迟尖峰。重连只是后台尽力而为：即使全部尝试失败，下一次真实调用
 * 仍会通过连接池按需建链，不会影响可用性。</p>
 *
 * <p>退避公式：{@code delay = min(base << (attempt-1), max)}，避免停机期间高频重试打爆对端。</p>
 */
public class ReconnectManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReconnectManager.class);

    /** 重连器：真正去建立并握手连接（由连接池提供，单飞） */
    @FunctionalInterface
    public interface Reconnector {
        CompletableFuture<Channel> reconnect(InetSocketAddress address);
    }

    private final Reconnector reconnector;
    private final int maxAttempts;
    private final long baseDelayMillis;
    private final long maxDelayMillis;
    private final ScheduledExecutorService scheduler;
    /** 正在排队/进行重连的地址集合：同一地址同一时刻只有一条重连链路 */
    private final Set<InetSocketAddress> inProgress = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    public ReconnectManager(Reconnector reconnector, int maxAttempts, long baseDelayMillis, long maxDelayMillis) {
        this.reconnector = reconnector;
        this.maxAttempts = maxAttempts;
        this.baseDelayMillis = baseDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory("conduit-reconnect"));
    }

    /** 连接断开回调：开始（或继续）该地址的后台重连 */
    public void onDisconnected(InetSocketAddress address) {
        if (closed || maxAttempts <= 0) {
            return;
        }
        if (!inProgress.add(address)) {
            return; // 已有重连链路在跑
        }
        schedule(address, 1);
    }

    private void schedule(InetSocketAddress address, int attempt) {
        if (closed) {
            inProgress.remove(address);
            return;
        }
        if (attempt > maxAttempts) {
            inProgress.remove(address);
            log.warn("[Reconnect] {} 重连{}次仍未成功，停止后台重连（下次调用将按需建链）", address, maxAttempts);
            return;
        }
        long delay = backoffMillis(attempt);
        log.info("[Reconnect] {} 连接断开，{}ms后发起第{}/{}次重连", address, delay, attempt, maxAttempts);
        try {
            scheduler.schedule(() -> tryReconnect(address, attempt), delay, TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            // 调度器已关闭（客户端正在关闭）：静默退出
            inProgress.remove(address);
            log.debug("[Reconnect] 调度重连失败: {}", e.toString());
        }
    }

    private void tryReconnect(InetSocketAddress address, int attempt) {
        if (closed) {
            inProgress.remove(address);
            return;
        }
        CompletableFuture<Channel> future;
        try {
            future = reconnector.reconnect(address);
        } catch (Throwable t) {
            log.debug("[Reconnect] 第{}次重连异常 {}: {}", attempt, address, t.toString());
            schedule(address, attempt + 1);
            return;
        }
        if (future == null) {
            schedule(address, attempt + 1);
            return;
        }
        future.whenComplete((channel, error) -> {
            inProgress.remove(address);
            if (error == null && channel != null && channel.isActive()) {
                log.info("[Reconnect] {} 重连成功", address);
                return;
            }
            if (closed) {
                return;
            }
            log.debug("[Reconnect] 第{}次重连失败 {}: {}", attempt, address,
                    error != null ? error.toString() : "连接不可用");
            schedule(address, attempt + 1);
        });
    }

    private long backoffMillis(int attempt) {
        int shift = Math.min(Math.max(0, attempt - 1), 20);
        long delay = baseDelayMillis << shift;
        return Math.min(delay, maxDelayMillis);
    }

    @Override
    public void close() {
        closed = true;
        inProgress.clear();
        scheduler.shutdownNow();
    }

    private static ThreadFactory daemonThreadFactory(String prefix) {
        AtomicInteger idx = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + idx.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
