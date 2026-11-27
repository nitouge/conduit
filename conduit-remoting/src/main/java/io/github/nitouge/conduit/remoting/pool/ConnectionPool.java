package io.github.nitouge.conduit.remoting.pool;

import io.github.nitouge.conduit.common.exception.RpcException;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;

/**
 * 连接池：按目标地址维护一组长连接，向上提供"取一条可用连接"的统一入口。
 *
 * <p>设计要点：</p>
 * <ul>
 *   <li><b>长连接复用</b>：同地址默认复用一条连接（{@code maxConnectionsPerAddress=1}）；
 *       配置>1时按<b>在途请求数最少</b>选连接，并在所有连接都有在途请求时异步补建，直到到达上限。</li>
 *   <li><b>单飞建链</b>：同一地址同一时刻只允许一个建链过程，其它请求等待同一个future，
 *       避免并发Connection Refused风暴。</li>
 *   <li><b>握手后可用</b>：{@link Connector} 返回的future只有在协议握手成功后才完成，
 *       所以池里取到的连接一定"协议一致、可直接承载业务帧"。</li>
 *   <li><b>自动剔除</b>：连接关闭即从池中移除，并通知 {@link ConnectionListener}（用于断线重连）。</li>
 *   <li><b>闲置回收</b>：无在途请求且空闲超过阈值的连接被后台线程关闭（保留 minIdle 条）。</li>
 * </ul>
 *
 * <p>线程安全：所有对内部列表的读写都在 {@code lock} 上同步；连接的实际建立/关闭都在锁外异步完成。</p>
 */
public class ConnectionPool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

    /** 连接最近一次被选中的时间戳（纳秒），用于闲置回收 */
    public static final AttributeKey<Long> LAST_USED = AttributeKey.valueOf("conduit.pool.lastUsed");

    /** 标记"由连接池主动回收（闲置）"的连接：关闭时不应触发断线重连 */
    private static final AttributeKey<Boolean> EVICTED = AttributeKey.valueOf("conduit.pool.evicted");

    /** 建链器：返回的future应在TCP连接且协议握手都成功后完成 */
    @FunctionalInterface
    public interface Connector {
        CompletableFuture<Channel> connect(InetSocketAddress address);
    }

    /** 连接关闭回调（用于触发断线重连） */
    @FunctionalInterface
    public interface ConnectionListener {
        void onConnectionClosed(InetSocketAddress address);
    }

    private final Connector connector;
    private final ToIntFunction<Channel> loadFactor;
    private final int maxConnectionsPerAddress;
    private final int minIdleConnectionsPerAddress;
    private final long idleTimeoutMillis;
    private final Map<InetSocketAddress, AddressPool> pools = new ConcurrentHashMap<>();
    private final ScheduledExecutorService evictor;
    private volatile ConnectionListener connectionListener;
    private volatile boolean closed;

    public ConnectionPool(Connector connector, ToIntFunction<Channel> loadFactor,
                          int maxConnectionsPerAddress, int minIdleConnectionsPerAddress,
                          long idleTimeoutMillis) {
        this.connector = connector;
        this.loadFactor = loadFactor;
        this.maxConnectionsPerAddress = Math.max(1, maxConnectionsPerAddress);
        this.minIdleConnectionsPerAddress = Math.max(0, minIdleConnectionsPerAddress);
        this.idleTimeoutMillis = idleTimeoutMillis;
        if (idleTimeoutMillis > 0) {
            this.evictor = Executors.newSingleThreadScheduledExecutor(
                    daemonThreadFactory("conduit-pool-evictor")
            );
            long interval = Math.max(100L, idleTimeoutMillis / 4);
            this.evictor.scheduleWithFixedDelay(this::evictIdle, interval, interval, TimeUnit.MILLISECONDS);
        } else {
            this.evictor = null;
        }
    }

    public void setConnectionListener(ConnectionListener connectionListener) {
        this.connectionListener = connectionListener;
    }

    /**
     * 取一条到目标地址的可用连接（异步）。
     *
     * <p>已有可用连接立即返回；没有则触发建链，并发调用共享同一次建链。</p>
     */
    public CompletableFuture<Channel> acquire(InetSocketAddress address) {
        if (closed) {
            return CompletableFuture.failedFuture(new RpcException("连接池已关闭，无法获取连接: " + address));
        }
        return pools.computeIfAbsent(address, AddressPool::new).acquire();
    }

    /**
     * 确保目标地址至少有一条可用连接（断线重连用）；已有可用连接时直接返回它。
     */
    public CompletableFuture<Channel> ensureConnection(InetSocketAddress address) {
        if (closed) {
            return CompletableFuture.failedFuture(new RpcException("连接池已关闭，无法重连: " + address));
        }
        return pools.computeIfAbsent(address, AddressPool::new).ensureConnection();
    }

    /** 目标地址当前活跃连接数 */
    public int activeCount(InetSocketAddress address) {
        AddressPool pool = pools.get(address);
        return pool == null ? 0 : pool.activeCount();
    }

    /** 全池活跃连接数 */
    public int totalActiveCount() {
        int total = 0;
        for (AddressPool pool : pools.values()) {
            total += pool.activeCount();
        }
        return total;
    }

    /** 池中已登记的目标地址数 */
    public int addressCount() {
        return pools.size();
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (evictor != null) {
            evictor.shutdownNow();
        }
        for (AddressPool pool : pools.values()) {
            pool.closeAll();
        }
        pools.clear();
    }

    private void evictIdle() {
        if (closed) {
            return;
        }
        for (AddressPool pool : pools.values()) {
            try {
                pool.evictIdle();
            } catch (Throwable t) {
                log.debug("[ConnectionPool] 闲置回收异常: {}", t.toString());
            }
        }
    }

    private static ThreadFactory daemonThreadFactory(String prefix) {
        AtomicInteger idx = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + idx.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /** 一个目标地址下的连接集合 */
    private final class AddressPool {

        private final InetSocketAddress address;
        private final Object lock = new Object();
        private final List<CompletableFuture<Channel>> holders = new ArrayList<>();
        /** 建链中的future（单飞）；null表示当前没有建链进行中 */
        private CompletableFuture<Channel> creating;

        private AddressPool(InetSocketAddress address) {
            this.address = address;
        }

        private CompletableFuture<Channel> acquire() {
            CompletableFuture<Channel> chosen;
            synchronized (lock) {
                pruneLocked();
                chosen = pickLocked();
                if (chosen == null) {
                    return createLocked();
                }
                markUsed(chosen);
                // 所有连接都有在途请求且未达上限：后台补一条，供后续请求分流
                if (shouldGrowLocked()) {
                    createLocked();
                }
            }
            return chosen;
        }

        private CompletableFuture<Channel> ensureConnection() {
            synchronized (lock) {
                pruneLocked();
                CompletableFuture<Channel> chosen = pickLocked();
                if (chosen != null) {
                    return chosen;
                }
                return createLocked();
            }
        }

        /** 建链（单飞）。必须在持锁时调用。 */
        private CompletableFuture<Channel> createLocked() {
            if (creating != null) {
                return creating;
            }
            CompletableFuture<Channel> future;
            try {
                future = connector.connect(address);
            } catch (Throwable t) {
                return CompletableFuture.failedFuture(t instanceof RpcException ?
                        t : new RpcException("建链异常: " + address, t));
            }
            if (future == null) {
                return CompletableFuture.failedFuture(new RpcException("建链器返回null: " + address));
            }
            creating = future;
            holders.add(future);

            future.whenComplete((channel, error) -> {
                synchronized (lock) {
                    if (creating == future) {
                        creating = null;
                    }
                    if (error != null) {
                        holders.remove(future);
                    }
                }
                if (error != null) {
                    log.debug("[ConnectionPool] 建链失败 {}: {}", address, String.valueOf(error));
                    return;
                }
                if (channel != null) {
                    // 池已关闭却刚好建链完成：立即关闭，避免"游离连接"泄漏
                    if (closed) {
                        channel.close();
                        return;
                    }
                    markUsed(future);
                    channel.closeFuture().addListener(f -> {
                        synchronized (lock) {
                            holders.remove(future);
                        }
                        log.debug("[ConnectionPool] 连接关闭 {}: {}", address, channel.remoteAddress());
                        // 闲置回收是主动行为：不应触发后台重连，否则会"刚回收又重连"造成连接抖动
                        boolean evicted = Boolean.TRUE.equals(channel.attr(EVICTED).get());
                        ConnectionListener listener = connectionListener;
                        if (listener != null && !closed && !evicted) {
                            listener.onConnectionClosed(address);
                        }
                    });
                }
            });
            return future;
        }

        private void markUsed(CompletableFuture<Channel> holder) {
            Channel channel = activeChannel(holder);
            if (channel != null) {
                channel.attr(LAST_USED).set(System.nanoTime());
            }
        }

        /** 选在途负载最小的活跃连接；没有活跃连接返回null。必须在持锁时调用。 */
        private CompletableFuture<Channel> pickLocked() {
            CompletableFuture<Channel> best = null;
            int bestLoad = Integer.MAX_VALUE;
            for (CompletableFuture<Channel> holder : holders) {
                Channel channel = activeChannel(holder);
                if (channel == null) {
                    continue;
                }
                int load = loadFactor.applyAsInt(channel);
                if (load < bestLoad) {
                    best = holder;
                    bestLoad = load;
                }
            }
            return best;
        }

        private boolean shouldGrowLocked() {
            if (holders.size() >= maxConnectionsPerAddress) {
                return false;
            }
            int minLoad = Integer.MAX_VALUE;
            int active = 0;
            for (CompletableFuture<Channel> holder : holders) {
                Channel channel = activeChannel(holder);
                if (channel == null) {
                    continue;
                }
                active++;
                minLoad = Math.min(minLoad, loadFactor.applyAsInt(channel));
            }
            return active > 0 && minLoad > 0;
        }

        private int countActiveLocked() {
            int active = 0;
            for (CompletableFuture<Channel> holder : holders) {
                if (activeChannel(holder) != null) {
                    active++;
                }
            }
            return active;
        }

        private void pruneLocked() {
            holders.removeIf(holder -> activeChannel(holder) == null
                    && (holder.isDone() || holder.isCompletedExceptionally()));
        }

        private void evictIdle() {
            List<Channel> toClose = new ArrayList<>();
            List<CompletableFuture<Channel>> evictedHolders = new ArrayList<>();
            synchronized (lock) {
                pruneLocked();
                int active = countActiveLocked();
                long now = System.nanoTime();
                long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(idleTimeoutMillis);
                for (CompletableFuture<Channel> holder : holders) {
                    if (active <= minIdleConnectionsPerAddress) {
                        break;
                    }
                    Channel channel = activeChannel(holder);
                    if (channel == null) {
                        continue;
                    }
                    if (loadFactor.applyAsInt(channel) > 0) {
                        continue; // 有在途请求，不算闲置
                    }
                    Long lastUsed = channel.attr(LAST_USED).get();
                    long idleNanos = lastUsed == null ? Long.MAX_VALUE : now - lastUsed;
                    if (idleNanos >= timeoutNanos) {
                        toClose.add(channel);
                        evictedHolders.add(holder);
                        active--;
                    }
                }
                // 先在锁内从池中摘除，避免"回收中的连接又被acquire选中"的竞态
                holders.removeAll(evictedHolders);
            }
            for (Channel channel : toClose) {
                log.debug("[ConnectionPool] 回收闲置连接 {}: {}", address, channel.remoteAddress());
                channel.attr(EVICTED).set(Boolean.TRUE);
                channel.close();
            }
        }

        private void closeAll() {
            List<Channel> toClose = new ArrayList<>();
            synchronized (lock) {
                for (CompletableFuture<Channel> holder : holders) {
                    Channel channel = activeChannel(holder);
                    if (channel != null) {
                        toClose.add(channel);
                    }
                }
                holders.clear();
                creating = null;
            }
            for (Channel channel : toClose) {
                channel.close();
            }
        }

        private int activeCount() {
            synchronized (lock) {
                pruneLocked();
                return countActiveLocked();
            }
        }
    }

    private static Channel activeChannel(CompletableFuture<Channel> holder) {
        if (!holder.isDone() || holder.isCompletedExceptionally() || holder.isCancelled()) {
            return null;
        }
        Channel channel = holder.getNow(null);
        return (channel != null && channel.isActive()) ? channel : null;
    }
}
