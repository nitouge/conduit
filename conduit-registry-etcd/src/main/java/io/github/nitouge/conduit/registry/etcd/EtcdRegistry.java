package io.github.nitouge.conduit.registry.etcd;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.registry.NotifyListener;
import io.github.nitouge.conduit.registry.support.AbstractRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于Etcd（jetcd）的注册中心实现。
 *
 * <p>存储模型：</p>
 * <pre>
 * /conduit/services/{serviceName}/{host}:{port}   value={weight}，带TTL租约
 * </pre>
 * <ul>
 *   <li>注册 = 申请租约（默认30s）+ 带租约写入；后台每10s续租；续租失败自动重新注册；</li>
 *   <li>发现 = 按服务前缀range；</li>
 *   <li>监听 = 前缀watch，变更后重新发现并回调；</li>
 *   <li>本地缓存兜底/订阅幂等由 {@link AbstractRegistry} 统一处理。</li>
 * </ul>
 */
public class EtcdRegistry extends AbstractRegistry {

    private static final Logger log = LoggerFactory.getLogger(EtcdRegistry.class);

    static final String BASE_PATH = "/conduit/services";
    static final long LEASE_TTL_SECONDS = 30;
    static final long KEEPALIVE_INTERVAL_SECONDS = 10;

    private final EtcdOperations ops;
    private final ScheduledExecutorService scheduler;
    private final Map<String, LeaseHandle> leases = new ConcurrentHashMap<>();
    private final Map<String, AutoCloseable> watchers = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public EtcdRegistry(String address) {
        this(JetcdOperations.connect(address));
    }

    /** 可注入操作实现的构造（测试用） */
    EtcdRegistry(EtcdOperations ops) {
        this.ops = ops;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "conduit-etcd-keepalive");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    protected synchronized void doRegister(ServiceMeta service) {
        String key = keyOf(service);
        cancelLease(key);
        try {
            long leaseId = ops.grantLease(LEASE_TTL_SECONDS);
            ops.put(key, String.valueOf(service.getWeight()), leaseId);
            ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(
                    () -> keepAlive(service, key, leaseId),
                    KEEPALIVE_INTERVAL_SECONDS, KEEPALIVE_INTERVAL_SECONDS, TimeUnit.SECONDS);
            leases.put(key, new LeaseHandle(leaseId, task));
            log.debug("[EtcdRegistry] 注册 {} -> {} (lease={})", service, key, leaseId);
        } catch (RuntimeException e) {
            throw (e instanceof RpcException) ? e : new RpcException("注册服务到Etcd失败: " + service, e);
        }
    }

    private void keepAlive(ServiceMeta service, String key, long leaseId) {
        if (closed.get()) {
            return;
        }
        try {
            ops.keepAliveOnce(leaseId);
        } catch (Throwable t) {
            log.warn("[EtcdRegistry] 续租失败，尝试重新注册 {}: {}", key, t.toString());
            try {
                doRegister(service);
            } catch (Throwable e) {
                log.warn("[EtcdRegistry] 重新注册失败 {}: {}", key, e.toString());
            }
        }
    }

    @Override
    protected synchronized void doUnregister(ServiceMeta service) {
        String key = keyOf(service);
        LeaseHandle handle = leases.remove(key);
        if (handle != null) {
            handle.cancel();
        }
        try {
            ops.delete(key);
        } catch (RuntimeException e) {
            log.warn("[EtcdRegistry] 删除Etcd key失败 {}: {}", key, e.toString());
        }
        if (handle != null) {
            try {
                ops.revokeLease(handle.leaseId());
            } catch (RuntimeException e) {
                log.debug("[EtcdRegistry] 撤销租约失败 {}: {}", handle.leaseId(), e.toString());
            }
        }
    }

    @Override
    protected List<ServiceMeta> doDiscover(String serviceName) {
        String prefix = prefixOf(serviceName);
        List<ServiceMeta> result = new ArrayList<>();
        for (EtcdOperations.KeyValue kv : ops.getByPrefix(prefix)) {
            ServiceMeta meta = parse(serviceName, prefix, kv);
            if (meta != null) {
                result.add(meta);
            }
        }
        return result;
    }

    @Override
    protected void doSubscribe(String serviceName, NotifyListener listener) {
        String prefix = prefixOf(serviceName);
        AutoCloseable previous = watchers.remove(serviceName);
        closeQuietly(previous);
        try {
            AutoCloseable watcher = ops.watchPrefix(prefix, () -> {
                try {
                    listener.notify(doDiscover(serviceName));
                } catch (Throwable t) {
                    log.warn("[EtcdRegistry] 推送订阅变更失败 {}: {}", serviceName, t.toString());
                }
            });
            watchers.put(serviceName, watcher);
            listener.notify(doDiscover(serviceName));
            log.debug("[EtcdRegistry] 订阅 {} 成功", serviceName);
        } catch (RuntimeException e) {
            throw new RpcException("订阅Etcd服务失败: " + serviceName, e);
        }
    }

    @Override
    public void destroy() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (LeaseHandle handle : leases.values()) {
            handle.cancel();
        }
        leases.clear();
        for (AutoCloseable watcher : watchers.values()) {
            closeQuietly(watcher);
        }
        watchers.clear();
        scheduler.shutdownNow();
        try {
            ops.close();
        } catch (Exception e) {
            log.debug("[EtcdRegistry] 关闭Etcd客户端失败: {}", e.toString());
        }
        log.info("[EtcdRegistry] 已关闭");
    }

    private void cancelLease(String key) {
        LeaseHandle previous = leases.remove(key);
        if (previous != null) {
            previous.cancel();
        }
    }

    private static String prefixOf(String serviceName) {
        return BASE_PATH + "/" + serviceName + "/";
    }

    private static String keyOf(ServiceMeta service) {
        return prefixOf(service.getServiceName()) + service.getHost() + ":" + service.getPort();
    }

    /** 从key的剩余部分解析host:port，value解析weight */
    private static ServiceMeta parse(String serviceName, String prefix, EtcdOperations.KeyValue kv) {
        String path = kv.key();
        if (path == null || !path.startsWith(prefix)) {
            return null;
        }
        String node = path.substring(prefix.length());
        if (node.isEmpty() || node.contains("/")) {
            return null;
        }
        int separator = node.lastIndexOf(':');
        if (separator <= 0 || separator == node.length() - 1) {
            return null;
        }
        try {
            int port = Integer.parseInt(node.substring(separator + 1));
            int weight = 100;
            if (kv.value() != null && !kv.value().isBlank()) {
                weight = Integer.parseInt(kv.value().trim());
            }
            return new ServiceMeta(serviceName, node.substring(0, separator), port, weight);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            log.debug("[EtcdRegistry] 关闭资源失败: {}", e.toString());
        }
    }

    private record LeaseHandle(long leaseId, ScheduledFuture<?> task) {
        void cancel() {
            task.cancel(false);
        }
    }
}
