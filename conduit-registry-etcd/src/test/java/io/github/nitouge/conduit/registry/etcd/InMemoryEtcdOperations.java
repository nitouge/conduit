package io.github.nitouge.conduit.registry.etcd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** EtcdOperations的内存假实现：验证注册中心逻辑，无需真实Etcd。 */
class InMemoryEtcdOperations implements EtcdOperations {

    final Map<String, String> kv = new ConcurrentHashMap<>();
    final List<Watcher> watchers = new CopyOnWriteArrayList<>();
    private long leaseSeq = 0;
    volatile boolean failKeepAlive = false;

    @Override
    public long grantLease(long ttlSeconds) {
        return ++leaseSeq;
    }

    @Override
    public void put(String key, String value, long leaseId) {
        kv.put(key, value);
        fire(key);
    }

    @Override
    public void delete(String key) {
        kv.remove(key);
        fire(key);
    }

    @Override
    public List<KeyValue> getByPrefix(String prefix) {
        List<KeyValue> result = new ArrayList<>();
        kv.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                result.add(new KeyValue(key, value));
            }
        });
        return result;
    }

    @Override
    public void keepAliveOnce(long leaseId) {
        if (failKeepAlive) {
            throw new IllegalStateException("keepalive failed");
        }
    }

    @Override
    public void revokeLease(long leaseId) {
        // 内存实现无需额外处理
    }

    @Override
    public AutoCloseable watchPrefix(String prefix, Runnable onChange) {
        Watcher watcher = new Watcher(prefix, onChange);
        watchers.add(watcher);
        return () -> watchers.remove(watcher);
    }

    @Override
    public void close() {
        kv.clear();
        watchers.clear();
    }

    private void fire(String key) {
        for (Watcher watcher : List.copyOf(watchers)) {
            if (key.startsWith(watcher.prefix)) {
                watcher.onChange.run();
            }
        }
    }

    private record Watcher(String prefix, Runnable onChange) {
    }
}
