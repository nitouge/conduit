package io.github.nitouge.conduit.registry.etcd;

import java.util.List;

/**
 * Etcd操作的最小抽象：隔离jetcd API，便于用内存假实现做单元测试。
 */
interface EtcdOperations extends AutoCloseable {

    /** 授予租约，返回leaseId */
    long grantLease(long ttlSeconds);

    /** 带租约写入key */
    void put(String key, String value, long leaseId);

    /** 删除key */
    void delete(String key);

    /** 按前缀读取key/value */
    List<KeyValue> getByPrefix(String prefix);

    /** 续租一次（失败通常意味着lease已过期） */
    void keepAliveOnce(long leaseId);

    /** 撤销租约（其下key一并删除） */
    void revokeLease(long leaseId);

    /**
     * 监听前缀变更；变更时回调 {@code onChange}。
     *
     * @return 可关闭的监听句柄
     */
    AutoCloseable watchPrefix(String prefix, Runnable onChange);

    @Override
    void close();

    /** 键值对 */
    record KeyValue(String key, String value) {
    }
}
