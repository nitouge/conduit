package io.github.nitouge.conduit.registry.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于jetcd的 {@link EtcdOperations} 实现。
 */
final class JetcdOperations implements EtcdOperations {

    private final Client client;

    private JetcdOperations(Client client) {
        this.client = client;
    }

    static EtcdOperations connect(String address) {
        if (StringUtils.isBlank(address)) {
            throw new RpcException("Etcd注册中心地址不能为空");
        }
        String endpoint = address.contains("://") ? address : "http://" + address;
        try {
            Client client = Client.builder().endpoints(URI.create(endpoint)).build();
            return new JetcdOperations(client);
        } catch (Exception e) {
            throw new RpcException("连接Etcd失败: " + address, e);
        }
    }

    @Override
    public long grantLease(long ttlSeconds) {
        try {
            return client.getLeaseClient().grant(ttlSeconds).get().getID();
        } catch (Exception e) {
            throw new RpcException("Etcd授予租约失败", e);
        }
    }

    @Override
    public void put(String key, String value, long leaseId) {
        try {
            client.getKVClient().put(bytes(key), bytes(value),
                    PutOption.builder().withLeaseId(leaseId).build()).get();
        } catch (Exception e) {
            throw new RpcException("Etcd写入失败: " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            client.getKVClient().delete(bytes(key)).get();
        } catch (Exception e) {
            throw new RpcException("Etcd删除失败: " + key, e);
        }
    }

    @Override
    public List<KeyValue> getByPrefix(String prefix) {
        try {
            List<KeyValue> result = new ArrayList<>();
            client.getKVClient().get(bytes(prefix), GetOption.builder().isPrefix(true).build())
                    .get().getKvs()
                    .forEach(kv -> result.add(new KeyValue(
                            kv.getKey().toString(StandardCharsets.UTF_8),
                            kv.getValue().toString(StandardCharsets.UTF_8))));
            return result;
        } catch (Exception e) {
            throw new RpcException("Etcd按前缀查询失败: " + prefix, e);
        }
    }

    @Override
    public void keepAliveOnce(long leaseId) {
        try {
            client.getLeaseClient().keepAliveOnce(leaseId).get();
        } catch (Exception e) {
            throw new RpcException("Etcd续租失败: leaseId=" + leaseId, e);
        }
    }

    @Override
    public void revokeLease(long leaseId) {
        try {
            client.getLeaseClient().revoke(leaseId).get();
        } catch (Exception e) {
            throw new RpcException("Etcd撤销租约失败: leaseId=" + leaseId, e);
        }
    }

    @Override
    public AutoCloseable watchPrefix(String prefix, Runnable onChange) {
        Watch.Watcher watcher = client.getWatchClient().watch(
                bytes(prefix),
                WatchOption.builder().isPrefix(true).build(),
                Watch.listener(response -> onChange.run()));
        return watcher;
    }

    @Override
    public void close() {
        client.close();
    }

    private static ByteSequence bytes(String value) {
        return ByteSequence.from(value, StandardCharsets.UTF_8);
    }
}
