package io.github.nitouge.conduit.registry.zookeeper;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.registry.NotifyListener;
import io.github.nitouge.conduit.registry.support.AbstractRegistry;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于ZooKeeper（Curator）的注册中心实现。
 *
 * <p>存储模型：</p>
 * <pre>
 * /conduit/services/{serviceName}/{host}:{port}   （EPHEMERAL节点，value=host|port|weight）
 * </pre>
 * <ul>
 *   <li>注册 = 创建临时节点（会话断开自动删除，等价Nacos临时实例）；</li>
 *   <li>发现 = 读子节点 + 节点数据；</li>
 *   <li>监听 = {@link CuratorCache} 监听子节点增删改，变更后重新发现并回调；</li>
 *   <li>本地缓存兜底/订阅幂等由 {@link AbstractRegistry} 统一处理。</li>
 * </ul>
 */
public class ZookeeperRegistry extends AbstractRegistry {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperRegistry.class);

    /**
     * 所有服务实例的根路径
     */
    static final String BASE_PATH = "/conduit/services";

    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
    private static final int DEFAULT_SESSION_TIMEOUT_MS = 10_000;

    private final CuratorFramework client;
    private final Map<String, PersistentNode> nodes = new ConcurrentHashMap<>();
    private final Map<String, CuratorCache> caches = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ZookeeperRegistry(String address) {
        this(address, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_SESSION_TIMEOUT_MS);
    }

    /**
     * 可指定超时的构造（测试/快速失败用）
     */
    ZookeeperRegistry(String address, int connectTimeoutMs, int sessionTimeoutMs) {
        if (StringUtils.isBlank(address)) {
            throw new RpcException("ZooKeeper注册中心地址不能为空");
        }
        this.client = CuratorFrameworkFactory.builder()
                .connectString(address)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectTimeoutMs)
                .retryPolicy(new ExponentialBackoffRetry(200, 3))
                .build();
        this.client.start();
        try {
            if (!client.blockUntilConnected(connectTimeoutMs, TimeUnit.MILLISECONDS)) {
                client.close();
                throw new RpcException("连接ZooKeeper超时: " + address);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            client.close();
            throw new RpcException("连接ZooKeeper被中断: " + address, e);
        }
        log.info("[ZookeeperRegistry] 已连接ZooKeeper: {}", address);
    }

    @Override
    protected void doRegister(ServiceMeta service) {
        try {
            ensureServicePath(service.getServiceName());
            String nodePath = nodePath(service);

            // 先清理同路径旧节点（同一实例重复注册时更新权重）
            PersistentNode previous = nodes.remove(nodePath);
            if (previous != null) {
                closeQuietly(previous);
            }
            try {
                client.delete().forPath(nodePath);
            } catch (KeeperException.NoNodeException ignored) {
                // 首次注册
            }

            PersistentNode node = new PersistentNode(client,
                    CreateMode.EPHEMERAL,
                    false,
                    nodePath,
                    encode(service)
            );
            node.start();
            // 等首次创建真正落地，保证register返回后即可发现（避免注册后立刻发现偶发为空）
            try {
                if (!node.waitForInitialCreate(5, TimeUnit.SECONDS)) {
                    closeQuietly(node);
                    throw new RpcException("创建ZooKeeper临时节点超时: " + nodePath);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                closeQuietly(node);
                throw new RpcException("创建ZooKeeper临时节点被中断: " + nodePath, e);
            }
            nodes.put(nodePath, node);
            log.debug("[ZookeeperRegistry] 注册 {} -> {}", service, nodePath);
        } catch (Exception e) {
            throw new RpcException("注册服务到ZooKeeper失败: " + service, e);
        }
    }

    @Override
    protected void doUnregister(ServiceMeta service) {
        String nodePath = nodePath(service);
        PersistentNode node = nodes.remove(nodePath);
        if (node != null) {
            closeQuietly(node);
        }
        try {
            client.delete().forPath(nodePath);
            log.debug("[ZookeeperRegistry] 注销 {} -> {}", service, nodePath);
        } catch (KeeperException.NoNodeException ignored) {
            log.debug("[ZookeeperRegistry] 节点已不存在，忽略注销: {}", nodePath);
        } catch (Exception e) {
            throw new RpcException("从ZooKeeper注销服务失败: " + service, e);
        }
    }

    @Override
    protected List<ServiceMeta> doDiscover(String serviceName) {
        String servicePath = servicePath(serviceName);
        try {
            if (client.checkExists().forPath(servicePath) == null) {
                return new ArrayList<>();
            }
            List<String> children = client.getChildren().forPath(servicePath);
            List<ServiceMeta> result = new ArrayList<>(children.size());
            for (String child : children) {
                String childPath = servicePath + "/" + child;
                try {
                    ServiceMeta meta = decode(serviceName, client.getData().forPath(childPath));
                    if (meta != null) {
                        result.add(meta);
                    }
                } catch (KeeperException.NoNodeException ignored) {
                    // 子节点在读取间隙被删除：忽略
                }
            }
            return result;
        } catch (Exception e) {
            throw new RpcException("从ZooKeeper发现服务失败: " + serviceName, e);
        }
    }

    @Override
    protected void doSubscribe(String serviceName, NotifyListener listener) {
        try {
            ensureServicePath(serviceName);
            String servicePath = servicePath(serviceName);
            CuratorCache cache = CuratorCache.build(client, servicePath);
            cache.listenable().addListener((type, oldData, data) -> {
                try {
                    listener.notify(discover(serviceName));
                } catch (Throwable t) {
                    log.warn("[ZookeeperRegistry] 推送订阅变更失败 {}: {}", serviceName, t.toString());
                }
            });
            cache.start();
            CuratorCache previous = caches.put(serviceName, cache);
            if (previous != null) {
                previous.close();
            }
            // 立即回放当前列表（CuratorCache的初始事件可能晚于订阅返回）
            listener.notify(discover(serviceName));
            log.debug("[ZookeeperRegistry] 订阅 {} 成功", serviceName);
        } catch (Exception e) {
            throw new RpcException("订阅ZooKeeper服务失败: " + serviceName, e);
        }
    }

    @Override
    public void destroy() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (CuratorCache cache : caches.values()) {
            try {
                cache.close();
            } catch (Exception e) {
                log.debug("[ZookeeperRegistry] 关闭CuratorCache失败: {}", e.toString());
            }
        }
        caches.clear();
        for (PersistentNode node : nodes.values()) {
            closeQuietly(node);
        }
        nodes.clear();
        client.close();
        log.info("[ZookeeperRegistry] 已关闭");
    }

    private static void closeQuietly(PersistentNode node) {
        try {
            node.close();
        } catch (IOException e) {
            log.debug("[ZookeeperRegistry] 关闭临时节点失败: {}", e.toString());
        }
    }

    private void ensureServicePath(String serviceName) throws Exception {
        String path = servicePath(serviceName);
        try {
            client.create().creatingParentContainersIfNeeded().forPath(path, new byte[0]);
        } catch (KeeperException.NodeExistsException ignored) {
            // 已存在
        }
    }

    private static String servicePath(String serviceName) {
        return BASE_PATH + "/" + escape(serviceName);
    }

    private static String nodePath(ServiceMeta service) {
        return servicePath(service.getServiceName()) + "/" + service.getHost() + ":" + service.getPort();
    }

    /**
     * 节点名中只可能出现接口名，替换ZK路径分隔符以防非法路径
     */
    private static String escape(String serviceName) {
        return serviceName.replace('/', '_');
    }

    private static byte[] encode(ServiceMeta service) {
        return (service.getHost() + "|" + service.getPort() + "|" + service.getWeight())
                .getBytes(StandardCharsets.UTF_8);
    }

    private static ServiceMeta decode(String serviceName, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        String text = new String(data, StandardCharsets.UTF_8);
        String[] parts = text.split("\\|");
        if (parts.length < 2) {
            return null;
        }
        try {
            int port = Integer.parseInt(parts[1].trim());
            int weight = parts.length > 2 ? Integer.parseInt(parts[2].trim()) : 100;
            return new ServiceMeta(serviceName, parts[0].trim(), port, weight);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
