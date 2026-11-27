package io.github.nitouge.conduit.registry.nacos;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.registry.NotifyListener;
import io.github.nitouge.conduit.registry.support.AbstractRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于Nacos的注册中心实现。把四个能力映射到Nacos NamingService：
 *
 * <ul>
 *   <li>注册 = 注册临时实例（ephemeral=true），实例metadata带上version/group/tag/zone</li>
 *   <li>注销 = deregisterInstance</li>
 *   <li>发现 = selectInstances(healthy=true)</li>
 *   <li>监听 = subscribe（Nacos push推送变更）</li>
 * </ul>
 *
 * <p>本地缓存兜底由父类 {@link AbstractRegistry} 提供。</p>
 */
public class NacosRegistry extends AbstractRegistry {

    private static final Logger log = LoggerFactory.getLogger(NacosRegistry.class);

    /** Nacos实例metadata里承载路由属性的键（带命名空间前缀，避免与业务metadata撞名） */
    static final String META_VERSION = "conduit.version";
    static final String META_GROUP = "conduit.group";
    static final String META_TAG = "conduit.tag";
    static final String META_ZONE = "conduit.zone";

    private final NamingService namingService;

    public NacosRegistry(String address) {
        try {
            this.namingService = NamingFactory.createNamingService(address);
            log.info("[NacosRegistry] 已连接Nacos: {}", address);
        } catch (NacosException e) {
            throw new RpcException("连接Nacos失败: " + address, e);
        }
    }

    @Override
    protected void doRegister(ServiceMeta service) {
        try {
            Instance instance = new Instance();
            instance.setIp(service.getHost());
            instance.setPort(service.getPort());
            instance.setWeight(service.getWeight());
            instance.setEphemeral(true);
            // 版本/分组/标签/机房放进Nacos实例的metadata：消费侧selectInstances时随实例一起返回，
            // 路由才有依据（Nacos的ip/port/weight三个字段装不下这些）
            Map<String, String> metadata = routeMetadata(service);
            if (!metadata.isEmpty()) {
                instance.setMetadata(metadata);
            }
            namingService.registerInstance(service.getServiceName(), instance);
            log.debug("[NacosRegistry] Nacos返回registerInstance成功 {}:{}:{} metadata={}",
                    service.getServiceName(), service.getHost(), service.getPort(), metadata);
        } catch (NacosException e) {
            throw new RpcException("注册服务到Nacos失败: " + service, e);
        }
    }

    @Override
    protected void doUnregister(ServiceMeta service) {
        try {
            namingService.deregisterInstance(service.getServiceName(), service.getHost(), service.getPort());
            log.debug("[NacosRegistry] Nacos返回deregisterInstance成功 {}:{}:{}",
                    service.getServiceName(), service.getHost(), service.getPort());
        } catch (NacosException e) {
            throw new RpcException("从Nacos注销服务失败: " + service, e);
        }
    }

    @Override
    protected List<ServiceMeta> doDiscover(String serviceName) {
        try {
            List<Instance> instances = namingService.selectInstances(serviceName, true);
            log.debug("[NacosRegistry] Nacos返回selectInstances {}: {}个healthy实例",
                    serviceName, instances == null ? 0 : instances.size());
            return toServiceMetas(serviceName, instances);
        } catch (NacosException e) {
            throw new RpcException("从Nacos发现服务失败: " + serviceName, e);
        }
    }

    @Override
    protected void doSubscribe(String serviceName, NotifyListener listener) {
        try {
            namingService.subscribe(serviceName, event -> {
                if (event instanceof NamingEvent) {
                    List<Instance> instances = ((NamingEvent) event).getInstances();
                    log.debug("[NacosRegistry] Nacos push事件 {}: {}个实例",
                            serviceName, instances == null ? 0 : instances.size());
                    listener.notify(toServiceMetas(serviceName, instances));
                }
            });
            log.debug("[NacosRegistry] Nacos返回subscribe成功 {}", serviceName);
        } catch (NacosException e) {
            throw new RpcException("订阅Nacos服务失败: " + serviceName, e);
        }
    }

    @Override
    public void destroy() {
        try {
            namingService.shutDown();
            log.info("[NacosRegistry] NamingService已关闭");
        } catch (NacosException e) {
            throw new RpcException("关闭Nacos客户端失败", e);
        }
    }

    /** 把Nacos的 {@link Instance} 列表映射成Conduit的 {@link ServiceMeta}（ip/port/weight + 路由属性），空列表安全。 */
    private static List<ServiceMeta> toServiceMetas(String serviceName, List<Instance> instances) {
        List<ServiceMeta> metas = new ArrayList<>();
        if (instances != null) {
            for (Instance ins : instances) {
                ServiceMeta meta = new ServiceMeta(serviceName, ins.getIp(), ins.getPort(), (int) ins.getWeight());
                Map<String, String> metadata = ins.getMetadata();
                if (metadata != null && !metadata.isEmpty()) {
                    meta.routeAttributes(metadata.get(META_VERSION), metadata.get(META_GROUP),
                            metadata.get(META_TAG), metadata.get(META_ZONE));
                }
                metas.add(meta);
            }
        }
        return metas;
    }

    /** 实例的路由属性 → Nacos metadata（只放非空的，避免给控制台塞一堆空键） */
    private static Map<String, String> routeMetadata(ServiceMeta service) {
        Map<String, String> metadata = new HashMap<>();
        putIfPresent(metadata, META_VERSION, service.getVersion());
        putIfPresent(metadata, META_GROUP, service.getGroup());
        putIfPresent(metadata, META_TAG, service.getTag());
        putIfPresent(metadata, META_ZONE, service.getZone());
        return metadata;
    }

    private static void putIfPresent(Map<String, String> metadata, String key, String value) {
        if (value != null && !value.isBlank()) {
            metadata.put(key, value);
        }
    }
}
