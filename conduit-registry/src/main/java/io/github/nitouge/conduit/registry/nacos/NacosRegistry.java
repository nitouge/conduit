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
import java.util.List;

/**
 * 基于Nacos的注册中心实现（M2默认）。把四个能力映射到Nacos NamingService：
 *
 * <ul>
 *   <li>注册 = 注册临时实例（ephemeral=true）</li>
 *   <li>注销 = deregisterInstance</li>
 *   <li>发现 = selectInstances(healthy=true)</li>
 *   <li>监听 = subscribe（Nacos push推送变更）</li>
 * </ul>
 *
 * <p>本地缓存兜底由父类 {@link AbstractRegistry} 提供。</p>
 */
public class NacosRegistry extends AbstractRegistry {

    private static final Logger log = LoggerFactory.getLogger(NacosRegistry.class);

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
            namingService.registerInstance(service.getServiceName(), instance);
            log.debug("[NacosRegistry] Nacos返回 registerInstance成功 {}:{}:{}",
                    service.getServiceName(), service.getHost(), service.getPort());
        } catch (NacosException e) {
            throw new RpcException("注册服务到Nacos失败: " + service, e);
        }
    }

    @Override
    protected void doUnregister(ServiceMeta service) {
        try {
            namingService.deregisterInstance(service.getServiceName(), service.getHost(), service.getPort());
            log.debug("[NacosRegistry] Nacos返回 deregisterInstance成功 {}:{}:{}",
                    service.getServiceName(), service.getHost(), service.getPort());
        } catch (NacosException e) {
            throw new RpcException("从Nacos注销服务失败: " + service, e);
        }
    }

    @Override
    protected List<ServiceMeta> doDiscover(String serviceName) {
        try {
            List<Instance> instances = namingService.selectInstances(serviceName, true);
            log.debug("[NacosRegistry] Nacos返回 selectInstances {}: {}个healthy实例",
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
                    log.debug("[NacosRegistry] Nacos push 事件 {}: {}个实例",
                            serviceName, instances == null ? 0 : instances.size());
                    listener.notify(toServiceMetas(serviceName, instances));
                }
            });
            log.debug("[NacosRegistry] Nacos返回 subscribe成功 {}", serviceName);
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

    /** 把Nacos的 {@link Instance} 列表映射成Conduit的 {@link ServiceMeta}（取ip/port/weight），空列表安全。 */
    private static List<ServiceMeta> toServiceMetas(String serviceName, List<Instance> instances) {
        List<ServiceMeta> metas = new ArrayList<>();
        if (instances != null) {
            for (Instance ins : instances) {
                metas.add(new ServiceMeta(serviceName, ins.getIp(), ins.getPort(), (int) ins.getWeight()));
            }
        }
        return metas;
    }
}
