package io.github.nitouge.conduit.registry.support;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.registry.NotifyListener;
import io.github.nitouge.conduit.registry.Registry;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 注册中心抽象基类：用模板方法把可用性兜底收敛在一处，具体后端只需实现do*方法。
 *
 * <p>核心能力——<b>本地缓存兜底</b>：每次成功discover都缓存最近一次结果；当注册中心不可用时，
 * discover退回到本地缓存，让消费者在注册中心宕机期间仍能调用既有提供者。subscribe收到推送时
 * 也会刷新该缓存。</p>
 *
 * <p>说明：失败重试（failback：注册/订阅失败后台重试）是更完整的增强，列入后续；M2先做读路径兜底。</p>
 */
public abstract class AbstractRegistry implements Registry {

    private static final Logger log = LoggerFactory.getLogger(AbstractRegistry.class);

    /** 本地缓存：serviceName -> 最近一次已知的实例列表 */
    protected final Map<String, List<ServiceMeta>> cache = new ConcurrentHashMap<>();

    /** 已订阅服务的业务监听器：serviceName -> listeners。每个服务只向后端注册一个监听器，本地fan-out。 */
    private final Map<String, CopyOnWriteArrayList<NotifyListener>> subscribers = new ConcurrentHashMap<>();

    @Override
    public void register(ServiceMeta service) {
        log.debug("[AbstractRegistry] register {}:{}:{} weight={}",
                service.getServiceName(), service.getHost(), service.getPort(), service.getWeight());
        doRegister(service);
    }

    @Override
    public void unregister(ServiceMeta service) {
        log.debug("[AbstractRegistry] unregister {}:{}:{}",
                service.getServiceName(), service.getHost(), service.getPort());
        doUnregister(service);
    }

    @Override
    public List<ServiceMeta> discover(String serviceName) {
        try {
            List<ServiceMeta> live = doDiscover(serviceName);
            // 只缓存非空结果：一次成功但为空的发现（provider临时全下线）不应覆盖之前的好缓存，
            // 否则随后注册中心宕机时兜底会退化成空。空结果本次照常返回（反映实时状态）。
            if (CollectionUtils.isNotEmpty(live)) {
                cache.put(serviceName, live);
            }
            log.debug("[AbstractRegistry] discover {} -> {}个实例 (cache size={})",
                    serviceName, live == null ? 0 : live.size(),
                    cache.containsKey(serviceName) ? cache.get(serviceName).size() : 0);
            return live;
        } catch (Throwable t) {
            List<ServiceMeta> cached = cache.get(serviceName);
            if (cached != null) {
                log.warn("发现服务{}失败，使用本地缓存兜底（{}个实例）: {}", serviceName, cached.size(), t.toString());
                return cached;
            }
            throw new RpcException("发现服务失败且无本地缓存: " + serviceName, t);
        }
    }

    @Override
    public void subscribe(String serviceName, NotifyListener listener) {
        // 幂等订阅：同一serviceName只向后端（如Nacos）注册一个监听器，多个业务listener在本地fan-out。
        // 否则同一接口被多个@RpcReference引用时会在Nacos侧累积监听器、每次push触发N次回调、且无法逐个反订阅。
        CopyOnWriteArrayList<NotifyListener> list = subscribers.get(serviceName);
        if (list != null) {
            list.add(listener);
            replayCache(serviceName, listener); // 复用已有后端订阅，给新listener立即回放当前缓存
            log.debug("[AbstractRegistry] subscribe {} 复用已有订阅，追加listener", serviceName);
            return;
        }
        synchronized (subscribers) {
            list = subscribers.get(serviceName);
            if (list == null) {
                CopyOnWriteArrayList<NotifyListener> created = new CopyOnWriteArrayList<>();
                created.add(listener);
                subscribers.put(serviceName, created);
                // 首次订阅：注册唯一的后端分发器。每次推送先刷新本地缓存（只缓存非空，避免空事件毒化兜底缓存），
                // 再fan-out给所有业务listener；业务listener仍能收到空列表以感知下线。
                doSubscribe(serviceName, services -> {
                    int count = services == null ? 0 : services.size();
                    log.debug("[AbstractRegistry] subscribe push 收到 {} 实例数={} (非空才缓存: {})",
                            serviceName, count, CollectionUtils.isNotEmpty(services));
                    if (CollectionUtils.isNotEmpty(services)) {
                        cache.put(serviceName, services);
                    }
                    for (NotifyListener nl : created) {
                        nl.notify(services);
                    }
                });
                log.debug("[AbstractRegistry] subscribe {} 注册新的后端订阅", serviceName);
                return;
            }
        }
        // 竞态：刚好被别的线程建好，退化为追加 + 回放
        list.add(listener);
        replayCache(serviceName, listener);
    }

    private void replayCache(String serviceName, NotifyListener listener) {
        List<ServiceMeta> known = cache.get(serviceName);
        if (known != null) {
            listener.notify(known);
        }
    }

    protected abstract void doRegister(ServiceMeta service);

    protected abstract void doUnregister(ServiceMeta service);

    protected abstract List<ServiceMeta> doDiscover(String serviceName);

    protected abstract void doSubscribe(String serviceName, NotifyListener listener);

    @Override
    public abstract void destroy();
}
