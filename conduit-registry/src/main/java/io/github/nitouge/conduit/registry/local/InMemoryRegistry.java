package io.github.nitouge.conduit.registry.local;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.registry.NotifyListener;
import io.github.nitouge.conduit.registry.Registry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 进程内注册中心：注册/发现都在本JVM的内存里完成，无需任何外部组件。
 *
 * <p>用于单元/集成测试与in-JVM场景（同一进程内既是provider又是consumer）。
 * 通过SPI名 {@code local} 启用：{@code conduit.registry.type=local}。</p>
 */
public class InMemoryRegistry implements Registry {

    private final Map<String, CopyOnWriteArrayList<ServiceMeta>> store = new ConcurrentHashMap<>();
    private final Map<String, List<NotifyListener>> listeners = new ConcurrentHashMap<>();

    @Override
    public void register(ServiceMeta service) {
        // addIfAbsent在CopyOnWriteArrayList上是原子的，避免并发重复注册
        store.computeIfAbsent(service.getServiceName(), k -> new CopyOnWriteArrayList<>())
                .addIfAbsent(service);
        notifyListeners(service.getServiceName());
    }

    @Override
    public void unregister(ServiceMeta service) {
        List<ServiceMeta> list = store.get(service.getServiceName());
        if (list != null) {
            list.remove(service);
        }
        notifyListeners(service.getServiceName());
    }

    @Override
    public List<ServiceMeta> discover(String serviceName) {
        List<ServiceMeta> list = store.get(serviceName);
        return list == null ? new ArrayList<>() : new ArrayList<>(list);
    }

    @Override
    public void subscribe(String serviceName, NotifyListener listener) {
        listeners.computeIfAbsent(serviceName, k -> new CopyOnWriteArrayList<>()).add(listener);
        listener.notify(discover(serviceName)); // 立即推一次当前列表
    }

    @Override
    public void destroy() {
        store.clear();
        listeners.clear();
    }

    private void notifyListeners(String serviceName) {
        List<NotifyListener> ls = listeners.get(serviceName);
        if (ls != null) {
            List<ServiceMeta> current = discover(serviceName);
            for (NotifyListener l : ls) {
                l.notify(current);
            }
        }
    }
}
