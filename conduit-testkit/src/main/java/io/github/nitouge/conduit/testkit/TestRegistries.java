package io.github.nitouge.conduit.testkit;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.registry.NotifyListener;
import io.github.nitouge.conduit.registry.Registry;
import jdk.jfr.Recording;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 注册中心替身：把"发现结果"与"注册行为"从真实注册中心里剥出来，便于精准构造场景。
 *
 * <p>三种用法：</p>
 * <ul>
 *   <li>{@link #fixed(List)} —— discover永远返回给定节点（用于"含死节点"的容错/熔断测试）；</li>
 *   <li>{@link #empty()} —— 永远没有提供者（用于"无可用服务"断言）；</li>
 *   <li>{@link #mutable()} / {@link #recording()} —— 可注册/可注销，并记录所有register/unregister调用。</li>
 * </ul>
 *
 * <p>真实注册中心（Nacos/ZooKeeper/Etcd）的端到端测试不要用这里，应直接用 {@code Registries}。</p>
 */
public final class TestRegistries {

    private TestRegistries() {
    }

    /** 固定返回给定节点列表（不可变） */
    public static Registry fixed(List<ServiceMeta> nodes) {
        List<ServiceMeta> snapshot = List.copyOf(nodes);
        return new BaseRegistry() {
            @Override
            public List<ServiceMeta> discover(String serviceName) {
                return new ArrayList<>(snapshot);
            }
        };
    }

    /** 永远没有提供者 */
    public static Registry empty() {
        return fixed(List.of());
    }

    /** 可注册/可注销的进程内注册中心，支持订阅推送 */
    public static MutableRegistry mutable() {
        return new MutableRegistry();
    }

    /** 可注册/可注销，并记录调用轨迹（用 {@link Recording#registered()} 断言服务端确实注册过） */
    public static RecordingRegistry recording() {
        return new RecordingRegistry();
    }

    /** 记录注册/注销轨迹的注册中心 */
    public static final class RecordingRegistry extends MutableRegistry {
        private final List<ServiceMeta> registered = new CopyOnWriteArrayList<>();
        private final List<ServiceMeta> unregistered = new CopyOnWriteArrayList<>();

        public List<ServiceMeta> registered() {
            return new ArrayList<>(registered);
        }

        public List<ServiceMeta> unregistered() {
            return new ArrayList<>(unregistered);
        }

        @Override
        public void register(ServiceMeta service) {
            registered.add(service);
            super.register(service);
        }

        @Override
        public void unregister(ServiceMeta service) {
            unregistered.add(service);
            super.unregister(service);
        }
    }

    /** 可变的进程内注册中心：注册/发现/订阅都可用 */
    public static class MutableRegistry extends BaseRegistry {
        private final Map<String, CopyOnWriteArrayList<ServiceMeta>> store = new ConcurrentHashMap<>();
        private final Map<String, CopyOnWriteArrayList<NotifyListener>> listeners = new ConcurrentHashMap<>();

        @Override
        public void register(ServiceMeta service) {
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
            listener.notify(discover(serviceName));
        }

        @Override
        public void destroy() {
            store.clear();
            listeners.clear();
        }

        private void notifyListeners(String serviceName) {
            List<NotifyListener> ls = listeners.get(serviceName);
            if (ls == null) {
                return;
            }
            for (NotifyListener listener : ls) {
                listener.notify(discover(serviceName));
            }
        }
    }

    /** 默认空实现，子类只覆盖需要的方法 */
    private abstract static class BaseRegistry implements Registry {
        @Override
        public void register(ServiceMeta service) {
        }

        @Override
        public void unregister(ServiceMeta service) {
        }

        @Override
        public List<ServiceMeta> discover(String serviceName) {
            return new ArrayList<>();
        }

        @Override
        public void subscribe(String serviceName, NotifyListener listener) {
        }

        @Override
        public void destroy() {
        }
    }
}
