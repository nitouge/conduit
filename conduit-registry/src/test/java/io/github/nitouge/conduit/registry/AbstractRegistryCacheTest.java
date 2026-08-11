package io.github.nitouge.conduit.registry;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.registry.support.AbstractRegistry;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 本地缓存兜底逻辑的单测（不依赖Nacos，始终运行）。
 */
class AbstractRegistryCacheTest {

    /** 后端可控的内存注册中心：fail=true时doDiscover抛异常，用来验证缓存兜底 */
    private static AbstractRegistry memoryRegistry(AtomicBoolean fail, List<ServiceMeta> live) {
        return new AbstractRegistry() {
            @Override
            protected void doRegister(ServiceMeta service) {
            }

            @Override
            protected void doUnregister(ServiceMeta service) {
            }

            @Override
            protected List<ServiceMeta> doDiscover(String serviceName) {
                if (fail.get()) {
                    throw new RuntimeException("backend down");
                }
                return live;
            }

            @Override
            protected void doSubscribe(String serviceName, NotifyListener listener) {
            }

            @Override
            public void destroy() {
            }
        };
    }

    @Test
    void discoverFallsBackToCacheWhenBackendDown() {
        AtomicBoolean fail = new AtomicBoolean(false);
        List<ServiceMeta> live = Collections.singletonList(new ServiceMeta("svc", "10.0.0.1", 20880));
        AbstractRegistry reg = memoryRegistry(fail, live);

        // 第一次成功 -> 写入缓存
        assertEquals(1, reg.discover("svc").size());

        // 后端宕机 -> 退回本地缓存
        fail.set(true);
        List<ServiceMeta> cached = reg.discover("svc");
        assertEquals(1, cached.size());
        assertEquals("10.0.0.1", cached.get(0).getHost());
    }

    @Test
    void discoverThrowsWhenBackendDownAndNoCache() {
        AtomicBoolean fail = new AtomicBoolean(true);
        AbstractRegistry reg = memoryRegistry(fail, Collections.emptyList());

        // 从未成功过 -> 无缓存可兜底 -> 抛RpcException
        assertThrows(RpcException.class, () -> reg.discover("never-seen"));
    }

    @Test
    void emptySuccessfulDiscoverDoesNotOverwriteGoodCache() {
        AtomicBoolean fail = new AtomicBoolean(false);
        AtomicReference<List<ServiceMeta>> result = new AtomicReference<>(Collections.singletonList(
                new ServiceMeta("svc", "10.0.0.1", 20880))
        );
        AbstractRegistry reg = new AbstractRegistry() {
            @Override
            protected void doRegister(ServiceMeta service) {
            }

            @Override
            protected void doUnregister(ServiceMeta service) {
            }

            @Override
            protected List<ServiceMeta> doDiscover(String serviceName) {
                if (fail.get()) {
                    throw new RuntimeException("backend down");
                }
                return result.get();
            }

            @Override
            protected void doSubscribe(String serviceName, NotifyListener listener) {
            }

            @Override
            public void destroy() {
            }
        };

        // 1) 首次成功非空 -> 写入缓存
        assertEquals(1, reg.discover("svc").size());

        // 2) 成功但为空 -> 实时返回空，但不应覆盖缓存
        result.set(Collections.emptyList());
        assertEquals(0, reg.discover("svc").size());

        // 3) 注册中心宕机 -> 兜底应仍是步骤1的非空缓存（未被步骤2的空覆盖）
        fail.set(true);
        List<ServiceMeta> fallback = reg.discover("svc");
        assertEquals(1, fallback.size());
        assertEquals("10.0.0.1", fallback.get(0).getHost());
    }
}
