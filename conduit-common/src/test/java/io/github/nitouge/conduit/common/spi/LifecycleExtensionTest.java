package io.github.nitouge.conduit.common.spi;

import io.github.nitouge.conduit.common.exception.RpcException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 扩展生命周期：init在创建后调用一次、destroy在销毁时调用一次，失败不缓存。
 */
class LifecycleExtensionTest {

    private final ExtensionLoader<Greeter> loader = ExtensionLoader.getExtensionLoader(Greeter.class);

    @BeforeEach
    void setUp() {
        loader.destroyAll();
        LifecycleGreeter.resetCounters();
    }

    @AfterEach
    void tearDown() {
        loader.destroyAll();
    }

    @Test
    void initOnceOnCreationAndDestroyOncePerInstance() {
        Greeter first = loader.getExtension("lifecycle");
        assertEquals(1, LifecycleGreeter.INIT_COUNT.get(), "创建后应调用一次init");

        assertSame(first, loader.getExtension("lifecycle"));
        assertEquals(1, LifecycleGreeter.INIT_COUNT.get(), "缓存单例不应重复init");

        loader.destroyExtension("lifecycle");
        assertEquals(1, LifecycleGreeter.DESTROY_COUNT.get(), "销毁应调用一次destroy");

        Greeter recreated = loader.getExtension("lifecycle");
        assertNotSame(first, recreated, "销毁后再获取应重建实例");
        assertEquals(2, LifecycleGreeter.INIT_COUNT.get());
    }

    @Test
    void destroyAllIsIdempotent() {
        loader.getExtension("lifecycle");
        loader.destroyAll();
        assertEquals(1, LifecycleGreeter.DESTROY_COUNT.get());

        loader.destroyAll();
        assertEquals(1, LifecycleGreeter.DESTROY_COUNT.get(), "重复destroyAll不应重复destroy");
    }

    @Test
    void initFailureAbortsCreationAndIsNotCached() {
        RpcException first = assertThrows(RpcException.class, () -> loader.getExtension("broken"));
        assertTrue(first.getMessage().contains("扩展初始化失败"), first.getMessage());

        // 失败实例不应进入缓存：再次获取会重新创建并再次失败
        assertThrows(RpcException.class, () -> loader.getExtension("broken"));
    }

    @Test
    void extensionsWithoutLifecycleAreUnaffected() {
        assertDoesNotThrow(() -> loader.getExtension("en"));
        assertDoesNotThrow(loader::destroyAll);
    }
}
