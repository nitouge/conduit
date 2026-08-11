package io.github.nitouge.conduit.common.spi;

import io.github.nitouge.conduit.common.exception.RpcException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExtensionLoaderTest {

    @Test
    void getDefaultExtensionUsesSpiValue() {
        Greeter g = ExtensionLoader.getExtensionLoader(Greeter.class).getDefaultExtension();
        assertEquals("hello", g.greet()); // @SPI("en") -> EnglishGreeter
    }

    @Test
    void getExtensionByName() {
        Greeter zh = ExtensionLoader.getExtensionLoader(Greeter.class).getExtension("zh");
        assertEquals("你好", zh.greet());
    }

    @Test
    void extensionsAreCachedSingletons() {
        ExtensionLoader<Greeter> loader = ExtensionLoader.getExtensionLoader(Greeter.class);
        assertSame(loader.getExtension("en"), loader.getExtension("en"));
    }

    @Test
    void unknownExtensionThrows() {
        assertThrows(RpcException.class,
                () -> ExtensionLoader.getExtensionLoader(Greeter.class).getExtension("nope"));
    }

    @Test
    void nonSpiInterfaceRejected() {
        // Runnable没有@SPI注解
        assertThrows(IllegalArgumentException.class,
                () -> ExtensionLoader.getExtensionLoader(Runnable.class));
    }
}
