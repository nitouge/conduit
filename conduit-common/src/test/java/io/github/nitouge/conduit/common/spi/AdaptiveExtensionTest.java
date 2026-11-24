package io.github.nitouge.conduit.common.spi;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 自适应扩展：按方法参数（Map / RpcRequest.attachments）实时选择实现，未取到回退默认。
 */
class AdaptiveExtensionTest {

    private final ExtensionLoader<Translator> loader = ExtensionLoader.getExtensionLoader(Translator.class);

    @Test
    void selectsImplementationByMapKey() {
        Translator adaptive = loader.getAdaptiveExtension();

        assertEquals("zh", adaptive.translate(Map.of("lang", "zh")));
        assertEquals("en", adaptive.translate(Map.of("lang", "en")));
    }

    @Test
    void selectsImplementationByRpcRequestAttachment() {
        RpcRequest request = new RpcRequest();
        request.setAttachment("lang", "zh");

        assertEquals("zh", loader.getAdaptiveExtension().translate(request));
        assertEquals("en", loader.getAdaptiveExtension().translate(new RpcRequest()));
    }

    @Test
    void fallsBackToSpiDefaultWhenNoDynamicValue() {
        Translator adaptive = loader.getAdaptiveExtension();

        assertEquals("en", adaptive.translate(Map.of()));
        assertEquals("en", adaptive.translate((Map<String, String>) null));
    }

    @Test
    void preferredDefaultUsedWhenNoDynamicValue() {
        Translator adaptive = loader.getAdaptiveExtension("zh");

        assertEquals("zh", adaptive.translate(Map.of()));
    }

    @Test
    void explicitDefaultValueFallsBackToPreferredName() {
        Translator adaptive = loader.getAdaptiveExtension("zh");

        assertEquals("zh", adaptive.translate(Map.of("lang", "default")));
    }

    @Test
    void unknownExtensionNameFailsFast() {
        assertThrows(RpcException.class,
                () -> loader.getAdaptiveExtension().translate(Map.of("lang", "fr")));
    }

    @Test
    void adaptiveProxyIsCachedAndBehavesAsObject() {
        Translator first = loader.getAdaptiveExtension();
        Translator second = loader.getAdaptiveExtension();

        assertSame(first, second, "同一默认名的自适应代理应复用");
        assertEquals(first, first);
        assertNotNull(first.toString());
    }
}
