package io.github.nitouge.conduit.registry.support;

import io.github.nitouge.conduit.api.ServiceMeta;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 注册信息编解码测试：路由属性必须能跨"存成一行文本"这一跳存活，老格式必须仍可读。
 */
class ServiceMetaCodecTest {

    @Test
    void roundTripKeepsRoutingAttributes() {
        ServiceMeta meta = new ServiceMeta("com.example.Svc", "10.0.0.1", 20880, 42)
                .routeAttributes("2.0.0", "pay", "gray", "sh-1");

        ServiceMeta decoded = ServiceMetaCodec.decode("com.example.Svc", ServiceMetaCodec.encode(meta));

        assertEquals("10.0.0.1", decoded.getHost());
        assertEquals(20880, decoded.getPort());
        assertEquals(42, decoded.getWeight());
        assertEquals("2.0.0", decoded.getVersion());
        assertEquals("pay", decoded.getGroup());
        assertEquals("gray", decoded.getTag());
        assertEquals("sh-1", decoded.getZone());
    }

    @Test
    void separatorAndSpaceInValuesAreEscaped() {
        ServiceMeta meta = new ServiceMeta("svc", "10.0.0.2", 1, 100)
                .routeAttributes("1.0|beta", "group with space", "gray=1", "sh-1");

        ServiceMeta decoded = ServiceMetaCodec.decode("svc", ServiceMetaCodec.encode(meta));

        // 字段分隔符与空格若未编码，会把整行切错，实例属性跟着串位
        assertEquals("1.0|beta", decoded.getVersion());
        assertEquals("group with space", decoded.getGroup());
        assertEquals("gray=1", decoded.getTag());
        assertEquals("sh-1", decoded.getZone());
    }

    @Test
    void legacyFormatWithoutRoutingAttributesStillDecodes() {
        // 老版本写入的是 host|port|weight，升级后仍必须能读到实例，只是没有路由属性
        ServiceMeta decoded = ServiceMetaCodec.decode("svc", "10.0.0.3|20881|7");

        assertEquals("10.0.0.3", decoded.getHost());
        assertEquals(20881, decoded.getPort());
        assertEquals(7, decoded.getWeight());
        assertNull(decoded.getVersion());
        assertNull(decoded.getGroup());
        assertNull(decoded.getTag());
        assertNull(decoded.getZone());
    }

    @Test
    void illegalTextDecodesToNull() {
        assertNull(ServiceMetaCodec.decode("svc", null));
        assertNull(ServiceMetaCodec.decode("svc", "   "));
        assertNull(ServiceMetaCodec.decode("svc", "no-port-here"));
    }
}
