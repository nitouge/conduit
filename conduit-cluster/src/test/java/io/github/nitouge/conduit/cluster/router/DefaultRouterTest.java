package io.github.nitouge.conduit.cluster.router;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.config.ConduitConfig;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 默认路由规则单元测试：版本/分组/标签硬过滤、机房软优先、空结果与通配语义。
 */
class DefaultRouterTest {

    private final DefaultRouter router = new DefaultRouter();

    @AfterEach
    void clearZoneConfig() {
        System.clearProperty(DefaultRouter.ZONE_CONFIG_KEY);
        ConduitConfig.clearSource();
    }

    @Test
    void noConstraintKeepsAllProviders() {
        List<ServiceMeta> providers = List.of(node("a", "1.0.0", null, null, null),
                node("b", "2.0.0", null, null, null));

        List<ServiceMeta> routed = router.route(providers, request(null, null, null));

        assertEquals(providers, routed, "没有路由条件时不应改变候选集合");
    }

    @Test
    void versionFiltersOutOtherVersions() {
        List<ServiceMeta> providers = List.of(node("a", "1.0.0", null, null, null),
                node("b", "2.0.0", null, null, null));

        assertEquals(List.of(providers.get(1)), router.route(providers, request("2.0.0", null, null)));
        assertEquals(List.of(providers.get(0)), router.route(providers, request("1.0.0", null, null)));
    }

    @Test
    void groupFiltersIndependentlyOfVersion() {
        List<ServiceMeta> providers = List.of(node("a", "1.0.0", "pay", null, null),
                node("b", "1.0.0", "user", null, null));

        assertEquals(List.of(providers.get(0)), router.route(providers, request(null, "pay", null)));
    }

    @Test
    void tagRoutesToTaggedInstances() {
        List<ServiceMeta> providers = List.of(node("stable", "1.0.0", null, null, null),
                node("gray", "1.0.0", null, "gray", null));

        assertEquals(List.of(providers.get(1)), router.route(providers, request(null, null, "gray")));
    }

    @Test
    void versionAndTagCombine() {
        List<ServiceMeta> providers = List.of(node("v1", "1.0.0", null, null, null),
                node("v2", "2.0.0", null, null, null),
                node("v2-gray", "2.0.0", null, "gray", null));

        assertEquals(List.of(providers.get(2)), router.route(providers, request("2.0.0", null, "gray")));
    }

    @Test
    void unknownVersionFailsWithActionableMessage() {
        List<ServiceMeta> providers = List.of(node("a", "1.0.0", null, null, null));

        RpcException error = assertThrows(RpcException.class,
                () -> router.route(providers, request("9.9.9", null, null)));

        String message = error.getMessage();
        assertTrue(message.contains("没有匹配的路由实例"), message);
        assertTrue(message.contains("version=9.9.9"), "错误里应带上写错的约束值: " + message);
        assertTrue(message.contains("1.0.0"), "错误里应带上候选实例的实际属性: " + message);
    }

    @Test
    void wildcardMeansNoConstraint() {
        List<ServiceMeta> providers = List.of(node("a", "1.0.0", null, null, null),
                node("b", "2.0.0", null, null, null));

        assertEquals(providers, router.route(providers, request("*", null, null)));
    }

    @Test
    void sameZoneIsPreferredOverOtherZones() {
        List<ServiceMeta> providers = List.of(node("other", "1.0.0", null, null, "bj-1"),
                node("same", "1.0.0", null, null, "sh-1"));

        RpcRequest request = request(null, null, null);
        request.setAttachment(RpcAttachments.ZONE, "sh-1");

        assertEquals(List.of(providers.get(1)), router.route(providers, request),
                "应优先只保留同机房实例");
    }

    @Test
    void zoneFallsBackToOtherZonesWhenNoLocalInstance() {
        List<ServiceMeta> providers = List.of(node("other", "1.0.0", null, null, "bj-1"));

        RpcRequest request = request(null, null, null);
        request.setAttachment(RpcAttachments.ZONE, "sh-1");

        assertEquals(providers, router.route(providers, request),
                "没有同机房实例时应跨机房兜底，而不是直接失败");
    }

    @Test
    void zoneFromConfigAppliesWhenAttachmentMissing() {
        System.setProperty(DefaultRouter.ZONE_CONFIG_KEY, "sh-1");
        List<ServiceMeta> providers = List.of(node("other", "1.0.0", null, null, "bj-1"),
                node("same", "1.0.0", null, null, "sh-1"));

        assertEquals(List.of(providers.get(1)), router.route(providers, request(null, null, null)),
                "配置的机房应作为默认值生效");
    }

    @Test
    void emptyOrNullProvidersAreReturnedAsEmpty() {
        assertTrue(router.route(null, request("1.0.0", null, null)).isEmpty());
        assertTrue(router.route(List.of(), request("1.0.0", null, null)).isEmpty());
    }

    private static RpcRequest request(String version, String group, String tag) {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("com.example.DemoService");
        request.setMethodName("hello");
        request.setVersion(version);
        request.setGroup(group);
        if (tag != null) {
            request.setAttachment(RpcAttachments.TAG, tag);
        }
        return request;
    }

    private static ServiceMeta node(String host, String version, String group, String tag, String zone) {
        return new ServiceMeta("com.example.DemoService", host, 10_000 + Math.abs(host.hashCode() % 1000))
                .routeAttributes(version, group, tag, zone);
    }
}
