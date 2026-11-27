package io.github.nitouge.conduit.benchmark;

import io.github.nitouge.conduit.api.RpcRequest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 基准数据工厂：固定payload形态，保证不同序列化实现之间、不同批次之间可比。
 *
 * <p>两种规模：</p>
 * <ul>
 *   <li>{@code small} —— 贴近常见的"查一次、改一次"调用（约百字节量级）；</li>
 *   <li>{@code large} —— 列表/Map都放大，用来观察压缩与序列化的规模效应。</li>
 * </ul>
 */
public final class BenchData {

    /** 模拟的接口全限定名（只用于序列化体积，真实基准走真实接口） */
    private static final String INTERFACE = BenchService.class.getName();

    private BenchData() {
    }

    /**
     * 构造一帧业务请求（与真实调用同构：接口名 + 方法名 + 参数类型 + 实参 + attachments）。
     *
     * @param payloadSize small / large
     */
    public static RpcRequest request(String payloadSize) {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName(INTERFACE);
        request.setMethodName("roundTrip");
        request.setParamTypes(new Class<?>[]{BenchPayload.class});
        request.setArgs(new Object[]{payload(payloadSize)});
        request.setVersion("1.0.0");
        request.setGroup("bench");
        // 与TraceFilter一致：真实请求通常带traceId attachment
        request.setAttachment("traceId", "0123456789abcdef0123456789abcdef");
        return request;
    }

    /** 构造业务载荷 */
    public static BenchPayload payload(String payloadSize) {
        boolean large = "large".equalsIgnoreCase(payloadSize);
        int tagCount = large ? 64 : 4;
        int attrCount = large ? 16 : 2;
        int textLength = large ? 40 : 8;

        List<String> tags = new ArrayList<>(tagCount);
        for (int i = 0; i < tagCount; i++) {
            tags.add(pad("tag" + i, textLength));
        }
        Map<String, String> attributes = new LinkedHashMap<>();
        for (int i = 0; i < attrCount; i++) {
            attributes.put(pad("k" + i, textLength / 2), pad("v" + i, textLength));
        }
        return new BenchPayload(large ? 20261124L : 1L, pad("conduit-bench", textLength), tags, attributes);
    }

    /** 右侧补点，保证不同标签长度一致（避免字符串压缩效果随机波动） */
    private static String pad(String value, int length) {
        if (value.length() >= length) {
            return value.substring(0, length);
        }
        StringBuilder sb = new StringBuilder(value);
        while (sb.length() < length) {
            sb.append('.');
        }
        return sb.toString();
    }
}
