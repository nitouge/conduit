package io.github.nitouge.conduit.registry.support;

import io.github.nitouge.conduit.api.ServiceMeta;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * {@link ServiceMeta} 的注册中心存储编码：一行文本，便于放进 ZK 节点数据、Etcd value 这类
 * 只接受字节串的位置。
 *
 * <p>格式：{@code host|port|weight|version|group|tag|zone}，后四个字段（路由属性）经过
 * URL 编码，因此可以安全地包含 {@code |}、空格等字符；缺失的字段留空占位，位置固定。</p>
 *
 * <p><b>向后兼容：</b>老数据只有 {@code host|port|weight}（甚至 Etcd 只有 {@code weight}），
 * 解码时按 {@code parts.length} 判断，多出来的字段不存在就当「未声明」，等到下次注册自然补齐。</p>
 *
 * <p>为什么集中在这里：ZK 与 Etcd 两个适配器都要写同一份结构，分开实现迟早会漂移成「ZK 认
 * version、Etcd 不认」这种最难查的问题。</p>
 */
public final class ServiceMetaCodec {

    private static final char FIELD_SEPARATOR = '|';

    private ServiceMetaCodec() {
    }

    /** 编码为可存储的一行文本；路由属性为空时留空占位（保持字段位置） */
    public static String encode(ServiceMeta meta) {
        StringBuilder sb = new StringBuilder(64);
        sb.append(meta.getHost() == null ? "" : meta.getHost())
                .append(FIELD_SEPARATOR).append(meta.getPort())
                .append(FIELD_SEPARATOR).append(meta.getWeight())
                .append(FIELD_SEPARATOR).append(quote(meta.getVersion()))
                .append(FIELD_SEPARATOR).append(quote(meta.getGroup()))
                .append(FIELD_SEPARATOR).append(quote(meta.getTag()))
                .append(FIELD_SEPARATOR).append(quote(meta.getZone()));
        return sb.toString();
    }

    /**
     * 解码注册信息。
     *
     * @param serviceName 服务名（存储里通常不重复保存，由调用方提供）
     * @param text        编码后的文本
     * @return 解析结果；文本为空或非法时返回 {@code null}
     */
    public static ServiceMeta decode(String serviceName, String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        String[] parts = text.split("\\" + FIELD_SEPARATOR, 7);
        if (parts.length < 2) {
            return null;
        }
        try {
            int port = Integer.parseInt(parts[1].trim());
            int weight = 100;
            if (parts.length > 2 && !parts[2].isBlank()) {
                weight = Integer.parseInt(parts[2].trim());
            }
            ServiceMeta meta = new ServiceMeta(serviceName, unquote(parts[0]), port, weight);
            meta.routeAttributes(field(parts, 3), field(parts, 4), field(parts, 5), field(parts, 6));
            return meta;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String field(String[] parts, int index) {
        return index < parts.length ? unquote(parts[index]) : null;
    }

    private static String quote(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        // 空格编码成 %20 而不是 +，避免与字面上的加号混淆
        return URLEncoder.encode(value.trim(), StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String unquote(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return URLDecoder.decode(value.trim(), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            // 非法转义（如老数据里直接写了 %）：按原文使用，不能因此丢掉整个实例
            return value.trim();
        }
    }
}
