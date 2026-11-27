package io.github.nitouge.conduit.benchmark;

import java.io.Serial;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 基准用的业务载荷：模拟一个典型的RPC入参（基本类型 + 字符串 + 列表 + Map）。
 *
 * <p>要求有无参构造与getter/setter：JDK序列化要 {@link Serializable}，
 * Kryo/Protostuff/JSON 需要可访问的属性。</p>
 */
public class BenchPayload implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private long id;
    private String name;
    private List<String> tags;
    private Map<String, String> attributes;

    public BenchPayload() {
    }

    public BenchPayload(long id, String name, List<String> tags, Map<String, String> attributes) {
        this.id = id;
        this.name = name;
        this.tags = tags;
        this.attributes = attributes;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    /** 便于报告输出：把Map暴露成可读文本 */
    public String attributesText() {
        return attributes == null ? "{}" : new LinkedHashMap<>(attributes).toString();
    }

    @Override
    public String toString() {
        return "BenchPayload{id=" + id + ", name='" + name + "', tags=" + tags
                + ", attributes=" + attributes + '}';
    }
}
