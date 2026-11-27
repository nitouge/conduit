package io.github.nitouge.conduit.api;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * 注册中心里一个服务实例的元信息。
 *
 * <p>它是{@code Registry} 注册/发现的基本单位，也是 {@code LoadBalancer} 选址的输入元素。</p>
 *
 * <p><b>为什么带 version/group/tag/zone：</b>不知道实例属于哪个版本、哪个分组、哪组标签，路由就
 * 无从谈起——负载均衡只能在「给定候选」里挑，而候选集合本身要先被这些属性收窄（灰度、多版本、
 * 同机房优先）。这些字段由提供侧在暴露服务时声明（{@code @RpcService}），随注册中心一起发布，
 * 消费侧在 {@code discover} 之后据此过滤。</p>
 */
public class ServiceMeta implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** 服务名 = 接口全名，必要时拼接 version/group */
    private String serviceName;

    /** 服务实例所在主机 */
    private String host;

    /** 服务实例端口 */
    private int port;

    /** 权重，默认 100，用于带权重负载均衡 */
    private int weight = 100;

    /** 服务版本（如 1.0.0 / 2.0.0）；{@code null} 表示提供方未声明版本 */
    private String version;

    /** 服务分组（如 pay / user）；{@code null} 表示未声明分组 */
    private String group;

    /** 实例标签（如 gray / canary），用于灰度与标签路由；{@code null} 表示未打标签 */
    private String tag;

    /** 实例所在机房/可用区（如 sh-1），用于同机房优先；{@code null} 表示机房未知 */
    private String zone;

    public ServiceMeta() {
    }

    public ServiceMeta(String serviceName, String host, int port) {
        this.serviceName = serviceName;
        this.host = host;
        this.port = port;
    }

    public ServiceMeta(String serviceName, String host, int port, int weight) {
        this.serviceName = serviceName;
        this.host = host;
        this.port = port;
        this.weight = weight;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    /** 路由属性（版本/分组/标签/机房）一次性写入，便于构造注册元信息时链式表达 */
    public ServiceMeta routeAttributes(String version, String group, String tag, String zone) {
        this.version = version;
        this.group = group;
        this.tag = tag;
        this.zone = zone;
        return this;
    }

    // equals/hashCode刻意只比较serviceName + host + port（节点身份），不含weight：
    // 同一节点改了权重仍应视作同一个实例，避免在去重/上下线对比时把它当成新节点。
    // 版本/分组/标签同理不入身份：同一个进程换了版本只是"同一节点的新属性"，
    // 若算进身份，注册中心里会残留一条同端口的旧记录，discover时选到已不存在的版本。
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ServiceMeta that) {
            return port == that.port
                    && Objects.equals(serviceName, that.serviceName)
                    && Objects.equals(host, that.host);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, host, port);
    }

    @Override
    public String toString() {
        return "ServiceMeta{" +
                "serviceName='" + serviceName + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", weight=" + weight +
                ", version='" + version + '\'' +
                ", group='" + group + '\'' +
                ", tag='" + tag + '\'' +
                ", zone='" + zone + '\'' +
                '}';
    }
}
