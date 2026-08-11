package io.github.nitouge.conduit.api;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * 注册中心里一个服务实例的元信息。
 *
 * <p>它是{@code Registry} 注册/发现的基本单位，也是 {@code LoadBalancer} 选址的输入元素。</p>
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

    // equals/hashCode刻意只比较serviceName + host + port（节点身份），不含weight：
    // 同一节点改了权重仍应视作同一个实例，避免在去重/上下线对比时把它当成新节点。
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
                '}';
    }
}
