package io.github.nitouge.conduit.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Conduit配置项，绑定application.yml 中 {@code conduit.*}。
 *
 * <pre>
 * conduit:
 *   application:
 *     name: greeting-provider
 *   registry:
 *     type: nacos   # nacos/local / (后续zookeeper/etcd)
 *     address: 127.0.0.1:8848
 *   protocol:
 *     host: 127.0.0.1
 *     port: 20880
 *     serialization: jdk     # jdk/json/kryo/protostuff
 * </pre>
 */
@ConfigurationProperties(prefix = "conduit")
public class ConduitProperties {

    /** 是否启用Conduit自动装配（默认true，设false可关闭整个starter） */
    private boolean enabled = true;

    private final ApplicationConfig application = new ApplicationConfig();
    private final RegistryConfig registry = new RegistryConfig();
    private final ProtocolConfig protocol = new ProtocolConfig();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public ApplicationConfig getApplication() {
        return application;
    }

    public RegistryConfig getRegistry() {
        return registry;
    }

    public ProtocolConfig getProtocol() {
        return protocol;
    }

    public static class ApplicationConfig {
        private String name = "conduit-app";

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class RegistryConfig {
        private String type = "nacos";
        private String address = "127.0.0.1:8848";

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }
    }

    public static class ProtocolConfig {
        private String host = "127.0.0.1";
        private int port = 20880;
        private String serialization = "jdk";

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

        public String getSerialization() {
            return serialization;
        }

        public void setSerialization(String serialization) {
            this.serialization = serialization;
        }
    }
}
