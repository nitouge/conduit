package io.github.nitouge.conduit.registry;

import io.github.nitouge.conduit.common.exception.RpcException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 核心注册中心模块只带 local；缺失适配模块时应给出明确引入提示。
 */
class RegistriesErrorMessageTest {

    @Test
    void missingNacosModuleGivesHint() {
        RpcException error = assertThrows(RpcException.class,
                () -> Registries.getRegistry("127.0.0.1:8848"));

        assertTrue(error.getMessage().contains("conduit-registry-nacos"), error.getMessage());
    }

    @Test
    void missingZookeeperModuleGivesHint() {
        RpcException error = assertThrows(RpcException.class,
                () -> Registries.getRegistry("zookeeper", "127.0.0.1:2181"));

        assertTrue(error.getMessage().contains("conduit-registry-zookeeper"), error.getMessage());
    }

    @Test
    void missingEtcdModuleGivesHint() {
        RpcException error = assertThrows(RpcException.class,
                () -> Registries.getRegistry("etcd", "127.0.0.1:2379"));

        assertTrue(error.getMessage().contains("conduit-registry-etcd"), error.getMessage());
    }

    @Test
    void localIsBuiltIn() {
        Registry registry = Registries.getRegistry("local", "registry-hint-test");
        try {
            assertNotNull(registry);
        } finally {
            registry.destroy();
        }
    }
}
