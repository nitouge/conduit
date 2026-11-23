package io.github.nitouge.conduit.spring;

import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Conduit自动配置（Spring Boot Starter的核心）。
 *
 * <p>作为 {@code @AutoConfiguration} 通过 {@code META-INF/spring/...AutoConfiguration.imports} 自动激活——
 * 即依赖即装配：Spring Boot应用只要classpath上有本模块（或conduit-spring-boot-starter），
 * 无需 {@code @EnableConduit} 也会自动配置。可用 {@code conduit.enabled=false} 关闭。</p>
 *
 * <p>非Spring Boot的普通Spring应用（不读imports）仍可用 {@code @EnableConduit}（@Import本类）显式开启。
 * 所有Bean标 {@code @ConditionalOnMissingBean}：无论经哪条路径、是否重复导入，都只装配一份，且允许用户覆盖。</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(ConduitProperties.class)
@ConditionalOnProperty(prefix = "conduit", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ConduitAutoConfiguration {

    @Bean(destroyMethod = "destroy")
    @ConditionalOnMissingBean
    public Registry conduitRegistry(ConduitProperties props) {
        // destroyMethod=destroy：容器关闭时调用Registry.destroy()释放Nacos连接/线程，避免资源泄漏
        return Registries.getRegistry(props.getRegistry().getType(), props.getRegistry().getAddress());
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public RpcClient conduitRpcClient(ConduitProperties props) {
        // 传输配置（压缩/连接池/心跳/重连/TLS）统一由conduit.*构建，业务代码零感知
        return new RpcClient(props.toClientOptions());
    }

    @Bean
    @ConditionalOnMissingBean
    public ServiceExporter conduitServiceExporter(ConduitProperties props, Registry registry) {
        return new ServiceExporter(props, registry);
    }

    @Bean
    @ConditionalOnMissingBean
    static ReferenceAnnotationBeanPostProcessor conduitReferenceProcessor(ObjectProvider<Registry> registryProvider,
                                                                          ObjectProvider<RpcClient> clientProvider) {
        // 必须static：BeanPostProcessor的@Bean工厂方法若非static，会强制本配置类（及其依赖
        // ConduitProperties/Registry/RpcClient）过早实例化、绕过部分后处理。配合ObjectProvider延迟取值，
        // 让Registry/RpcClient在正常阶段才创建。
        return new ReferenceAnnotationBeanPostProcessor(registryProvider, clientProvider);
    }
}
