package io.github.nitouge.conduit.spring;

import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreakerRegistry;
import io.github.nitouge.conduit.common.config.ConduitConfig;
import io.github.nitouge.conduit.common.filter.FilterLoader;
import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.lang.NonNull;

/**
 * Conduit自动配置（Spring Boot Starter的核心）。
 *
 * <p>作为 {@code @AutoConfiguration} 通过 {@code META-INF/spring/...AutoConfiguration.imports} 自动激活——
 * 即依赖即装配：Spring Boot应用只要classpath上有本模块（或conduit-spring-boot-starter），
 * 无需 {@code @EnableConduit} 也会自动配置。可用 {@code conduit.enabled=false} 关闭。</p>
 *
 * <p>非Spring Boot的普通Spring应用（不读imports）仍可用 {@code @EnableConduit}（@Import本类）显式开启。
 * 所有Bean标 {@code @ConditionalOnMissingBean}：无论经哪条路径、是否重复导入，都只装配一份，且允许用户覆盖。</p>
 *
 * <p>本类同时把Spring {@link Environment} 登记为Conduit的配置源（见 {@link ConduitConfig}），
 * 于是框架开关（如 {@code conduit.filter.metrics.enabled}）既能用 JVM 参数 {@code -D...}，
 * 也能写进 {@code application.yml}、用 {@code --key=value} 程序参数或环境变量。</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(ConduitProperties.class)
@ConditionalOnProperty(prefix = "conduit", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ConduitAutoConfiguration implements EnvironmentAware {

    /**
     * Spring环境就绪时安装配置源，并丢弃此前可能已缓存的filter快照。
     *
     * <p>本类实例化先于任何 {@code @RpcReference}/{@code @RpcService} 触发的filter装载，
     * 因此正常启动路径下filter首次装载就能读到yml/程序参数里的开关；
     * {@link FilterLoader#reset()} 则兜住"更早用编程式API装载过"的场景。</p>
     */
    @Override
    public void setEnvironment(@NonNull Environment environment) {
        ConduitConfig.installSource(environment::getProperty);
        FilterLoader.reset();
    }

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
        // 熔断器是进程内节点级全局状态，启动时按conduit.cluster.breaker.*应用一次
        CircuitBreakerRegistry.configure(props.toClusterOptions().getCircuitBreaker());
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
                                                                          ObjectProvider<RpcClient> clientProvider,
                                                                          ObjectProvider<ConduitProperties> propertiesProvider) {
        // 必须static：BeanPostProcessor的@Bean工厂方法若非static，会强制本配置类（及其依赖
        // ConduitProperties/Registry/RpcClient）过早实例化、绕过部分后处理。配合ObjectProvider延迟取值，
        // 让Registry/RpcClient/ConduitProperties在正常阶段才创建。
        return new ReferenceAnnotationBeanPostProcessor(registryProvider, clientProvider, propertiesProvider);
    }
}
