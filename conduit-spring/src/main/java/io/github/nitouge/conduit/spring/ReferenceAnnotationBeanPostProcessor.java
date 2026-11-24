package io.github.nitouge.conduit.spring;

import io.github.nitouge.conduit.api.annotation.RpcReference;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.cluster.ClusterOptions;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.registry.Registry;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.lang.NonNull;

import java.lang.reflect.Field;

/**
 * 处理 {@code @RpcReference} 字段：为其创建一个走注册中心发现 + 集群容错 + 负载均衡的动态代理并注入。
 *
 * <p>作为 {@link BeanPostProcessor} 必须早于普通Bean创建，故对 {@link Registry} / {@link RpcClient}
 * 采用 {@link ObjectProvider} 延迟获取——仅在首次处理到 {@code @RpcReference} 字段时才解析，
 * 避免把它们过早进入BPP注册阶段（否则它们会绕过部分BeanPostProcessor，Spring会发WARN）。</p>
 */
public class ReferenceAnnotationBeanPostProcessor implements BeanPostProcessor {

    private final ObjectProvider<Registry> registryProvider;
    private final ObjectProvider<RpcClient> clientProvider;
    private final ObjectProvider<ConduitProperties> propertiesProvider;

    // 延迟解析后缓存（Bean后处理发生在容器刷新的单线程阶段，volatile足以保证可见性）
    private volatile Registry registry;
    private volatile RpcClient client;
    private volatile ConduitProperties properties;

    public ReferenceAnnotationBeanPostProcessor(ObjectProvider<Registry> registryProvider,
                                                ObjectProvider<RpcClient> clientProvider,
                                                ObjectProvider<ConduitProperties> propertiesProvider) {
        this.registryProvider = registryProvider;
        this.clientProvider = clientProvider;
        this.propertiesProvider = propertiesProvider;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, @NonNull String beanName) throws BeansException {
        // 沿继承链向上遍历，确保父类里的@RpcReference字段也被注入
        for (Class<?> clazz = bean.getClass(); clazz != null && clazz != Object.class; clazz = clazz.getSuperclass()) {
            for (Field field : clazz.getDeclaredFields()) {
                RpcReference ref = field.getAnnotation(RpcReference.class);
                if (ref == null) {
                    continue;
                }
                if (!field.getType().isInterface()) {
                    throw new RpcException("@RpcReference必须标注在接口类型的字段上: "
                            + clazz.getName() + "#" + field.getName()
                            + "的类型" + field.getType().getName() + "不是接口");
                }
                Object proxy = RpcClientProxy.create(
                        field.getType(),
                        registry(),
                        client(),
                        ref.timeout(),
                        ref.cluster().getType(),
                        ref.loadbalance().getType(),
                        clusterOptions(ref)
                );
                field.setAccessible(true);
                try {
                    field.set(bean, proxy);
                } catch (IllegalAccessException e) {
                    throw new RpcException("注入@RpcReference失败: " + clazz.getName() + "#" + field.getName(), e);
                }
            }
        }
        return bean;
    }

    /**
     * 首次用到时才从容器取Registry（getObject返回同一单例，竞态下重复赋值无害）
     */
    private Registry registry() {
        Registry r = this.registry;
        if (r == null) {
            r = this.registry = registryProvider.getObject();
        }
        return r;
    }

    /**
     * 首次用到时才从容器取RpcClient
     */
    private RpcClient client() {
        RpcClient c = this.client;
        if (c == null) {
            c = this.client = clientProvider.getObject();
        }
        return c;
    }

    /**
     * 字段级重试配置覆盖全局配置（注解-1表示沿用全局）
     */
    private ClusterOptions clusterOptions(RpcReference ref) {
        ClusterOptions global = properties().toClusterOptions();
        return global.toBuilder()
                .retries(ref.retries() >= 0 ? ref.retries() : global.getRetries())
                .retryBackoffMs(ref.retryBackoffMs() >= 0
                        ? ref.retryBackoffMs() : global.getRetryBackoffMs())
                .build();
    }

    /**
     * 首次用到时才从容器取ConduitProperties
     */
    private ConduitProperties properties() {
        ConduitProperties p = this.properties;
        if (p == null) {
            p = this.properties = propertiesProvider.getObject();
        }
        return p;
    }
}
