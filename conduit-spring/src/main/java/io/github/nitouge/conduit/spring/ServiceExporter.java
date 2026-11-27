package io.github.nitouge.conduit.spring;

import io.github.nitouge.conduit.api.annotation.RpcService;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.server.RpcServer;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.lang.NonNull;
import org.springframework.util.ClassUtils;

import java.util.Map;

/**
 * 服务暴露器：所有单例创建完成后，扫描带 {@code @RpcService} 的Bean，按其实现的接口逐一暴露 +
 * 注册到注册中心，并启动Netty服务端；容器关闭时优雅停机。
 */
public class ServiceExporter implements SmartInitializingSingleton, DisposableBean, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(ServiceExporter.class);

    private final ConduitProperties props;
    private final Registry registry;
    private ApplicationContext applicationContext;
    private RpcServer server;

    public ServiceExporter(ConduitProperties props, Registry registry) {
        this.props = props;
        this.registry = registry;
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Map<String, Object> services = applicationContext.getBeansWithAnnotation(RpcService.class);
        if (MapUtils.isEmpty(services)) {
            return; // 纯消费者应用：无需启动服务端
        }
        server = new RpcServer(props.getProtocol().getHost(), props.getProtocol().getPort(), registry,
                props.toServerOptions());
        for (Object bean : services.values()) {
            int exported = 0;
            // 用ClassUtils.getUserClass还原CGLIB代理（如被@Transactional/@Async增强）的原始类，
            // 否则代理子类的getInterfaces()不含业务接口，会导致@RpcService漏暴露。
            Class<?> userClass = ClassUtils.getUserClass(bean);
            // 版本/分组/标签/机房随实例注册出去，消费侧才可能按它们路由
            RpcService service = userClass.getAnnotation(RpcService.class);
            for (Class<?> iface : userClass.getInterfaces()) {
                if (isFrameworkInterface(iface)) {
                    continue; // 跳过Serializable/Comparable、Spring代理接口等非业务接口
                }
                server.export(iface, bean, service);
                exported++;
            }
            if (exported == 0) {
                log.warn("@RpcService {}未实现任何业务接口，跳过暴露", userClass.getName());
            }
        }
        server.start();
        log.info("Conduit服务端已启动并暴露{}个服务: {}:{}", services.size(), props.getProtocol().getHost(), props.getProtocol().getPort());
    }

    @Override
    public void destroy() {
        if (server != null) {
            server.close(); // 先从注册中心注销，再关闭Netty
        }
    }

    /** 是否为非业务接口（标记接口/JDK/Spring代理接口），暴露时应跳过 */
    private static boolean isFrameworkInterface(Class<?> iface) {
        String pkg = iface.getPackageName();
        return pkg.startsWith("java.") || pkg.startsWith("javax.")
                || pkg.startsWith("jakarta.") || pkg.startsWith("org.springframework.");
    }
}

