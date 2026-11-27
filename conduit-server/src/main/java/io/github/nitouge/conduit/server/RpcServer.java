package io.github.nitouge.conduit.server;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.api.annotation.RpcService;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.remoting.ServerOptions;
import io.github.nitouge.conduit.remoting.netty.NettyServer;
import io.github.nitouge.conduit.server.support.RpcRequestHandler;
import io.github.nitouge.conduit.server.support.ServiceCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 服务端入口：把实现对象放进服务缓存、（M2起）注册到注册中心，并启动Netty监听。
 *
 * <p>两种模式：</p>
 * <ul>
 *   <li><b>直连（M1）</b>：{@code new RpcServer(host, port)}，registry为null，export只进缓存。</li>
 *   <li><b>注册中心（M2）</b>：{@code new RpcServer(host, port, registry)}，export时把接口包装成
 *   {@link ServiceMeta} 注册；优雅停机时先从注册中心注销，再关闭Netty。</li>
 * </ul>
 */
public class RpcServer {

    private static final Logger log = LoggerFactory.getLogger(RpcServer.class);

    private final ServiceCache serviceCache = new ServiceCache();
    private final NettyServer nettyServer;
    private final String host;
    private final int port;
    private final Registry registry; // 可为null（直连模式）
    private final List<ServiceMeta> exported = new CopyOnWriteArrayList<>();
    private volatile boolean started = false;

    /** 直连模式（M1）：不接入注册中心 */
    public RpcServer(String host, int port) {
        this(host, port, null, ServerOptions.defaults());
    }

    /** 直连模式 + 自定义传输配置（压缩、TLS、读写空闲等） */
    public RpcServer(String host, int port, ServerOptions options) {
        this(host, port, null, options);
    }

    /** 注册中心模式（M2）：export时注册到registry */
    public RpcServer(String host, int port, Registry registry) {
        this(host, port, registry, ServerOptions.defaults());
    }

    /** 注册中心模式 + 自定义传输配置 */
    public RpcServer(String host, int port, Registry registry, ServerOptions options) {
        this.host = host;
        this.port = port;
        this.registry = registry;
        // 请求处理器注入传输层，业务与传输解耦
        this.nettyServer = new NettyServer(host, port, new RpcRequestHandler(serviceCache), options);
    }

    /**
     * 暴露一个服务：进缓存；若配置了注册中心，则登记待注册的 {@link ServiceMeta}。
     *
     * <p>关键次序——<b>先bind端口、后注册到注册中心</b>，保证可被发现即可被连接，
     * 消除注册早于监听造成的Connection refused窗口：若尚未 {@link #start()}，注册推迟到start之后统一进行；
     * 若已start（端口已就绪），则立即注册。</p>
     */
    public void export(Class<?> interfaceClass, Object impl) {
        export(interfaceClass, impl, null, null, null, null, 100);
    }

    /**
     * 暴露一个服务，并把 {@code @RpcService} 声明的路由属性（version/group/tag/zone/weight）
     * 一并写进注册信息。
     *
     * <p>版本/分组/标签/机房是「消费侧怎么选中我」的声明，必须随实例一起发布；只放进注册缓存而不
     * 发布，消费侧发现到的就是一堆无法区分版本的节点，灰度/多版本就无从谈起。</p>
     */
    public void export(Class<?> interfaceClass, Object impl, RpcService service) {
        if (service == null) {
            export(interfaceClass, impl);
            return;
        }
        export(interfaceClass, impl, service.version(), service.group(),
                service.tag(), service.zone(), service.weight());
    }

    /**
     * 以编程方式暴露服务并声明路由属性（非Spring场景，或测试里直接构造多版本/多标签集群）。
     *
     * @param version 服务版本；空表示未声明（消费侧指定版本时不会命中）
     * @param group   服务分组；空表示未声明
     * @param tag     实例标签（gray/canary）；空表示未打标签
     * @param zone    实例机房/可用区；空表示未知
     * @param weight  负载均衡权重
     */
    public void export(Class<?> interfaceClass, Object impl, String version, String group,
                       String tag, String zone, int weight) {
        String serviceName = interfaceClass.getName(); // 必要时拼接version/group
        serviceCache.register(serviceName, impl);

        ServiceMeta meta = new ServiceMeta(serviceName, host, port, weight <= 0 ? 100 : weight);
        meta.routeAttributes(blankToNull(version), blankToNull(group), blankToNull(tag), blankToNull(zone));

        if (registry == null) {
            log.info("已暴露服务(直连): {} -> {} {}", serviceName, impl.getClass().getName(), routeText(meta));
            return;
        }
        exported.add(meta);
        if (started) {
            registry.register(meta); // 端口已就绪，立即注册
            log.info("已暴露并注册服务: {} -> {}:{} {}", serviceName, host, port, routeText(meta));
        } else {
            log.info("已暴露服务(待端口就绪后注册): {} -> {}:{} {}", serviceName, host, port, routeText(meta));
        }
    }

    /** 注册信息里的路由属性，仅用于日志（未声明时不输出噪音） */
    private static String routeText(ServiceMeta meta) {
        StringBuilder sb = new StringBuilder();
        appendIfPresent(sb, "version", meta.getVersion());
        appendIfPresent(sb, "group", meta.getGroup());
        appendIfPresent(sb, "tag", meta.getTag());
        appendIfPresent(sb, "zone", meta.getZone());
        return sb.isEmpty() ? "" : sb.toString();
    }

    private static void appendIfPresent(StringBuilder sb, String name, String value) {
        if (value != null && !value.isBlank()) {
            sb.append(name).append('=').append(value).append(' ');
        }
    }

    private static String blankToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    /** 启动Netty并在端口bind完成后再把已暴露服务注册到注册中心 */
    public void start() {
        nettyServer.start();   // 先 bind 端口
        started = true;
        if (registry != null && !exported.isEmpty()) {
            for (ServiceMeta meta : exported) {
                registry.register(meta); // 端口就绪后再注册：可被发现即可被连接
            }
            log.info("Conduit服务端已就绪，注册{}个服务到注册中心", exported.size());
        }
    }

    /** 优雅停机：先从注册中心注销（不再被发现），再关闭Netty */
    public void close() {
        if (registry != null) {
            for (ServiceMeta meta : exported) {
                try {
                    registry.unregister(meta);
                } catch (Exception e) {
                    log.warn("注销服务失败: {}", meta, e);
                }
            }
            exported.clear();
        }
        nettyServer.close();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
