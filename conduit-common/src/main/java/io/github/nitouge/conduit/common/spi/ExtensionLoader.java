package io.github.nitouge.conduit.common.spi;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 扩展加载器：按名字加载 {@code @SPI} 接口的实现，缓存单例、线程安全、按需实例化。
 *
 * <p>配置约定：{@code META-INF/rpc/<接口全限定名>}，每行 {@code name=实现类全限定名}（# 开头为注释）。
 * 这是可替换的统一落地机制——上层只 {@link #getExtension(String)} / {@link #getDefaultExtension()}，
 * 绝不 {@code new} 具体实现。</p>
 *
 * <p>除按名加载外，还提供两项能力：</p>
 * <ul>
 *   <li><b>生命周期</b>：扩展实现若同时实现 {@link Lifecycle}，实例创建后自动 {@code init()}，
 *       销毁时调用 {@code destroy()}（{@link #destroyExtension(String)} / {@link #destroyAll()} /
 *       静态 {@link #shutdownAll()} 与JVM关闭钩子）。</li>
 *   <li><b>自适应加载</b>：{@link #getAdaptiveExtension()} 返回动态代理，按方法上的 {@link Adaptive}
 *       注解从调用参数（{@code Map} / {@code RpcRequest.attachments}）里取扩展名，实时选择实现；
 *       未取到则回退到传入的默认名或 {@code @SPI} 默认值。</li>
 * </ul>
 */
public class ExtensionLoader<T> {

    private static final Logger log = LoggerFactory.getLogger(ExtensionLoader.class);

    private static final String DIRECTORY = "META-INF/rpc/";

    /** 每个@SPI接口一个ExtensionLoader */
    private static final Map<Class<?>, ExtensionLoader<?>> LOADERS = new ConcurrentHashMap<>();

    /** JVM关闭钩子只注册一次 */
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);

    private final Class<T> type;
    private final String defaultName;

    /** name -> 实现类（懒加载配置文件后填充） */
    private final Map<String, Class<?>> nameToClass = new ConcurrentHashMap<>();
    /** name -> 单例实例 */
    private final Map<String, Object> instances = new ConcurrentHashMap<>();
    /** 默认扩展名（"" 表示用@SPI的默认值） -> 自适应代理 */
    private final Map<String, T> adaptiveExtensions = new ConcurrentHashMap<>();

    private volatile boolean configLoaded = false;

    private ExtensionLoader(Class<T> type) {
        this.type = type;
        SPI spi = type.getAnnotation(SPI.class);
        this.defaultName = (spi != null) ? spi.value() : "";
    }

    @SuppressWarnings("unchecked")
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        if (type == null) {
            throw new IllegalArgumentException("扩展类型不能为null");
        }
        if (!type.isInterface()) {
            throw new IllegalArgumentException(type + "不是接口");
        }
        if (type.getAnnotation(SPI.class) == null) {
            throw new IllegalArgumentException(type + "没有@SPI注解");
        }
        registerShutdownHook();
        return (ExtensionLoader<T>) LOADERS.computeIfAbsent(type, ExtensionLoader::new);
    }

    /** 取默认扩展（由 @SPI 的 value 指定） */
    public T getDefaultExtension() {
        if (StringUtils.isBlank(defaultName)) {
            throw new IllegalStateException(type + "未配置默认扩展（@SPI value为空）");
        }
        return getExtension(defaultName);
    }

    /** 按名字取扩展（单例，按需创建；实现 {@link Lifecycle} 时创建后自动init） */
    @SuppressWarnings("unchecked")
    public T getExtension(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("扩展名不能为空");
        }
        Object instance = instances.get(name);
        if (instance == null) {
            synchronized (this) {
                instance = instances.get(name);
                if (instance == null) {
                    instance = createExtension(name);
                    instances.put(name, instance);
                }
            }
        }
        return (T) instance;
    }

    /**
     * 取"自适应扩展"：返回一个动态代理，按被调方法上的 {@link Adaptive} 注解在每次调用时选择实现。
     *
     * <p>等价于 {@code getAdaptiveExtension("")}：未从参数取到扩展名时回退到 {@code @SPI} 默认扩展。</p>
     */
    public T getAdaptiveExtension() {
        return getAdaptiveExtension("");
    }

    /**
     * 取"自适应扩展"，并指定取不到动态值时的默认扩展名。
     *
     * @param preferredName 默认扩展名（可为空；空则用 {@code @SPI} 默认值）
     */
    public T getAdaptiveExtension(String preferredName) {
        String cacheKey = preferredName == null ? "" : preferredName;
        return adaptiveExtensions.computeIfAbsent(cacheKey, this::createAdaptiveExtension);
    }

    /** 是否存在某个扩展名 */
    public boolean hasExtension(String name) {
        loadConfigIfNeeded();
        return nameToClass.containsKey(name);
    }

    /**
     * 取本接口配置的<b>全部</b>扩展实例（单例、按需创建）。
     *
     * <p>用于多实例扩展点——如 {@code Filter} 链需要加载所有已注册实现而非选一个默认。
     * 每个实例仍复用 {@link #getExtension(String)} 的单例缓存；返回顺序不保证，
     * 需要有序请由调用方按自身规则（如 {@code order()}）排序。</p>
     */
    public List<T> getExtensions() {
        loadConfigIfNeeded();
        List<T> list = new ArrayList<>(nameToClass.size());
        for (String name : nameToClass.keySet()) {
            list.add(getExtension(name));
        }
        return list;
    }

    /**
     * 取本接口配置的<b>全部</b>扩展，携带SPI注册名。
     *
     * <p>与 {@link #getExtensions()} 的区别：返回 {@code Map<SPI名, 实例>}，供需要按名查开关
     * 之类的场景使用——典型如 {@link io.github.nitouge.conduit.common.filter.FilterLoader}，要根据
     * {@code conduit.filter.<spiName>.enabled} 决定是否装载。</p>
     */
    public Map<String, T> getExtensionInfos() {
        loadConfigIfNeeded();
        LinkedHashMap<String, T> map = new LinkedHashMap<>(nameToClass.size());
        for (String name : nameToClass.keySet()) {
            map.put(name, getExtension(name));
        }
        return map;
    }

    /**
     * 销毁单个扩展实例（调用其 {@link Lifecycle#destroy()} 并从缓存移除）。
     *
     * <p>移除后再次 {@code getExtension(name)} 会重新创建并init。destroy异常只记日志，
     * 不影响其它扩展。</p>
     */
    public void destroyExtension(String name) {
        if (StringUtils.isBlank(name)) {
            return;
        }
        Object instance;
        synchronized (this) {
            instance = instances.remove(name);
        }
        if (instance instanceof Lifecycle lifecycle) {
            try {
                lifecycle.destroy();
                log.debug("[SPI] 销毁扩展 {}/{}", type.getSimpleName(), name);
            } catch (Throwable t) {
                log.warn("[SPI] 销毁扩展 {}/{} 失败: {}", type.getSimpleName(), name, t.toString(), t);
            }
        }
    }

    /** 销毁本扩展点的全部实例（幂等） */
    public void destroyAll() {
        for (String name : new ArrayList<>(instances.keySet())) {
            destroyExtension(name);
        }
    }

    /** 销毁所有扩展点的全部实例；由JVM关闭钩子调用，也可在容器关闭时显式调用（幂等） */
    public static void shutdownAll() {
        for (ExtensionLoader<?> loader : LOADERS.values()) {
            try {
                loader.destroyAll();
            } catch (Throwable t) {
                log.warn("[SPI] 销毁扩展点 {} 失败: {}", loader.type.getName(), t.toString(), t);
            }
        }
    }

    private T createExtension(String name) {
        loadConfigIfNeeded();
        Class<?> clazz = nameToClass.get(name);
        if (clazz == null) {
            throw new RpcException("找不到扩展" + type.getName() + "的实现：name=" + name
                    + "（检查" + DIRECTORY + type.getName() + "配置）");
        }
        try {
            @SuppressWarnings("unchecked")
            T instance = (T) clazz.getDeclaredConstructor().newInstance();
            if (instance instanceof Lifecycle lifecycle) {
                // 先放入缓存再init：init 内若间接回调本loader可取到同一实例，避免递归创建
                instances.putIfAbsent(name, instance);
                try {
                    lifecycle.init();
                    log.debug("[SPI] 初始化扩展 {}/{}", type.getSimpleName(), name);
                } catch (Throwable t) {
                    instances.remove(name, instance);
                    throw new RpcException("扩展初始化失败：" + clazz.getName(), t);
                }
            }
            return instance;
        } catch (RpcException e) {
            throw e;
        } catch (Exception e) {
            throw new RpcException("创建扩展实例失败：" + clazz.getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private T createAdaptiveExtension(String preferredName) {
        log.debug("[SPI] 创建自适应扩展代理 {}{}", type.getSimpleName(),
                StringUtils.isBlank(preferredName) ? "" : "（默认 " + preferredName + "）");
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                new AdaptiveInvocationHandler(preferredName));
    }

    /** 自适应代理：每次调用按参数选扩展，再委托给选中实例 */
    private final class AdaptiveInvocationHandler implements InvocationHandler {

        private final String preferredName;

        private AdaptiveInvocationHandler(String preferredName) {
            this.preferredName = preferredName;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == Object.class) {
                return invokeObjectMethod(proxy, method, args);
            }
            String extensionName = resolveExtensionName(method, args);
            T extension = getExtension(extensionName);
            try {
                return method.invoke(extension, args);
            } catch (InvocationTargetException e) {
                throw (e.getCause() != null) ? e.getCause() : e;
            }
        }

        private Object invokeObjectMethod(Object proxy, Method method, Object[] args) {
            return switch (method.getName()) {
                case "toString" -> "Adaptive" + type.getSimpleName() + "@"
                        + Integer.toHexString(System.identityHashCode(proxy));
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == (args != null && args.length == 1 ? args[0] : null);
                default -> throw new UnsupportedOperationException("不支持的Object方法: " + method.getName());
            };
        }

        private String resolveExtensionName(Method method, Object[] args) {
            Adaptive adaptive = method.getAnnotation(Adaptive.class);
            if (adaptive != null && args != null && args.length > 0) {
                for (String key : adaptive.value()) {
                    if (StringUtils.isBlank(key)) {
                        continue;
                    }
                    String value = extractAdaptiveValue(args, key);
                    if (StringUtils.isNotBlank(value)) {
                        String trimmed = value.trim();
                        if (!"default".equalsIgnoreCase(trimmed)) {
                            return trimmed;
                        }
                        break;
                    }
                }
            }
            if (StringUtils.isNotBlank(preferredName)) {
                return preferredName;
            }
            if (StringUtils.isBlank(defaultName)) {
                throw new IllegalStateException(type + "未配置默认扩展，且本次调用未提供自适应扩展名");
            }
            return defaultName;
        }

        /** 从参数中按key取值：支持 Map 与 RpcRequest.attachments */
        private String extractAdaptiveValue(Object[] args, String key) {
            for (Object arg : args) {
                if (arg instanceof Map<?, ?> map) {
                    Object value = map.get(key);
                    if (value != null) {
                        return String.valueOf(value);
                    }
                } else if (arg instanceof RpcRequest request) {
                    String value = request.getAttachment(key);
                    if (value != null) {
                        return value;
                    }
                }
            }
            return null;
        }
    }

    /**
     * 懒加载并解析本接口的SPI配置文件（{@code META-INF/rpc/<接口名>}），只执行一次。
     *
     * <p>双重检查锁（DCL）：{@code configLoaded} 用 {@code volatile} 保证可见性与happens-before——
     * 退出synchronized块前对 {@code nameToClass} 的写入，对随后读到 {@code configLoaded==true} 的线程一定可见，
     * 因此快路径无锁读 {@code nameToClass} 是安全的。{@code getResources} 会合并所有jar里的同名配置文件。</p>
     */
    private void loadConfigIfNeeded() {
        if (configLoaded) {
            return;
        }
        synchronized (this) {
            if (configLoaded) {
                return;
            }
            String fileName = DIRECTORY + type.getName();
            ClassLoader cl = ExtensionLoader.class.getClassLoader();
            try {
                Enumeration<URL> urls = cl.getResources(fileName);
                while (urls.hasMoreElements()) {
                    parseConfig(urls.nextElement(), cl);
                }
            } catch (Exception e) {
                throw new RpcException("加载扩展配置失败：" + fileName, e);
            }
            configLoaded = true;
        }
    }

    private void parseConfig(URL url, ClassLoader cl) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                int eq = line.indexOf('=');
                if (eq <= 0) {
                    continue;
                }
                String name = line.substring(0, eq).trim();
                String className = line.substring(eq + 1).trim();
                if (name.isEmpty() || className.isEmpty()) {
                    continue;
                }
                Class<?> clazz = Class.forName(className, false, cl);
                if (!type.isAssignableFrom(clazz)) {
                    throw new RpcException(className + " 不是 " + type.getName() + " 的实现");
                }
                nameToClass.put(name, clazz);
            }
        } catch (RpcException e) {
            throw e;
        } catch (Exception e) {
            throw new RpcException("解析扩展配置失败：" + url, e);
        }
    }

    private static void registerShutdownHook() {
        if (!SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            return;
        }
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(ExtensionLoader::shutdownAll, "conduit-spi-shutdown"));
        } catch (Throwable t) {
            // 安全管理器/受限环境下允许不注册；显式调用 shutdownAll 仍生效
            log.debug("[SPI] 注册SPI关闭钩子失败: {}", t.toString());
        }
    }
}
