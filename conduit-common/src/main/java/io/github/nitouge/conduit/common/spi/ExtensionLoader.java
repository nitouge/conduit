package io.github.nitouge.conduit.common.spi;

import io.github.nitouge.conduit.common.exception.RpcException;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 扩展加载器：按名字加载 {@code @SPI} 接口的实现，缓存单例、线程安全、按需实例化。
 *
 * <p>配置约定：{@code META-INF/rpc/<接口全限定名>}，每行 {@code name=实现类全限定名}（# 开头为注释）。
 * 这是可替换的统一落地机制——上层只 {@link #getExtension(String)} / {@link #getDefaultExtension()}，
 * 绝不 {@code new} 具体实现。</p>
 *
 * <p>刻意保持简单：只做命名扩展 + 缓存。Dubbo的 {@code @Adaptive} 代码生成 / {@code @Activate} / IOC
 * 注入不在此实现（列为可选后续）。</p>
 */
public class ExtensionLoader<T> {

    private static final String DIRECTORY = "META-INF/rpc/";

    /** 每个@SPI接口一个ExtensionLoader */
    private static final Map<Class<?>, ExtensionLoader<?>> LOADERS = new ConcurrentHashMap<>();

    private final Class<T> type;
    private final String defaultName;

    /** name -> 实现类（懒加载配置文件后填充） */
    private final Map<String, Class<?>> nameToClass = new ConcurrentHashMap<>();
    /** name -> 单例实例 */
    private final Map<String, Object> instances = new ConcurrentHashMap<>();
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
        return (ExtensionLoader<T>) LOADERS.computeIfAbsent(type, ExtensionLoader::new);
    }

    /** 取默认扩展（由 @SPI 的 value 指定） */
    public T getDefaultExtension() {
        if (StringUtils.isBlank(defaultName)) {
            throw new IllegalStateException(type + "未配置默认扩展（@SPI value为空）");
        }
        return getExtension(defaultName);
    }

    /** 按名字取扩展（单例，按需创建） */
    @SuppressWarnings("unchecked")
    public T getExtension(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("扩展名不能为空");
        }
        Object instance = instances.get(name);
        if (instance == null) {
            instance = instances.computeIfAbsent(name, this::createExtension);
        }
        return (T) instance;
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
    public java.util.Map<String, T> getExtensionInfos() {
        loadConfigIfNeeded();
        LinkedHashMap<String, T> map = new LinkedHashMap<>(nameToClass.size());
        for (String name : nameToClass.keySet()) {
            map.put(name, getExtension(name));
        }
        return map;
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
            return instance;
        } catch (Exception e) {
            throw new RpcException("创建扩展实例失败：" + clazz.getName(), e);
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
}
