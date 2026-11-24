package io.github.nitouge.conduit.common.spi;

/**
 * SPI扩展组件的生命周期回调。
 *
 * <p>{@link ExtensionLoader} 保证：</p>
 * <ul>
 *   <li>{@link #init()} 在扩展实例创建后、对外返回前调用<b>一次</b>；</li>
 *   <li>{@link #destroy()} 在 {@link ExtensionLoader#destroyExtension(String)} /
 *       {@link ExtensionLoader#destroyAll()} / JVM关闭钩子中调用<b>一次</b>。</li>
 * </ul>
 *
 * <p>扩展实现按需覆盖（默认空实现）。init抛异常会导致扩展创建失败；destroy抛异常只记日志、
 * 不影响其它组件释放。注意：init内不要再回调同一ExtensionLoader的按名加载，避免重复初始化。</p>
 */
public interface Lifecycle {

    /** 创建后初始化资源（连接、线程池、本地缓存等） */
    default void init() {
    }

    /** 关闭时释放资源；实现应保证幂等，不要抛出受检异常 */
    default void destroy() {
    }
}
