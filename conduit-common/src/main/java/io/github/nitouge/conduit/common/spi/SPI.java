package io.github.nitouge.conduit.common.spi;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 扩展点标记注解：被它标注的接口是一个可插拔的SPI扩展点。
 *
 * <p>{@link #value()} 是默认扩展名（如Serializer默认"jdk"、Registry默认"nacos"）。</p>
 *
 * <p>注意：M1仅定义该标记注解，按名加载扩展的 {@link ExtensionLoader} 在M3实现。
 * 在此之前，框架内部用各模块的简单工厂（如SerializerFactory）按名/按id取实现，
 * M3接入ExtensionLoader后这些工厂改为委托给它，接口与调用方均不变。</p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SPI {

    /** 默认扩展名 */
    String value() default "";
}
