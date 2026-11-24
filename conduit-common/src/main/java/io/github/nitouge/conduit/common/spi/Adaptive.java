package io.github.nitouge.conduit.common.spi;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 自适应扩展选择：标注在SPI接口方法上，声明"本次调用用哪个扩展实现"。
 *
 * <p>{@link ExtensionLoader#getAdaptiveExtension()} 返回一个动态代理：每次调用该方法时，按
 * {@link #value()} 给出的key顺序，从方法参数中取值，再按值去 {@code getExtension(name)} 取实现。
 * 取值来源：</p>
 * <ul>
 *   <li>{@code java.util.Map} 参数：{@code map.get(key)}；</li>
 *   <li>{@code RpcRequest} 参数：{@code request.getAttachment(key)}（配合 {@code RpcContext} 做每次调用覆盖）。</li>
 * </ul>
 *
 * <p>未取到值（或值为 {@code default}）时回退到：代理创建时传入的默认扩展名 → {@code @SPI} 注解的默认扩展名。
 * 取到的名字不存在会直接抛 {@code RpcException}，不做静默降级——配置错误应尽快暴露。</p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Adaptive {

    /** 参数中查找的key，按顺序尝试；为空表示总是使用默认扩展 */
    String[] value() default {};
}
