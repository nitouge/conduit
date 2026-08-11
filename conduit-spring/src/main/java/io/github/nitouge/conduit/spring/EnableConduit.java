package io.github.nitouge.conduit.spring;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 开启Conduit Spring整合：导入自动配置，启用 {@code @RpcService} 自动暴露与 {@code @RpcReference} 自动注入。
 *
 * <p>标在Spring配置类或启动类上即可。</p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Import(ConduitAutoConfiguration.class)
public @interface EnableConduit {
}
