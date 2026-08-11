package io.github.nitouge.conduit.api.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注在服务实现类上：声明它是一个需要对外暴露的RPC服务。
 *
 * <p>由conduit-spring在启动时扫描并自动暴露 + 注册到注册中心。被标注的类需同时是一个Spring Bean
 * （如加 {@code @Component} 或以 {@code @Bean} 声明）。本注解是纯标记，不依赖Spring。</p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RpcService {

    /** 服务版本（多版本共存用） */
    String version() default "";

    /** 服务分组 */
    String group() default "";

    /** 权重，用于带权负载均衡 */
    int weight() default 100;
}
