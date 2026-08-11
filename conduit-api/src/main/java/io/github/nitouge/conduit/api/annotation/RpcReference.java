package io.github.nitouge.conduit.api.annotation;

import io.github.nitouge.conduit.api.enums.ClusterPolicyEnum;
import io.github.nitouge.conduit.api.enums.LoadbalancePolicyEnum;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注在消费者字段上：声明需要注入一个远程服务的动态代理。
 *
 * <p>由conduit-spring的BeanPostProcessor自动创建代理并注入。本注解是纯标记，不依赖Spring。</p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface RpcReference {

    /** 服务版本 */
    String version() default "";

    /** 服务分组 */
    String group() default "";

    /** 调用超时（毫秒） */
    long timeout() default 3000;

    /** 集群容错策略：failover/failfast/failsafe/failback */
    ClusterPolicyEnum cluster() default ClusterPolicyEnum.FAIL_OVER;

    /** 负载均衡策略：random/roundrobin/consistenthash */
    LoadbalancePolicyEnum loadbalance() default LoadbalancePolicyEnum.RANDOM;

}
