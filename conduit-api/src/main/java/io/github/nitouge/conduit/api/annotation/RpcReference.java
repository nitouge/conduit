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

    /**
     * 默认路由标签（灰度/压测等）。
     *
     * <p>只是默认值：单次调用可用 {@code RpcContext.getContext().setAttachment("conduit.tag", "gray")}
     * 临时覆盖，用于灰度切流。</p>
     */
    String tag() default "";

    /** 调用超时（毫秒） */
    long timeout() default 3000;

    /** 集群容错策略：failover/failfast/failsafe/failback */
    ClusterPolicyEnum cluster() default ClusterPolicyEnum.FAIL_OVER;

    /** 负载均衡策略：random/roundrobin/consistenthash */
    LoadbalancePolicyEnum loadbalance() default LoadbalancePolicyEnum.RANDOM;

    /**
     * 额外重试次数（不含首次调用，仅failover生效）。
     *
     * <p>{@code -1} 表示使用全局配置 {@code conduit.cluster.retries}（默认2）。</p>
     */
    int retries() default -1;

    /**
     * 重试退避时长（毫秒，仅failover生效）。
     *
     * <p>{@code -1} 表示使用全局配置 {@code conduit.cluster.retry-backoff-ms}（默认0，立即重试）。</p>
     */
    long retryBackoffMs() default -1;

}
