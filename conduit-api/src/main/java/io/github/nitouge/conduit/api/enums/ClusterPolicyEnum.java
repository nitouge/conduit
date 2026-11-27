package io.github.nitouge.conduit.api.enums;

/**
 * 集群容错策略：一次远程调用失败之后怎么办。
 *
 * <p>回答的是失败了换节点重试，还是直接认输这个问题。由 {@code @RpcReference#cluster()} 或
 * 全局配置 {@code conduit.cluster.strategy} 指定，也可在调用前用 {@code RpcContext} 的
 * {@code cluster} attachment 做单次覆盖。</p>
 *
 * <p><b>与SPI的关系：</b>枚举的 {@code type} 就是 {@code Cluster} 的SPI扩展名，与
 * {@code conduit-cluster} 中 {@code META-INF/rpc} 配置的key、以及配置文件里写的字符串
 * （如 {@code failover}）逐一对应——改这里的 {@code type} 等于改SPI查找用的名字。</p>
 *
 * <p><b>选型口诀：</b>幂等读用 {@link #FAIL_OVER}，非幂等写用 {@link #FAIL_FAST}，
 * 可丢的旁路调用用 {@link #FAIL_SAFE}，最终一致的通知类用 {@link #FAIL_BACK}。</p>
 */
public enum ClusterPolicyEnum {

    /**
     * 失败自动切换（默认策略）：换一个未试过且未熔断的候选节点再试。
     *
     * <p>最多尝试 {@code retries + 1} 次（默认 {@code retries=2}，即最多打3个不同节点）：
     * 每轮从剩余候选里重选，避免重复打同一坏节点；熔断打开的节点<b>零成本跳过</b>，
     * 既不消耗重试预算也不退避；真实失败后按 {@code retry-backoff-ms} 异步退避，
     * 重试本身是异步递归，不占用线程等待。全部失败才抛异常（cause为最后一次的原始异常）。</p>
     *
     * <p>会重放请求，<b>只适合幂等读</b>；写操作请用 {@link #FAIL_FAST}。</p>
     */
    FAIL_OVER("failover"),

    /**
     * 快速失败：本次调用失败后立即抛出异常，不做任何重试。
     *
     * <p>{@code retries} / {@code retry-backoff-ms} 对它无效。异常原样抛给调用方，
     * 便于上层自行降级或补偿——<b>非幂等写操作（下单、扣款）推荐用它</b>。</p>
     */
    FAIL_FAST("failfast"),

    /**
     * 安全失败：吞掉异常并记日志，返回一个空的响应。
     *
     * <p>调用方拿到的是空 {@code RpcResponse}（{@code data=null}、{@code exception=null}），
     * 业务侧表现为返回 {@code null}——基本类型返回值需注意拆箱空指针。节点选择阶段的同步异常
     * 与远程调用future的异常完成都会被接住。适合埋点、审计、缓存刷新等<b>失败可丢</b>的旁路调用。</p>
     */
    FAIL_SAFE("failsafe"),

    /**
     * 失败后台重试：前台立即返回空结果，把失败任务丢到后台定时重试（best-effort）。
     *
     * <p>调用方<b>永远看不到异常</b>：成功则带真实响应返回，失败则返回空 {@code RpcResponse}；
     * 重试跑在独立的daemon线程上（固定间隔、有限次数），与前台调用的超时预算完全脱钩。
     * 适合消息通知等<b>最终一致</b>的场景，但不保证一定送达。</p>
     */
    FAIL_BACK("failback");

    /** SPI扩展名，与 {@code META-INF/rpc} 配置key及配置文件取值一致 */
    private final String type;

    ClusterPolicyEnum(String type) {
        this.type = type;
    }

    /**
     * 取得SPI扩展名。
     *
     * @return 形如 {@code failover} 的小写串，供 {@code ExtensionLoader} 查找 {@code Cluster} 实现
     */
    public String getType() {
        return type;
    }

}
