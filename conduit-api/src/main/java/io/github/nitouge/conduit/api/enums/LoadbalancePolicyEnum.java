package io.github.nitouge.conduit.api.enums;

/**
 * 负载均衡策略：从注册中心给出的候选实例里挑一个来发起调用。
 *
 * <p>与 {@link ClusterPolicyEnum} 是两层职责——本枚举解决<b>选谁</b>，集群容错策略解决
 * 选中的那个失败之后怎么办。由 {@code @RpcReference#loadbalance()} 或全局配置
 * {@code conduit.cluster.loadbalance} 指定，也可在调用前用 {@code RpcContext} 的
 * {@code loadbalance} attachment做单次覆盖。</p>
 *
 * <p><b>与SPI的关系：</b>枚举的 {@code type} 就是 {@code LoadBalancer} 的SPI扩展名，与
 * {@code conduit-cluster} 中 {@code META-INF/rpc} 配置的key、以及配置文件里写的字符串
 * （如 {@code roundrobin}）逐一对应——改这里的 {@code type} 等于改SPI查找用的名字。</p>
 */
public enum LoadbalancePolicyEnum {

    /**
     * 加权随机（默认）：按 {@code weight} 占比随机选一个实例，权重越大被选中概率越高。
     *
     * <p>实现上在 {@code [0, totalWeight)} 取随机落点后依次扣减各节点权重，落点归零即选中；
     * 权重全相同（或总和为0）时退化为均匀随机。实现简单、无需状态，无需会话保持时首选。</p>
     */
    RANDOM("random"),

    /**
     * 轮询：逐个实例轮流分发，把请求均匀摊到各节点上。
     *
     * <p>用 {@code AtomicInteger} 自增后取模（先与 {@code Integer.MAX_VALUE} 相与，避免长期累加
     * 溢出成负数）。{@code LoadBalancer} 是SPI单例，同一策略的计数天然全局共享，所以多个
     * 引用共用一条轮询序列。</p>
     */
    ROUND_ROBIN("roundrobin"),

    /**
     * 一致性哈希：相同参数的调用稳定路由到同一实例（适合有状态、缓存亲和的场景）。
     *
     * <p>每个物理实例复制成若干个虚拟节点散布到哈希环上以均衡分布，请求key取哈希后
     * 顺时针取环上第一个实例；<b>路由key = 接口名 + 方法名 + 参数</b>，因此同参数 → 同节点。
     * 节点增减时只有相邻一小段key需要重新映射，大幅减少缓存失效。</p>
     *
     * <p><b>注意：</b>节点上下线会改变部分key的归属，且该实现每次 {@code select} 重建哈希环，
     * 高频场景下开销为O(节点数 × 虚拟节点数)。</p>
     */
    CONSISTENT_HASH("consistenthash");

    /** SPI扩展名，与 {@code META-INF/rpc} 配置key及配置文件取值一致 */
    private final String type;

    LoadbalancePolicyEnum(String type) {
        this.type = type;
    }

    /**
     * 取得SPI扩展名。
     *
     * @return 形如 {@code roundrobin} 的小写串，供 {@code ExtensionLoader} 查找 {@code LoadBalancer} 实现
     */
    public String getType() {
        return type;
    }

}
