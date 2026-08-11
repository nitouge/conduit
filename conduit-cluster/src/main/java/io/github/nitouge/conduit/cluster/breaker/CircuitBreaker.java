package io.github.nitouge.conduit.cluster.breaker;

/**
 * 熔断器（参考Hystrix）：三态状态机CLOSED/OPEN/HALF_OPEN，按节点维度使用。
 *
 * <ul>
 *   <li>CLOSED：正常放行；连续失败达阈值 → 转OPEN。</li>
 *   <li>OPEN：直接拒绝；经过openDuration后 → 转HALF_OPEN，<b>只放一个探测</b>。</li>
 *   <li>HALF_OPEN：仅放行单个探测请求，其余拒绝；探测成功 → CLOSED；失败 → 回OPEN。</li>
 * </ul>
 *
 * <pre>
 * 状态转换图：
 *                  failure >= threshold
 *       +------------------------------------+
 *       |                                    |
 *       v                                    |
 *   +---------+      timeout      +---------+
 *   | CLOSED  | ----------------> |  OPEN   |
 *   +---------+                   +---------+
 *        ^                             |
 *        |                             |
 *        | probe success               | allow one probe
 *        |                             v
 *        +-----------------------+-----------+
 *                                | HALF_OPEN |
 *                                +-----------+
 *                                      |
 *                                      +---- probe failure ----> OPEN
 * </pre>
 *
 * <p><b>隐式契约：</b>{@code allowRequest()} 在半开期放行探测后，调用方<b>必须恰好回报一次</b>
 * {@link #onSuccess()} 或 {@link #onFailure()}，否则 {@code probeInFlight} 不会复位、将永久卡在HALF_OPEN
 * 拒绝一切请求。框架内由 {@code AbstractCluster.callWithBreaker} 的try/catch保证这一点（成功/异常各回报一次）。</p>
 *
 * <p>所有状态读写都在 {@code synchronized} 下进行（状态机简单、争用低），无需再叠加volatile/Atomic。</p>
 */
public class CircuitBreaker {

    public enum State {CLOSED, OPEN, HALF_OPEN}

    private final int failureThreshold;
    private final long openDurationMs;

    private State state = State.CLOSED;
    private int consecutiveFailures = 0;
    private long openedAt;
    /** 半开期是否已放出探测（保证半开只放一个探测请求） */
    private boolean probeInFlight = false;

    public CircuitBreaker(int failureThreshold, long openDurationMs) {
        this.failureThreshold = failureThreshold;
        this.openDurationMs = openDurationMs;
    }

    /** 是否放行本次请求 */
    public synchronized boolean allowRequest() {
        return switch (state) {
            case OPEN -> {
                if (System.currentTimeMillis() - openedAt >= openDurationMs) {
                    state = State.HALF_OPEN;
                    probeInFlight = true; // 放且仅放这一个探测
                    yield true;
                }
                yield false;
            }
            case HALF_OPEN -> {
                if (!probeInFlight) {
                    probeInFlight = true;
                    yield true;
                }
                yield false;
            }
            default -> true;
        };
    }

    /** 调用成功回调 */
    public synchronized void onSuccess() {
        if (state == State.OPEN) {
            // 迟到的成功（请求在转OPEN之前发出）：不撤销熔断、不动计数
            return;
        }
        if (state == State.HALF_OPEN) {
            state = State.CLOSED; // 探测成功，恢复
        }
        consecutiveFailures = 0;
        probeInFlight = false;
    }

    /** 调用失败回调 */
    public synchronized void onFailure() {
        if (state == State.OPEN) {
            return; // 已打开，忽略迟到失败
        }
        if (state == State.HALF_OPEN) {
            // 探测失败，重新打开
            state = State.OPEN;
            openedAt = System.currentTimeMillis();
            probeInFlight = false;
            return;
        }
        // CLOSED
        consecutiveFailures++;
        if (consecutiveFailures >= failureThreshold) {
            state = State.OPEN;
            openedAt = System.currentTimeMillis();
        }
    }

    public synchronized State getState() {
        return state;
    }
}
