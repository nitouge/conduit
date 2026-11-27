package io.github.nitouge.conduit.testkit.linearizability;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 线性一致性（Linearizability）检测器：Wing &amp; Gong 算法的寄存器模型实现。
 *
 * <p><b>模型：</b>一个寄存器，初始值为 {@code null}；{@code write(v)} 把值设为 v（总成功）；
 * {@code read()} 返回当前值。判定标准：存在一个全局顺序，使</p>
 * <ol>
 *   <li>每个读返回它在该顺序中"之前的最近一次写"的值；</li>
 *   <li>若操作 A 在真实时间上先于 B 结束/开始（A.end &lt; B.start），则顺序中 A 必须在 B 之前。</li>
 * </ol>
 *
 * <p><b>算法：</b>按"可线性化点必须落在操作区间内"做回溯搜索，每步只选"所有真实时间前驱都已执行"的操作；
 * 以 (已执行集合, 当前值) 做记忆化，避免指数级重复。操作数上限 62（位掩码）。</p>
 *
 * <p><b>能验证什么：</b>凡是语义可归约为"读/写一个寄存器"的组件，例如
 * {@code InMemoryRegistry} 中"某实例是否已注册"（register=写true、unregister=写false、
 * discover=读）。不能验证带复杂语义的对象（如队列、锁），那需要各自的模型。</p>
 */
public final class LinearizabilityChecker {

    /** 操作上限：用 long 位掩码表示已执行集合 */
    public static final int MAX_OPERATIONS = 62;

    /** 初始值（null）对应的状态编号 */
    private static final int STATE_NULL = 0;

    private LinearizabilityChecker() {
    }

    /**
     * 判断历史是否线性一致（寄存器初值为 {@code null}）。
     *
     * @see #isLinearizable(List, Object)
     */
    public static boolean isLinearizable(List<HistoryOperation> history) {
        return isLinearizable(history, null);
    }

    /**
     * 判断历史是否线性一致，指定寄存器初值。
     *
     * <p>历史里每个操作都应有完整的调用/返回区间（不支持挂起未返回的操作）。</p>
     *
     * @param initialValue 寄存器初始值（只支持 null/Boolean）；例如"实例是否已注册"模型初值是 {@code false}
     * @throws IllegalArgumentException 操作数超过 {@value #MAX_OPERATIONS}，或初值不是 null/Boolean
     */
    public static boolean isLinearizable(List<HistoryOperation> history, Object initialValue) {
        if (history == null || history.isEmpty()) {
            return true;
        }
        if (history.size() > MAX_OPERATIONS) {
            throw new IllegalArgumentException("操作数超过上限 " + MAX_OPERATIONS + "，请缩小并发规模: " + history.size());
        }

        List<HistoryOperation> ops = new ArrayList<>(history);
        ops.sort((a, b) -> Long.compare(a.startNanos(), b.startNanos()));
        int n = ops.size();

        long[] mustPrecede = new long[n];
        for (int i = 0; i < n; i++) {
            long mask = 0L;
            for (int j = 0; j < n; j++) {
                // 真实时间顺序：j 在 i 开始之前就结束了 → j 必须先线性化
                if (i != j && ops.get(j).endNanos() < ops.get(i).startNanos()) {
                    mask |= bit(j);
                }
            }
            mustPrecede[i] = mask;
        }

        long full = n == 62 ? -1L : (1L << n) - 1;
        Map<Long, Boolean> memo = new HashMap<>();
        return dfs(0L, full, stateIndexOf(initialValue), ops, mustPrecede, memo);
    }

    private static boolean dfs(long applied, long full, int state,
                               List<HistoryOperation> ops, long[] mustPrecede,
                               Map<Long, Boolean> memo) {
        if (applied == full) {
            return true;
        }
        long key = applied * 3L + state;
        Boolean cached = memo.get(key);
        if (cached != null) {
            return cached;
        }

        for (int i = 0; i < ops.size(); i++) {
            long mask = bit(i);
            if ((applied & mask) != 0) {
                continue;
            }
            if ((mustPrecede[i] & ~applied) != 0) {
                continue; // 还有真实时间前驱没执行
            }

            HistoryOperation op = ops.get(i);
            int nextState;
            if (op.type() == HistoryOperation.Type.WRITE) {
                nextState = stateIndexOf(op.value());
            } else {
                if (!Objects.equals(stateValue(state), op.result())) {
                    continue; // 读到的值不是当前值 → 该操作不能在此刻线性化
                }
                nextState = state;
            }

            if (dfs(applied | mask, full, nextState, ops, mustPrecede, memo)) {
                memo.put(key, Boolean.TRUE);
                return true;
            }
        }

        memo.put(key, Boolean.FALSE);
        return false;
    }

    private static long bit(int index) {
        return 1L << index;
    }

    /** null → 0，false → 1，true → 2；其他值（如字符串）按 hashCode 折叠到 3 个槽位之外会冲突，这里只支持 null/布尔 */
    private static int stateIndexOf(Object value) {
        if (value == null) {
            return STATE_NULL;
        }
        if (Boolean.FALSE.equals(value)) {
            return 1;
        }
        if (Boolean.TRUE.equals(value)) {
            return 2;
        }
        throw new IllegalArgumentException("寄存器模型目前只支持 null/Boolean 值: " + value);
    }

    private static Object stateValue(int state) {
        return switch (state) {
            case STATE_NULL -> null;
            case 1 -> Boolean.FALSE;
            case 2 -> Boolean.TRUE;
            default -> throw new IllegalStateException("非法状态: " + state);
        };
    }
}
