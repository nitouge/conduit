package io.github.nitouge.conduit.testkit.linearizability;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 检测器自检：先确认它能把"明显线性一致"和"明显不一致"的历史区分开，再用它测真实组件。
 */
class LinearizabilityCheckerTest {

    @Test
    void nullRegisterIsLinearizable() {
        assertTrue(LinearizabilityChecker.isLinearizable(List.of()));
    }

    @Test
    void sequentialWriteThenReadIsLinearizable() {
        assertTrue(LinearizabilityChecker.isLinearizable(List.of(
                HistoryOperation.write("t1", Boolean.TRUE, 0, 10),
                HistoryOperation.read("t2", Boolean.TRUE, 20, 30))));
    }

    @Test
    void staggeredReadMaySeeOldValueWhenOverlapping() {
        // 写还没完成时，读可以先线性化 → 读到初始值 null 是合法的
        assertTrue(LinearizabilityChecker.isLinearizable(List.of(
                HistoryOperation.write("t1", Boolean.TRUE, 0, 100),
                HistoryOperation.read("t2", null, 40, 50))));
    }

    @Test
    void readAfterWriteMustSeeNewValue() {
        assertFalse(LinearizabilityChecker.isLinearizable(List.of(
                HistoryOperation.write("t1", Boolean.TRUE, 0, 10),
                HistoryOperation.read("t2", Boolean.FALSE, 20, 30))),
                "写完再读，不能读到从未写过的值");
    }

    @Test
    void readBeforeAnyWriteMustSeeNull() {
        assertFalse(LinearizabilityChecker.isLinearizable(List.of(
                HistoryOperation.read("t1", Boolean.TRUE, 0, 10),
                HistoryOperation.write("t2", Boolean.TRUE, 20, 30))),
                "写发生之前读到true是不可能的");
    }

    @Test
    void writesCanBeOrderedAroundConcurrentRead() {
        // 两个重叠写 + 之后一次读；读到的值必须是"被排在最后的那个写"的值 → 可满足
        assertTrue(LinearizabilityChecker.isLinearizable(List.of(
                HistoryOperation.write("t1", Boolean.TRUE, 0, 50),
                HistoryOperation.write("t2", Boolean.FALSE, 10, 60),
                HistoryOperation.read("t3", Boolean.TRUE, 70, 80))));
    }

    @Test
    void rejectsImpossibleReadSequence() {
        assertFalse(LinearizabilityChecker.isLinearizable(List.of(
                HistoryOperation.write("t1", Boolean.TRUE, 0, 10),
                HistoryOperation.read("t2", Boolean.TRUE, 20, 30),
                HistoryOperation.read("t3", Boolean.FALSE, 40, 50))),
                "写之后两次读不可能读到不同值（中间没有写）");
    }

    @Test
    void rejectsTooLargeHistory() {
        HistoryOperation[] ops = new HistoryOperation[LinearizabilityChecker.MAX_OPERATIONS + 1];
        for (int i = 0; i < ops.length; i++) {
            ops[i] = HistoryOperation.write("t", Boolean.TRUE, i * 10L, i * 10L + 1);
        }
        assertThrows(IllegalArgumentException.class,
                () -> LinearizabilityChecker.isLinearizable(List.of(ops)));
    }
}
