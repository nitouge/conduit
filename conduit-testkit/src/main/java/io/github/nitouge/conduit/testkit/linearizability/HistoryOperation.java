package io.github.nitouge.conduit.testkit.linearizability;

/**
 * 一次操作的历史记录：操作区间 [startNanos, endNanos] + 观察到的结果。
 *
 * <p>只支持"读写寄存器"这一最常用模型：{@link Type#WRITE} 写值（结果视为成功），
 * {@link Type#READ} 读取当前值（{@code result} 是调用方实际观察到的返回值）。
 * 并发组件的语义能归约成"寄存器"时（如注册中心里"某实例是否存在"），就可以用这个模型验证线性一致性。</p>
 */
public final class HistoryOperation {

    public enum Type {
        /** 写操作：设置寄存器值 */
        WRITE,
        /** 读操作：读取寄存器当前值 */
        READ
    }

    private final Type type;
    private final String process;
    private final Object value;
    private final Object result;
    private final long startNanos;
    private final long endNanos;

    private HistoryOperation(Type type, String process, Object value, Object result,
                             long startNanos, long endNanos) {
        this.type = type;
        this.process = process;
        this.value = value;
        this.result = result;
        this.startNanos = startNanos;
        this.endNanos = endNanos;
        if (endNanos < startNanos) {
            throw new IllegalArgumentException("endNanos 不能早于 startNanos");
        }
    }

    /** 写操作：在 [start,end] 区间内把寄存器置为 value */
    public static HistoryOperation write(String process, Object value, long startNanos, long endNanos) {
        return new HistoryOperation(Type.WRITE, process, value, null, startNanos, endNanos);
    }

    /** 读操作：在 [start,end] 区间内返回了 result */
    public static HistoryOperation read(String process, Object result, long startNanos, long endNanos) {
        return new HistoryOperation(Type.READ, process, null, result, startNanos, endNanos);
    }

    public Type type() {
        return type;
    }

    public String process() {
        return process;
    }

    public Object value() {
        return value;
    }

    public Object result() {
        return result;
    }

    public long startNanos() {
        return startNanos;
    }

    public long endNanos() {
        return endNanos;
    }

    @Override
    public String toString() {
        return type == Type.WRITE
                ? process + " write(" + value + ") [" + startNanos + "," + endNanos + "]"
                : process + " read() -> " + result + " [" + startNanos + "," + endNanos + "]";
    }
}
