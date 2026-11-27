package io.github.nitouge.conduit.remoting;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务端在途调用登记表：让「别跑了」能真正落到正在执行的线程上。
 *
 * <p>两条路径共用同一套取消机制：</p>
 * <ul>
 *   <li><b>对端取消</b>：客户端超时/放弃时下发 CANCEL 控制帧，服务端按<b>连接 + requestId</b> 找到在途调用；</li>
 *   <li><b>本地 deadline</b>：请求 attachment 里带了截止时间，到点自己取消，不必等对端提醒。</li>
 * </ul>
 *
 * <p><b>为什么键里必须有连接</b>：requestId 是「每个客户端进程内」自增的，两个消费者各自从 1 开始
 * 计数，在同一个服务端上完全可能同时出现同一个 id。只按 requestId 记录，A 的超时就会中断 B 正在
 * 执行的业务方法——这是查起来极其困难的串扰。CANCEL 帧本来就与请求走同一条连接（响应也从这条连接
 * 回去），所以「连接 + requestId」才是这次调用的真实身份。</p>
 *
 * <p><b>中断语义：</b>取消时会 {@link Thread#interrupt()} 正在执行业务方法的线程。业务方法若在
 * {@code sleep}/{@code wait}/阻塞IO上，会立刻以 {@link InterruptedException} 结束；若它不响应中断，
 * 框架没有更强的手段（也不该强杀线程），此时只是「不再回写响应」——对调用方而言结果都一样：早就
 * 不等了。线程归还业务线程池前会清掉中断标记（见 {@code NettyServerHandler}），避免把标记带给
 * 线程池里的下一个任务。</p>
 *
 * <p>与客户端 {@link PendingRequests} 对称：那边是「等结果的一方」的超时表，这边是「干活的一方」的
 * 在途表。它是静态的，与 {@link PendingRequests} 保持同样简洁的用法（测试可用 {@link #reset()} 清场）。</p>
 */
public final class ServerCallRegistry {

    private static final Logger log = LoggerFactory.getLogger(ServerCallRegistry.class);

    /** "channelId:requestId" -> 在途调用；取消/完成时移除，所以不会泄漏 */
    private static final Map<String, Call> CALLS = new ConcurrentHashMap<>();

    /** deadline 定时器：单线程足够（到点只做「标记 + 中断」，毫秒级操作） */
    private static final ScheduledExecutorService SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(new DeadlineThreadFactory());

    private ServerCallRegistry() {
    }

    /**
     * 一次在途调用的取消句柄。
     *
     * <p>由 {@code NettyServerHandler} 持有：取消可能来自 I/O 线程（CANCEL 帧）、定时器（deadline）
     * 或业务线程自己（完成时注销），三个线程都要安全。</p>
     */
    public static final class Call {

        private final String key;
        private final Channel channel;
        private final long requestId;
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        /** 取消原因；只有 {@link #isCancelled()} 为真时有意义 */
        private volatile byte reason = ProtocolConstants.CANCEL_UNKNOWN;

        /** 正在执行业务方法的线程；业务方法返回后置空，避免中断波及线程池里的下一个任务 */
        private volatile Thread worker;

        /** deadline 定时任务；取消/完成时撤销，避免定时器堆积 */
        private volatile ScheduledFuture<?> deadlineTask;

        private Call(String key, Channel channel, long requestId) {
            this.key = key;
            this.channel = channel;
            this.requestId = requestId;
        }

        public long requestId() {
            return requestId;
        }

        /** 是否已被取消（被取消后即使业务方法跑完，也不再回写响应） */
        public boolean isCancelled() {
            return cancelled.get();
        }

        /** 取消原因（{@code ProtocolConstants.CANCEL_*}），未取消时为 {@code CANCEL_UNKNOWN} */
        public byte reason() {
            return reason;
        }

        private String key() {
            return key;
        }

        private boolean markCancelled(byte why) {
            if (cancelled.compareAndSet(false, true)) {
                this.reason = why;
                return true;
            }
            return false;
        }

        private void cancelDeadline() {
            ScheduledFuture<?> task = deadlineTask;
            if (task != null) {
                task.cancel(false);
            }
        }

        private void interruptWorker() {
            Thread thread = worker;
            if (thread != null && !thread.isInterrupted()) {
                thread.interrupt();
            }
        }
    }

    /**
     * 登记一个即将执行的请求。
     *
     * <p>必须在派发业务线程<b>之前</b>登记：CANCEL 帧是 I/O 线程收到的，它要和业务线程的启动
     * 抢时间，登记得晚了这次取消就会被丢掉。</p>
     */
    public static Call register(Channel channel, long requestId) {
        Call call = new Call(keyOf(channel, requestId), channel, requestId);
        Call previous = CALLS.put(call.key(), call);
        if (previous != null) {
            // 同一连接上 requestId 重复：只可能是一端实现有误（正常由全局自增生成）
            log.debug("[ServerCallRegistry] 同一连接上 requestId={} 被重复登记，覆盖旧条目", requestId);
            previous.cancelDeadline();
        }
        return call;
    }

    /** 请求结束（成功/失败/被取消）后注销；重复注销是安全的 */
    public static void unregister(Call call) {
        if (call == null) {
            return;
        }
        call.cancelDeadline();
        CALLS.remove(call.key(), call);
    }

    /**
     * 记录正在执行业务方法的线程，之后取消就能中断它。
     *
     * <p>取消可能<b>先于</b>这一步到达（客户端超时早于业务线程调度），此时立刻中断，
     * 不给「已经取消的请求仍完整执行完」留窗口。</p>
     */
    public static void attachWorker(Call call, Thread worker) {
        if (call == null || worker == null) {
            return;
        }
        call.worker = worker;
        if (call.isCancelled()) {
            if (!worker.isInterrupted()) {
                worker.interrupt();
            }
            log.debug("[ServerCallRegistry] requestId={} 在启动瞬间已被取消，立即中断业务线程",
                    call.requestId());
        }
    }

    /** 业务方法已返回：之后不再中断该线程（它马上要回到线程池执行别的任务） */
    public static void detachWorker(Call call) {
        if (call != null) {
            call.worker = null;
        }
    }

    /**
     * 按「连接 + requestId」取消在途调用。
     *
     * @param reason {@code ProtocolConstants.CANCEL_*} 取消原因
     * @return {@code true} 表示命中（确实有一次在途调用被取消）；{@code false} 表示请求已结束或从未到达
     */
    public static boolean cancel(Channel channel, long requestId, byte reason) {
        Call call = CALLS.remove(keyOf(channel, requestId));
        if (call == null) {
            // 常见且正常：请求已经返回（响应比取消跑得快），或取消帧是重发的
            log.debug("[ServerCallRegistry] 取消未命中 requestId={}（已结束或不存在），reason={}",
                    requestId, ProtocolConstants.cancelReasonText(reason));
            return false;
        }
        return cancel(call, reason);
    }

    private static boolean cancel(Call call, byte reason) {
        call.markCancelled(reason);
        call.cancelDeadline();
        call.interruptWorker();
        log.debug("[ServerCallRegistry] 已取消 requestId={}, reason={}, 运行线程={}",
                call.requestId(), ProtocolConstants.cancelReasonText(reason),
                call.worker == null ? "未开始/已返回" : call.worker.getName());
        return true;
    }

    /**
     * 为请求挂上 deadline 定时器：到点即取消（原因 {@code CANCEL_DEADLINE_EXCEEDED}）。
     *
     * <p>已经过期的 deadline 直接当场取消——这类请求本就该被丢弃，让它进业务线程只是浪费。</p>
     */
    public static void scheduleDeadline(Call call, long deadlineEpochMillis) {
        if (call == null || deadlineEpochMillis <= 0) {
            return;
        }
        long delay = deadlineEpochMillis - System.currentTimeMillis();
        if (delay <= 0) {
            log.debug("[ServerCallRegistry] requestId={} 的deadline已过期，立即取消", call.requestId());
            CALLS.remove(call.key(), call);
            cancel(call, ProtocolConstants.CANCEL_DEADLINE_EXCEEDED);
            return;
        }
        try {
            ScheduledFuture<?> task = SCHEDULER.schedule(() -> {
                log.debug("[ServerCallRegistry] requestId={} 到达deadline，取消执行", call.requestId());
                CALLS.remove(call.key(), call);
                cancel(call, ProtocolConstants.CANCEL_DEADLINE_EXCEEDED);
            }, delay, TimeUnit.MILLISECONDS);
            call.deadlineTask = task;
            // 竞态：定时器挂上之前请求已经被取消/完成，撤销这个刚建好的任务
            if (call.isCancelled() || !CALLS.containsKey(call.key())) {
                task.cancel(false);
            }
        } catch (RejectedExecutionException e) {
            // 调度器只在本进程关闭时停：此时请求也无需再管
            log.debug("[ServerCallRegistry] deadline调度器已停止，requestId={} 跳过定时取消", call.requestId());
        }
    }

    /** 当前在途调用数（监控/测试断言用） */
    public static int activeCount() {
        return CALLS.size();
    }

    /** 清空登记表（测试用）：撤销定时器，不中断任何线程 */
    public static void reset() {
        for (Call call : CALLS.values()) {
            call.cancelDeadline();
        }
        CALLS.clear();
    }

    private static String keyOf(Channel channel, long requestId) {
        return channel.id().asLongText() + ':' + requestId;
    }

    private static final class DeadlineThreadFactory implements ThreadFactory {

        private final AtomicInteger index = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "conduit-call-deadline-" + index.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
