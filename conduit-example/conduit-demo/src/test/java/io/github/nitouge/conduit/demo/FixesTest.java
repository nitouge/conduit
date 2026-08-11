package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.registry.NotifyListener;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.remoting.PendingRequests;
import io.github.nitouge.conduit.server.RpcServer;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 对抗性审查修复的回归测试。
 */
class FixesTest {

    private static final String HOST = "127.0.0.1";

    /** 慢服务：处理耗时超过客户端超时，用来触发超时路径 */
    public interface SlowService {
        String slow();
    }

    public static class SlowServiceImpl implements SlowService {
        @Override
        public String slow() {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "late";
        }
    }

    /**
     * 调用超时后，传输层应清理PendingRequests中的在途条目，不再泄漏。
     */
    @Test
    void timeoutCleansPendingRequests() throws Exception {
        RpcServer server = new RpcServer(HOST, 20893);
        server.export(SlowService.class, new SlowServiceImpl());
        server.start();

        RpcClient client = new RpcClient();
        try {
            int before = PendingRequests.pendingCount();

            ServiceMeta target = new ServiceMeta(SlowService.class.getName(), HOST, 20893);
            SlowService slow = RpcClientProxy.create(SlowService.class, target, client, 500); // 500ms < 2000ms

            assertThrows(RpcException.class, slow::slow);

            // 等待500ms的超时清理任务执行完
            Thread.sleep(800);
            assertEquals(before, PendingRequests.pendingCount(), "超时后在途请求应被清理，不泄漏");
        } finally {
            client.close();
            server.close();
        }
    }

    /**
     * 注册中心订阅失败不应导致代理创建失败（启动不强耦合注册中心可用性）。
     */
    @Test
    void proxyCreationSurvivesSubscribeFailure() {
        RpcClient client = new RpcClient();
        try {
            Registry failingRegistry = new Registry() {
                @Override
                public void register(ServiceMeta service) {
                }

                @Override
                public void unregister(ServiceMeta service) {
                }

                @Override
                public List<ServiceMeta> discover(String serviceName) {
                    return Collections.emptyList();
                }

                @Override
                public void subscribe(String serviceName, NotifyListener listener) {
                    throw new RpcException("registry down"); // 模拟注册中心不可达
                }

                @Override
                public void destroy() {
                }
            };

            // 订阅失败被吞掉，代理仍应创建成功
            HelloService hello = RpcClientProxy.create(HelloService.class, failingRegistry, client, 1000);
            assertNotNull(hello);
        } finally {
            client.close();
        }
    }
}
