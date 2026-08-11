package io.github.nitouge.conduit.registry;

import io.github.nitouge.conduit.api.ServiceMeta;

import java.util.List;

/**
 * 服务实例变更通知回调：注册中心推送某服务的最新实例列表时被调用。
 */
@FunctionalInterface
public interface NotifyListener {

    void notify(List<ServiceMeta> services);
}
