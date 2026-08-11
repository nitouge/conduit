# Changelog

本文件记录 Conduit 各版本间的显著变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本 SemVer](https://semver.org/lang/zh-CN/)。

> 项目目前尚未打过正式 Git tag / GitHub Release，当前版本号为 `1.0.0-SNAPSHOT`（开发中）。下面的 `[Unreleased]` 汇总了主线 M1-M5 已完成的能力；第一次正式打 tag 时，把对应内容挪到一个具体版本号下即可。

## [Unreleased]

### Added

- **M1 直连调用**：自定义协议（定长头+变长体）、Netty 传输、JDK 序列化、JDK 动态代理，端到端同步调用打通。
- **M2 服务发现**：接入 Nacos 注册中心，消费者去掉写死地址。
- **M3 可替换 SPI**：`@SPI` + `ExtensionLoader` 机制；序列化扩展到 JDK / JSON(Jackson) / Kryo / Protostuff，换实现只改配置。
- **M4 异步与治理**：`CompletableFuture` 异步调用；负载均衡 Random(带权重) / RoundRobin / ConsistentHash；集群容错 FailFast / FailOver / FailSafe / FailBack；三态熔断器。
- **M5 Spring 集成**：`@EnableConduit` / `@RpcService` / `@RpcReference` 注解驱动，`application.yml` 自动配置。
- Filter 拦截链 SPI（消费侧 + 提供侧）与 `RpcRequest` 隐式附加参数 `attachments`（配合 `RpcContext`）。
- 真正的 Spring Boot Starter（`conduit-spring-boot-starter`）与跨进程 `conduit-starter-demo` 示例。
- Provider 端非阻塞异步调用支持。

### Docs

- 补齐开源基础文档：`LICENSE`（Apache-2.0）、`CONTRIBUTING.md`、`CHANGELOG.md`。

<!--
后续每次发版前，在此文件顶部新增一个版本块，格式例如：

## [1.0.0] - 2026-01-01

### Added
- ...

### Changed
- ...

### Fixed
- ...
-->
