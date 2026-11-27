# 贡献指南

感谢你对 Conduit 感兴趣。这是一个 Java RPC 框架，欢迎 Issue、讨论和 Pull Request —— 不管是修 bug、补文档，还是实现 [docs/BACKLOG.md](temp/BACKLOG.md) 里的某一项增强。

## 开始之前

- **构建需要 JDK 17**（Spring Boot 3.x 要求），`JAVA_HOME` 指向 JDK 17 后执行 `mvn -B clean test`。
- 熟悉一下项目的核心设计原则：**可替换**（面向接口 + SPI，业务代码不 `new` 具体实现），以及**下层绝不依赖上层**的模块分层。展开说明见 [docs/ARCHITECTURE.md](temp/ARCHITECTURE.md)。

## 报告问题 / 提出建议

- Bug：请在 Issue 里给出复现步骤、期望行为与实际行为、`mvn -version` 和操作系统信息。
- 新功能 / 增强：先看看 [docs/BACKLOG.md](temp/BACKLOG.md) 是否已经规划过；没有的话，开一个 Issue 简单描述目标和涉及模块，方便讨论方案后再动手，避免白写。

## 开发流程

1. **从 `main` 拉分支**，命名带上类型前缀，例如 `feat/etcd-registry`、`fix/decoder-oob`、`docs/update-roadmap`。
2. 开发时遵循已有模块的代码风格与分层约定（新增能力优先做成 `@SPI` 扩展点，不要引入新的分层）。
3. **提交信息**用 [Conventional Commits](https://www.conventionalcommits.org/)：`<type>(<scope>): <中文简述>`，例如 `fix(remoting): 修复心跳超时未重连`。常用 type：`feat` / `fix` / `docs` / `refactor` / `test` / `chore`。
4. 提交前确保本地测试全绿：
   ```bash
   export JAVA_HOME=<你的 JDK17 路径>   # Windows PowerShell: $env:JAVA_HOME = "..."
   mvn -B clean test
   ```
5. 新增功能请**补充单元测试**；修 bug 时如果能加一个复现该 bug 的测试更好。
6. 涉及公开行为或配置项变化时，同步更新相关文档（`README.md` / `docs/*.md`）。

## 提交 Pull Request

- PR 目标分支为 **`main`**。
- PR 标题也遵循 Conventional Commits 风格，描述里说明**改了什么、为什么改**，如果关联 Issue 请用 `Closes #xxx`。
- 保持 PR 聚焦单一变更；大的功能建议先按 [docs/BACKLOG.md](temp/BACKLOG.md) 里的拆解粒度分块提交。
- CI（如果已配置）与人工 review 通过后才会合并；仓库已开启分支保护，`main` 不接受直接 push，所有变更都必须经过 PR。
- 合并策略统一为仓库设置里锁定的方式，不需要贡献者自己纠结 merge/rebase/squash。

## 许可

本项目采用 [Apache-2.0](LICENSE) 许可。除非你在提交时明确说明，否则你提交的任何贡献都将默认按 Apache-2.0 第 5 条接受同一许可条款。

## 行为准则

保持友善、尊重、聚焦于技术讨论本身。目前项目规模较小，暂未单独维护 `CODE_OF_CONDUCT.md`，后续社区规模扩大时会补上。
