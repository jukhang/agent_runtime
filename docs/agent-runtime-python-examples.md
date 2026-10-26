# 纯 Python SDK 示例规格

## 范围

新增 `packages/agent-runtime/examples/python`，不依赖 Web UI、不改 SDK 核心行为，
不修改已冻结的发行包。教学示例直接使用现有公开契约和端口，不创造 Agent.run 等
尚不存在的接口，也不导入 tests 或旧 app。

## 覆盖

1. Agent、模型绑定、指令、时间、预算及能力引用。
2. 自定义 ToolSpec、Pydantic 输入输出及纯业务函数；清楚区分函数测试和 ToolHost 授权执行。
3. ContextRuntime、SourceReader、版本/ACL、快照、预算、恢复；内存端口只用于教学。
4. ModelAgentLoop 的一次决策：Context→Provider→候选；提供离线协议演示及显式真实模型模式。
5. MCP 的固定 Schema、Agent绑定、目录校验安装、Broker逐调用授权适配；不假装兼容所有协议。
6. Skill 固定文件清单、摘要校验、在线代理安装、Agent绑定和受监督脚本适配；不在宿主运行代码。
7. Python 客户端 Runtime.submit/attach、Journal流和结果；复用 standalone 的非HTTP宿主
   装配，SQL迁移、主/子/维护Worker独立运行，支持planner/delegate/external_wait配置。
8. 资料/知识来源、可选历史与压缩端口说明，失败、控制、人工审批接口导航。

## 验证要求

所有离线入口可执行，契约和输出有自动化断言，包括失败边界。网络/SQL示例按需
配置显式启动，不在 import 时连接，不读取仓库根.env或埋凭据；未运行的外部路径
明确列为未验证。新示例不替代完整P05发行验收，不自动修改旧ACCEPTED manifest。

## 实现与验证记录（2026-10-26）

- 已交付独立 uv 示例项目、锁文件、配置模板和十个离线入口，以及durable纯Python
  客户端/Worker入口。README逐项列出装配层级、命令、授权与持久化边界。
- 自定义工具覆盖纯函数、Schema、SQL Registry发布和handler；单步Loop增加
  `--tool-decision`，验证只有invoke_tools候选、不执行工具、不伪造答案。
- 真实模型模式为显式 `--live`，可选择thinking；MCP和Skill提供受信端口接线函数，
  不直接把用户URL/token送入模型，不在宿主执行Skill脚本。
- 独立uv项目安装及Agent入口执行通过；独立环境21项测试通过，无skip；
  optional host安装和durable CLI帮助入口通过。最初optional依赖下载DNS失败，
  已重试成功，不将下载故障描述为代码测试失败或跳过通过。
- Ruff通过；12个示例模块mypy通过，但明确关闭no-untyped-call以适配复用的
  standalone宿主既有未标注方法，其他类型错误未忽略；不宣称全量strict无例外。
- 本轮未重跑真实Provider、SQL迁移/Worker、远端MCP/Git或沙箱执行。相应入口
  是可配置示例和已有后端装配复用，不以离线契约检查替代外部组合验收。
- 不启动已有app/web，不修改SDK核心、数据库模型或已冻结发行包，不执行Git提交。
