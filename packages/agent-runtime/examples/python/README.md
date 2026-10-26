# 后端 Python 示例

这些示例不启动 FastAPI、Vite 或浏览器，不依赖旧项目 app/web。
`Agent` 是声明；`ModelAgentLoop.decide` 产生一次候选；完整持久执行由
`Runtime.submit` → SQL → 独立 Worker → ToolHost → Observation 推进。
没有虚构的 `agent.run()` 或绕过权限的通用 `execute()`。

## 1. 安装与快速运行

在仓库根目录执行，Python 3.12+，使用 uv：

```bash
uv sync --project packages/agent-runtime/examples/python
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.agent_config
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.tools
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.context
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.loop
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.loop --tool-decision
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.knowledge
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.memory
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.mcp
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.skill
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.human
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.subagents
uv run --project packages/agent-runtime/examples/python pytest packages/agent-runtime/examples/python/tests -q
```

以上均可离线运行。`loop` 明确输出 `offline-scripted-provider`，用于展示真实 SDK
契约流转，不冒充模型语义测试。MCP/Skill 默认只验证声明与字节，不连接远端。

仓库外：安装自己的 SDK wheel 及其 provider/storage/mcp extras，把本目录作为
示例宿主；去掉 pyproject 的 `tool.uv.sources` 本地路径绑定再安装。它不是 SDK wheel
内置模块，不要从公开索引意外安装同名第三方包。

## 2. 示例地图

| 环节 | 模块 | 实际覆盖 |
| --- | --- | --- |
| 定义 Agent/模型 | `agent_config` | 指令、时间、时区、预算、版本固定的能力引用；不含密钥 |
| Python Tool | `tools` | 输入/输出Schema、业务函数、ToolSpec、SQL发布函数、ToolHost handler |
| Context | `context` | SourceReader/ACL、必需事实、快照版本、恢复及摘要完整性 |
| AgentLoop/Provider | `loop` | 准备上下文、受限预算、流事件、完整finish后返回候选；可显式连接真实Qwen |
| 知识库 | `knowledge` | KnowledgeBinding/检索与引用策略；不冒充完整RAG实现 |
| 历史/压缩 | `memory` | 纠正/否定事实作为来源；压缩逐字引用校验；非跨会话自动全局记忆 |
| MCP | `mcp` | 固定Schema、Agent绑定、目录比对安装、每次调用Broker grant |
| Skill | `skill` | 文件摘要、固定Git commit/子目录/受控代理、逐Run加载与沙箱执行适配 |
| 人工交互 | `human` | Runtime.interact和版本绑定审批决议构造 |
| Subagent | `subagents` | 父子定义、TaskPackage；实际预算/派发由SQL管理 |
| 完整持久循环 | `durable` | 迁移、Session、提交/重连/控制、事件和结果、独立主/子/维护进程 |

## 3. 自定义 Agent 和 Tool 如何真正生效

```python
from runtime_examples.agent_config import build_agent
from runtime_examples.tools import MultiplyHandler, install, schemas, tool_spec

agent = build_agent()
spec = tool_spec()
# 在自己的宿主中显式提供有权限校验的 SQL Registry：
# await install(actor, registry)
# read_tools = {spec.ref: MultiplyHandler()}
# SqlToolHost(..., schemas=schemas(), read_tools=read_tools)
```

`...` 表示宿主必须提供的实际端口，不是可直接执行的装配代码。完整可运行装配见
`../standalone/src/centn_example/host.py` 的 `initialize/worker`：
先用 `SqlAgentInstaller.install` 固定Agent修订，再把 ToolSpec、Schema、handler、
资源ACL、Policy/审批、SqlToolCapacity、SqlToolExecutionGate注入SqlToolHost。
在主Worker的模型输入工厂中同时提供可见工具定义。

增加 `Agent.tools` 不等于安装/授权工具；直接调用 `calculate` 只是纯函数测试。
写工具使用database_tools和同事务账本；外部写入需要幂等键和效果查询，不能简单
把read改成write。Hook可通过 `PinnedToolHooks` 注入，不能扩大grant或篡改结果。

## 4. 真实模型的一次决策

```bash
export CENTN_PROVIDER_URL='http://127.0.0.1:9919/v1'
export CENTN_PROVIDER_KEY='<从受控环境注入>'
export CENTN_MODEL='qwen3.6-27b'
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.loop --live
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.loop --live --thinking
uv run --project packages/agent-runtime/examples/python python -m runtime_examples.loop --live --tool-decision
```

Qwen adapter把thinking映射到chat_template_kwargs；密钥只进入服务端connection。
普通OpenAI兼容端点需要改用 `OpenAIAdapterProfile()` 和 `thinking="default"`，
不可假设所有厂商支持Qwen专有选项。事件打印可能包含私有思考或任务数据，仅在可信
终端调试，不直接转发公众日志。API凭据不会打印。

这个例子只执行一次 `ModelAgentLoop.decide`，不自动提交状态、执行工具、结算预算
或做持久恢复。内存事件/快照/答案随进程消失，不适合生产多Worker。完整链路用下一节。
`--tool-decision`展示可见工具的Schema绑定和invoke_tools候选；不会直接执行该候选，
也不会用本地计算伪造模型最终答案。真实模型不保证每次选择相同动作，应检查实际kind。

## 5. 纯 Python 持久执行，不启动Web

安装宿主装配依赖：

```bash
uv sync --project packages/agent-runtime/examples/python --extra host
```

复制 `runtime.env.example` 到私有路径并填实际配置；**不会自动读取根目录.env**。
所有进程使用同一份配置；schema应专属于本示例，迁移不创建/删除数据库。
下面的 `--env-file /private/path/runtime.env` 替换为自己的路径。

```bash
# 首次初始化一次，后续直接启动 Worker
uv run --project packages/agent-runtime/examples/python --extra host --env-file /private/path/runtime.env python -m runtime_examples.durable migrate

# 两个独立终端，maintenance必需；不在客户端内嵌Worker
uv run --project packages/agent-runtime/examples/python --extra host --env-file /private/path/runtime.env python -m runtime_examples.durable worker
uv run --project packages/agent-runtime/examples/python --extra host --env-file /private/path/runtime.env python -m runtime_examples.durable maintenance

# 创建Session，记下sessionId
uv run --project packages/agent-runtime/examples/python --extra host --env-file /private/path/runtime.env python -m runtime_examples.durable create-session

# 将SESSION_UUID替换为输出值，key由客户端生成并在网络重试中保持不变
uv run --project packages/agent-runtime/examples/python --extra host --env-file /private/path/runtime.env python -m runtime_examples.durable submit --session SESSION_UUID --key question-001 --text '计算23乘17，再乘19，解释结果'

# 记下首次返回的runId及已处理sessionSeq；重连绝不重发请求
uv run --project packages/agent-runtime/examples/python --extra host --env-file /private/path/runtime.env python -m runtime_examples.durable attach --session SESSION_UUID --run RUN_UUID --after 0

# expected-version取当前Run视图的stateVersion，不是事件cursor；同一命令重试保留key
uv run --project packages/agent-runtime/examples/python --extra host --env-file /private/path/runtime.env python -m runtime_examples.durable control --session SESSION_UUID --run RUN_UUID --kind pause --expected-version 3 --key pause-001
```

流按Session排序，Python客户端筛选本Run及其子Run，仍保留精确cursor；遇到
resync_required必须恢复授权快照，不能猜测越过空洞。客户端等待超时不等于取消。
Ctrl-C断开客户端不会停止持久任务，取消必须发显式control。失败结果保留failure/
recoveryOptions，不改为空答复；用量unknown不能当作零。Worker收到SIGTERM/SIGINT
执行drain。代码复用standalone的后端Host，但从不import或启动其API/UI。

`CENTN_AGENT_PROFILE=planner`使用计划工作流；`delegate`需再启动独立的
`python -m runtime_examples.durable child`（同样带上述uv配置）；`external_wait`
需宿主外部结果端口和maintenance唤醒。改变profile应先排空旧任务，不在执行中
随意替换Agent配置。conversation模式不需要子Worker。

## 6. MCP、Skill、知识库与自定义宿主

MCP配置路径：`mcp.tool_spec()` → 有Broker授权的discovery transport →
`install_reviewed_tool(actor, registry, discovery)` → 显式review/publish →
`mcp.make_handler(broker, blobs, issue_grant)`注册为ToolHost只读工具。
宿主的 `issue_grant` 必须核对当前ToolInvocation、资源ACL、期限、目的地和参数摘要；
不能返回跨Run共用的永久token。真实例子见 standalone 的 `ecosystem_mcp.py`。
**当前SDK固定MCP 2026-07-28协议，旧initialize版本并不自动兼容**，本轮不修改该限制。

Skill配置路径：管理员审核 `SkillSpec` 的commit/子目录/逐文件摘要 →
`skill.install_public_git(...)` 经允许的HTTPS域名和强制出口代理安装 →
`SqlSkillBundleStore.install` + Registry.publish → `Agent.skills`绑定 →
`skill.make_script_handler`每次加载授权后的固定字节 → 受监督沙箱返回pending_execution。
脚本不能直接在API/Worker宿主执行。required_tools还必须同时获父任务授权；
`Agent.skills`本身不授予工具权限。开放Git URL和branch给模型不属于这个示例。

知识库由自己的检索服务提供版本固定的ResourceRef，再经SourceReader重新检查主体/
资源ACL。知识片段是数据，不是system指令。必需事实超预算明确失败，不静默截断。
`memory`仅演示验证引用，真实压缩接线见standalone的 `compression.build_compressed_context`，
包含SqlContextCompression、共享Blob、Model审计、预算及当前租约；不能用内存版本替代。

真实组合可选择 `--extra ecosystem`，配置受控的 `CENTN_ECOSYSTEM_CONFIG`，沿用
standalone README中的Broker/MCP/Git/OpenSandbox硬限额池要求。本目录不附带
外部服务或长期凭据，不声称只写一个URL就完成生产安全装配。

人工澄清可调用 `human.reply(host.runtime, actor, ...)`；审批使用
`host.approvals.resolve(actor, session_id, human.approval_decision(...))`，需当前
approval.required事件中的ID及修订，不能模型自批。Subagent真实调度使用delegate
profile，SQL原子派发、收集唤醒及预算隔离不由这个客户端重做。

## 验证边界

测试目录覆盖离线入口和负面路径。离线Provider、声明校验、Git字节验证都不计作
真实网络/SQL/沙箱执行验收。测试不操作既有数据库，不启动现有应用，不更新已冻结
的0.1.0发行身份。是否连接真实服务以及结果必须单独记录。
