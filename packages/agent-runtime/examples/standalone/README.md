# 独立 Runtime 宿主示例

此目录是 SDK 的开发/验收示例，不导入现有 app，也不接入现有前端。API、Worker
分别运行，共享 Runtime 标准 PostgreSQL 表；关闭浏览器不会取消持久任务。

## 启动

从本目录执行 `uv sync`。配置以下环境变量，凭据请通过部署环境或私有凭据文件注入，
不要提交到仓库、命令行历史或浏览器 URL。

| 变量 | 含义 |
| --- | --- |
| CENTN_DATABASE_URL | postgresql+asyncpg 数据库连接 |
| CENTN_SCHEMA | 明确的隔离 schema；迁移仅作用于该 schema |
| CENTN_SCOPE / CENTN_ACTOR | 示例宿主管理的唯一受信主体；不接受请求自报身份 |
| CENTN_HOST_TOKEN | 至少 32 字符的随机开发令牌 |
| CENTN_PROVIDER_URL / CENTN_PROVIDER_KEY / CENTN_MODEL | 真实模型连接；本示例装配 Qwen 兼容适配器 |
| CENTN_THINKING | true 或 false，默认 false |
| CENTN_AGENT_PROFILE | conversation（默认）、planner 或 delegate；所有进程必须使用相同配置 |
| CENTN_CONTEXT_INPUT_TOKENS | Context 输入预算，默认 100000，上界 200000 |
| CENTN_COMPRESSION_MAX_INPUT_TOKENS | 单次可选历史压缩输入上界，默认 200000，上界 240000 |
| CENTN_COMPRESSION_TIMEOUT_SECONDS | 单次压缩时限，默认 90 秒，上界 240 秒 |

```bash
# 初始化 Runtime schema 和独立的 <CENTN_SCHEMA>_host 资料 schema，不创建数据库
uv run python -m centn_example migrate

# 三个独立终端/进程；不要在 API 内嵌 Worker
uv run python -m centn_example api --host 127.0.0.1 --port 8790
uv run python -m centn_example worker
uv run python -m centn_example maintenance

# delegate profile 额外运行独立子 Worker
uv run python -m centn_example child

# 可选：创建授权的物化投影，不删除任何历史
uv run python -m centn_example snapshot --session <session UUID>
```

浏览器访问 http://127.0.0.1:8790，输入宿主开发令牌。令牌只保留在页面内存，刷新后
需要重新输入；会话状态从授权快照恢复，不依赖 localStorage 缓存答案。

复制到其他项目时，先安装交付的 SDK wheel，再将本目录作为单独宿主包安装。
`tool.uv.sources` 是仓库内开发的相对路径绑定；仓库外使用发行包时删除该绑定，
显式安装本地 wheel 或已签认的私有索引版本，不要意外解析到同名第三方包。
也可使用 `uv pip install --no-sources ...` 忽略仓库内的 source 绑定。

## 验证

```bash
node --test tests/journal.test.mjs

# 真库/真模型/独立进程及浏览器；仅允许 aletai_wangxh，随机 schema 自动清理
uv run playwright install chromium
CENTN_LIVE=1 CENTN_SCREENSHOT_DIR=/absolute/project/output/playwright/p05 \
  uv run pytest tests/test_live.py -q
```

若使用已安装的 Chrome，设置 `CENTN_BROWSER_CHANNEL=chrome`。测试截图必须位于
项目 `output/playwright/`；截图、日志、数据库内容和测试令牌不能随 SDK 发布。

## MCP / Git Skill / OpenSandbox

生态装配为显式可选部署：`uv sync --extra ecosystem`，所有进程设置同一个
`CENTN_ECOSYSTEM_CONFIG` 私有 JSON 文件路径。未配置不注册生态工具；配置错误拒绝
启动，不降级为本机脚本。该文件包含 `mcpPort`、`mcpCa`、`mcpToken`、
`sandboxDomain`、`sandboxKey`、`gitUrl`、`gitProxy`、`gitCa`、固定 `skill` manifest
和唯一 `deploymentId`，令牌必须服务端私密注入，文件建议权限 0600。

当前示例把 MCP TLS 出口固定在 `egress.test` 的受控 loopback 地址，使用不同的
Broker destination 分离 discovery/catalog 与 read_metrics 调用。Git 必须经显式
CONNECT 代理，固定 commit、子目录和逐文件摘要；不能传入模型生成的 URL 或命令。
Skill 依赖绑定 `example.read-metrics@1`，运行入口固定为 `run.py`，模型只提供一份
与本 Run 已封存 MCP 数据完全一致的 JSON 参数。

OpenSandbox 使用独立、专属的 server 0.2.3 / SDK 0.1.16 硬限额池；服务器必须安装
`agent_runtime.sandbox.docker_profile` 与本例 `ecosystem.LIMITS` 相同的配置。
不允许用无硬限额的通用服务替代。固定镜像、只读根、非 root 用户程序、默认断网、
CPU/内存/PID、tmpfs 大小与产物 inode 限制均由该池执行。维护进程是必需依赖，
它重连沙箱、验证用户进程树已停、封存有界产物、持久唤醒主 Worker 后删除沙箱。

封存报告和 PNG 使用现有独立宿主 `report` 资源，不新增 Runtime 表或 Artifact
事件。PNG 预览返回 `image/png`，纯文本报告仍返回文本；预览/下载/撤销都重新检查
主体、Session、版本和内容摘要。图表由实际 Skill 根据输入绘制，非静态演示图片。

从仓库根目录执行完整真实验收（凭据仅此 driver 显式读取根 `.env`）：

```bash
uv run --with-editable './packages/agent-runtime/examples/standalone[ecosystem]' \
  --script scripts/verify_runtime_p05_ecosystem.py --output output/p05/ecosystem-new-run
```

该 driver 启动真实私有 TLS MCP 数据服务和受控在线 HTTPS Git 仓库，临时提交只在
验收目录生成，不提交工作仓库；它验证在线安装机制，不宣称第三方公网仓库已验证。
使用真实 Qwen、独立 API/主/维护进程、`aletai_wangxh` 随机 schema 和真实 Docker
沙箱。检查数据/报告算术、PNG 像素、Chrome 桌面/移动预览下载、ACL/撤销以及实际清理。
历史组件探针不会被计入本项通过。详细结果与已知边界见
`docs/agent-runtime-p05-ecosystem.md`。

额外传 `--include-faults` 会运行 create ack 丢失和已记 ID/未提交 execution 两个真实
SIGKILL 窗口，停止 API 后由独立维护 inventory 收敛，随后重启并检查不重派。
allocation intent 使用独立宿主表和迁移，在任何远端 create 之前持久化；TTL 不是
这两项故障验收的通过依据。缺少 ledger 迁移时生态宿主必须拒绝执行。

主 Worker 的 Context 工厂默认装配实际 SQL 历史压缩，仅在可选历史超出预算时触发；
当前输入、资料和工具事实仍为必需来源。压缩沿用宿主拥有的真实客户端，不读取根环境
文件、不创建第二套模型凭据或绕过预算；数量配置必须在所有进程保持一致。

## 当前边界

- 示例认证是单操作员开发令牌，不是生产用户系统；替换为宿主的身份/授权端口。
- 计算工具经过真实 ToolHost、SQL ACL、预算与共享容量。没有沙箱/MCP 配置时不
  注册这些工具，不制造伪造图表或固定工具答案。
- 默认 Agent 是 native answer/tool/clarification + finish 审核；完整 Plan/Delegate/Wait
  中，planner 已装配初始计划、执行和计划收尾。delegate 是明确选择的独立复核工作流：
  宿主安装 reviewer、提供唯一版本固定的 TaskPackage，主 Loop 提交 Delegate 后持久等待，
  子 Worker 使用独立预算且无工具授权，封存后唤醒主 Worker，核对输出归属/版本/封存并
  结算预算，再由主模型形成回答。结构验收不代表结论正确；任意 timer 不在本示例范围。
- `CENTN_AGENT_PROFILE=external_wait` 安装外部完成等待示例。模型只能选择宿主生成的
  WaitSpec；向 `POST /api/sessions/{sessionId}/runs/{runId}/external-result` 提交
  `{"text":"外部任务的真实完成结果"}`（沿用宿主 Bearer 身份）。结果保存为该 Run
  唯一的不可变资料资源；相同内容重试复用，异内容拒绝，不能跨 Session 或终态写入。
  独立 maintenance 重新检查当前授权及完成事实后 CAS 唤醒；API/Worker 重启不会丢失
  通知。没有结果也轮转检查权限与期限，失效明确失败并给出下一步，不保持永久等待。
- 子失败/取消和孤儿恢复不能自动重派同一委托。maintenance 对终态子任务按模型审计
  对账，已知消耗只结算一次；未知用量保留预留供人工核实，不假退款。取消父任务
  必须先确认所有子任务停止，根结果用量汇总子调用，不能隐瞒 unknown。
- 子执行期限由请求、父任务及固定目标 Agent 的时间预算取交集，持久化后不随续租
  延长。暂停是安全点控制，不承诺立即抢占已领取的子模型调用；验收等待由实际子
  期限加收敛余量计算，并检查 SQL 各记录期限一致。
- 明确的依赖连接故障返回 503/DEPENDENCY_UNAVAILABLE、Retry-After 和下一步提示，
  不把读取失败变成 Run 失败。提交重试必须保留原幂等键，不新建重复任务；SSE
  已发送响应头后断开仍走原游标重连，不伪造终态事件。
- 文本资料上传/预览/下载/撤销使用独立宿主表与 Alembic 迁移，受当前 Session ACL、
  主体、版本和摘要约束；知识来源是显式 SourceReader 示例，不是完整 RAG 产品。
  仅支持 UTF-8 文本、每件最多 1 MiB，列表最多 100 件；不接受本机路径或远程 URL。
- save-report 是实际数据库写入工具。人工审批有效期最多 15 分钟且不超过 Run 截止时间，
  不沿用工具的 10 秒执行超时。报告与 ToolHost 操作账本同事务提交；审批绑定原参数、
  操作身份和模型所见资料版本。拒绝/撤权不保存报告，维护 Worker 负责崩溃后持久唤醒。
  审批展示来自 Journal 的 approval.required/approval.updated 事实，实时与历史回放一致，
  不另外轮询 SQL 审批列表。报告是宿主资源，可预览下载，不伪造 SDK Artifact 事件。
- POST `/api/interactions` 接受版本绑定的澄清答复；UI 可显式选择回复问题或新任务，
  网络失败后原文重试保留幂等身份。刷新会恢复问题、工具、产物元数据及失败详情。
- SQL 快照支持物化前缀加增量尾部；旧授权或缺失/损坏前缀会明确拒绝。物化不是
  清理许可，开启裁剪前须单独验证执行、审计与恢复所需的全部保留政策。
- 新增 opt-in 真实测试：human reply + Worker 重启、planner 工具/计划关闭、暂停/
  恢复/生成中取消、1/2/4/8 并发。planner 测试须设置 `CENTN_AGENT_PROFILE=planner`；
  会话并发测试使用 conversation。不要把被 skip 的 profile 场景计为验收通过。
- 默认公开展示视图会移除受限思考及其结束摘要；启用思考的宿主使用显式授权视图，
  实时与历史均重新验证访问权限，内部模型/凭据事件不会被一并送到浏览器。
- 此示例的局部验收不等于 P05 稳定版发布；总状态见 `docs/agent-runtime-p05-plan.md`。
