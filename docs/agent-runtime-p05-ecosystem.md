# P05 S03 独立宿主生态组合

状态：READY_FOR_REVIEW（2026-10-20）。本线程范围完成，执行代码冻结。
仅覆盖独立 examples，不修改 app/web、审批或 SDK 核心协议；不代替全局稳定发行签认。

## 执行与安全约定

- 真实 Qwen 通过独立主 Worker 调用已安装 MCP 数据工具，再调用固定 Git commit
  Skill。MCP 必须经 CredentialBroker、SQL grant 和固定 TLS 出口，不直连模型网络。
- Skill 仅由受控 HTTPS Git/CONNECT 安装器获取，核对 commit、manifest、文件摘要；
  SQL Registry 发布和 Run Agent skills 绑定是执行前提。测试用受控在线 Git 服务
  不等同于已验证公网第三方 Skill 仓库，证据必须标明来源。
- 每次执行使用专属 OpenSandbox、固定镜像摘要、默认断网、非 root、只读根文件系统、
  CPU/内存/PID 和 workspace/artifact/log/inode 硬限额。模型只提供数据参数，不能提供命令。
- SQL ToolHost、当前 ACL、上下文来源、预算、容量和执行授权共同约束调用。
  独立 maintenance Worker 重连远端、检查进程树/资源、读取有界产物、封存、唤醒和删除。
- PNG/报告登记为独立宿主资源，不伪造 SDK Artifact 事件。登记必须与结果封存同事务；
  主体、Session、版本、撤销、摘要校验沿用资料资源规则。预览仅允许 image/png，
  文本报告使用纯文本；下载不接受远程 URL 或本机路径。
- 凭据只由验收 driver 私密加载根 .env 并显式传给进程。SDK/宿主不读取根 .env。
  真库仅 aletai_wangxh 的随机 schema；清理记录不能省略，禁止 aletai_dev。

## 验收要求

同一新宿主 API/主 Worker/维护 Worker 中完成真实模型任务。核对 MCP 调用与数据、
Skill 固定来源、SQL 工具账本、真实 PNG 解码/像素、报告数据、封存后预览下载摘要、
错主体/Session/版本/撤销拒绝，以及远端容器和临时 schema 清理。
组件探针历史通过结果不是本项通过。A2A 延期；不提交 Git，不修改历史 manifest。

## Allocation 闭环

远端 create 前，独立宿主 schema 事务内持久化唯一 operation allocation，重新验证
当前 Actor/Session/Run/claim、owner/epoch、截止和权限。远端 metadata 绑定 allocation、
部署、scope、Run、operation 及 claim；create ack 只补记唯一 ID，不允许旧 intent
重派。execution intent 与 allocation bind 使用同一 SQL 事务。

独立维护进程并行运行 SDK execution supervisor 与真实 OpenSandbox inventory。
SQL cleanup claim 先确认原 claim 已失效、取消/到期或对应 execution 已确认删除；
仍活跃的创建和仍受 execution 监督的沙箱不得抢清理。inventory 分页读取服务端，
完整匹配归属 metadata，删除后只接受 SDK typed 404 确认；网络错误不等于不存在。
丢 ack 可从 metadata 找回唯一 ID 并在 cleanup 状态补记，不能转回可派发状态。
没有 ID 且暂时空 inventory 的 unknown 继续对账，不虚报 cleaned。

真实故障验收在 create 返回/记 ID 前和记 ID/execution 提交前分别挂起，再 SIGKILL
主 Worker 并关闭 API。只保留维护进程，在远端 TTL 前确认删除和 SQL 清理；重启后
检查零 execution/零重派、持久失败和可执行恢复说明。两项已在最终联合轮实际通过。

## 实施记录

- 2026-10-20：读取 AGENTS、P05 plan/release matrix 和独立宿主 README。
  实际独立包路径为 packages/agent-runtime/examples/standalone。
- 不自行签认全局 ACCEPTED；本线程实际验收见下文，供主线程独立 review。
- 正常链：`ecosystem-run6` 1 passed / 184.71s，`ecosystem-final` 1 passed /
  94.77s。后者包含 SQL 模型观察 summary-only、无 PNG base64、篡改数据拒绝、
  PNG 解码/柱形像素、报告算术、真实 Chrome 桌面/移动预览下载、权限/撤销与正常清理。
  两轮均零容器/sidecar 残留、无 fallback、临时 schema 已清理。后续 allocation 与
  默认压缩接线修改必须重新跑，不能直接继承这些版本的通过。
- 已新增 ecosystem / ecosystem_catalog / ecosystem_config / ecosystem_mcp /
  ecosystem_inventory，报告 Skill 源资产、真实 TLS MCP/Git 服务 driver、正常与 SIGKILL
  验收文件。修改范围限独立宿主 host/config/resources/API/UI、README/依赖；ledger
  与独立宿主迁移也由本生态线程实现与验证，主线程未创建这些文件。
  未修改 app/web/审批 UI 事实源或历史 manifest。

## Review 边界与阻断

1. 原 create-before-intent 缺口已由宿主 allocation ledger/inventory 收口，并通过
   create ack/记 ID 两个窗口的真实 SIGKILL 验收，不以 TTL 或旧探针计通过。
   恢复语义是明确失败并要求查看已有事实/发起新任务，不承诺原 Jupyter/进程恢复。
   无 ID 且 inventory 暂空的未知分配继续留账，不虚报不存在；服务端永久丢失的
   unknown 需要人工核对，这是 fail-closed 边界，不会自动 create 或执行第二次。
2. 结果文件目前经 OpenSandbox SDK 完整读入后检查长度。内存有界性依赖专属池
   `/artifacts` 1 MiB tmpfs、16 件产物硬 inode 上限、受信 guard 的实际资源测量及
   用户进程树停止证据，不是客户端 streaming/range 限流。服务端未装配对应 profile
   不得部署此示例。安装只允许与本例已审阅 SKILL.md/run.py 完全一致的 Git 字节；
   不接受任意第三方脚本，未来扩展必须重新审阅读协议/压缩炸弹与额度边界。
3. 第三轮真实链发现 SDK 通用 collector 的 observation 元数据不兼容严格
   ToolTranscript。主线程已在 SDK storage/sandbox_results.py 修复：模型 observation
   直接由 ToolTranscript 生成，执行元数据仍独立保留 collection_ref 与 SQL 行；
   新增各 public result policy/失败路径严格解析回归。宿主临时规范化覆写已删除。
4. 正常路径完整组合、产物权限与清理，以及 allocation 两个崩溃窗口均已通过。
   本生态线程负责 ledger/迁移、分配接线、独立维护 inventory 与真实
   SIGKILL 验收。主线程仅负责审批、SDK collector 修复及其 effect recovery 验收。
   不从旧组件测试继承通过。

## 最终真实证据

```bash
uv run --with-editable './packages/agent-runtime/examples/standalone[ecosystem]' \
  --script scripts/verify_runtime_p05_ecosystem.py --include-faults \
  --output output/p05/ecosystem-verified
```

- `3 passed in 458.80s`，driver `status=passed`。真实 Qwen `qwen3.6-27b`，
  endpoint 为用户配置的私有部署（不随公开发行提供其地址）。MCP discovery/catalog/read 均通过
  SQL CredentialBroker；正常任务完成 1612/1373、85.17% 的真实数据报告与 PNG。
- 固定在线 Git commit `1e32916b8ec579de43c04ad1a654a3e3462e1e6f`，bundle digest
  `140b593b814bf002c1c4343235079d34e1dc905bcebcda7af86e60b0d55e1d93`。
  这是受控 HTTPS Git 服务，不宣称已验收公网第三方任意 Skill。
- 正常链检验实际 PNG 解码/元数据/柱形像素、报告算术、MCP 来源绑定和篡改拒绝、
  模型观察 summary-only（无图片 base64）、资源预览下载摘要、跨主体/Session/版本及
  撤销拒绝、allocation 跨 scope/错误 cleanup owner 拒绝与正常 ledger 清理。
- Chrome 桌面与移动预览、下载实际 PNG 通过。截图位于
  `output/playwright/p05-ecosystem/ecosystem-verified/`，不进入仓库根目录或发布包。
- `create_ack` / `recorded_ack` 两个真实 kill(-9) 窗口，API 停止后仅独立维护进程
  清理。包含重启验收的测量窗口分别为 43.49s / 48.80s，均在远端 TTL 前。
  两者只有一个 allocation，零 execution/重派；恢复结果为 `SIDE_EFFECT_UNKNOWN`，
  原 Provider 用量保留，公开恢复说明非空，不虚构成功或按零用量结算。
- 实际观测三个沙箱均满足只读根、CPU/内存/PID、tmpfs/inode、无 host bind、
  无控制凭据及 loopback 端口；最终容器=0、sidecar=0、fallback=false、server 已停止。
  三个不同的 `aletai_wangxh` 临时 schema 及其宿主 schema 全部清理，未连接 aletai_dev。
- 私有配置由 driver 显式生成并在退出时删除，根凭据只由 driver 私密加载，未输出密钥。

证据索引：`output/p05/ecosystem-verified/evidence.json`、`tests.xml`、`host-checks.json`、
`model-chart-observation.json`、`model-result.json`、`chart.png`、`report.json`、
`allocation-crash-create_ack.json`、`allocation-crash-recorded_ack.json`。
失败的迁移草稿、模型参数拒绝和 wrapper 缺口保留在旧轮目录，不计通过。

## 文件与回归

- 新增 `centn_example/ecosystem.py`、`ecosystem_catalog.py`、`ecosystem_config.py`、
  `ecosystem_mcp.py`、`ecosystem_inventory.py`、`ecosystem_allocations.py`、
  `ecosystem_assets/SKILL.md` / `run.py`。
- 独立宿主修改：`host.py`、`config.py`、`resources.py`、`resource_models.py`、
  `resource_migrate.py`、`api.py`、`ui/app.mjs`、`README.md`、`pyproject.toml` / `uv.lock`。
  审批仍使用 Journal state.approvals，未恢复 SQL 审批轮询。
- 本线程生成并验证独立宿主迁移 `7fb888270332`（down `6a25eb5de03f`），所有 schema
  均由显式 Alembic 配置动态解析。通过 driver 使用 Alembic autogenerate/upgrade，
  最终三份 fresh-host 随机 schema 均实际运行迁移；没有 SDK 迁移或手写建表 SQL。
- 新增 `test_ecosystem_contracts.py`、`test_ecosystem_live.py`、
  `test_ecosystem_crash_live.py`；未修改 `tests/test_live.py`。
- 新增根 scripts：`runtime_ecosystem_services.py`、`runtime_ecosystem_crash_host.py`、
  `verify_runtime_p05_ecosystem.py`。现有 P04 代码只复用真实 Git/服务启动基础设施，
  不使用其固定答案、volatile authority 或旧检查结果代替本项真实任务。
- 离线：`uv run --directory packages/agent-runtime/examples/standalone --extra ecosystem
  pytest tests/test_resource_contracts.py tests/test_ecosystem_contracts.py -q`：25 passed。
  Journal Node 回归：13 passed；修改文件 Ruff 与 compileall 通过。
- 默认宿主已接入 Boole 的实际压缩工厂；本线程只负责 host/config 数量接线，
  压缩长历史/模型审计验收仍以其独立报告为准，不用本轮短任务替代。
- 未提交 Git、未改历史 manifest、未构建/上传稳定发行包。全局回归与发行冻结由主线程执行。
