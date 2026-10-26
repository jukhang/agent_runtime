# DS-P05：独立 SDK 集成、示例与首个稳定发布

- owner: Codex
- status: ACCEPTED
- 开始：2026-10-19
- 前置：DS-P03 与 DS-P04 R01/R02/R03/R04 已接受；R05 A2A 经用户明确延期。
- 禁止修改现有 app/、web/ 的生产入口；所有演示宿主和 UI 位于独立 examples。

## 范围与退出条件

| 工作包 | 交付 | 必须验证 |
| --- | --- | --- |
| P05-01 | 标准 SDK 客户端/SQL 后端、宿主身份与资源端口、独立 API/Worker/Recovery 示例；可注册规划 profile | 从 wheel 独立安装，不 import app/Agno；SQL Admission、当前授权、真实模型/工具、等待、控制和接管 |
| P05-02 | 框架无关事件 reducer、独立示例 UI、POST 受理/GET 流、快照恢复 | 去重、缺口重同步、正文/思考隔离、动态计划、工具/产物、失败下一步；真实浏览器与刷新/后台恢复测试 |
| P05-03 | examples 场景测试与发布报告、wheel/sdist/校验和、安装与升级说明 | 问答、资料问答、多轮、Plan/Execute、thinking、MCP/Skill/沙箱、审批/取消/恢复、1/2/4/8 并发、上下文/预算/故障、语义与产物验证 |

## 强制边界

1. SDK 不读取项目 .env、不构造全局客户端、不隐式建库；宿主显式配置并拥有生命周期。
2. 用户身份由宿主认证解析，不接受请求正文自报身份。所有读取、命令和领取重新校验权限。
3. API 只提交 SQL 意图及读取 Journal；执行必须由独立 Worker 领取，不依赖 API 进程内任务。
4. 快照与水位同一 MVCC 事务；受限事件不可公开。游标是十进制字符串，不转 JS Number。
5. UI 是测试/开发示例，不是对现有产品的替换；不能用 mock UI 截图代替真实后端组合验收。
6. 现有 R03 冻结清单作为前置历史快照保留；P05 修改另建交付快照，不追写旧清单。
7. A2A 不纳入首版功能声明，不以其延期阻塞独立集成；DS-P04-03 整包仍未关闭。
8. 不向 PyPI 或其他外部仓库自动上传。首版首先交付可安装的本地发行包；名称、许可、
   对外发布目标和凭据须经维护者确认，未确认不得冒充外部已发布。

## 接口收敛

- `Runtime`：依赖注入 Admission 与 RunBackend，返回带受信 Actor 的 RunHandle；本身不执行 Loop。
- `SqlRunBackend`：Journal 订阅、RunResult 读取、控制提交、快照；以当前授权和持久状态为准。
- `SqlSessionStore`：受信宿主创建/列出本人 Session，不新增另一套表或数据库事实源。
- 示例入口显式导入宿主 factory；没有有效配置则拒绝启动，不能回落到静态答案或假 Worker。
- Profile 能力必须由工厂支持矩阵显式声明；未接入知识/规划/压缩时不得宣传可用。

### 本轮闭环实现规格

1. Human interaction 独立于 agent_message：请求含 session/run、expectedVersion、
   idempotencyKey 与非空有界文本，身份仅从宿主解析。只接收本人当前 AskUser 的
   waiting_user/message 等待，不允许写入其它 Run 或已取消/过期/已改变的等待。
2. 答复使用 completed 的 user_reply Timeline Item 与不可变 Blob；同事务锁定
   Slot/Run、重验授权、绑定当前问题、创建下一检查点、写 input.accepted 与 queued
   状态事件及 Outbox、创建唯一 advance_run。精确重试返回原 receipt，异载荷拒绝；
   不修改 inter-agent pending/applied 水位，不新增虚构 senderRun。
3. Context SourceReader 明确支持 interaction_input；下一轮从持久用户答复和问题
   构造上下文，Worker 不依赖 API 内存。AskUser 与答复均可刷新恢复。
4. Snapshot 必须恢复工具、产物、等待请求及完整失败/下一步；投影与客户端共用
   语义。result 轮询只检查授权与目标 Run 终态，终态后才组装历史结果。
5. 完整 profile 分离初始规划、native answer/tool 和回答审核；Plan/Delegate/Wait
   身份与版本来自宿主事实。权限/预算仍在 SQL 提交与 ToolHost 重验，不能由模型生成。
6. 真实组合与并发门禁维持原范围；无法通过的项保留明确失败证据，不降级为 mock。
7. Planning profile 使用独立 initial/review 输出 schema。增加同步只读候选校验端口，
   在模型完整输出之后、DecisionStore 持久化之前校验宿主绑定的 Plan/Step 身份、
   版本、证据、子任务预算及已注册等待条件。Provider 忽略 JSON Schema 时也不得绕过。
   native 回答和业务工具保持原流式通道；现有 SQL CAS/ACL/预算仍是最终授权。
8. 回答审核是内部工作，不是新的用户目标，也不能要求用户为 Runtime 评判答案。
   审核请求显式标识 operation/reviewer/待核验任务/封存输出；原始需求与澄清仍是
   持久 Context 来源。只允许真实缺失的任务事实触发 AskUser，不通过关键词拦截。
9. 物化投影使用现有 projection + immutable context_blob，不新增快照事实源。
   授权版本、水位、内容摘要、投影版本与尾部连续性全部校验；缓存保存终态事件
   与 item→run 归属。GC 保留 projection.state_ref。缓存不兼容/损坏/缺失时拒绝，
   不能以空历史替代。创建物化快照不代表允许裁剪执行与审计需要的 Journal。
10. UI 必须让用户选择回复哪个等待或发送新任务；原文网络重试沿用同一请求身份。
    不能因存在待答问题就静默吞并新的目标。重同步后删除已不在授权快照中的 DOM。
11. 澄清通过 native 决策阶段的保留 Runtime 控制定义提交，映射为 AskUser，而非
    ToolHost 外部副作用。控制定义绑定 Context Snapshot、完整模型终结和参数校验；
    不可与业务工具同批调用，不消耗业务工具额度。保留引用不可由宿主业务工具覆盖。
    默认回答后审核只允许 Finish（success/partial）；不再在该阶段要求模型二次猜测
    是否应该向用户提问。规划 profile 可在其明确的决策阶段保留 AskUser。

## 状态收口

### 最终发行复验（2026-10-22）

- 实施外部证据等待、连续通知水位、子终态回传/封存、暂停安全点、取消收敛与
  实际预算对账；主结果汇总根/子模型用量，未知消耗继续预留，不伪报退款。
- 真实故障修复含：预备检查点与失败检查点的表示版本冲突、模型绝对读取期限、
  下游发布拒绝时显式关闭模型流、已声明零分片消息的空通道封存。完成审核在计划
  确定关闭且无新输入/宿主待办时仅允许 Finish，模型仍可判 partial。
- 完整执行源码、示例、测试、Skill、配置及依赖在回归期间保持冻结；执行记录绑定
  executionId、前后源码摘要和本轮 XML 摘要。旧记录/失败/中断/skip 均不替代通过。
- 最后允许仅更新两份 README 的发行状态说明，必须人工审阅实际差异、记录前后
  摘要；不得添加、删除或变更其他包文件。随后重新构建并实跑仓库外安装，绑定
  新 wheel 摘要。最终 manifest 仍覆盖完整包源码、发行说明、证据和产物。
- 首版是本地 SDK 发行，不上传外部仓库、不创建 Git tag，也不迁入现有 app/web。
  全部要求通过前仍保持 IN_PROGRESS，最终结果以本轮 review/release-matrix 为准。

### R05 与最终发行执行窗口（2026-10-21）

- R05 保持 IN_PROGRESS。新增 `external` Wait 类型，等待条件引用必须由宿主绑定；
  SQL Waker 仅通过显式注册、事务内检查的证据端口唤醒，不把 HTTP 通知或模型参数
  当成完成事实。不配置端口时拒绝；仍校验 Slot、当前授权、控制字段、Run 版本和期限。
- 独立宿主提供受认证的外部结果提交示例，使用不可变宿主资源保存结果；维护进程
  从 SQL 重新扫描，API/Worker/维护进程重启不丢失信号，重复通知不能多派工作。
- Delegate 补齐失败/取消通知消费及终态子任务预算维护对账。确定用量仅结算一次；
  未知用量保留预留及明确的人工核对提示，不伪报零消耗或自动重放。
- 必须真实验证等待前/后通知、重启、重复及错误身份通知、等待取消，子任务成功/
  失败/取消及零活动遗留。随后冻结源码统一复跑已有发布矩阵，不无限增加新功能。
- 最终发行须经代码审核、同快照证据核对、摘要封印和仓库外安装验证；任一必需项
  未通过则保留 IN_PROGRESS，不因已有候选 wheel 改写为 ACCEPTED。

### 本次三项关闭规格（2026-10-20）

1. `approval.updated` 是同事务写入的终态决议事件，含审批引用/修订、终态、到期时间
   和公开原因。人工决议、过期、Run 终态和撤销授权清理均 CAS 更新 Approval，并写
   Journal/Outbox。清理每件独立事务、Slot→Run→Approval→Session 加锁；已决议不可覆盖。
   实时和历史使用相同 reducer；没有纯 SQL 轮询补历史的第二展示事实源。
2. 生态示例只能注册实际安装的 MCP、固定 Git commit Skill 与沙箱工具。模型产生
   调用，ToolHost/SQL 授权、Broker 出口、独立维护进程、封存和图表下载全链验证。
   不允许把直连演示、预生成图表或已有组件探针当作本项通过。
3. 压缩只针对可选历史；摘要必须有原来源版本/摘要版本/预算和模型审计，重读仍检查
   原 ACL，必要事实不压缩。发布故障与性能以独立进程、真实模型和冻结源码身份验证，
   记录首 Delta、完成时延、失败及清理。未通过项不得签认 ACCEPTED 或稳定发行。
4. 沙箱恢复后的模型 Observation 必须使用统一 `ToolTranscript(call,outcome)`，不得
   混入执行元数据而破坏严格解析。executionId/generation/原始收集结果保留在独立
   collection Blob 与 SQL 执行账本，摘要/引用投影仍由 ToolSpec 决定，不暴露私有数据。
5. Provider 上下文超限优先识别结构化错误码。本地 Qwen/vLLM 只给通用 400 时，
   允许显式适配器使用有界、完整的厂商错误语法，校验 type/code、输入/输出/总量及
   上下文上限的数值关系；不以 message 中出现 context 等词作为依据。未知格式仍拒绝
   为普通错误；缺少用量数据仍记 unknown，不以“拒绝请求”擅自推断零计费。

### 剩余项本次执行清单

| 子项 | 本次交付边界 | 验收 |
| --- | --- | --- |
| S01 资料与资源 | 独立宿主拥有资源元数据/不可变内容，独立 Alembic schema；SDK 按 claim 注入 Context。文本资料/知识来源可上传、版本固定、预览、下载、撤销 | 双主体/跨 Session/错版本/撤销拒绝；真实资料问答；预览与下载摘要一致 |
| S02 委托/等待/审批 | 安装真实子 Agent、隔离预算/资源、子 Worker 和维护进程；审批决议与等待结果来自 SQL | 重启后继续、重复决议、撤权、取消优先和持久唤醒；UI 不伪造批准 |
| S03 生态组合 | 独立宿主注册受控 MCP、在线 Skill 和 OpenSandbox，真实模型选择与执行 | 验证工具调用、报告/图表真实内容、封存/权限和执行资源清理 |
| S04 发布矩阵 | 上下文压缩/预算/超时/恢复/硬崩溃，以及 SDK 独立安装、源码 review | 真实模型+独立进程证据；不以 skip 或历史组件测试代替；最后才冻结稳定发行 |

资料宿主不把附件伪装为 Artifact/Task/Run：使用宿主独立 schema，不修改 Runtime
表或现有 app/web。此处的知识来源示例是显式资料端口，不声称实现完整 RAG 索引。
创建资源绑定受信用户和 Session；内容、摘要与 MIME 固定，撤销与读取以 SQL 行锁
串行化，效果提交中的读取使用同一事务。预览为纯文本，下载不接受客户端路径。

### 2026-10-20 实际交付与边界

- S01 已通过资料 CRUD/ACL/版本/摘要、真实问答和 Chrome 预览下载/移动端验证。
  独立宿主迁移 `6a25eb5de03f` 创建 `<runtime_schema>_host.resource`；拒绝其它角色
  或含无关表的既有 schema，不修改 Runtime 标准表。文本上限 1 MiB、列表上限
  100 件，当前不提供 PDF/OCR 或完整知识检索索引。
- `MainHostPorts.context` 注入完整 `SqlContextComponents`，工厂和主 Worker 共享
  同一实例。修复 Provider 已使用宿主资料端口，但 Worker 仍另建默认 Context 的
  装配错误；已有默认行为保持兼容。单测和真实资料问答均覆盖。
- S02 增加 delegate 独立复核 profile 与 child/maintenance 进程。TaskPackage、
  接受契约、预算分配及截止时间由宿主绑定；子工具额度为零。收集核对真实输出
  所属子 Run、版本、Provider 封存，并在 SQL 事务内结算和唤醒。结构验收不代表
  结论正确；最终真实模型回答已核对。此处仅覆盖子结果消息等待，不开放任意外部 Wait。
- save-report 为受审批保护的事务型写工具，资源内容与操作账本同事务提交。
  审批绑定完整 Context 中实际使用的来源和版本；读取/批准/执行均重新检查 ACL。
  人工审批最长 15 分钟且不超过 Run 截止；单次执行仍为 10 秒，二者不得混用。
  维护 Worker 在 API/主 Worker 退出后仍能根据持久决议唤醒。
- 审批 UI 已改为 Journal 的 required/updated 事件与授权快照驱动，支持批准/拒绝
  和精确网络重试；决议按 ID 定点查询，不依赖列表分页。迁移 `0009_approval_events`
  仅扩展事件 CHECK；审批决议/到期/任务结束或授权失效清理与事件、Outbox 同事务。
  迟到的 required 不能重新激活终态卡片。报告是宿主资源，不伪造 SDK Artifact 事件。
- S03 已完成新独立宿主中 MCP→固定 commit 在线 Git Skill→OpenSandbox→PNG/报告
  完整真实模型任务及桌面/移动预览下载；宿主新增 allocation 迁移 `7fb888270332`。
  intent 先写、独立维护 inventory 与租约/CAS 收口 create_ack/recorded_ack 两个真实
  SIGKILL 窗口，TTL 前清理、无重派及零残留；未知无 ID 分配继续留账，不虚报不存在。
- S04 已实现并接线真正任务相关逐字摘录压缩；必须来源不压缩，摘要绑定原版本、
  真实渲染请求/模型审计/预算，恢复重验 ACL/内容/删除。真实压缩、七类组件故障与
  四类主链故障、输出/SQL 写入/外部分配 SIGKILL 及 15 会话 SSE 时延基线均有实测。
  全局剩余项和准确的组合边界见 `agent-runtime-p05-release-matrix.md`。
- 独立交叉 review 发现来源撤权未主动取消 pending 审批，已新增事务内
  ApprovalResourceValidity 端口并由宿主明确注入；当前 grant/Run/Session 修订/资源失效
  转 cancelled 并发事件。只确定拒绝收口，未知依赖/DB 错误回滚；有效 pending 的
  updated_at 用作持久轮转水位，避免固定首批饥饿。11 SQL＋1 真实撤权自动收口通过。

### 当前交付状态

| 工作包 | 当前状态 | 未关闭的关键门槛 |
| --- | --- | --- |
| P05-01 | ACCEPTED | 外部 Wait/Delegate/控制/预算/恢复及 SQL57 通过；未知用量保留账目 |
| P05-02 | ACCEPTED | Chrome 流式/刷新/移动端、资料与审批回放完成同源码验证 |
| P05-03 | ACCEPTED | 同源码真实42项、SQL57、离线1440/397 skip、安装及15会话基线和发行门禁通过 |

历史 `output/p05/dist/` 不作为本次发行包。最终本地发行包在
`output/p05/release-a8d767f711bd/dist/`；实际范围、源码身份、包摘要及非阻塞后续项
见 `agent-runtime-p05-release-acceptance.md`。没有上传或打 tag。

### 联调发现与修复规格

- 示例只读策略不能返回空 evidenceRefs；绑定本轮 turn_input 并经现有 SQL ACL 重验。
- 长答案路径存在逐 token 同步 SQL 背压。增加显式批量 record_many，单批仍受
  ProducerBatch 64 条/64 KiB 契约限制；主输出首通道首片立即提交，后续按数量、
  字节和分片到达时的时间间隔刷入。Provider 终结和输出封存前强制 flush。
  不创建进程外所有权、不把未提交缓冲发布给浏览器、不用前端播放动画冒充流式。
  中断时未提交缓冲不能被宣称为已持久输出；恢复只使用已提交的 Journal。
- StreamingResponse 断开时，短 SQL 读取/连接归还须屏蔽重复取消，并配置语句时限；
  不通过吞掉数据库错误来伪造空事件页。
- 真实 thinking 联调发现：公开读取过滤受限思考，但 started/completed 仍包含其通道和
  摘要，导致公开快照不能验证。增加一致的展示视图：默认公开视图同步裁剪思考声明、
  摘要和水位；显式 include_analysis 由宿主固定配置且每次按受限读取重新授权，只增加
  analysis.delta，不把 model.prepared、凭据等内部事件带到 UI。实时与快照复用该投影，
  不改写原始 Journal、producer identity 或持久摘要。

逐包 IN_PROGRESS → READY_FOR_REVIEW → 独立复跑/review → ACCEPTED。
只有三个工作包全部满足要求，才能签认首个稳定发行版；缺少真实服务的项记为未验证，
不以离线 skip、既有探针或重命名版本替代。本轮数据库测试仅使用 aletai_wangxh 随机 schema。

### 2026-10-23 最终验收窗口

- 本轮范围仍为外部 Wait、Delegate 失败/取消/收集/预算以及同源码发布验证，不扩展
  A2A 或现有 app/web 迁移。状态保持 IN_PROGRESS，最终以发行验收报告和门禁为准。
- 补齐固定目标 Agent 的运行期限：spawn 事务内取请求、父任务和 Agent 预算的交集，
  Task/Run/派发记录/工作项/授权一致，重领不延长；期限失败与用户取消分开回传。
- 最后 strict mypy 发现 ChildErrorSink 回调关键字参数不匹配，修复为协议要求的
  `lease`。这是类型契约修复，不改运行语义；修复前的验证不直接继承到最终快照。
- `output/p05/release-20261023/` 为本轮最终执行目录。执行记录绑定完整包源码的
  前后 SHA-256、独立 executionId、退出码和 XML/安装回执摘要。测试开始后不再
  修改包内 README、实现、测试或依赖；本轮不使用 README 状态修改例外。
- 独立审查保留历史失败及修复记录；所有必需场景、实际包内容与仓库外安装都通过
  后才允许签认。`output/p05/final-20261023/` 中被主动中断或类型修复前的结果仅
  作诊断，不替代最终发行证据。
