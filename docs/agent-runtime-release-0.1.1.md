# Agent Runtime 0.1.1 发布记录

状态：发布验证 ACCEPTED。目标：公开仓库 `agent_runtime`，Apache-2.0；
远端PUBLISHED状态以实际仓库、commit、tag和Release及附带验收摘要为准。
不发布PyPI，不上传现有业务仓库历史、业务app/web、根环境配置或原始对话数据。

## 版本范围

- 保留已验收0.1.0发行物及其历史清单，不覆盖旧包。0.1.1为新增Python示例、
  独立UI及发布工程化的增量；另修复模型调用入账的并发唯一索引冲突，SQL迁移未变。
- 后端示例覆盖Agent、Tool、Context、Loop、MCP、Skill、知识来源、历史压缩、
  人工交互、子任务与纯Python SQL客户端/Worker。
- 独立UI使用左右气泡、IME安全Enter发送、折叠思考及Journal增量呈现。
- Apache-2.0许可证随核心与示例源码/发行物提供；第三方依赖仍遵守各自许可证。
- 0.x版本不承诺未声明的跨协议兼容或任意外部服务认证。A2A、现有产品迁入、
  任意MCP版本和公网Skill认证不属于本次发行验收。

## 独立审核返修

1. Python客户端stream失败后，必须等待所有监听/结果任务清理再关闭宿主客户端。
2. 两个示例CLI均接受SDK的`up-to-date`，重复迁移继续执行宿主资源迁移。
3. 提交被拒绝时UI恢复当前Journal事实状态，不残留虚假“正在分析”。
4. 独立公开仓库保持`packages/agent-runtime`布局，README命令无需重写；Git HTTPS
   测试所需的`runtime_skill_git_probe`及其依赖随白名单导出，干净检出执行测试。

## 发布门槛

- 源码、锁文件、依赖配置固定后，复跑离线核心/示例/Node/类型/Schema检查，
  并执行实际SQL恢复回归。跳过项目逐项记录，不算外部验收通过。
- Runtime核心历史真实组合证据只归属于原快照；本轮不声称所有历史42场景均重跑。
  增量实际结果、外部未验证项与历史继承理由分别记载。
- 从白名单导出的干净独立目录重新收集测试、构建wheel/sdist，在仓库外安装验证
  包内容、许可证、Schema/迁移/UI资源和Python示例。不得引用原仓库app或agno。
- 检查源码及发行物不包含私有环境文件、已知真实凭据、调试日志或本机缓存；
  Git公开仓库从新历史初始化，不推送原业务仓库历史。
- 源码commit、安装结果、发行物摘要与冻结清单绑定后才创建正式GitHub Release；
  若账号未授权或网络阻断，明确保留待发布状态，不假称已经上传。

## 运行要求

Python 3.12+；核心安装仅需Pydantic/tzdata。数据库持久化启用storage extra，
需要PostgreSQL；独立API、主Worker、maintenance分进程运行，delegate再启子Worker。
Provider、MCP、Git出口、OpenSandbox及凭据端口需要宿主独立配置；详见部署文档。
首响与完成时延受模型、并发和资源影响，不承诺无限任务或所有问题语义正确。

## 本次实际验证（2026-10-26）

| 门禁 | 实际结果 |
| --- | --- |
| 独立导出仓库离线核心 | 1440 passed，397 skipped；零失败/错误 |
| 真实SQL恢复/取消/预算/工具回归 | 57 passed，零跳过，1143.33秒；仅aletai_wangxh随机隔离schema |
| Python后端示例及返修 | 24 passed，零跳过 |
| 独立宿主静态资源/重复迁移/依赖故障 | 16 passed，零跳过；依赖库有一项弃用warning |
| Node Journal/IME/提交拒绝 | 19 passed |
| 静态与契约 | Ruff、strict mypy224文件、Schema/storage无漂移通过 |
| 三发行物仓库外安装 | 通过；SDK382/宿主32/Python示例12个包文件与冻结源码逐字节一致 |
| 公开导出敏感值检查 | 605份源码文件与本地4个长敏感配置值比对，零命中；不上传配置本身 |

SQL执行identity：`a1bafa2b-7930-4c25-af68-5dad89c15c8e`；该次执行源码前后相等。
后续CI修正旧head断言、冻结表清单和授权时间测试数据，并发现模型入账并发缺陷；
已修复并增强同一测试中的八轮竞态回归。迁移未变，不将早先完整文件映射
冒称为最终源码映射；最终源码的远端SQL作业包含这57项回归。
该次完整SDK文件映射的SHA-256（JSON排序键、紧凑编码）为
`b28940a620bcb746a7607e426377cc5d986ceb8529a4f600f247384dfed4f1d3`。
397个离线跳过是外部依赖/显式opt-in，不计外部通过。原矩阵真实模型/浏览器/
OpenSandbox组合未在此增量全部重跑，其证据属于原0.1.0快照，不混作本轮实测。

初次SQL执行因默认-k表达式将57项全部deselect，未计通过；已在新目录完整重跑。
官方CLI设备登录遇到网络超时/EOF，不据此声称已创建仓库。采用同官方公开OAuth应用、
同GitHub域名、保持TLS校验的替代传输；授权由用户亲自在浏览器完成，凭据不进入源码。

独立复审确认返修关闭、导出仓库可独立检出及安装；CI配置显式开启严格mypy后需
更新最终manifest。原始测试、安装回执留在本地，不上传可能含环境诊断的日志；
公开发布附SHA256SUMS与脱敏验收摘要，源码commit与最终文件摘要另行冻结绑定。

### 远端CI发现与复验

初次PostgreSQL CI发现七个旧测试仍把head断言为0008，已改为0009；
扩展回归发现冻结表清单遗漏三个凭据表，以及过期授权测试在快速数据库中
可能将expires_at设在issued_at之前。修正清单并构造合法历史签发时间，
不修改Runtime、迁移或数据库约束，不删除测试、不把失败改为跳过。
随后同一模型调用并发入账偶发撞到作用域唯一索引：改为覆盖所有唯一冲突的
DO NOTHING，仍按完整请求身份校验已有记录。保留冲突拒绝和唯一赢家断言，
将并发开始测试扩为八轮；此项确实修改Runtime，不能归为纯测试修正。
最终远端CI链接、逐项结果及其源码commit由Release验收摘要绑定；
本地57项证据保留原快照身份，不取代最终CI验证。

### SDK发行物摘要

- `agent_runtime-0.1.1-py3-none-any.whl`：
  `585876431e3b57d32f536e748409c3ca508b5dd25af909babc6bce9c94a1aade`。
- `centn_runtime_example-0.1.1-py3-none-any.whl`：
  `487566c4a494fcec44e5dd268649143d1eaedb77147c16752db0f558169b0bc9`。
- `centn_runtime_python_examples-0.1.1-py3-none-any.whl`：
  `8016e7ce6e987826f73eaf017941858e27e35ccb6894c97bdc3f6f95f22454f9`。

其他发行物摘要及其文件名以Release所附SHA256SUMS为准。旧0.1.0包和清单未覆盖。
