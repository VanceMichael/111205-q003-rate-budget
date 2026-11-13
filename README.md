# 合作方异步导出接口的分片采集

## 现有能力

现有截止时间判断、假 API 响应器和异步导出模拟器可独立运行。

`service.py` 保持已有规范化接口。根目录 `test_service.py` 为原有回归测试，`tests/test_baseline.py` 会同时加载原测试和离线演示验证。

## 新增的导出采集流程

异步导出不再被当作一次性请求。业务提交的是**采集意图**：

- 业务身份 = 租户 + 规范化后的筛选条件（`export_store.intent_key`）。
- 合作方任务号（`E1`…）与下载地址只是意图的属性，不是身份。
- 截止时间属于意图，不属于身份；同一筛选条件再次提交返回原意图，不会重建任务。
- 筛选条件改变会产生新意图，旧意图被原子地标记为 `superseded`，晚到的旧报表不能发布。

状态机（`intents.state`）：

```
submitted → applying → generating → ready → downloading → verifying → published
                                         ↘ failed / cancelled / expired / superseded
```

`expired` 只表示**本地截止时间已到**，与合作方的“生成中”、“任务失败”、“任务取消”、
“下载地址失效”分开记录；每种终态都在 `intents.decision_*` 和 `events` 中留下不可变记录。

## 关键保证

| 场景 | 保证 | 位置 |
| --- | --- | --- |
| 进程退出后恢复 | 已记录的 `job_id` 优先；若崩溃发生在 apply 之后、写库之前，带幂等键的 apply 返回原任务，不会新建第二份 | `ExportEngine.recover` / `ensure_submitted` |
| 多进程恢复同一意图 | SQLite 写事务 + 每意图唯一发布记录 + 每租户唯一 active 发布，只有一个进程能发布 | `ExportStore.finalize`、`publications` 唯一索引 |
| 通知早于文件可下载 | 只登记成员声明，逐文件下载校验，缺失成员时停在 `incomplete`，不发布 | `record_ready` / `download_member` |
| 按月/按行拆分的多个文件 | 按声明逐个匹配成员名、行数；列顺序按成员分别保存，允许文件间列序不同 | `members` 表、`record_download` |
| 地址过期 | 只为**同一** `job_id` 续取地址；外来任务的地址会被拒绝并记录事件 | `fetch_links` / `record_links` / `PartnerAdapter.links` |
| 最后分片与取消交错 | 取消与分片写入在同一事务中裁决；取消先提交则分片被拒绝，发布先提交则取消被拒绝并附上发布决定 | `request_cancel` / `record_download` |
| 旧报表晚到 | 已被新意图取代的旧意图管线直接终止，绝不覆盖新报表 | `submit_intent` / `finalize` |
| 失败/过期后的重试 | 通过 `retry()` 显式创建新的 attempt，新的本地意图和新的合作方任务，不与旧报表混用 | `ExportEngine.retry` |

审计记录：`events` 表记录每次 poll、成员声明、地址续取、下载校验、冲突、取消裁决、
发布和截止时间；`intents.decision_outcome` 是最终采用的唯一决定（`published` / `cancelled` /
`failed` / `expired` / `superseded`）。

## 环境和运行

使用 Python 3.11.10，仅依赖标准库（含 `sqlite3`），无需登录账号、安装第三方包或下载数据。
`.python-version` 固定解释器版本；`pyproject.toml` 仅登记 unittest 入口，项目不需要打包安装。

在仓库根目录执行：

```sh
python3 -m unittest discover -s tests -v
python3 demo.py
```

完整演示命令：

```sh
python3 -m unittest discover -s tests -v && python3 demo.py
```

演示实时读取合成输入并运行当前接口，通常在 90 秒以内结束，不回放历史日志。状态写入临时目录，
重复执行不污染仓库；本地 HTTP 示例只访问回环地址，不连接生产系统。

## 合成来源

`fixtures/exports.json` 与 `export_api.py` 提供异步导出服务。`apply` 创建远端任务，
`status` 查询状态，`links` 续取地址，`download` 返回成员；所有时间均由调用方传入。
`export_sim.py` 提供内存中的确定性合作方，用于表达失败、合作方取消、通知早于成员可下载、
地址失效等情形；`ExportAPI` 仍是文件后端的合作方，两条路径共用同一采集引擎。

`export_sim.ScenarioAPI` 的 `ready_notification_at`、`member_available_after`、
`expire_member_call`、`fail_at`、`cancel_at` 都在虚拟时间上生效。

## 演示内容

`demo.py` 输出 `legacy`、`collection`、`cancellation`、`deadline` 四部分：

- `collection`：t=0 申请并轮询，t=4 重启后按已保存任务续取，t=5 收到完成通知但第二个成员
  尚未可下载，t=6 下载第一个文件，t=8 用过期地址续取同一任务，t=9 校验完成后发布。
- `cancellation`：取消先到（t=6）则取消生效、分片被拒；发布先到（t=8）则取消被拒，
  决定中给出发布事件号。
- `deadline`：本地截止时间已到，状态为 `expired`，没有发布。
- `restart.apply_calls == 1`：重启没有重新申请报表。

## 能力边界

自动化验证覆盖上述编排、持久化决策和跨进程恢复；它只针对合成合作方，不代表已接入真实
生产接口，也不处理真实签名 URL 的解析（可通过 `PartnerAdapter(api, address_job=...)`
注入地址归属解析）。演示输出不代表真实报表已生成。
