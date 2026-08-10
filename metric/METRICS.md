# 指标口径

## 要支持的决策

这套指标用于回答三个决策，而不是生成一个营销总分：

1. Awaitless 是否足够可靠，可以让 Agent 无人值守地管理长任务？
2. 相比原生 tmux，它是否减少了 Agent 轮次和上下文；相比增强 tmux，它是否减少了
   用户自建协议和维护负担？
3. 这些收益是否以不可接受的延迟、资源开销或取消安全性为代价？

所有比例都同时报告分子、分母和 Wilson 95% 置信区间。所有延迟/成本分布至少报告
中位数与 P90。不得只报告成功样本的成本。

## 主指标

### 1. `end_to_end_result_fidelity_rate`

一次 trial 的 `result_correct` 仅在以下所有适用字段都正确时为真：

- `state_correct`：终态语义正确；
- `exit_code_correct`：真实退出码正确，取消场景可为不适用；
- `log_contract_correct`：要求的尾部标记存在；如果没有返回完整日志，必须明确
  `truncated=true`；
- `artifact_correct`：声明的 JSON Artifact 存在且结构化内容正确；
- 取消场景还要求 `cancel_cleanup_success=true`。

公式：

```text
端到端结果正确率 = result_correct 的 trial 数 / 全部 trial 数
```

它是首要结果指标。单独的“命令返回了 0”或“session 还存在”不能算正确结果。

### 2. `unattended_recovery_success_rate`

仅在注入客户端/waiter 中断的 trial 上计算。恢复后必须无需用户补充 session 路径、PID、
日志位置或下一步命令，并正确返回最终结果。

```text
无人干预恢复成功率 = recovery_success 的次数 / 注入中断的次数
```

本地实验验证新进程仅凭 `job_id`/session ID 恢复；正式实验还必须覆盖 SSH 暂时不可用和
独立 Agent 会话。

### 3. `agent_cost_per_correct_job`

这是一个成本向量，不合并成加权分数：

```text
每个正确结果的工具调用 = 所有 trial 的 agent_tool_calls 总和 / 正确结果数
每个正确结果的可见字节 = 所有 trial 的 agent_visible_bytes 总和 / 正确结果数
每个正确结果的 usage token = 所有 trial 的 usage token 总和 / 正确结果数
```

失败 trial 的成本留在分子中，因此不能通过丢弃失败来美化结果。token 分开记录：

- `input_tokens`；
- `output_tokens`；
- `cached_input_tokens`；
- 平台提供时的 `reasoning_tokens`。

只有 API/Agent 平台的 usage 或指定模型的真实 tokenizer 结果才能填 token。若只有字节，
token 字段必须为 `null`。

## 诊断指标

| 指标 | 计算/含义 | 用来解释什么 |
|---|---|---|
| `state_accuracy` | 正确 state / 全部适用 trial | 状态机是否可靠 |
| `exit_code_accuracy` | 正确退出码 / 全部适用 trial | 是否需要人工猜测失败 |
| `artifact_accuracy` | 正确 JSON Artifact / 全部适用 trial | 是否仍需从日志解析结果 |
| `log_contract_accuracy` | 尾部与截断标记都正确 / 全部适用 trial | 大日志是否污染或误导上下文 |
| `median_agent_tool_calls` | 每 trial 调用数中位数 | 是否减少模型轮次 |
| `p90_agent_visible_bytes` | 每 trial 返回字节 P90 | 长尾上下文风险 |
| `duplicated_log_bytes` | 重复返回的相同日志字节 | 轮询造成的上下文浪费 |
| `system_command_invocations` | 管理器内部命令次数 | 防止把内部开销藏在一次 Agent 调用中 |
| `custom_glue_sloc` | 非空、非纯注释的用户自有 wrapper 行数 | 需要自己维护多少协议代码 |
| `manual_interventions` | 用户提供下一步指令/路径/PID 的次数 | 无人值守能力 |
| `api_requests` | trial 内服务端模型请求数 | 工具调用之外的模型轮次成本 |
| `reasoning_tokens` | 服务端 usage 中的 reasoning token | 解释 completion 成本，不与 output 重复相加 |

## 护栏指标

| 护栏 | 报告方式 | 失败条件 |
|---|---|---|
| `cancel_cleanup_success` | 成功率 + 遗留 PID 数 | 普通子/孙进程仍存活 |
| `wall_time_seconds` | median / P90 和相对 workload 开销 | 为节省调用引入明显延迟 |
| `cpu_time_seconds` | median / P90 | 管理面忙轮询或高 CPU |
| `peak_rss_bytes` | median / P90；不可测则为空 | 内存开销不可接受 |
| `disk_bytes` | 完整日志、状态和 Artifact 占用 | 有界返回变成无界磁盘风险 |
| `ssh_request_count` | 每 trial 总数 | Agent 调用少但控制面请求异常多 |
| `false_terminal_count` | 将断线/未知误报为成功或失败的次数 | 任意一次都必须单列调查 |
| `llm_length_truncations` | `finish_reason=length` 的模型请求数 | 大于 0 时不得用该批比较 Agent 正确率 |

主动逃离进程组、双重 fork 或容器外进程不属于普通 `cancel_tree` 成功标准，除非所有方案都
预先声明支持；否则会把不同的安全边界混在一起。

## 暂定验收门槛，不是既有结论

- 确定性 CI 场景：Awaitless 必须 100% 正确，取消后普通进程树遗留数为 0。
- 比较实验：每个方案 × 场景至少 20 次并展示 95% CI；高可靠性宣传需要更多样本，20 次
  全成功并不能证明 99% 可靠。
- Awaitless 端到端正确率不得显著低于增强 tmux。
- P90 wall time 不应比相同 workload 的增强 tmux 高 5% 以上；排队时间单独报告。
- 在取得真实 usage 前，不设定也不宣传 token 节省百分比。
- 真实 Agent 批次若有 completion 上限截断，必须提高上限并完整重跑；不得只补跑失败 arm。
- 原生 tmux 的调用/字节收益可以作为效率对照；增强 tmux 的主要对照是可靠性和自有维护面，
  不预设 Awaitless 在 token 上一定获胜。

## 定性维度

定性评审不与性能指标相加。至少两名评审者分别按
[`qualitative/RUBRIC.md`](qualitative/RUBRIC.md) 评分，并保留证据和分歧。特别保留
“交互调试适用性”这一 tmux 可能获胜的维度，避免量表只包含 Awaitless 擅长的能力。

## Blocking 长任务补充指标

Blocking 对照的首要时间指标是 `agent_blocked_seconds`：所有 Agent 可见同步调用时间区间的
并集。它与模型 reasoning 不同；工具调用期间没有 usage 证据时，`reasoning_idle_seconds` 和
token 字段必须为 `null`。

长任务还报告：

- `time_to_agent_release_seconds`：启动全部任务后 Agent 最早可继续编排的时间；
- `agent_available_seconds`：makespan 内不在同步工具调用中的墙钟时间；
- `parallelism_factor`：已完成任务内部时长之和 / case makespan；
- `wall_time_seconds`：防止更早释放 Agent 却显著增加最终结果时延；
- `blocking_parallel` 强基线：防止把单槽工具宿主的限制误归因于所有 Blocking 工具。

完整协议和暂定门槛见 [`LONG_RUNNING.md`](LONG_RUNNING.md)。
