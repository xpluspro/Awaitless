# 公平对照实验协议

## 对照组

| Arm | 允许的能力 | 目的 |
|---|---|---|
| `tmux_plain` | 原生 `new-session`、`display-message`、`capture-pane`、`kill-session` 和直接读取已知 Artifact | 代表没有自制作业协议的常见 tmux 使用方式 |
| `tmux_wrapped` | [`baselines/tmux_job.py`](baselines/tmux_job.py)：稳定 ID、状态文件、真实退出码、日志预算、JSON Artifact、`tmux wait-for` 和进程组取消 | 强基线；证明差异不是故意选择弱 tmux |
| `awaitless` | 发布版 Awaitless CLI/MCP 的公开接口 | 被评估方案 |

三个方案运行完全相同的 argv、cwd、Artifact 期望和随机 seed。`tmux_plain` 可以读取已知的
`result.json`，但如果要获得持久状态、有限日志或结构化解析，必须把实现放入 wrapper 并计入
`custom_glue_sloc`，不能把实验 harness 的“上帝视角”当作 Agent 能力。

## 场景矩阵

| 场景 | 注入 | 必须正确报告 |
|---|---|---|
| `normal` | 随机运行时长、持续 stdout/stderr、JSON Artifact | succeeded、0、日志尾、Artifact |
| `failure` | 从 1、7、124 中随机退出 | failed、真实退出码、日志尾、Artifact |
| `large_log` | 正式 profile 产生 100 MiB 日志 | 有界返回、明确截断、最终标记、Artifact |
| `recovery` | 中止第一个 waiter，再由全新客户端恢复 | 仅凭稳定 ID 得到完整最终结果 |
| `cancel_tree` | 父、子、孙进程运行中取消 | cancelled，普通进程树无遗留 |
| `ssh_disconnect` | wait 中暂时拒绝 SSH，随后恢复 | 远端任务继续且状态不被误判 |

本地 runner 覆盖前五个场景。`ssh_disconnect` 需要一台授权的真实主机或可控 SSH
fixture，结果使用同一 JSONL 契约写入。登录节点只运行 sleep 和少量日志；100 MiB 和计算密集
场景应使用专用测试主机或 Slurm 计算节点。

## 执行规则

1. 在看结果之前固定配置、指标定义和分析脚本；记录 git commit、Awaitless/tmux/Python 版本。
2. 每个 trial 使用同一 seed 构造三组 workload；trial 内随机化 arm 顺序，避免热缓存和机器
   负载总是偏向同一方案。
3. 正式运行前做一次不计入结果的 warm-up。每个方案 × 场景至少 20 次；可靠性下界要求高时
   按目标置信区间另算样本量。
4. 同一台机器串行运行 arm，或在并行时隔离 CPU/磁盘并记录分配；不要让三个方案争用同一
   workload 资源。
5. harness 预先保存期望 state、exit code、Artifact 和最终日志标记。观察值只能来自被测方案
   暴露给 Agent 的接口；`/proc` 仅作为取消后的 oracle。
6. 保留每一次失败、超时和异常。不得重跑后只保留成功结果；环境无效需用预先定义的
   `invalid_reason` 标记并同时报告数量。
7. 原始 JSONL 只追加不覆盖。汇总由 `analyze.py` 从原始记录生成，禁止手工改汇总数字。
8. 任一模型请求出现 `finish_reason=length` 时，该批次可用于调试，但不得用于比较 Agent
   正确率；提高 completion 上限后必须对所有 arm 完整重跑，不能只重跑失败项。

## 调用、字节与 token 的边界

- `agent_tool_calls`：会把结果送回模型并可能触发下一轮推理的逻辑调用。
- `system_command_invocations`：一次逻辑调用内部实际执行的 tmux/SSH/CLI 命令数。
- `agent_visible_bytes`：逻辑调用 stdout + stderr 的 UTF-8 原始字节数。
- token：同一模型 tokenizer 对完整工具消息的计数，或 API response 的 usage 累计。
- 计费比较需同时给出 cached token；不能用 `bytes / 4`、字符数或日志字节百分比替代。

本地 runner 精确记录前两类调用和返回字节，但默认没有真实模型 usage，因此 token 为
`null`。真实 Agent 实验必须保存完整工具轨迹的 hash、usage 和模型版本。

## 真实 Agent 实验

给相同模型、system prompt、温度、上下文上限和全新会话，只替换可用工具。推荐任务：

> 在远程机器执行 benchmark。任务中途第一个客户端会关闭。恢复后报告退出码、最后 50 行
> 日志，以及 `result.json` 中的 `score`。

每个 arm 至少 20 次，随机化运行时长、退出码和断线时点。记录：

- 每一轮工具名、开始/结束时间、返回字节与内容 hash；
- API `input_tokens`、`output_tokens`、`cached_input_tokens`、`reasoning_tokens`（如有）；
- 是否给出正确最终答案以及人工干预次数；
- Agent 是否自己创建了 wrapper；若创建，代码计入该 arm 的 glue SLOC。

如果增强 tmux 与 Awaitless token 相近，应如实报告。Awaitless 的剩余价值应由恢复正确性、
统一 local/SSH/Slurm 接口和零用户自制协议来支持，而不是继续寻找更弱的轮询对手。

仓库中的 [`run_agent.py`](run_agent.py) 实现了这个三臂实验。它在提交后重置消息历史，新客户端
只收到稳定 ID；记录服务端 prompt/completion/cache/reasoning usage 和每次工具返回。对于要求
保留 `reasoning_content` 的 thinking-mode 工具调用，该内容只传回 API，不写入原始记录。

## 必须同时披露的限制

- tmux 的优势是交互终端，本协议不衡量 TUI/REPL 体验。
- Awaitless 内部仍可能轮询本地状态或调度器；“事件驱动”收益指不触发 Agent 推理轮次，
  不是宣称操作系统层面零轮询。
- wrapper SLOC 是维护代理指标，受语言和格式影响；同时报告文件数、依赖和支持 backend。
- 本地 smoke 只证明 harness 能工作；不能外推到 SSH、Slurm 或真实 token。
- 单次 12→2 实验是案例，不是总体分布。

## Blocking 长任务实验

直接同步命令与 Durable Job Protocol 的对照使用独立 runner 和 schema，见
[`LONG_RUNNING.md`](LONG_RUNNING.md)。正式比较必须同时包含 `blocking_parallel`，或把并发结论
明确限定为单槽 Blocking executor。墙钟阻塞不能命名为 reasoning token/时间；Awaitless 只有
在 submit 后释放 Agent、稍后收集时才获得可用时间，如果立即调用 `wait`，Agent 仍会等待。
