# DeepSeek 长任务 Agent 对照实验（2026-08-10）

## 结论

在本次 20 个严格配对的“提交后客户端丢失、仅凭稳定 ID 恢复”任务中，Awaitless 相比普通
tmux 将中位 Agent 工具调用从 7 次降到 2 次，将 P90 Agent 可见输出从 43.1 KiB 降到
12.6 KiB，并将每个正确任务的实际 API usage 从 25,974.2 token 降到 3,820.8 token，分别
减少 71.4%、70.8% 和 85.3%。

增强 tmux 证明了边界：它同样只需 2 次工具调用，每个正确任务使用 3,498.2 token，比
Awaitless 少 9.2%。Awaitless 在这条强基线上没有 token 优势；可量化价值是把该基线所需的
319 行自制 wrapper 收敛成内置协议，并提供相同的 local/SSH/Slurm 接口。

## 可复现批次

| 字段 | 值 |
|---|---|
| 模型 | `deepseek-v4-flash` |
| 模式 | provider-default thinking |
| 配置 | `metric/configs/agent-evidence.json` |
| 样本 | 20 cases × 3 arms = 60 trials |
| 场景 | 提交后重置客户端；新客户端只获得 job/session ID |
| Workload | 6–8 秒，16–24 行 × 512 B 日志，退出码从 0/1/7/124 中抽样 |
| 随机化 | case 内三臂共享 seed/期望值，并随机化执行顺序 |
| completion 上限 | 4096 token |
| 服务端 usage 总计 | 662,045 token |
| completion 截断 | 0 |

三个 arm 使用同一个 system prompt、模型参数和 workload，只替换任务管理工具：普通 tmux
获得 `poll_job`/`read_artifact`，增强 tmux和 Awaitless 获得阻塞式 `wait_for_job`。第一阶段
提交完成后，第二阶段不会继承第一阶段消息或工具结果，只携带稳定 ID。

## 结果

| Arm | 端到端正确率 | 中位工具调用 | P90 可见输出 | usage / 正确任务 | usage 总计 | 自制 glue SLOC |
|---|---:|---:|---:|---:|---:|---:|
| Awaitless | 19/20（95.0%；95% CI 76.4%–99.1%） | 2 | 12.6 KiB | 3,820.8 | 72,595 | 0 |
| 普通 tmux | 20/20（100%；95% CI 83.9%–100%） | 7 | 43.1 KiB | 25,974.2 | 519,485 | 0 |
| 增强 tmux | 20/20（100%；95% CI 83.9%–100%） | 2 | 12.2 KiB | 3,498.2 | 69,965 | 319 |

Awaitless 的状态、退出码、Artifact 和日志契约均为 20/20 正确。唯一失败发生在模型最终汇报
步骤：管理接口已经正确返回 failed 状态和 Artifact，但模型只生成了 35 个 reasoning token，
可见 content 为空，因此严格的端到端指标计为失败。该失败保留在分母和成本分子中，没有重跑
或剔除。20 次样本的正确率置信区间高度重叠，不能据此声称任一方案更可靠。

## Token 与缓存

`usage token` 是 DeepSeek API 返回的 `prompt_tokens + completion_tokens`，不是日志字节换算。
三组的 cached prompt token 总计分别为 Awaitless 20,480、普通 tmux 263,040、增强 tmux
20,480；reasoning token 总计分别为 5,636、7,869、6,270。普通 tmux 的 KV cache 命中较多，
可能降低账单，但不会消除反复携带轮询历史所占的上下文窗口。本报告没有把 usage token 直接
换算为货币成本。

## 数据质量与排除项

- 60 条 trial ID 唯一，组成 20 个完整三臂 case；每个 case 的 seed 和 expected payload
  在三组间完全一致。
- 每条 trial 的 prompt/completion token 与逐请求 usage 对平，可见字节与逐事件响应对平。
- 所有 token 字段完整，`finish_reason=length` 为 0；API key 扫描通过。
- 本批 `duplicated_log_bytes` 诊断字段仍使用了占位值 0，因此没有用于任何结论；主指标
  `agent_visible_bytes` 来自逐事件实际响应长度并已对平。runner 已在批次完成后改为从每次
  pane 快照计算重复字节，后续批次可使用该诊断指标。
- 更早的一批 v1 使用 512 completion-token 上限，6 个请求的 thinking 内容耗尽上限后被截断。
  分析器将它标为无效批次；该批数据被保留用于审计，但没有混入本报告。
- 本实验只覆盖本机恢复场景，不证明 SSH 临时断线、Slurm 调度、取消进程树或 100 MiB 日志下
  的效果。它也不衡量 tmux 的交互调试、TUI 和 REPL 优势。

## 复现

在仓库根目录 `.env` 配置 `LLM_API_KEY`、`LLM_BASE_URL`、`LLM_MODEL` 和
`LLM_TIMEOUT_SECONDS` 后运行：

```bash
python3 metric/run_agent.py --config metric/configs/agent-smoke.json --preflight

python3 metric/run_agent.py \
  --config metric/configs/agent-evidence.json \
  --output metric/results/raw/deepseek-agent-evidence.jsonl

python3 metric/analyze.py metric/results/raw/deepseek-agent-evidence.jsonl \
  --json-out metric/results/deepseek-agent-summary.json \
  --markdown-out metric/results/deepseek-agent-summary.md
```

原始 JSONL、生成的 JSON/Markdown summary 默认被 git 忽略；runner 拒绝覆盖已有输出文件。
如果真实 trial 失败，runner 会保留记录并返回非零状态。
