# Awaitless 价值度量

这个目录回答一个比“能不能让进程留在后台”更严格的问题：

> 在 Agent 管理长任务时，Awaitless 能否比原生 tmux 和一套认真编写的 tmux
> wrapper 更可靠地返回结果，并减少 Agent 调用、上下文和用户维护成本？

这里不会把 Awaitless 定义成更好的交互终端。需要人工 attach、操作 TUI、REPL
或现场调试时，tmux 仍然是更合适的工具。度量对象是非交互编译、测试、benchmark
以及需要恢复、退出状态、有限日志和结构化 Artifact 的任务。

## 三个主指标

1. **端到端结果正确率**：状态、退出码、日志契约和 Artifact 同时正确的试验比例。
2. **无人干预恢复成功率**：客户端或 waiter 中断后，仅凭稳定 ID 能正确恢复结果的比例。
3. **每个正确结果的 Agent 成本**：获得一个正确结果所消耗的工具调用、Agent 可见字节和
   实际 usage token。没有 tokenizer/API usage 时，token 必须保留为空，不能用字节数冒充。

取消后孤儿进程、P90 延迟、CPU/内存/磁盘、SSH 请求数是护栏指标。自制 glue
代码量、配置步骤和定性评审独立报告，不折成一个可以任意调权重的“价值总分”。完整定义见
[`METRICS.md`](METRICS.md)，公平实验要求见 [`PROTOCOL.md`](PROTOCOL.md)。

## 快速运行

本地 smoke 会运行相同 workload 的三个方案：

- `tmux_plain`：原生 `new-session`、`display-message`、`capture-pane`；
- `tmux_wrapped`：带退出码、有限日志、Artifact 和阻塞等待的参考 wrapper；
- `awaitless`：`submit`、`wait`、`cancel`。

```bash
python3 metric/run_local.py \
  --config metric/configs/smoke.json \
  --output metric/results/raw/local-smoke.jsonl

python3 metric/analyze.py metric/results/raw/local-smoke.jsonl \
  --json-out metric/results/summary.json \
  --markdown-out metric/results/summary.md
```

smoke 只验证采集链路，不能用于宣传。准备对外结论时使用
[`configs/evidence-local.json`](configs/evidence-local.json)，每个“方案 × 场景”至少 20
次，并另外运行真实 SSH 断线和真实 Agent usage 实验。登录节点上的任务必须保持为
sleep 和少量 I/O；计算密集 workload 应由 Slurm 调度。

## 真实 DeepSeek Agent 实验

[`run_agent.py`](run_agent.py) 使用同一个模型、system prompt、workload 和随机 seed，分别只
暴露普通 tmux、增强 tmux 或 Awaitless 工具。提交后会丢弃第一阶段消息历史，第二个客户端只
获得稳定 ID，从而测量全新 Agent 会话的恢复成本。API usage 直接采用服务端返回的 token
字段；不从字节数推算 token。

在仓库根目录的 `.env` 中配置以下变量（该文件已被 git 忽略）：

```dotenv
LLM_API_KEY=...
LLM_BASE_URL=...
LLM_MODEL=deepseek-v4-flash
LLM_TIMEOUT_SECONDS=60
```

先预检和 smoke，再运行 20 次配对实验：

```bash
python3 metric/run_agent.py --config metric/configs/agent-smoke.json --preflight

python3 metric/run_agent.py \
  --config metric/configs/agent-smoke.json \
  --output metric/results/raw/deepseek-agent-smoke.jsonl

python3 metric/run_agent.py \
  --config metric/configs/agent-evidence.json \
  --output metric/results/raw/deepseek-agent-evidence.jsonl

python3 metric/analyze.py metric/results/raw/deepseek-agent-evidence.jsonl \
  --json-out metric/results/deepseek-agent-summary.json \
  --markdown-out metric/results/deepseek-agent-summary.md
```

输出文件默认拒绝覆盖。DeepSeek thinking 模式的 `reasoning_content` 会按接口要求传回后续
请求，但不会写入实验记录；只保存 usage、finish reason、工具名和响应哈希。任何请求若以
`finish_reason=length` 结束，分析器会明确把该批比较标为需要提高上限后重跑。

2026-08-10 的 20×3 实测结果和限制见
[`results/deepseek-agent-v2-report.md`](results/deepseek-agent-v2-report.md)。这批数据对普通
tmux 显示每个正确任务的 usage token 减少 85.3%，但对增强 tmux 没有 token 优势；不能把
前一个数字推广为所有任务的固定节省比例。

## Blocking vs Awaitless 长任务 Benchmark

[`LONG_RUNNING.md`](LONG_RUNNING.md) 定义了另一条独立证据链：直接同步 Blocking、支持并发
工具调用的强 Blocking 基线和 Awaitless 分别执行 `cargo build`、pytest、Docker build、
`npm install`、模型推理以及 sleep 校准。它测量 Agent 同步阻塞时间、释放时间、batch
makespan、断线恢复和结果正确性。

这里不会把墙钟等待写成“reasoning idle”：模型在工具调用期间没有持续推理，真正可测的是
Agent 的独占工具槽被同步调用占用多久。1 秒 smoke 和 5 秒 demo 只验证采集链路；正式配置为
60–180 秒、每格 20 次，并在运行前自动记录不可用 workload 的 skip 原因。

## 目录结构

```text
metric/
├── README.md                    # 入口与复现命令
├── METRICS.md                   # 指标口径、公式、护栏与决策规则
├── PROTOCOL.md                  # 对照组、场景、随机化和报告规范
├── LONG_RUNNING.md              # Blocking vs Awaitless 长任务基准
├── analyze.py                   # JSONL 校验、聚合、置信区间与 Markdown 报告
├── analyze_long_running.py      # 长任务 blocked/release/makespan 分析
├── long_workload.py             # cargo/pytest/docker/npm/inference 受控适配器
├── run_agent.py                 # DeepSeek 工具调用、断线恢复与真实 API usage 实验
├── run_long_running.py          # Blocking/并发 Blocking/Awaitless runner
├── run_local.py                 # 本地 tmux / 增强 tmux / Awaitless 实验
├── workload.py                  # 三组共用的确定性 workload
├── baselines/tmux_job.py        # 可审计的增强 tmux 参考实现
├── configs/                     # 本地和真实 Agent 的 smoke / 正式实验配置
├── qualitative/                 # 双人定性评审量表与空白模板
├── schemas/trial.schema.json    # 一次“方案 × 场景 × trial”的原始记录契约
└── results/                     # 原始记录和汇总结果；生成物默认不提交
```

## 结果解释边界

- `agent_visible_bytes` 是精确的工具返回字节，不是 token 估计。
- `agent_tool_calls` 是会触发 Agent/tool 轮次的逻辑调用；内部 tmux、SSH 或 Awaitless
  控制操作另记为 `system_command_invocations`。
- `tmux_wrapped` 在简单本地任务上很可能与 Awaitless 同样只需两次 Agent 调用；这不是
  失败，而是把比较转向恢复正确性、统一协议和免维护。
- wrapper SLOC 只能说明用户自有维护面，不能证明运行时质量。
- 只报告预先定义的场景、全部失败和置信区间；不挑选 Awaitless 获胜的 trial。
- 当前 `benchmarks/polling_vs_awaitless.py` 的 13 → 2 调用和 85.5% 返回日志字节减少是
  一次历史观测，不应改写成“节省 85.5% token”。
- `run_agent.py` 的真实 usage 只代表记录的模型、提示、场景和日期。模型版本、缓存策略、
  thinking 模式或任务时长改变后必须重测。
