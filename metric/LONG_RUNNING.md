# Blocking vs Awaitless 长任务 Benchmark

这个 benchmark 测量的不是“谁执行 `cargo build` 更快”，而是谁能让 Agent 在长命令运行期间
继续编排其他工作，并在客户端丢失后可靠取回结果。

## 要支持的决策

1. 长任务是否值得从同步 Blocking 工具迁移到 Awaitless？
2. 收益来自更早释放 Agent、任务并发，还是仅仅选择了一个不能并发的弱基线？
3. 这些收益是否以更多调用、额外时延或结果可靠性下降为代价？

## 对照组

| Arm | 行为 | 用途 |
|---|---|---|
| `blocking` | 一个 Agent 工具槽串行执行命令，命令结束后调用才返回 | 常见同步 shell/terminal 基线 |
| `blocking_parallel` | 工具宿主允许多个同步命令并发，但 Agent 等待整组返回 | 强基线；隔离“后端并发”与“durable protocol” |
| `awaitless` | 先快速提交所有任务，Agent 可释放；稍后由稳定 job ID 收集 | 被评估方案 |

单任务上 Blocking 只需一次调用，Awaitless 通常需要 `submit + wait` 两次；断线恢复需要
`submit + interrupted wait + wait` 三次。调用数增加是明确的成本，不应隐藏。

## KPI 定义

### 主指标

| KPI | 定义 | 决策含义 |
|---|---|---|
| `result_correct` | 所有任务 state、exit code、Artifact 和最终日志标记都正确 | 任何效率收益都不能牺牲结果契约 |
| `agent_blocked_seconds` | Agent 可见同步调用时间区间的并集 | Agent 的独占工具槽被占用多久 |
| `time_to_agent_release_seconds` | case 开始到全部任务已启动且 Agent 可继续工作的时间 | 多快能去做别的事 |
| `recovery_success` | 注入断线后，新客户端仅凭 ID 得到正确结果 | Durable 生命周期的核心价值 |

### 驱动和护栏

| 指标 | 解释 |
|---|---|
| `agent_tool_calls` | 模型/Agent 可见工具调用数；Blocking 通常更少 |
| `wall_time_seconds` | 端到端 makespan；防止“释放 Agent”却显著拖慢任务 |
| `parallelism_factor` | 已完成任务内部时长之和 / makespan；大于 1 表示发生重叠 |
| `agent_visible_bytes` | 工具返回给 Agent 的实际字节，不换算成 token |
| `agent_available_seconds` | makespan 中没有同步调用占用 Agent 的时间；不保证真的做了有用推理 |
| `reasoning_idle_seconds` | 固定为 `null`；墙钟等待不是模型 reasoning，不能声称产生了推理 token |

对 60 秒以上的受控任务，暂定验收门槛是：非断线场景 100% 正确；Awaitless 单任务的
`agent_blocked_seconds / wall_time_seconds <= 5%`；batch makespan 不比
`blocking_parallel` 高 10% 以上；断线恢复 100%。这些是基于固定协议开销的暂定门槛，必须
用 20 次正式结果验证，不是已有结论。

## 场景

| 场景 | Blocking | Awaitless |
|---|---|---|
| `single` | 同步执行一个任务 | submit，释放 Agent，延迟收集 |
| `batch` | 串行执行 N 个任务 | 先提交 N 个任务，再收集；任务可并发 |
| `disconnect` | 运行中的同步工具进程组随客户端中断而终止 | 中断第一个 waiter，新客户端用 job ID 继续 wait |

`disconnect` 对 Blocking 的语义是“直接工具调用由客户端拥有，客户端中断会终止其进程组”。
有些平台可能让进程残留，但仍没有稳定 ID、持久退出码和结果协议；如平台有更强语义，应作为
新的强基线实现并单独报告。

## Workload 适配器

| Adapter | 实际命令 | 受控长阶段 | 自动 skip 条件 |
|---|---|---|---|
| `sleep` | Python sleep | sleep | Python 不可用 |
| `cargo_build` | `cargo build --offline` | `build.rs` sleep | Cargo 不可用 |
| `pytest` | `python -m pytest -q` | test 内 sleep | pytest 模块不可用 |
| `docker_build` | `docker build --no-cache` | Dockerfile `RUN sleep` | daemon 或本地 base image 不可用 |
| `npm_install` | `npm install` | install script timer | npm 或原生 node 不可用 |
| `model_inference` | OpenAI-compatible Chat Completions (`gpt-5.6-luna`) | 多次真实推理请求 | `.env` LLM 配置不完整 |
| `command` | 用户提供的 argv | 由真实项目决定 | required command/cwd 不可用 |

前六个是受控 fixture：三组执行完全相同的真实命令，同时用固定长阶段降低机器和缓存噪声。
这只能证明编排特性，不能代表真实项目构建性能。`command` adapter 用于最终外部验证，例如：

```json
{
  "id": "project_cargo_build",
  "adapter": "command",
  "command": ["cargo", "build", "--release"],
  "cwd": "{workspace}",
  "required_commands": ["cargo"],
  "duration_seconds": 0,
  "timeout_seconds": 1800
}
```

真实项目命令可能修改 build cache、`node_modules` 或镜像。应在 disposable clone 中运行，固定
冷/热缓存策略，并在三组之间使用独立副本。Docker fixture 不自动拉取镜像；需提前准备配置中
的本地 base image。模型推理会产生真实 API usage 和费用。

## 运行

先查看哪些 adapter 可用：

```bash
python3 metric/run_long_running.py \
  --config metric/configs/long-running-evidence.json \
  --probe-only
```

快速 smoke 和 5 秒方向性 demo：

```bash
python3 metric/run_long_running.py \
  --config metric/configs/long-running-smoke.json \
  --output metric/results/raw/long-running-smoke.jsonl

python3 metric/run_long_running.py \
  --config metric/configs/long-running-demo.json \
  --output metric/results/raw/long-running-demo.jsonl
```

60–180 秒、20 次正式矩阵是显式 opt-in；全部 adapter 可用时可能运行数十小时并产生模型费用：

```bash
python3 metric/run_long_running.py \
  --config metric/configs/long-running-evidence.json \
  --output metric/results/raw/long-running-evidence.jsonl

python3 metric/analyze_long_running.py metric/results/raw/long-running-evidence.jsonl \
  --json-out metric/results/long-running-summary.json \
  --markdown-out metric/results/long-running-summary.md
```

可用 `--workload cargo_build` 重复筛选任务，也可用 `--trials` 覆盖样本数。不可用 adapter 会写
一条带 probe reason 的 `skip` 记录，不会伪造成失败或成功。

## 历史采集说明

旧版本的单次 sleep 校准仅用于验证采集链路，不是 v0.8 性能证据，发布报告不得引用其数字。
