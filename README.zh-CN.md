# Awaitless

[![CI](https://github.com/xpluspro/Awaitless/actions/workflows/ci.yml/badge.svg)](https://github.com/xpluspro/Awaitless/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/awaitless-runner.svg)](https://pypi.org/project/awaitless-runner/)
[![Python](https://img.shields.io/pypi/pyversions/awaitless-runner.svg)](https://pypi.org/project/awaitless-runner/)

面向 AI 编程 Agent 的持久化、有限返回、事件驱动作业运行器。

Awaitless 把本地、SSH 或 Slurm 长命令变成带稳定 `job_id` 的持久化作业。
Agent 通过 MCP 提交一次、等待一次，就能收到退出码、有限日志和声明的 JSON
Artifact，无需手写 shell 命令，也无需反复轮询并消耗上下文。

[English](https://github.com/xpluspro/Awaitless/blob/main/README.md)

![Awaitless SSH 提交、断开、恢复与 Artifact 演示](https://raw.githubusercontent.com/xpluspro/Awaitless/main/assets/awaitless-demo.gif)

## 为什么使用 Awaitless

- **客户端退出不影响作业：** 关闭终端或中断 `wait` 不会取消任务，新客户端用同一 ID 恢复即可。
- **Agent 原生 MCP 工具：** 标准 stdio 协议提供 `submit_job`、`wait_for_job`、
  `get_job_status`、`get_job_logs`、`cancel_job` 和 `list_jobs`。
- **支持集群调度：** Slurm backend 持久化调度器 ID，并映射队列/accounting
  状态、退出码、取消、日志与 Artifact。
- **返回量有上限：** stdout/stderr 尾部共享可配置字节预算，完整日志仍保留在磁盘。
- **直接返回结构化结果：** 声明的 JSON Artifact 会解析为 `parsed_results`。
- **处理真实集群边界：** SSH 存活判断采用 wrapper 自有 heartbeat，不假设不同登录会话能看到同一 PID 命名空间。

## 安装

PyPI 分发名为 `awaitless-runner`，安装后同时提供 `awaitless` CLI 和
`awaitless-mcp` stdio Server。需要 Linux、Python 3.10+ 和 Bash；SSH/Slurm
主机还要求本地具备 OpenSSH 的 `ssh` 与 `sftp`。

```bash
python -m pip install awaitless-runner
awaitless doctor --json
```

从源码安装：

```bash
python -m pip install -e .
```

## Agent 原生 MCP 快速开始

让 MCP 客户端启动安装好的 stdio 命令（最外层字段请按具体客户端格式调整）：

```json
{
  "mcpServers": {
    "awaitless": {
      "command": "awaitless-mcp",
      "args": ["--config", "/home/me/.config/awaitless/config.toml"]
    }
  }
}
```

Server 基于官方
[`modelcontextprotocol/python-sdk`](https://github.com/modelcontextprotocol/python-sdk)。
Agent 随后直接用 argv 数组调用 `submit_job`，再用返回的 ID 调用
`wait_for_job`。每次 MCP 调用都复用现有 `Service` 与 SQLite；不新增 Awaitless
daemon、HTTP 端点或 Web 服务。关闭 stdio Server 不会停止已经提交的作业。

> 验收标准：用户安装 PyPI 包、配置一个 MCP Server 后，Agent 无需手写
> Awaitless CLI，就能在 Slurm 集群提交任务、断线恢复并获得结构化结果。

## 快速开始

`submit` 会在任务结束前返回：

```bash
awaitless submit --json --name build -- ninja -C build
```

```json
{"job_id":"job_019F...","state":"running","backend":"local"}
```

随后只做一次阻塞式调用：

```bash
awaitless wait job_019F... --json
```

如果客户端关闭或被中断，在新客户端中对保存的 ID 重新执行同一条 `wait`；受管任务会继续运行。

常用的一次性操作：

```bash
awaitless status <job-id> --json
awaitless logs <job-id> --tail 200 --json
awaitless cancel <job-id> --grace-period 5s --json
awaitless list --state running --json
awaitless inspect <job-id> --json
```

## SSH 与结构化 Artifact

在 `~/.config/awaitless/config.toml` 中声明主机：

```toml
[defaults]
backend = "local"
log_tail_lines = 200
max_return_bytes = 65536
poll_interval = 2

[hosts.gpu]
hostname = "gpu.example.com"
port = 22
user = "developer"
identity_file = "~/.ssh/id_ed25519"
remote_job_dir = "~/.awaitless/jobs"
# gssapi_authentication = false
# connect_timeout = 8
# operation_timeout = 20
```

`operation_timeout` 是一次 SSH 控制操作的最低超时，不是任务运行上限；任务自身用
`submit --timeout` 限制。

提交远端命令并声明结果：

```bash
awaitless submit --json \
  --host gpu \
  --cwd /workspace/project \
  --timeout 2h \
  --artifact results/benchmark.json \
  -- ./run_benchmark.sh
```

完成时，`wait --json` 会返回 Artifact 的存在性、大小和修改时间；预算内的 JSON 文件还会直接解析：

```json
{
  "state": "succeeded",
  "exit_code": 0,
  "truncated": false,
  "parsed_results": {
    "correctness": true,
    "latency_us": 24.7
  }
}
```

本地相对 Artifact 始终按提交时的工作目录解析，即使恢复等待的客户端位于其他目录。
`--log-dir /path/to/logs` 会为每个任务创建隔离的 `/path/to/logs/<job-id>/`。

## Slurm backend

配置调度集群及默认资源请求：

```toml
[defaults]
backend = "slurm"
host = "cluster"
poll_interval = 10
log_tail_lines = 200
max_return_bytes = 65536

[hosts.cluster]
hostname = "login.cluster.example"
user = "developer"
backend = "slurm"
gssapi_authentication = false
operation_timeout = 30
slurm_accounting_grace = 120
slurm_job_dir = ".awaitless/slurm/jobs"

[hosts.cluster.slurm]
partition = "compute"
account = "research"
nodes = 1
ntasks = 1
cpus_per_task = 1
time = "00:30:00"
```

配置 `defaults.host` 后，MCP 调用可以同时省略 `backend` 和 `host`。
`submit_job` 可通过 `slurm_options` 覆盖白名单内的 `account`、`constraint`、
`cpus_per_task`、`gres`、`mem`、`nodes`、`ntasks`、`partition`、`qos` 和
`time`。backend 通过 stdin 把 batch script 交给 `sbatch`，持久化 Slurm ID，
用 `squeue` 查询活跃状态，用 `sacct` 恢复终态、退出码和运行时长，用
`scancel` 取消。用户计算只会由 Slurm 投递到分配的计算节点，绝不会作为进程
直接运行在 SSH 登录节点；独立的 SFTP 数据通道只负责创建私有作业目录，并按
预算读取日志尾部和声明的 Artifact。

状态映射如下：Slurm `PENDING` → Awaitless `pending`；活跃态 → `running`；
`COMPLETED` → `succeeded`；`CANCELLED` → `cancelled`；`TIMEOUT`/`DEADLINE`
→ `timed_out`；调度器、节点、启动、OOM 与抢占失败 → `failed`。`7:0` 等
`ExitCode` 以及信号终止都会转换为进程风格退出码。

### 真实 MCP → Slurm 断线恢复演示

2026-08-10，两个完全独立的 MCP stdio 客户端在真实 Slurm 25.11.2 集群完成：

| 阶段 | 实测结果 |
|---|---|
| 客户端 1 `submit_job` | Awaitless `job_019FE9CB2847AC929E0B2F`，Slurm `60597793`，`pending` |
| 客户端 1 退出 | 没有 daemon 或 waiter 继续附着 |
| 客户端 2 `wait_for_job` | `succeeded`，退出码 `0`，运行 `8.0s` |
| 有限 stdout | `compute_host=node099 slurm_job_id=60597793`（43 B） |
| JSON Artifact | 解析为 `{ "ok": true, "compute_host": "node099", "slurm_job_id": "60597793" }` |

`node099` 是 Slurm 分配的计算节点。可复现脚本位于
[`scripts/mcp_slurm_demo.py`](https://github.com/xpluspro/Awaitless/blob/main/scripts/mcp_slurm_demo.py)，
原始结构化证据位于
[`assets/mcp-slurm-demo.json`](https://github.com/xpluspro/Awaitless/blob/main/assets/mcp-slurm-demo.json)。

## 真实实验：12 次轮询变为 2 次调用

2026-08-10 的可复现实验在真实 SSH 登录节点运行完全相同的 sleep-only workload：
每隔 4.5 秒写一条 1 KiB 日志，共 12 条，不执行任何 CPU/GPU 高占用工作。
传统侧完整读取累计日志快照 12 次；Awaitless 侧只调用一次 `submit` 和一次 `wait`。

| 实测结果 | 传统 SSH 轮询 | Awaitless |
|---|---:|---:|
| 启动后的轮询/检查调用 | 12 | 0 |
| 包含启动在内、Agent 可见的 CLI 调用 | 13 | 2 |
| 返回的逻辑日志字节 | 84,992 B | 12,288 B |
| 重复返回的日志字节 | 72,704 B | 0 B |
| 退出码 | 0 | 0 |
| 解析后的 JSON Artifact | 无 | 有 |

结果是：**少返回 72,704 B 日志（85.5%）**，且 **Agent 可见的 CLI 调用从
13 次降至 2 次（84.6%）**。传统侧 12 个日志快照分别为
`[1024, 2048, 3072, 4096, 5120, 6144, 8192, 9216, 10240, 11264, 12288, 12288]`
字节。这里的“调用”指 Agent 可见的 CLI 调用；Awaitless 内部 SSH 控制操作不会触发
新的 Agent 轮次。字节数统计解码后的日志内容，不是估算 token 或网络线速字节。

可运行方法和原始结果位于
[`benchmarks/`](https://github.com/xpluspro/Awaitless/tree/main/benchmarks)。

## Awaitless 与其他方案

| 工具 | 核心抽象 | 客户端退出后继续 | 持久状态/退出码 | Agent 友好的有限 JSON 结果 | 调度/资源分配 | 最适合 |
|---|---|:---:|:---:|:---:|:---:|---|
| **Awaitless** | 本地、SSH 或 Slurm 作业 ID + MCP 工具 | 是 | 是 | 是 | Slurm | 需要调度、恢复、有限日志和 Artifact 的 Agent 原生作业 |
| **nohup** | 忽略 SIGHUP + 重定向输出 | 通常 | 手工 | 否 | 否 | 手工管理 PID/日志即可的单条 Shell 命令 |
| **tmux** | 持久化交互终端 | 是 | 手工 | 否 | 否 | 人类离开后重新接入交互 Shell |
| **Pueue** | daemon 支持的本地任务队列 | 是 | 是 | 部分；状态/日志 JSON | 仅本地队列 | 单机上由人操作的队列和并行任务组 |
| **Slurm** | 集群 workload manager | 是 | 是，含 accounting | 由作业自行定义 | 是 | 分配与调度集群 CPU/GPU 资源 |
| **Codex Goal mode** | 跨多轮的持久 Agent 目标 | 是 | 不是进程监督器 | 取决于工具 | 否 | 多轮 Agent 编排；与 Awaitless 互补 |

资料说明：GNU [`nohup`](https://www.gnu.org/software/coreutils/manual/html_node/nohup-invocation.html)、
[`tmux`](https://tmux.github.io/)、[`Pueue`](https://github.com/Nukesor/pueue)、
Slurm [概览](https://slurm.schedmd.com/overview.html)和 Codex
[Goal mode 指南](https://learn.chatgpt.com/use-cases/follow-goals)。Awaitless 使用
Slurm 完成资源分配，而不是替代集群调度器。

## 可靠性模型

- Local runner 与用户命令使用独立会话和进程组；取消会终止整个已校验的进程组。
- SQLite 使用 WAL；活跃态到终态的变化通过事务保护，完成、取消和停滞检测不会互相覆盖。
- SSH wrapper 原子保存 `exit_code` 和 `finished_at`。轻量 heartbeat 适配不同 SSH
  会话不能查看同一 PID 命名空间的主机，PID、进程组和 `/proc` 启动时钟作为兼容回退。
- SSH 取消会先持久化意图，再向已校验的进程组发信号；主机密钥检查沿用 OpenSSH 安全默认值。
- Slurm 控制面的 SSH 调用严格限制为 `sbatch`/`squeue`/`sacct`/`scancel`；
  文件通道使用 SFTP，任意用户计算只存在于已提交的 batch script 中。
- 疑似凭证的值在元数据中会被遮蔽，实际运行规格文件权限为 `0600`。

状态包括 `starting`、`running`、`stalled`、`succeeded`、`failed`、`cancelled`、
`timed_out` 和 `lost`。`--stall-timeout 20m` 只报告停滞，不会自动取消。

CLI 退出码：0 成功，1 内部错误，2 参数错误，3 作业失败，4 作业或客户端等待超时，
5 已取消，6 状态丢失，7 SSH 连接失败。

## 开发与测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
ruff check src tests benchmarks
```

GitHub Actions 会在每次变更中覆盖 CPython 3.10–3.14，随后构建发布包、检查
PyPI README，并通过安装后的 wheel 执行 CLI/Artifact 冒烟；版本 tag 使用
PyPI Trusted Publishing，不在仓库保存 API token。

Codex Skill 位于
[`skills/awaitless`](https://github.com/xpluspro/Awaitless/tree/main/skills/awaitless)，
v0.1 产品需求位于
[`docs/PRD.zh-CN.md`](https://github.com/xpluspro/Awaitless/blob/main/docs/PRD.zh-CN.md)，
v0.2 Agent/Slurm 验收契约位于
[`docs/v0.2.zh-CN.md`](https://github.com/xpluspro/Awaitless/blob/main/docs/v0.2.zh-CN.md)。

## 许可证

[MIT](https://github.com/xpluspro/Awaitless/blob/main/LICENSE)
