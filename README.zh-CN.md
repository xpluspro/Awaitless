# Awaitless

面向 AI 编程 Agent 的持久化、有限返回、事件驱动作业运行器。

Awaitless 把本地或 SSH 长命令变成带稳定 `job_id` 的持久化作业。Agent
只需提交一次、等待一次，就能收到退出码、有限日志和声明的 JSON Artifact，
无需反复调用 `sleep`、`ps`、`tail` 或 SSH 轮询并消耗上下文。

[English](https://github.com/xpluspro/Awaitless/blob/main/README.md)

![Awaitless SSH 提交、断开、恢复与 Artifact 演示](https://raw.githubusercontent.com/xpluspro/Awaitless/main/assets/awaitless-demo.gif)

## 为什么使用 Awaitless

- **客户端退出不影响作业：** 关闭终端或中断 `wait` 不会取消任务，新客户端用同一 ID 恢复即可。
- **支持本地与 SSH：** 每个任务都有持久化元数据、日志、退出状态和远端 wrapper。
- **返回量有上限：** stdout/stderr 尾部共享可配置字节预算，完整日志仍保留在磁盘。
- **直接返回结构化结果：** 声明的 JSON Artifact 会解析为 `parsed_results`。
- **处理真实集群边界：** SSH 存活判断采用 wrapper 自有 heartbeat，不假设不同登录会话能看到同一 PID 命名空间。

## 安装

PyPI 分发名为 `awaitless-runner`，命令仍为 `awaitless`。需要 Linux、Python
3.10+ 和 Bash；SSH 后端还需要 OpenSSH 客户端。

```bash
python -m pip install awaitless-runner
awaitless doctor --json
```

从源码安装：

```bash
python -m pip install -e .
```

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
| **Awaitless** | 本地或 SSH 作业 ID | 是 | 是 | 是 | 否 | 需要恢复、有限日志和 Artifact 的非交互 Agent 作业 |
| **nohup** | 忽略 SIGHUP + 重定向输出 | 通常 | 手工 | 否 | 否 | 手工管理 PID/日志即可的单条 Shell 命令 |
| **tmux** | 持久化交互终端 | 是 | 手工 | 否 | 否 | 人类离开后重新接入交互 Shell |
| **Pueue** | daemon 支持的本地任务队列 | 是 | 是 | 部分；状态/日志 JSON | 仅本地队列 | 单机上由人操作的队列和并行任务组 |
| **Slurm** | 集群 workload manager | 是 | 是，含 accounting | 由作业自行定义 | 是 | 分配与调度集群 CPU/GPU 资源 |
| **Codex Goal mode** | 跨多轮的持久 Agent 目标 | 是 | 不是进程监督器 | 取决于工具 | 否 | 多轮 Agent 编排；与 Awaitless 互补 |

资料说明：GNU [`nohup`](https://www.gnu.org/software/coreutils/manual/html_node/nohup-invocation.html)、
[`tmux`](https://tmux.github.io/)、[`Pueue`](https://github.com/Nukesor/pueue)、
Slurm [概览](https://slurm.schedmd.com/overview.html)和 Codex
[Goal mode 指南](https://learn.chatgpt.com/use-cases/follow-goals)。如果集群要求 Slurm，
应由 Slurm 负责资源分配；Awaitless v0.1 不是调度器。

## 可靠性模型

- Local runner 与用户命令使用独立会话和进程组；取消会终止整个已校验的进程组。
- SQLite 使用 WAL；活跃态到终态的变化通过事务保护，完成、取消和停滞检测不会互相覆盖。
- SSH wrapper 原子保存 `exit_code` 和 `finished_at`。轻量 heartbeat 适配不同 SSH
  会话不能查看同一 PID 命名空间的主机，PID、进程组和 `/proc` 启动时钟作为兼容回退。
- SSH 取消会先持久化意图，再向已校验的进程组发信号；主机密钥检查沿用 OpenSSH 安全默认值。
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

Codex Skill 位于
[`skills/awaitless`](https://github.com/xpluspro/Awaitless/tree/main/skills/awaitless)，
v0.1 产品需求位于
[`docs/PRD.zh-CN.md`](https://github.com/xpluspro/Awaitless/blob/main/docs/PRD.zh-CN.md)。

## 许可证

[MIT](https://github.com/xpluspro/Awaitless/blob/main/LICENSE)
