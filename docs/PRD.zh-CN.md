# Awaitless 产品需求文档

版本：v0.1
状态：Draft
项目类型：开源开发者工具
目标用户：使用 Codex、Claude Code、Cursor 等编程 Agent 执行远程实验、编译和基准测试的开发者

---

## 1. 项目概述

Awaitless 是一个面向 AI 编程 Agent 的持久化任务管理工具。

它允许 Agent 将长时间运行的本地或远程命令提交为独立作业，并通过稳定的 `job_id` 查询、等待、恢复和取消任务。作业运行期间无需由语言模型反复执行 `sleep`、`ps`、`tail` 或 SSH 查询。

Awaitless 的核心目标是：

> 将 AI Agent 对长任务的高频轮询，转换为普通进程内部的低成本等待和事件通知。

典型使用方式：

```bash
awaitless submit \
  --host dcu-server \
  --cwd /workspace/vllm \
  -- ./run_microbench.sh
```

返回：

```text
job_01JZ8E2K7F6R
```

随后 Agent 执行：

```bash
awaitless wait job_01JZ8E2K7F6R
```

该命令阻塞到任务完成、失败、超时或进入异常状态，并一次性返回结构化结果。

---

## 2. 背景与问题

AI 编程 Agent 在处理长时间运行的命令时，通常无法可靠估计运行时长。

常见行为如下：

```bash
sleep 45
ps -p <pid>
tail -n 100 run.log
```

如果任务尚未结束，Agent 会重复上述过程。

这一模式存在以下问题：

1. 每次检查都会产生新的工具调用和模型推理。
2. 重复返回的日志会不断扩大上下文。
3. `sleep 45` 的间隔与实际运行时间无关。
4. 不同 SSH 调用运行在不同 Shell 中，无法直接使用 `wait <pid>`。
5. SSH 或 Agent 会话中断后，任务状态可能丢失。
6. Agent 容易误判进程状态、PID 复用或日志停滞。
7. 长时间实验会产生大量无实际决策价值的 token 消耗。
8. 任务完成后，Agent 经常需要额外解析大段非结构化日志。

Codex PTY 或持久终端能够解决同一终端会话中的部分等待问题，但不能完整解决：

* SSH 断线后任务继续运行；
* Codex 重启后重新连接任务；
* 跨 Shell 获取退出码；
* 持久化作业状态；
* 多种任务后端统一管理；
* 结构化提取 benchmark 结果；
* Agent 只在状态变化时重新推理。

---

## 3. 产品目标

### 3.1 核心目标

Awaitless 必须实现：

1. 将命令提交为持久化作业。
2. 为每个作业生成稳定且唯一的 `job_id`。
3. 支持一次阻塞式等待，而不是模型驱动的轮询。
4. 保留任务退出码、日志、运行时长和状态。
5. SSH 连接中断后，远端作业继续运行。
6. Agent 或 Awaitless 客户端重启后，可以重新查询作业。
7. 限制返回给 Agent 的日志量。
8. 通过 CLI 提供稳定接口。
9. 提供适合 AI Agent 使用的结构化 JSON 输出。
10. 提供 Codex Skill 或 MCP 集成，引导 Agent 正确使用工具。

### 3.2 非目标

Awaitless v0.1 不计划：

* 替代 Slurm、Kubernetes 或其他集群调度器；
* 实现完整的分布式任务调度平台；
* 实现 GPU 资源分配；
* 提供多租户权限管理；
* 提供大型 Web 控制台；
* 准确预测所有命令的运行时间；
* 自动修改或优化用户代码；
* 直接替代 PTY 或终端模拟器；
* 保证远程机器宕机后的任务恢复；
* 支持任意复杂的工作流 DAG。

---

## 4. 目标用户

### 4.1 AI 辅助性能优化开发者

典型任务：

* GPU kernel microbenchmark；
* CUDA、HIP、Triton 或 C++ 扩展编译；
* profiler 采集；
* correctness 测试；
* 多组固定 shape benchmark；
* 模型推理性能测试。

用户希望 Agent 在任务完成后继续分析，而不是持续查询。

### 4.2 HPC 和科研用户

典型任务：

* SSH 远程实验；
* Slurm 作业；
* 数值模拟；
* 模型训练；
* 数据预处理；
* 长时间单元测试。

用户需要稳定保存任务状态，并在断线后重新连接。

### 4.3 AI Agent 工具开发者

工具开发者希望通过统一接口，为不同 Agent 提供：

* 提交任务；
* 等待完成；
* 查询状态；
* 获取日志；
* 取消任务；
* 读取结构化结果。

---

## 5. 典型用户场景

### 场景一：远程 GPU microbenchmark

Agent 修改一个 HIP kernel 后，需要在远端 DCU 机器执行 benchmark。

传统方式：

1. SSH 上传代码；
2. 启动 benchmark；
3. 每隔 45 秒查询一次；
4. 重复读取日志；
5. benchmark 结束后解析结果。

Awaitless 方式：

1. Agent 调用 `awaitless submit`；
2. Awaitless 在远端持久化启动任务；
3. Agent 调用一次 `awaitless wait`；
4. Awaitless 内部等待任务结束；
5. Agent 收到 correctness、latency、退出码和日志摘要。

### 场景二：长时间编译

Agent 执行需要十几分钟的编译：

```bash
awaitless submit -- ninja -C build
```

即使 Codex 终端暂时断开，编译仍继续。重新连接后：

```bash
awaitless status <job-id>
```

即可恢复状态。

### 场景三：Slurm 作业

用户提交：

```bash
awaitless submit \
  --backend slurm \
  -- sbatch benchmark.slurm
```

Awaitless 将 Slurm job ID 映射为统一的 Awaitless job ID。

Agent 不需要理解各类调度器的具体状态格式。

---

## 6. 核心概念

### 6.1 Job

Job 表示一个由 Awaitless 管理的持久化任务。

每个 Job 至少包含：

```json
{
  "job_id": "job_01JZ8E2K7F6R",
  "backend": "ssh",
  "state": "running",
  "host": "dcu-server",
  "command": "./run_microbench.sh",
  "cwd": "/workspace/vllm",
  "created_at": "2026-07-16T10:00:00Z",
  "started_at": "2026-07-16T10:00:01Z",
  "finished_at": null,
  "exit_code": null
}
```

### 6.2 Backend

Backend 负责实际运行和管理任务。

v0.1 支持：

* `local`
* `ssh`

后续计划支持：

* `systemd`
* `tmux`
* `slurm`
* `kubernetes`

### 6.3 Job State

统一状态模型：

```text
pending
starting
running
succeeded
failed
cancelled
timed_out
stalled
lost
```

状态说明：

* `pending`：任务已创建，尚未开始。
* `starting`：正在准备执行环境。
* `running`：任务正在运行。
* `succeeded`：任务退出码为 0。
* `failed`：任务退出码非 0。
* `cancelled`：用户主动取消。
* `timed_out`：超过配置的最大运行时间。
* `stalled`：任务仍存在，但满足停滞判定条件。
* `lost`：无法确认任务是否仍然存在。

终止状态包括：

```text
succeeded
failed
cancelled
timed_out
lost
```

`stalled` 默认不是终止状态，除非用户配置自动终止。

---

## 7. 功能需求

## 7.1 提交任务

CLI：

```bash
awaitless submit [OPTIONS] -- COMMAND...
```

示例：

```bash
awaitless submit \
  --host dcu-server \
  --cwd /workspace/project \
  --env BENCHMARK_MODE=1 \
  --timeout 2h \
  -- ./run.sh
```

必须支持：

* 指定命令；
* 指定工作目录；
* 指定环境变量；
* 指定本地或 SSH 后端；
* 指定最大运行时间；
* 指定日志目录；
* 指定结果文件；
* 指定任务名称；
* JSON 输出。

成功时返回：

```json
{
  "job_id": "job_01JZ8E2K7F6R",
  "state": "running",
  "backend": "ssh"
}
```

### 验收要求

* 提交命令必须在 5 秒内返回 `job_id`，不等待任务结束。
* SSH 断开后，远端任务仍继续运行。
* 任务命令和元数据必须持久化。
* 启动失败时必须返回明确错误，不得生成伪运行状态。

---

## 7.2 等待任务

CLI：

```bash
awaitless wait <job-id> [OPTIONS]
```

示例：

```bash
awaitless wait job_01JZ8E2K7F6R --timeout 3h --json
```

行为：

1. 如果任务已结束，立即返回结果。
2. 如果任务正在运行，阻塞等待状态变化。
3. 等待循环发生在 Awaitless 进程内部。
4. 不要求 AI Agent 周期性调用状态接口。
5. 超过客户端等待超时后，可以返回但不得终止远端任务，除非显式配置。

返回示例：

```json
{
  "job_id": "job_01JZ8E2K7F6R",
  "state": "succeeded",
  "exit_code": 0,
  "duration_seconds": 823.4,
  "stdout_tail": "candidate latency: 24.7 us",
  "stderr_tail": "",
  "artifacts": [
    {
      "path": "results/benchmark.json",
      "size_bytes": 1832
    }
  ]
}
```

### 验收要求

对于一个运行 10 分钟的任务，Agent 只需要：

1. 一次 `submit`；
2. 一次 `wait`；
3. 任务完成后一次结果分析。

不应要求 Agent 发起中间状态查询。

---

## 7.3 查询任务状态

CLI：

```bash
awaitless status <job-id>
```

返回字段至少包括：

* 当前状态；
* PID 或后端任务标识；
* 创建时间；
* 开始时间；
* 已运行时间；
* 退出码；
* 最后一次日志更新时间；
* stdout 和 stderr 大小；
* 后端连接状态。

示例：

```json
{
  "job_id": "job_01JZ8E2K7F6R",
  "state": "running",
  "elapsed_seconds": 312.5,
  "last_output_at": "2026-07-16T10:04:52Z",
  "stdout_bytes": 18320,
  "stderr_bytes": 0
}
```

---

## 7.4 查看日志

CLI：

```bash
awaitless logs <job-id>
awaitless logs <job-id> --tail 200
awaitless logs <job-id> --follow
```

默认行为：

* 默认只返回最后 200 行；
* 设置最大返回字节数；
* stdout 和 stderr 分离；
* 完整日志保存在磁盘；
* 日志截断时必须显式标记。

示例：

```json
{
  "truncated": true,
  "stdout_tail": "...",
  "stderr_tail": "..."
}
```

不得默认将完整大型日志发送给 Agent。

---

## 7.5 取消任务

CLI：

```bash
awaitless cancel <job-id>
```

取消过程：

1. 向主进程发送温和终止信号；
2. 等待配置的 grace period；
3. 如果仍未退出，则强制终止；
4. 尽可能终止对应进程组；
5. 保存最终状态和退出信息。

必须避免只终止父 Shell 而留下 benchmark 子进程。

---

## 7.6 列出任务

CLI：

```bash
awaitless list
awaitless list --state running
awaitless list --host dcu-server
```

输出字段：

* job ID；
* 名称；
* 后端；
* 主机；
* 状态；
* 创建时间；
* 已运行时间；
* 退出码。

---

## 7.7 结果文件与 Artifact

用户可以在提交任务时声明结果文件：

```bash
awaitless submit \
  --artifact results/benchmark.json \
  --artifact profiles/kernel.csv \
  -- ./run_benchmark.sh
```

任务结束后，Awaitless 返回：

* 文件是否存在；
* 文件大小；
* 修改时间；
* 可选内容摘要；
* 可选本地同步路径。

v0.1 不要求自动解析任意格式，但必须支持 JSON 文件读取。

示例：

```json
{
  "parsed_results": {
    "correctness": true,
    "baseline_us": 31.2,
    "candidate_us": 24.7,
    "speedup": 1.263
  }
}
```

---

## 7.8 任务恢复

Awaitless 客户端重启后，必须能够通过持久化元数据恢复任务。

恢复后：

```bash
awaitless status <job-id>
awaitless wait <job-id>
```

仍应可用。

恢复逻辑不能仅依赖内存中的子进程对象。

---

## 7.9 停滞检测

可选配置：

```bash
--stall-timeout 20m
```

当任务满足以下条件时，可标记为 `stalled`：

* 进程仍存在；
* 日志在指定时间内无变化；
* 可选的 GPU 或 CPU 活跃度低于阈值；
* 结果文件没有更新。

v0.1 最低要求只基于日志更新时间判断。

默认情况下，`stalled` 只触发状态提示，不自动终止任务。

---

## 8. SSH 后端需求

远端任务必须使用独立目录：

```text
~/.awaitless/jobs/<job-id>/
├── metadata.json
├── command.sh
├── stdout.log
├── stderr.log
├── pid
├── pgid
├── started_at
├── finished_at
└── exit_code
```

远端 wrapper 必须：

1. 设置工作目录；
2. 设置环境变量；
3. 启动用户命令；
4. 捕获 PID 和进程组；
5. 保存退出码；
6. 保存开始和结束时间；
7. 即使 SSH 连接断开，任务仍继续；
8. 以原子方式写入终止状态文件。

建议第一版使用：

```bash
setsid nohup bash command.sh ...
```

任务完成后必须写入：

```text
exit_code
finished_at
```

不得仅通过 `ps` 判断任务是否成功。

---

## 9. CLI 设计

完整命令集合：

```text
awaitless submit
awaitless wait
awaitless status
awaitless logs
awaitless cancel
awaitless list
awaitless inspect
awaitless doctor
```

### 全局选项

```text
--json
--config <path>
--verbose
--quiet
```

### 退出码约定

* `0`：操作成功，或等待的任务成功。
* `1`：Awaitless 内部错误。
* `2`：参数错误。
* `3`：任务失败。
* `4`：任务超时。
* `5`：任务被取消。
* `6`：任务状态丢失。
* `7`：连接失败。

---

## 10. MCP 接口

Awaitless 后续应提供 MCP server。

建议工具：

```text
submit_job
wait_for_job
get_job_status
get_job_logs
cancel_job
list_jobs
```

### submit_job

输入：

```json
{
  "backend": "ssh",
  "host": "dcu-server",
  "command": "./run_microbench.sh",
  "cwd": "/workspace/vllm",
  "timeout_seconds": 7200
}
```

输出：

```json
{
  "job_id": "job_01JZ8E2K7F6R",
  "state": "running"
}
```

### wait_for_job

输入：

```json
{
  "job_id": "job_01JZ8E2K7F6R",
  "timeout_seconds": 10800
}
```

输出：

```json
{
  "state": "succeeded",
  "exit_code": 0,
  "duration_seconds": 823.4,
  "stdout_tail": "...",
  "stderr_tail": ""
}
```

MCP 返回内容必须设置严格大小限制，避免把完整日志放入模型上下文。

---

## 11. Codex Skill 行为约束

仓库应提供一个可安装的 `SKILL.md`，指导 Agent：

```text
对于预计运行时间超过 30 秒的命令：

1. 使用 Awaitless 提交任务。
2. 获取 job_id 后调用一次 wait。
3. 不要使用重复的 sleep、ps、tail 或 SSH 查询。
4. 只有在 wait 返回失败、超时或 stalled 时才读取日志。
5. 默认只读取有限数量的日志尾部。
6. 完成后优先读取结构化 benchmark 结果。
```

Skill 应明确区分：

* 普通短命令：直接执行；
* 可交互命令：使用 PTY；
* 长时间持久任务：使用 Awaitless；
* 调度集群任务：使用对应 Awaitless backend。

---

## 12. 数据存储

本地使用 SQLite 保存：

* Job 元数据；
* 后端信息；
* 状态变化；
* 本地日志路径；
* Artifact；
* 历史运行时间；
* 错误记录。

建议数据目录：

```text
~/.local/share/awaitless/
├── awaitless.db
├── jobs/
└── logs/
```

配置目录：

```text
~/.config/awaitless/config.toml
```

示例配置：

```toml
[defaults]
backend = "local"
log_tail_lines = 200
max_return_bytes = 65536

[hosts.dcu]
hostname = "60.204.206.121"
port = 20012
user = "root"
identity_file = "~/.ssh/id_ed25519"
remote_job_dir = "~/.awaitless/jobs"
```

敏感凭证不得写入 Job 元数据或日志。

---

## 13. 非功能需求

### 13.1 可靠性

* 客户端崩溃不得直接终止远端任务。
* 元数据写入应尽可能原子化。
* 必须防止部分写入导致错误终止状态。
* 必须正确处理 SSH 暂时不可用。
* 必须避免 PID 复用导致误判。
* 应优先使用 job 目录、启动时间和进程组共同识别任务。

### 13.2 性能

* `submit` 本地开销目标小于 1 秒。
* SSH `submit` 在连接可用时目标小于 5 秒。
* 状态查询目标小于 2 秒。
* Awaitless 自身 CPU 使用率应接近空闲。
* 内部轮询不得高频执行，默认间隔建议为 2 至 10 秒。

### 13.3 安全性

* 默认不启用 Shell 字符串拼接执行。
* 命令参数应尽可能以数组形式处理。
* SSH 主机密钥检查默认开启。
* 不记录私钥、密码或 token。
* 远端任务目录权限默认设置为仅当前用户可访问。
* `cancel` 只能操作 Awaitless 创建的任务。

### 13.4 可移植性

最低支持：

* Linux；
* Python 3.10 及以上；
* OpenSSH 客户端；
* Bash。

后续可支持 macOS。Windows 原生支持不属于 v0.1 范围。

---

## 14. 运行时间估计

运行时间估计属于增强功能，不是 MVP 的必要条件。

Awaitless 可以记录：

* 命令 fingerprint；
* 主机；
* GPU 型号；
* Git commit；
* 工作目录；
* 参数；
* 历史运行时长；
* 成功或失败状态。

初始估计方法：

* 相同 fingerprint 的历史中位数；
* P10 和 P90 区间；
* 指数移动平均；
* 样本数量和置信度。

示例：

```json
{
  "estimated_duration_seconds": 52,
  "estimated_range_seconds": [36, 81],
  "sample_count": 12,
  "confidence": "medium"
}
```

运行时间估计只用于：

* 用户界面展示；
* 异常检测；
* stall threshold 建议。

不得依赖估计时间决定 Agent 的下一次模型轮询。

---

## 15. MVP 范围

v0.1 必须完成：

* Python CLI；
* Local backend；
* SSH backend；
* `submit`；
* `wait`；
* `status`；
* `logs`；
* `cancel`；
* `list`；
* SQLite 持久化；
* stdout 和 stderr 文件；
* 退出码记录；
* 客户端重启后恢复；
* JSON 输出；
* Codex Skill；
* 基础测试和示例。

v0.1 可以暂不完成：

* MCP server；
* Slurm backend；
* systemd backend；
* Web UI；
* GPU 利用率检测；
* 运行时间预测；
* Artifact 自动下载；
* 多任务工作流；
* 多用户服务端模式。

---

## 16. 验收标准

### 16.1 基础任务

提交：

```bash
awaitless submit -- bash -c "sleep 60; exit 0"
```

要求：

* 立即返回 job ID；
* 状态为 `running`；
* 60 秒后状态为 `succeeded`；
* 退出码为 0；
* 记录正确运行时间。

### 16.2 失败任务

```bash
awaitless submit -- bash -c "echo error >&2; exit 7"
```

要求：

* 最终状态为 `failed`；
* 保存退出码 7；
* stderr 可读取。

### 16.3 SSH 断线

1. 通过 SSH backend 提交 5 分钟任务；
2. 断开 SSH；
3. 关闭 Awaitless 客户端；
4. 重新启动客户端；
5. 查询原 job ID。

要求：

* 远端任务继续执行；
* 状态可恢复；
* 最终退出码正确。

### 16.4 无模型轮询

对一个运行 10 分钟的任务，Agent 操作记录中只应包含：

```text
submit
wait
```

不得出现周期性：

```text
sleep
ps
tail
status
```

### 16.5 日志限制

生成 100 MB stdout。

要求：

* 完整日志保存到文件；
* 默认 JSON 返回不超过配置的最大字节数；
* 返回结果明确标记日志已截断。

### 16.6 取消任务

提交包含多个子进程的任务后执行：

```bash
awaitless cancel <job-id>
```

要求：

* 主进程和关联子进程均被终止；
* 状态为 `cancelled`；
* 不留下持续占用 GPU 的孤儿进程。

---

## 17. 里程碑

### Milestone 1：本地任务模型

实现：

* Job 数据模型；
* SQLite；
* Local backend；
* submit、status、wait；
* 日志和退出码。

### Milestone 2：SSH 持久化任务

实现：

* 主机配置；
* 远端 job 目录；
* SSH 提交；
* 断线继续运行；
* 状态恢复；
* cancel。

### Milestone 3：Agent 使用体验

实现：

* JSON schema；
* Codex Skill；
* 日志预算；
* Artifact JSON；
* 真实 GPU benchmark demo。

### Milestone 4：扩展能力

实现：

* MCP server；
* Slurm backend；
* systemd backend；
* 运行时间历史统计；
* stall detection。

---

## 18. 风险

### 18.1 Agent 不主动使用 Awaitless

即使安装工具，Agent 仍可能直接执行长命令。

缓解措施：

* 提供明确的 Codex Skill；
* 给出长任务判定标准；
* 文档中提供 Agent 配置示例；
* MCP 工具命名强调 `long_running_job`；
* 对重复轮询行为提供 lint 或警告。

### 18.2 远端进程状态不可靠

仅保存 PID 会受到 PID 复用影响。

缓解措施：

* 保存进程启动时间；
* 保存进程组；
* 使用独立 job 目录；
* 保存 wrapper 完成文件；
* 后续优先支持 systemd 或 Slurm 等可靠后端。

### 18.3 SSH 网络不稳定

等待过程中可能暂时断线。

缓解措施：

* 任务状态保存在远端；
* 客户端自动重连；
* 连接失败不立即将任务标记为失败；
* 使用 `unknown` 或 `lost` 前设置合理重试窗口。

### 18.4 长时间阻塞工具调用被平台中断

Codex 或其他 Agent 平台可能限制单次工具调用持续时间。

缓解措施：

* CLI 支持长阻塞；
* MCP 后续支持 completion event；
* 平台超时只终止本地 waiter，不终止远端任务；
* Agent 可重新调用相同 job ID 的 `wait`。

---

## 19. 成功指标

Awaitless 的效果应通过以下指标衡量：

* 每个长任务的 Agent 工具调用次数；
* 长任务期间模型推理轮数；
* 重复日志进入上下文的字节数；
* 长任务相关 token 消耗；
* SSH 断线后的任务恢复成功率；
* 退出码和任务状态准确率；
* 用户手动干预次数。

核心目标：

```text
10 分钟远程实验
传统方式：10 至 20 次轮询
Awaitless：1 次 submit + 1 次 wait
```

预期减少超过 80% 的长任务管理相关 Agent 调用。

---

## 20. 一句话定位

> Awaitless 是面向 AI 编程 Agent 的持久化任务运行器，让 Agent 不再浪费 token 轮询长时间运行的本地和远程作业。

英文副标题：

> Event-driven durable jobs for AI coding agents.

宣传语：

> Stop coding agents from wasting tokens polling long-running jobs.
