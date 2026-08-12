# Awaitless：面向 Coding Agents 的持久化执行层

状态：Current

产品基线：v0.5.0

更新日期：2026-08-12

> **Awaitless is a durable execution layer for coding agents.**

Awaitless 让 Claude Code、Codex、OpenCode 等 Coding Agent 只声明“我要执行什么”，
而不需要在推理循环里持续管理任务何时开始、资源何时空闲、连接是否存活，以及结果何时
可以取回。

一句话产品契约：

> **Agents submit work. Awaitless owns execution.**

这里的 `owns execution` 指 Awaitless 对外提供稳定的执行生命周期：持久化、准入、状态、
取消、结果与恢复。实际计算仍运行在用户自己的 Local、SSH 或 Slurm 基础设施上；使用
Slurm 时，Awaitless 把物理资源调度继续交给 Slurm。

---

## 1. 核心问题

Awaitless 解决的不是“Agent 不会等待一个命令”。聪明的 Agent 完全可以自己写：

```python
run()
sleep()
check()
```

真正的问题是，当 Agent 开始承担持续几十分钟甚至数小时的工程任务时，它会逐渐在每个
项目里重新拼装一套不完整的 execution substrate：

```text
Agent
  │
  │ submit work
  ▼
Awaitless
  ├── durable jobs
  ├── execution queues
  ├── resource ownership
  ├── concurrency control
  ├── completion / continuation
  ├── cancellation
  ├── bounded logs
  └── artifacts
       │
       ├── Local
       ├── SSH
       └── Slurm
```

Agent 不应该反复推理这些基础设施问题：

```text
Is the job done?
Is the GPU free?
Did the SSH process survive?
Should I start the next benchmark?
Where did the previous session leave off?
```

这些问题需要确定、持久、可恢复的执行语义，而不是更多模型推理。

## 2. 责任边界

### Agent 负责

- 理解用户目标；
- 决定运行什么；
- 选择实验、测试或优化方向；
- 解释结果并决定下一步；
- 在需要时明确取消或重新提交工作。

### Awaitless 负责

- 为一次逻辑提交提供稳定 Job ID 和幂等边界；
- 让 workload 生命周期独立于 Agent、终端和 MCP session；
- 在有限资源上持久排队，并按既定策略准入；
- 跟踪运行状态、退出码、取消和超时；
- 保存完整日志，同时只向 Agent 返回有限上下文；
- 保存并交付结构化 Artifact；
- 让完成结果在断线和重连后仍可恢复；
- 在不同 Backend 上保持相同的上层任务契约。

核心原则：

> **Reasoning belongs to the agent. Execution belongs to Awaitless.**

## 3. 三个核心能力

### 3.1 Durable Execution

任务必须独立于 Agent session 存活：

```text
submit
→ disconnect
→ reconnect later
→ recover by job ID
→ collect result
```

Agent session 的关闭、waiter 的中断或 stdio MCP server 的重启，都不应自动取消已经提交
的 workload。昂贵任务的逻辑重试还必须可幂等恢复，避免因为响应丢失而重复启动。

当前状态：v0.5 已在 Local、SSH 和 Slurm 上提供稳定 Job ID、持久状态、取消、有限日志、
JSON Artifact 和基于 `client_request_id` 的幂等提交。

### 3.2 Queue & Resource Ownership

有限资源不应该由 Agent 通过 `nvidia-smi`、`ps` 或 SSH polling 管理。例如一张 GPU：

```text
benchmark A    RUNNING
benchmark B    QUEUED
benchmark C    QUEUED
```

Agent 应该能够立即提交全部工作意图，由 Awaitless 决定任务何时获得执行资格。

当前状态：v0.5 已为 Local 和 SSH 提供持久命名 FIFO 队列、固定并发、非抢占准入和 queued
job cancellation。Slurm 仍是 Slurm Job 的唯一资源调度器，Awaitless 只统一它的任务状态
与结果契约。

当前队列不是通用资源调度器。它不提供 priority、抢占、GPU 自动发现、配额或 DAG。

### 3.3 Event-driven Continuation

最终目标不是让 Agent “更优雅地等待”，而是让执行结果成为一个可持久消费的事件：

```text
submit work
→ continue useful reasoning
→ work finishes
→ continuation becomes available
→ agent consumes the result
```

把：

```text
reason → poll → reason → poll → reason → poll
```

变成：

```text
reason → submit
       ↓
   execution
       ↓
reason ← result
```

当前状态：v0.5 已交付第一版持久 completion feed。`awaitless completions` 与
`wait_for_completions` 让 Agent 通过可重放 cursor 消费多个 Job 的有界结果；单 Job 的
`awaitless wait` 和 MCP Tasks 继续保持兼容。宿主级主动唤醒仍需要未来的 Agent adapter，
不属于底层 completion primitive。

“Continuation”不意味着 Awaitless 接管推理、自动修改代码或执行任意 Agent prompt。
Awaitless 只负责可靠地声明“哪个工作已经完成，以及结果是什么”；如何继续仍由 Agent 决定。

## 4. 目标用户

Awaitless 不是主要为传统 HPC Operator 设计的。它面向的是：

> **让 Coding Agents 操作真实计算资源的人。**

包括：

- 使用 Claude Code、Codex、OpenCode 等工具完成真实工程任务的开发者；
- 让 Agent 驱动 GPU/NPU 工作站、远程构建机或 Slurm 集群的团队；
- 构建 autonomous coding / research agent 的工具开发者；
- 希望保留现有基础设施，同时给 Agent 一套稳定任务语义的研究与工程团队。

Awaitless 的直接接口消费者通常是 Agent，但安装、配置资源边界并承担基础设施成本的是人。
因此产品必须同时做到：对 Agent 足够简单，对 Operator 足够透明和可控。

## 5. 典型工作负载

- CUDA、Ascend、HIP 或 Triton Kernel 优化；
- benchmark-driven optimization；
- ML training 与 experiment sweeps；
- 大型 test suite；
- remote builds；
- SSH workstation 上的编译、测试和实验；
- GPU/NPU scarce-resource workflows；
- Slurm scientific computing；
- autonomous coding / research agent 的长时间工具调用。

共同特征不是“命令一定很慢”，而是任务具有一个或多个执行基础设施需求：需要断线存活、
等待稀缺资源、限制并发、恢复状态、统一取消、限制返回日志，或在另一个 session 中取回结果。

短暂且无需恢复的同步命令应继续直接执行；需要人类交互的 REPL、TUI 和 Shell 应继续使用 PTY。

## 6. 上层契约与下层复用

```text
Coding Agent
     ↕  submit / recover / consume result
Awaitless
     ↕  launch / observe / cancel
Execution Infrastructure
```

Awaitless 给上层提供一致的 Job 语义：

- `queued | starting | running | stalled` 等活动状态；
- `succeeded | failed | cancelled | timed_out | lost` 等终止状态；
- 稳定 Job ID；
- 退出码与起止时间；
- 有界 stdout/stderr；
- 声明式 JSON Artifact；
- 幂等提交与恢复；
- 明确取消。

下层继续复用用户已有的执行环境：

| Backend | Awaitless 的职责 | 保留给下层的职责 |
|---|---|---|
| Local | 进程组、持久状态、取消、日志、命名队列 | 操作系统执行与隔离能力 |
| SSH | 远端持久 Job、状态恢复、远端队列、结果回收 | SSH 认证、主机可用性与机器资源 |
| Slurm | 提交映射、统一状态、取消、日志与 Artifact | 分区、配额、优先级和物理资源调度 |

## 7. 产品边界

Awaitless **不是**：

- 一个更复杂的 `sleep`；
- 一个只包装 Shell 的命令别名；
- `tmux` 的替代品；
- 一个新的 Slurm；
- 一个完整 Kubernetes Scheduler；
- 托管计算平台或远程 Sandbox；
- 自动编程或自动研究框架；
- 通用 Workflow DAG 引擎。

| 工具或系统 | 最适合解决的问题 | Awaitless 与它的关系 |
|---|---|---|
| 同步 Shell / PTY | 短命令和交互式操作 | 不替代；没有持久化需求时直接使用 |
| `nohup` / `tmux` | 人类 detach/attach 和临时后台进程 | 可作为人工工具；不提供统一 Agent Job 契约 |
| Slurm | 集群资源分配与调度策略 | 复用；Awaitless 是 Agent-facing adapter |
| Kubernetes | 服务编排与容器控制平面 | 不重建；未来只可能作为 Backend 适配 |
| Agent workflow framework | 推理、规划和多步决策 | 不接管；只提供执行事实和结果 |

## 8. 产品原则

### 8.1 Workload 生命周期独立于 session 生命周期

断开客户端只能终止 waiter，不能隐式终止 workload。恢复依据必须是持久 ID，而不是内存句柄。

### 8.2 提交是意图，执行是受控生命周期

资源暂时不可用时也允许提交。`queued` 是正常状态，不是提交失败。

### 8.3 上层统一，下层克制

Awaitless 统一 Job 契约，但不复制成熟基础设施已有的调度能力。尤其不能在 Slurm 外再建立
一套相互冲突的资源 Scheduler。

### 8.4 默认保护 Agent 上下文

完整日志留在磁盘或远端 Job 目录；Agent 默认只收到有界日志尾部和结构化结果。日志增长
不能线性扩大模型上下文。

### 8.5 重试不得轻易重复执行昂贵工作

创建响应可能丢失。相同 `client_request_id` 和相同启动参数必须恢复原 Job；冲突复用必须
失败。无法证明安全重启时，优先 at-most-once launch，而不是猜测性重复执行。

### 8.6 取消必须明确

wait 超时、MCP 断线或 Agent session 关闭都不是取消信号。只有显式 cancel 才能停止 Job。

### 8.7 Continuation 是可消费结果，不是隐藏自动化

完成事件必须可恢复、可重放、可去重；结果之后的推理仍由 Agent 驱动。Awaitless 不执行
用户未声明的回调，也不因任务完成而擅自启动后续命令。

## 9. 当前产品基线

| 能力 | v0.5 状态 | 下一步 |
|---|---|---|
| Durable Local / SSH / Slurm Jobs | 已交付 | 继续加强故障与升级兼容性 |
| 稳定 Job ID 与幂等提交 | 已交付 | 作为所有新接口的身份基础 |
| 有界日志与 JSON Artifact | 已交付 | completion 返回继续遵守同一预算 |
| 取消、超时与断线恢复 | 已交付 | 增加 completion 路径的恢复验收 |
| Local / SSH 固定并发 FIFO 队列 | MVP 已交付 | 暂不加入 priority、抢占和 GPU discovery |
| MCP Tasks | 兼容层已交付 | 不把协议 polling 当作产品最终语义 |
| 多 Job completion / continuation | 持久 feed 已交付 | 由真实使用决定是否增加宿主 wakeup adapter |

版本演进应围绕三条主线补齐产品契约，而不是以 Backend 数量或 CLI 子命令数量衡量进展。

## 10. 成功标准

### 执行正确性

- 断开和重连后，Job 状态、退出码与 Artifact 保持正确；
- 相同逻辑提交不会因响应丢失而重复启动；
- 取消不会遗漏被管理的子进程或已提交的 Scheduler Job；
- 队列并发从不超过声明上限，queued cancellation 不启动命令。

### Agent 体验

- Agent-visible 的任务管理调用不随任务时长线性增长；
- Agent 不需要用重复 `sleep`、`ps`、`tail`、`nvidia-smi` 或 SSH 查询管理生命周期；
- 大日志不会被重复注入上下文；
- 多任务完成后，Agent 能从一个稳定入口发现并消费结果。

### 产品边界

- 用户计算始终留在用户自己的基础设施；
- 短命令和交互任务不被强行导入 Awaitless；
- Awaitless 不复制 Slurm、Kubernetes 或 Agent framework 的职责。

## 11. 对外表达

首页 Tagline：

> **Durable execution for coding agents.**

副标题：

> Submit long-running work once. Awaitless handles queues, scarce resources,
> disconnects, and results across local, SSH, and Slurm.

更有攻击性的场景标题：

> **Your coding agent should write code, not babysit jobs.**

随后用一句话解释品类：

> Awaitless is the durable execution layer between coding agents and the compute they use.

“减少 polling、工具调用和 token”是可测量的结果与证明，不是产品品类本身。

## 12. v0.5 交付

v0.5 让单 Job 的可恢复等待升级为跨 Job 的持久 completion feed，并给 CLI 与 MCP 客户端
一个可阻塞、可重放、有界的结果入口。详细范围、非目标、里程碑与验收标准见
[v0.5 发布记录](v0.5.zh-CN.md)。后续是否增加宿主 wakeup adapter、named consumer 或
声明式依赖，由 v0.5 的真实恢复正确性和 Agent 调用轨迹决定。
