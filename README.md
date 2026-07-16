# Awaitless

面向 AI 编程 Agent 的持久化任务运行器。它把长命令交给独立进程或远端 wrapper，并用稳定的 `job_id` 完成等待、恢复、日志读取和取消，避免 Agent 重复执行 `sleep`、`ps`、`tail` 或 SSH 轮询。

> Event-driven durable jobs for AI coding agents.

## 安装

需要 Linux、Python 3.10+、Bash；SSH 后端还需要 OpenSSH 客户端。

```bash
python -m pip install -e .
awaitless doctor --json
```

默认数据保存在 `~/.local/share/awaitless`，也可通过 `AWAITLESS_DATA_DIR` 或配置文件修改。

## 快速开始

提交会在任务结束前返回：

```bash
awaitless submit --json --name build -- ninja -C build
```

```json
{"job_id":"job_019F...","state":"running","backend":"local"}
```

随后只调用一次阻塞式等待：

```bash
awaitless wait job_019F... --json
```

`wait` 被终端或平台中断不会终止任务；重新使用同一 `job_id` 即可恢复。

常用命令：

```bash
awaitless status <job-id> --json
awaitless logs <job-id> --tail 200 --json
awaitless cancel <job-id> --grace-period 5s --json
awaitless list --state running --json
awaitless inspect <job-id> --json
```

## 结构化结果

声明 Artifact 后，任务结束时会返回存在性、大小、修改时间；小于日志预算的 JSON 文件还会作为 `parsed_results` 返回。

```bash
awaitless submit --json \
  --cwd /workspace/project \
  --artifact results/benchmark.json \
  -- bash -c './benchmark > run.txt'
```

默认只返回 stdout/stderr 最后 200 行，合计内容预算为 64 KiB。完整日志仍保存在任务目录中，截断时 JSON 的 `truncated` 为 `true`。

## SSH 后端

在 `~/.config/awaitless/config.toml` 中配置别名：

```toml
[defaults]
backend = "local"
log_tail_lines = 200
max_return_bytes = 65536
poll_interval = 2

[hosts.dcu]
hostname = "gpu.example.com"
port = 22
user = "developer"
identity_file = "~/.ssh/id_ed25519"
remote_job_dir = "~/.awaitless/jobs"
```

```bash
awaitless submit --host dcu --cwd /workspace/vllm --env BENCHMARK_MODE=1 \
  --timeout 2h --artifact results.json -- ./run_microbench.sh
```

远端任务通过 `setsid nohup` 启动，状态与退出码原子写入独立目录。主机密钥检查沿用 OpenSSH 的安全默认值；Awaitless 不传递禁用检查的选项。

## 状态和退出码

状态：`starting`、`running`、`stalled`、`succeeded`、`failed`、`cancelled`、`timed_out`、`lost`。`--stall-timeout 20m` 只提示停滞，不自动取消。

CLI 退出码：0 成功，1 内部错误，2 参数错误，3 任务失败，4 任务或客户端等待超时，5 已取消，6 状态丢失，7 SSH 连接失败。

## 可靠性模型

- Local runner 与客户端会话分离，用户命令位于独立进程组；取消会终止整个进程组。
- SQLite 使用 WAL，状态变更带事件历史；PID 与 Linux `/proc` 启动时钟共同校验，降低 PID 复用误判。
- SSH 完成状态以 `exit_code` 和 `finished_at` 为准，不用 `ps` 推断成功。
- 环境变量名会记录用于诊断，疑似凭证的值在元数据中显示为 `<redacted>`；实际运行规格文件权限为 `0600`。

Codex Skill 位于 [`skills/awaitless`](skills/awaitless)，安装后会引导 Agent 对长任务执行一次 `submit` 和一次 `wait`。

## 开发与测试

```bash
python -m unittest discover -s tests -v
```

需求与验收边界见 [`docs/PRD.zh-CN.md`](docs/PRD.zh-CN.md)。
