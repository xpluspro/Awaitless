# 定性评审量表

定量结果无法覆盖“Agent/用户是否容易理解和维护这套方案”。每名评审者在没有看到性能
汇总的情况下，分别完成 [`review-template.json`](review-template.json)。至少两名评审者；先独立
评分，再讨论分歧。不得删除 tmux 获胜的维度，也不计算跨维度总分。

统一使用 1–5 分：

- **1**：能力缺失，或每次任务都需要临时人工推理；
- **2**：可以完成，但依赖未文档化约定或多次人工判断；
- **3**：有文档化步骤，需要少量自制 glue/解析；
- **4**：接口清楚、错误可诊断，只有边缘情况需要额外工作；
- **5**：一等能力，默认路径无需自制协议，并有可验证证据。

## 维度

| 维度 | 评审问题 | 必须查看的证据 |
|---|---|---|
| `result_interpretability` | Agent 能否直接区分 running/succeeded/failed，并取得真实退出码、有限日志和结构化结果？ | 原始返回、失败场景、large-log 场景 |
| `recovery_mental_model` | 新客户端只得到稳定 ID 时，下一步是否唯一且不需要找 PID/路径？ | recovery 轨迹和操作说明 |
| `failure_diagnosability` | 断线、任务失败、等待超时、任务超时和取消能否区分？ | 状态/退出码契约和注入结果 |
| `integration_effort` | Agent 或用户首次接入需要多少配置、命令和自制解析？ | 安装步骤、wrapper、工具定义 |
| `maintenance_burden` | 状态文件、锁、进程身份、日志上限和兼容性由谁长期维护？ | custom glue SLOC、依赖、backend 数 |
| `backend_portability` | local、SSH、Slurm 是否保持同一任务接口和结果形状？ | 三种 backend 的同场景证据；缺失不得猜测 |
| `interactive_debugging` | 人能否 attach、输入命令、操作 TUI/REPL 并保留现场？ | 真实交互演示；此维度预期 tmux 可能更强 |

每个分数必须包含一句可复核证据和一个主要限制。没有证据时填 `score: null`，不得按产品
宣传文案代填。
