# Awaitless v0.8 substitution-cost audit

| Arm | Capabilities | Backends | Consumer glue SLOC | Product implementation SLOC | Backend-specific SLOC | Evidence test files |
|---|---:|---|---:|---:|---:|---:|
| tmux_wrapped | 10/11 | local, ssh | 517 | 517 | 517 | 3 |
| awaitless | 11/11 | local, ssh, slurm | 0 | 4732 | 1286 | 6 |

| Capability | tmux wrapper | Awaitless |
|---|---:|---:|
| `stable_job_id` | yes | yes |
| `idempotent_submission` | yes | yes |
| `durable_recovery` | yes | yes |
| `bounded_logs` | yes | yes |
| `artifact_manifest` | yes | yes |
| `process_tree_cancel` | yes | yes |
| `named_concurrency_queue` | yes | yes |
| `ssh_backend` | yes | yes |
| `slurm_backend` | no | yes |
| `completion_cursor` | yes | yes |
| `disconnect_recovery` | yes | yes |

This is a static, evidence-linked maintenance audit, not a runtime reliability result. SLOC is a maintenance proxy and is not a quality score.
