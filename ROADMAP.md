# Awaitless Roadmap

## Current scope

Awaitless is maintained as a lightweight, self-hosted durable remote-job
execution layer for coding agents. The supported execution surfaces are:

- Local jobs with durable identity, recovery, bounded logs, artifacts, cancellation, and named queues.
- SSH jobs with the same lifecycle contract and daemonless remote installation.
- Slurm jobs where Slurm remains responsible for physical resource scheduling.

The repository includes a stable end-to-end recovery demo, an auditable benchmark
suite, and real SSH/CANN acceptance evidence. These are the project's primary
deliverables while the scope is frozen.

## Maintenance policy

Awaitless is in maintenance mode. The project hypothesis was tested against
carefully implemented `tmux`/shell workflows and existing `sbatch` primitives;
those tools already absorb much of the value for many users. The engineering
implementation remains useful and documented, but the project will not pursue
feature expansion to manufacture product-market fit.

Future work is accepted only when all of the following are true:

- A named user and a concrete workflow are identified.
- The same pain has appeared repeatedly in real use.
- The proposed change has a measurable improvement over the current workflow.
- Compatibility, recovery, and failure semantics can be tested.

## Explicit non-goals

The following are not planned without strong, repeated demand:

- A dashboard or hosted control plane.
- A hosted execution service or permissions system.
- Additional execution backends for their own sake.
- A generic scheduler, resource manager, or GPU topology service.
- Broad product packaging, growth work, or platform expansion.

This is a deliberately small boundary. Issues that do not name a real workflow
and a measurable need should remain discussion rather than become roadmap work.
