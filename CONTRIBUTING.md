# Contributing to Awaitless

Awaitless is a durable execution layer for coding agents using local, SSH, and
Slurm infrastructure. Contributions should preserve that narrow contract:
stable identity, safe recovery, ordered completion replay, bounded results, and
correct cancellation matter more than adding orchestration layers or new
dashboards.

## Development setup

Awaitless supports Python 3.10 through 3.14 on Linux.

```bash
python -m venv .venv
.venv/bin/python -m pip install -e . build ruff==0.15.21 twine
.venv/bin/python -m unittest discover -s tests -v
ruff check src tests benchmarks scripts metric
```

SSH and Slurm unit tests use controlled fakes and do not require a live cluster.
Never run a compute-heavy test on a shared login node. A real-cluster change
should include sanitized evidence from an allocated compute node.

## Change expectations

- Add a regression test for every lifecycle, recovery, or cancellation fix.
- Keep stdout/stderr returned to an Agent explicitly bounded and marked when
  truncated.
- Preserve real exit codes and scheduler IDs across client restarts.
- Use `client_request_id` for retryable submission paths; do not introduce a
  launch side effect before the durable idempotency reservation.
- Keep MCP Tasks wire models aligned with the current extension and retain the
  established MCP tools during the extension's migration period.
- Preserve one terminal completion per Job. Never advance a completion cursor
  past a result that could not be delivered.
- Update both English and Chinese README sections for user-visible behavior.

## Pull requests

Before opening a pull request, run the full test and Ruff commands above. For a
release-related change, also build and check the distributions:

```bash
.venv/bin/python -m build
.venv/bin/python -m twine check dist/*
```

Describe the failure mode, the durability invariant affected, and how the new
test proves the invariant. Performance claims must include raw JSON evidence,
the baseline implementation, and the exact metric definition.

## Releases

Maintainers update `pyproject.toml`, `src/awaitless/__init__.py`, `server.json`,
and `CHANGELOG.md` to the same version, then push an annotated `vX.Y.Z` tag. The
release workflow tests all supported Python versions, builds and smoke-tests the
wheel, publishes PyPI through Trusted Publishing, creates a GitHub Release, and
publishes `server.json` to the official MCP Registry with GitHub Actions OIDC.

Please report security-sensitive issues privately to the repository owner
instead of attaching exploit details to a public issue.
