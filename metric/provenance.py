"""Repository provenance helpers used by benchmark evidence runners."""

from __future__ import annotations

import subprocess
from pathlib import Path


def git_state(root: Path) -> tuple[str, bool, list[str]]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    ).stdout.strip()
    lines = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=root,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    ).stdout.splitlines()
    return commit, any(not line.startswith("?? ") for line in lines), [
        line[3:] for line in lines if line.startswith("?? ")
    ]
