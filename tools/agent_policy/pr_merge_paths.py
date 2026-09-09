"""Derive immutable first-parent path evidence for merge receipts."""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

Runner = Callable[..., Any]


def exact_first_parent_paths(
    root: Path,
    *,
    base_parent_sha: str,
    integration_commit_sha: str,
    runner: Runner = subprocess.run,
) -> list[str]:
    """Return the exact rename-disabled first-parent path set."""

    base = _required_sha(base_parent_sha, field="base_parent_sha")
    integration = _required_sha(integration_commit_sha, field="integration_commit_sha")
    result = runner(
        [
            "git",
            "-C",
            str(root),
            "diff",
            "--no-renames",
            "--name-only",
            "--diff-filter=ACDMRT",
            "-z",
            base,
            integration,
        ],
        capture_output=True,
        text=False,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace") if isinstance(result.stderr, bytes) else result.stderr
        diagnostic = " ".join((stderr or "git command failed").split())
        raise ValueError(f"git diff failed: {diagnostic}")
    stdout = result.stdout if isinstance(result.stdout, bytes) else result.stdout.encode()
    try:
        return sorted({item.decode("utf-8") for item in stdout.rstrip(b"\0").split(b"\0") if item})
    except UnicodeDecodeError as exc:
        raise ValueError("git diff paths must be UTF-8") from exc


def _required_sha(value: str, *, field: str) -> str:
    if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field} must be a full lowercase Git SHA")
    return value
