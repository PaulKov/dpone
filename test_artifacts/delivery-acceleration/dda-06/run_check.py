"""Execute one explicit command and retain its exact-source validation receipt.

Usage: uv run python <this file> <unique-name> <command> [arguments...].
Reports describe local check execution, never live certification authority.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUTPUT = Path(__file__).resolve().parent


def git(*arguments: str) -> str:
    return subprocess.check_output(["git", *arguments], cwd=ROOT, text=True).strip()


def identity() -> dict[str, str]:
    paths = git(
        "ls-files",
        "--cached",
        "--others",
        "--exclude-standard",
        "--",
        "src",
        "tests",
        "docs",
        "tools",
        "pyproject.toml",
        "uv.lock",
        "mypy.ini",
        "mkdocs.yml",
        "CHANGELOG.md",
    )
    digest = hashlib.sha256()
    for name in sorted(set(paths.splitlines())):
        path = ROOT / name
        digest.update(name.encode() + b"\0")
        digest.update(path.read_bytes() if path.is_file() else b"MISSING")
        digest.update(b"\0")
    return {
        "commit": git("rev-parse", "HEAD"),
        "tree": git("rev-parse", "HEAD^{tree}"),
        "source_sha256": digest.hexdigest(),
        "producer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "working_tree_dirty": git(
            "status",
            "--porcelain",
            "--",
            "src",
            "tests",
            "docs",
            "tools",
            "pyproject.toml",
            "uv.lock",
            "mypy.ini",
            "mkdocs.yml",
            "CHANGELOG.md",
        ),
    }


def run(name: str, command: list[str]) -> int:
    if not name or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for char in name) or not command:
        raise ValueError("Use a unique lowercase check name and an explicit command")
    report = OUTPUT / (name + ".json")
    log = OUTPUT / (name + ".log")
    before = identity()
    record = {
        "schema_version": 1,
        "name": name,
        "command": command,
        "source_before": before,
        "status": "UNVERIFIED",
        "reason": "Check has not completed",
        "execution": "local",
        "platform": platform.platform(),
        "python": platform.python_version(),
        "log": log.name,
        "pytest_auto_workers": 2,
    }
    with report.open("x") as stream:
        json.dump(record, stream, indent=2)
    started = time.monotonic()
    with log.open("x") as stream:
        process = subprocess.run(
            command,
            cwd=ROOT,
            env=dict(os.environ, PYTEST_XDIST_AUTO_NUM_WORKERS="2"),
            stdout=stream,
            stderr=subprocess.STDOUT,
            check=False,
        )
    after = identity()
    unchanged = before == after and not before["working_tree_dirty"]
    record.update(
        source_after=after,
        source_unchanged=unchanged,
        exit_code=process.returncode,
        duration_seconds=round(time.monotonic() - started, 3),
        status=("PASS" if process.returncode == 0 else "FAIL") if unchanged else "UNVERIFIED",
        reason=None if unchanged else "Source was dirty or source/producer identity changed during execution",
        log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
    )
    temporary = report.with_suffix(".tmp")
    temporary.write_text(json.dumps(record, indent=2) + "\n")
    temporary.replace(report)
    print(name, record["status"], record["duration_seconds"], flush=True)
    return process.returncode if unchanged else 2


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1], sys.argv[2:]))
