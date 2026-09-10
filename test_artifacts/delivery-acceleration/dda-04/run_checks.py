"""Produce command evidence for DDA-04; logs describe execution, not live proof."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUTPUT = Path(__file__).resolve().parent
OWNED_TESTS = [
    "tests/test_mssql_native_partition_switch.py",
    "tests/test_mssql_native_partition_switch_catalog.py",
    "tests/test_mssql_native_partition_switch_recovery.py",
    "tests/test_runtime_partition_replace_native_contracts.py",
]
CHECKS = {
    "focused": ["uv", "run", "pytest", *OWNED_TESTS, "-q", "--junitxml=" + str(OUTPUT / "focused-junit.xml")],
    "contract": [
        "uv",
        "run",
        "python",
        "tools/agent_policy/task_contract.py",
        "test_artifacts/delivery-acceleration/agent-task-contracts/dda-04-switch.yml",
        "--format",
        "json",
    ],
    "selection": ["uv", "run", "python", "tools/agent_policy/select_checks.py", "--base-ref", "origin/master"],
    "ruff": ["uv", "run", "ruff", "check", "."],
    "format": ["uv", "run", "ruff", "format", "--check", "."],
    "mypy": ["uv", "run", "mypy", "--config-file", "mypy.ini"],
    "mypy-component": [
        "uv",
        "run",
        "mypy",
        "--config-file",
        "mypy.ini",
        "src/dpone/contracts/native_mssql_switch.py",
        "src/dpone/ports/native_mssql_switch.py",
        "src/dpone/runtime/sinks/mssql_native_switch",
    ],
    "imports": ["uv", "run", "dpone", "docs", "check-import-rules"],
    "layers": ["uv", "run", "dpone", "docs", "check-layer-metrics", "--baseline", "docs/layer_metrics_baseline.json"],
    "architecture": ["uv", "run", "dpone", "docs", "check-architecture-fitness"],
    "pytest": ["uv", "run", "pytest", "-m", "not integration_live", "-n", "auto", "--dist", "loadfile"],
    "docs": ["uv", "run", "dpone", "docs", "check-docs"],
    "references": ["uv", "run", "dpone", "docs", "check-generated-references"],
    "docs-language": ["uv", "run", "pytest", "tests/test_docs_language_contracts.py", "-q"],
    "mkdocs": ["uv", "run", "mkdocs", "build", "--strict"],
}


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def run(name: str) -> None:
    head = git("rev-parse", "HEAD")
    command = CHECKS.get(name)
    if name == "module-size":
        base = git("merge-base", head, "origin/master")
        if base == head:
            base = git("rev-parse", head + "^")
        command = [
            "uv",
            "run",
            "dpone",
            "docs",
            "check-module-size",
            "--baseline",
            "docs/module_size_baseline.json",
            "--base-ref",
            base,
            "--head-ref",
            head,
        ]
    if command is None:
        raise ValueError(name)
    started = time.time()
    env = dict(os.environ, PYTEST_XDIST_AUTO_NUM_WORKERS="2")
    with (OUTPUT / (name + ".log")).open("w") as output:
        process = subprocess.run(command, cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
    record = {
        "name": name,
        "command": command,
        "head": head,
        "planning_dependency": "f3682940f8864563cde0e6b6ecee60f746b49020",
        "diff_sha256": hashlib.sha256(git("diff", "HEAD", "--", "src", "tests", "docs").encode()).hexdigest(),
        "dirty": bool(git("status", "--porcelain")),
        "status": "PASS" if process.returncode == 0 else "FAIL",
        "exit_code": process.returncode,
        "duration_seconds": round(time.time() - started, 3),
        "log": name + ".log",
        "execution": "hermetic",
        "pytest_auto_workers": 2,
    }
    (OUTPUT / (name + ".json")).write_text(json.dumps(record, indent=2) + "\n")
    print(name, record["status"], record["duration_seconds"], flush=True)


if __name__ == "__main__":
    for selected in sys.argv[1:]:
        run(selected)
