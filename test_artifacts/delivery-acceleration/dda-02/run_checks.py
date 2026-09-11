"""Reproduce DDA-02 hermetic checks and record their actual exit codes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUTPUT = Path(__file__).resolve().parent
FOCUSED = [
    "tests/test_mssql_native_integrity_readbacks.py",
    "tests/test_mssql_native_metadata_insert.py",
    "tests/test_mssql_native_metadata_insert_parity.py",
    "tests/test_mssql_native_staged_verification.py",
    "tests/test_mssql_native_lineage_authority.py",
]
CHECKS = {
    "focused": {"focused": ["pytest", *FOCUSED, "-q"]},
    "static": {
        "contract": [
            "python",
            "tools/agent_policy/task_contract.py",
            "test_artifacts/delivery-acceleration/agent-task-contracts/dda-02-preparation.yml",
        ],
        "plan": ["python", "tools/agent_policy/select_checks.py", "--base-ref", "origin/master"],
        "ruff": ["ruff", "check", "."],
        "format": ["ruff", "format", "--check", "."],
        "mypy": ["mypy", "--config-file", "mypy.ini"],
        "helper-mypy": [
            "mypy",
            "--config-file",
            "mypy.ini",
            "src/dpone/runtime/sinks/mssql_native_prepared_digests.py",
            "src/dpone/runtime/sinks/mssql_native_prepared_insert.py",
        ],
        "imports": ["dpone", "docs", "check-import-rules"],
        "layers": ["dpone", "docs", "check-layer-metrics", "--baseline", "docs/layer_metrics_baseline.json"],
    },
    "docs": {
        "docs": ["dpone", "docs", "check-docs"],
        "generated": ["dpone", "docs", "check-generated-references"],
        "docs-language": ["pytest", "tests/test_docs_language_contracts.py", "-q"],
        "mkdocs": ["mkdocs", "build", "--strict"],
    },
    "suite": {"pytest": ["pytest", "-m", "not integration_live", "-n", "auto", "--dist", "loadfile"]},
}


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("group", choices=(*CHECKS, "size"))
    args = parser.parse_args()
    head = git("rev-parse", "HEAD")
    if args.group == "size":
        base = git("merge-base", head, "origin/master")
        if base == head:
            base = git("rev-parse", head + "^")
        checks = {
            "size": [
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
        }
    else:
        checks = CHECKS[args.group]
    sources = [
        "src/dpone/runtime/sinks/mssql_native_prepared_digests.py",
        "src/dpone/runtime/sinks/mssql_native_prepared_insert.py",
        "docs/delivery-acceleration/preparation.md",
        *FOCUSED[:3],
    ]
    report = {
        "head": head,
        "planning_dependency": "f3682940f8864563cde0e6b6ecee60f746b49020",
        "git_status_before": git("status", "--short"),
        "source_sha256": {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in sources},
        "checks": [],
    }
    failures = 0
    for name, arguments in checks.items():
        command = ["uv", "run", *arguments]
        started = time.monotonic()
        print("Running " + " ".join(command), flush=True)
        with (OUTPUT / f"{name}.log").open("w") as log:
            result = subprocess.run(
                command,
                cwd=ROOT,
                env={**os.environ, "PYTEST_XDIST_AUTO_NUM_WORKERS": "2"},
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
            )
        status = "PASS" if result.returncode == 0 else "FAIL"
        failures += result.returncode != 0
        report["checks"].append(
            {
                "command": command,
                "status": status,
                "exit_code": result.returncode,
                "seconds": round(time.monotonic() - started, 3),
                "log": f"{name}.log",
            }
        )
        print(f"{name}: {status}", flush=True)
        (OUTPUT / f"{args.group}-results.json").write_text(json.dumps(report, indent=2) + "\n")
    return int(failures > 0)


if __name__ == "__main__":
    raise SystemExit(main())
