"""Capture DDA-03 check output and code identity without editing shared evidence.

Run from the repository root with ``uv run python <this-file> <check> [...]``.
Each named check replaces only its own log/JSON pair in this directory. No live
services or credentials are used. This is development evidence, not a runtime
cache or a performance certification producer.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import subprocess
import tempfile
import time
from pathlib import Path

ARTIFACTS = Path(__file__).resolve().parent
IMPLEMENTATION = (
    "src/dpone/runtime/mssql_native_sized_frames.py",
    "src/dpone/runtime/mssql_native_chunks_files.py",
    "tests/test_mssql_native_sized_frames.py",
    "tests/test_mssql_native_chunks_files.py",
    "docs/delivery-acceleration/frames.md",
)
CHECKS = {
    "ruff-check": "uv run ruff check .",
    "ruff-format": "uv run ruff format --check .",
    "mypy": "uv run mypy --config-file mypy.ini",
    "runtime-mypy": (
        "uv run mypy --follow-imports=silent src/dpone/runtime/mssql_native_sized_frames.py "
        "src/dpone/runtime/mssql_native_chunks_files.py"
    ),
    "import-rules": "uv run dpone docs check-import-rules",
    "layer-metrics": "uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json",
    "docs": "uv run dpone docs check-docs",
    "dev-metrics": "uv run dpone docs update-dev-metrics --check",
    "generated-references": "uv run dpone docs check-generated-references",
    "docs-language": "uv run pytest tests/test_docs_language_contracts.py -q",
    "mkdocs": "uv run mkdocs build --strict",
    "full-pytest": 'uv run pytest -m "not integration_live" -n auto --dist loadfile',
    "environment-regressions": (
        "uv run pytest tests/test_native_bcp_type_matrix.py tests/test_dbt_inline_publishing.py "
        "tests/test_semantic_refresh_clickhouse_http.py tests/test_postgres_snapshot_retry.py -q"
    ),
    "focused": (
        "uv run pytest tests/test_mssql_native_chunks_files.py tests/test_mssql_native_sized_frames.py "
        "tests/test_mssql_native_encoder.py tests/test_mssql_native_staged_values.py -q"
    ),
    "task-contract": (
        "uv run python tools/agent_policy/task_contract.py "
        "test_artifacts/delivery-acceleration/agent-task-contracts/dda-03-frames.yml"
    ),
    "selected-checks": "uv run python tools/agent_policy/select_checks.py --base-ref origin/master",
}


def git(*arguments: str) -> str:
    return subprocess.check_output(["git", *arguments], text=True).strip()


def identity() -> dict[str, object]:
    """Bind checked source bytes independently from evidence-only commits."""
    return {
        "head": git("rev-parse", "HEAD"),
        "implementation_dirty": bool(git("diff", "HEAD", "--", *IMPLEMENTATION)),
        "implementation_sha256": {path: hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in IMPLEMENTATION},
    }


def command_for(name: str) -> str:
    if name != "module-size":
        return CHECKS[name]
    head = git("rev-parse", "HEAD")
    base = git("merge-base", head, "origin/master")
    if base == head:
        base = git("rev-parse", f"{head}^")
    return (
        "uv run dpone docs check-module-size --baseline docs/module_size_baseline.json "
        f"--base-ref {base} --head-ref {head}"
    )


def run(name: str) -> bool:
    command, before = command_for(name), identity()
    temporary_root = Path(tempfile.gettempdir()) / f"dpone-dda03-{name}"
    temporary_root.mkdir(exist_ok=True)
    overrides = {"PYTEST_XDIST_AUTO_NUM_WORKERS": "2", "PYTEST_DEBUG_TEMPROOT": str(temporary_root)}
    environment = dict(os.environ, **overrides)
    start = time.monotonic()
    with (ARTIFACTS / f"{name}.log").open("w") as output:
        result = subprocess.run(shlex.split(command), env=environment, stdout=output, stderr=subprocess.STDOUT)
    after = identity()
    unchanged = before["implementation_sha256"] == after["implementation_sha256"]
    status = "UNVERIFIED" if not unchanged else "PASS" if result.returncode == 0 else "FAIL"
    record = {
        "command": command,
        "environment_override": overrides,
        "status": status,
        "exit_code": result.returncode,
        "elapsed_seconds": round(time.monotonic() - start, 2),
        "log": f"{name}.log",
        "before": before,
        "after": after,
    }
    (ARTIFACTS / f"{name}.json").write_text(json.dumps(record, indent=2) + "\n")
    print(f"{name}: {status} ({record['elapsed_seconds']}s)", flush=True)
    return status == "PASS"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checks", nargs="+", choices=sorted([*CHECKS, "module-size"]))
    arguments = parser.parse_args()
    results = [run(name) for name in arguments.checks]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
