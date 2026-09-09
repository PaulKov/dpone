#!/usr/bin/env python3
"""Benchmark 100 bounded hermetic tests without external services."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import resource
import tempfile
import time
from pathlib import Path
from typing import Any

import yaml

from dpone.services.hermetic_test_service import HermeticTestService


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tests", type=int, default=100)
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("test_artifacts/airflow-hermetic-tests-v1/benchmark.json"),
    )
    return parser


def _pipeline(pipeline_id: str) -> dict[str, Any]:
    return {
        "kind": "dpone.flow.v1",
        "authoring": {
            "mode": "flow",
            "source": f"pipelines/{pipeline_id}/pipeline.yaml",
        },
        "metadata": {"id": pipeline_id, "domain": "benchmark", "tags": ["hermetic"]},
        "processes": [
            {
                "name": pipeline_id,
                "source": {
                    "type": "mssql",
                    "connection_ref": "benchmark_source",
                    "table": {"schema": "dbo", "name": pipeline_id},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "benchmark_sink",
                    "table": {"schema": "benchmark", "name": pipeline_id},
                    "strategy": {"mode": "incremental_merge", "unique_key": "id"},
                },
            }
        ],
    }


def _test(pipeline_id: str) -> dict[str, Any]:
    return {
        "kind": "dpone.test.v1",
        "name": f"{pipeline_id}_happy_path",
        "pipeline": f"../pipelines/{pipeline_id}/pipeline.yaml",
        "input": {"fixture": f"fixtures/{pipeline_id}.input.jsonl", "format": "jsonl"},
        "expect": {"rows": 2, "rejected_rows": 0, "schema": {"id": "int64"}},
    }


def _create_fixture(root: Path, test_count: int) -> None:
    fixtures = root / "tests" / "fixtures"
    fixtures.mkdir(parents=True)
    for index in range(test_count):
        pipeline_id = f"benchmark_{index:03d}"
        pipeline_path = root / "pipelines" / pipeline_id / "pipeline.yaml"
        pipeline_path.parent.mkdir(parents=True)
        pipeline_path.write_text(yaml.safe_dump(_pipeline(pipeline_id), sort_keys=False), encoding="utf-8")
        (root / "tests" / f"{pipeline_id}.test.yaml").write_text(
            yaml.safe_dump(_test(pipeline_id), sort_keys=False),
            encoding="utf-8",
        )
        (fixtures / f"{pipeline_id}.input.jsonl").write_text(
            '{"id":1,"status":"new"}\n{"id":2,"status":"ready"}\n',
            encoding="utf-8",
        )


def _percentile(values: list[float], fraction: float) -> float:
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def _peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if platform.system() == "Darwin" else value * 1024)


def _run(root: Path, runs: int) -> tuple[list[float], list[str]]:
    elapsed: list[float] = []
    expected_ids: list[str] | None = None
    for _ in range(runs):
        started = time.perf_counter()
        report = HermeticTestService(root=root).run(".")
        elapsed.append(time.perf_counter() - started)
        if not report.passed:
            raise RuntimeError("benchmark suite did not pass")
        test_ids = [test.test_id for test in report.tests]
        if expected_ids is not None and test_ids != expected_ids:
            raise RuntimeError("test identity changed between benchmark runs")
        expected_ids = test_ids
    return elapsed, expected_ids or []


def main() -> int:
    args = _parser().parse_args()
    if args.tests < 1 or args.tests > 100 or args.runs < 1:
        raise SystemExit("--tests must be 1..100 and --runs must be positive")
    with tempfile.TemporaryDirectory(prefix="dpone-hermetic-benchmark-") as temporary:
        root = Path(temporary)
        _create_fixture(root, args.tests)
        elapsed, test_ids = _run(root, args.runs)
    payload = {
        "schema": "dpone.hermetic-test-benchmark.v1",
        "status": "PASS" if _percentile(elapsed, 0.95) <= 5.0 else "FAIL",
        "scenario": {"tests": args.tests, "rows_per_test": 2, "runs": args.runs},
        "runner": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
        },
        "measurements": {
            "elapsed_seconds": [round(value, 6) for value in elapsed],
            "p50_seconds": round(_percentile(elapsed, 0.50), 6),
            "p95_seconds": round(_percentile(elapsed, 0.95), 6),
            "peak_rss_bytes": _peak_rss_bytes(),
            "stable_test_ids": len(test_ids),
        },
        "budget": {"p95_seconds": 5.0},
        "scope": "credential-free hermetic contract; not route certification",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
