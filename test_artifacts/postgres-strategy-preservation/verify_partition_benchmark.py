"""Independently check benchmark coverage, attribution, row truth and summaries."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import subprocess
from pathlib import Path

from partition_benchmark_support import ROOT, source_identity

EXPECTED_ROWS = (100_000, 1_000_000, 5_000_000)
EXPECTED_ATTEMPTS = (1, 2, 3)


def equal_number(actual: float, expected: float) -> None:
    assert math.isfinite(actual) and math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-12)


def verify(path: Path, source_sha: str) -> dict:
    report = json.loads(path.read_text())
    assert report["status"] == "PASS" and report["source"]["git_head"] == source_sha
    current = source_identity()
    assert current["execution_inputs_sha256"] == report["source"]["execution_inputs_sha256"]
    subprocess.run(
        [
            "git",
            "diff",
            "--exit-code",
            source_sha,
            "HEAD",
            "--",
            "src",
            "tests",
            "packages",
            "pyproject.toml",
            "uv.lock",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    assert not subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "--", "src", "tests", "packages"],
        cwd=ROOT,
    ).strip(), "Untracked execution files cannot be attributed to the reviewed source"
    for name, digest in report["producer_sha256"].items():
        assert name in {"partition_benchmark.py", "partition_benchmark_support.py"}
        content = subprocess.check_output(
            ["git", "show", f"{source_sha}:test_artifacts/postgres-strategy-preservation/{name}"],
            cwd=ROOT,
        )
        assert hashlib.sha256(content).hexdigest() == digest
    assert len(report["producer_sha256"]) == 2
    cases = report["cases"]
    expected = {(rows, attempt) for rows in EXPECTED_ROWS for attempt in EXPECTED_ATTEMPTS}
    assert len(cases) == len(expected) and {(c["rows"], c["attempt"]) for c in cases} == expected
    for case in cases:
        rows = case["rows"]
        assert case["status"] == "PASS" and case["reader_blocked_by_sink"] is True
        assert case["elapsed_seconds"] >= case["lock_seconds"] > case["old_count_seconds"] > 0
        equal_number(case["rows_per_second"], rows / case["elapsed_seconds"])
        equal_number(case["old_count_share_of_lock"], case["old_count_seconds"] / case["lock_seconds"])
        reader = case["reader"]
        assert "error_type" not in reader and reader["observed_rows"] == rows + 1000
        equal_number(case["reader_elapsed_seconds"], (reader["finished_ns"] - reader["started_ns"]) / 1e9)
        counts = [op for op in case["operations"] if op.get("old_child_count") is True]
        assert len(counts) == 1 and counts[0]["count"] == rows
        equal_number(case["old_count_seconds"], counts[0]["elapsed_ns"] / 1e9)
        statements = [op["sql"] for op in case["operations"]]
        lock = next(i for i, statement in enumerate(statements) if statement.startswith("LOCK TABLE"))
        count = case["operations"].index(counts[0])
        detach = next(i for i, statement in enumerate(statements) if "DETACH PARTITION" in statement)
        attach = next(i for i, statement in enumerate(statements) if "ATTACH PARTITION" in statement)
        assert lock < count < detach < attach
        for key in ("loaded_rows", "inserted_rows", "replaced_rows", "staging_rows", "hard_deleted_rows"):
            assert case["public_result"][key] == rows
        assert case["public_result"]["final_rows"] == rows + 1000
        assert case["load_result"]["total_rows"] == rows + 1000
        verification = case["verification"]
        assert verification["parent_oid_preserved"] is True and verification["no_staging_leaks"] is True
        actual = verification["groups"]
        expected_groups = [
            ["2026-01-01", rows, 1, rows, str(rows * (rows + 1) // 2), True],
            ["2026-02-01", 1000, 1, 1000, "500500", True],
        ]
        assert actual == expected_groups
    summaries = report["summary"]
    assert len(summaries) == len(EXPECTED_ROWS) and {s["rows"] for s in summaries} == set(EXPECTED_ROWS)
    for summary in summaries:
        for metric in ("elapsed_seconds", "lock_seconds", "old_count_seconds", "rows_per_second"):
            values = [case[metric] for case in cases if case["rows"] == summary["rows"]]
            for label, value in (("min", min(values)), ("median", statistics.median(values)), ("max", max(values))):
                equal_number(summary[metric][label], value)
    return {
        "status": "PASS",
        "source_commit": source_sha,
        "execution_inputs_sha256": current["execution_inputs_sha256"],
        "report_path": str(path.relative_to(ROOT)),
        "report_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "case_count": len(cases),
        "rows_per_case": list(EXPECTED_ROWS),
        "attempts_per_size": 3,
        "checks": [
            "source_and_producers",
            "complete_case_matrix",
            "raw_count_timings",
            "native_operation_order",
            "row_metrics_and_payloads",
            "concurrent_reader",
            "summary_recalculation",
        ],
        "limits": "Verifies the recorded local experiment; does not establish a production SLA or cold-cache performance.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    receipt = verify(args.report.resolve(), args.source_sha)
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"PASS: {receipt['case_count']} cases; source {receipt['source_commit']}")
