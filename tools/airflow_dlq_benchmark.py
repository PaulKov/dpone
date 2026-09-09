"""Credential-free safety and determinism benchmark for first-class DLQ v1."""

from __future__ import annotations

import argparse
import json
import platform
import tempfile
import time
import tracemalloc
from pathlib import Path

from dpone.contracts.dlq import DlqPolicy, DlqReason
from dpone.ops.dlq import DlqService
from dpone.ops.dlq_replay import DlqReplayService
from dpone.ops.dlq_store import DlqFileStore
from dpone.ops.quarantine import QuarantineService


def run_benchmark(*, records: int = 10_000, plan_repeats: int = 20) -> dict[str, object]:
    """Measure the approved local scenario without connector or secret I/O."""

    canary = "dpone-pii-canary@example.invalid"
    tracemalloc.start()
    with tempfile.TemporaryDirectory(prefix="dpone-dlq-benchmark-") as temp:
        root = Path(temp) / "dlq"
        policy = DlqPolicy.from_config(
            {
                "directory": str(root),
                "pii_policy": "reference_only",
                "max_records_per_run": max(records, 1),
            }
        )
        service = DlqService(
            DlqFileStore(
                policy.directory,
                max_record_bytes=policy.max_record_bytes,
                max_diagnostic_bytes=policy.max_diagnostic_bytes,
                max_index_bytes=policy.max_index_bytes,
            ),
            policy=policy,
        )
        started = time.perf_counter()
        for row_index in range(records):
            service.put(
                run_id="benchmark-run",
                load_id="benchmark-load",
                row={"id": row_index, "email": canary},
                row_index=row_index,
                reason=DlqReason.schema_type_mismatch(column="email"),
                diagnostics={"expected_type": "email", "actual_type": "string"},
            )
        service.summary("benchmark-run")
        write_seconds = time.perf_counter() - started

        replay = DlqReplayService(service.store)
        plan_started = time.perf_counter()
        plan_ids = [
            replay.plan(
                run_id="benchmark-run",
                resolver_identity="benchmark-resolver:v1",
                target_identity="benchmark-sink:v1",
            ).plan_id
            for _ in range(plan_repeats)
        ]
        plan_seconds = time.perf_counter() - plan_started
        artifact_files = tuple(root.rglob("*.json"))
        leaked_files = [
            str(path.relative_to(root)) for path in artifact_files if canary in path.read_text(encoding="utf-8")
        ]
        legacy_result = QuarantineService(root).replay(run_id="benchmark-run", yes=True)
        _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    stable_plan_ids = len(set(plan_ids)) == 1
    false_applied_claims = int(legacy_result.applied or legacy_result.replayed_rows > 0)
    passed = not leaked_files and false_applied_claims == 0 and stable_plan_ids
    return {
        "schema": "dpone.airflow-dlq-benchmark.v1",
        "status": "passed" if passed else "failed",
        "scenario": "contract-enforced metadata-only DLQ",
        "records": records,
        "plan_repeats": plan_repeats,
        "metrics": {
            "write_and_index_seconds": round(write_seconds, 6),
            "plan_repeats_seconds": round(plan_seconds, 6),
            "peak_tracemalloc_bytes": peak_bytes,
            "unmasked_artifact_values": len(leaked_files),
            "false_applied_claims": false_applied_claims,
            "stable_replay_plan_ids": stable_plan_ids,
        },
        "limits": {
            "unmasked_artifact_values": 0,
            "false_applied_claims": 0,
            "stable_replay_plan_ids": True,
        },
        "leaked_files": leaked_files,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "live_connector_certification": "UNVERIFIED",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=10_000)
    parser.add_argument("--plan-repeats", type=int, default=20)
    parser.add_argument("--output", type=Path, default=Path("test_artifacts/airflow-dlq-v1/benchmark.json"))
    args = parser.parse_args()
    if args.records <= 0 or args.plan_repeats <= 0:
        parser.error("--records and --plan-repeats must be positive")
    report = run_benchmark(records=args.records, plan_repeats=args.plan_repeats)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
