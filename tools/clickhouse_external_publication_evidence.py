"""Produce commit-bound mocked ClickHouse external-publication evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "dpone.clickhouse.external-publication.mocked-evidence.v1"
DEFAULT_OUTPUT = Path("test_artifacts/clickhouse-external-publication/mocked-receipt.json")
TEST_MODULE = "tests/test_clickhouse_external_replication_runtime.py"
SCENARIOS = (
    (
        "complete_generation",
        f"{TEST_MODULE}::test_external_runtime_stages_and_publishes_complete_generation_on_every_member",
    ),
    ("lost_stage_ack", f"{TEST_MODULE}::test_lost_member_stage_ack_is_reconciled_without_duplicate_insert"),
    ("partial_stage_replay", f"{TEST_MODULE}::test_partial_member_stage_blocks_then_retry_rebuilds_exact_candidate"),
    (
        "divergent_generation",
        f"{TEST_MODULE}::test_divergent_member_digest_blocks_publication_and_preserves_candidates",
    ),
    ("completed_replay", f"{TEST_MODULE}::test_completed_same_operation_replay_has_zero_mutation_effects"),
    ("cleanup_recovery", f"{TEST_MODULE}::test_cleanup_interruption_resumes_original_cleanup_without_redispatch"),
    ("redacted_failure", f"{TEST_MODULE}::test_failure_evidence_is_redacted_and_contains_only_opaque_member_ids"),
)

Runner = Callable[[Sequence[str], Path], int]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    _require_clean_checkout(root)
    receipt = produce_receipt(root, runner=_run_scenario)
    output = args.output if args.output.is_absolute() else root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output.relative_to(root).as_posix())
    return 0 if receipt["status"] == "PASS" else 1


def produce_receipt(root: Path, *, runner: Runner) -> dict[str, Any]:
    """Run each bounded scenario and return a redacted deterministic receipt."""

    results = [
        {
            "scenario": name,
            "status": "PASS" if runner((sys.executable, "-m", "pytest", node, "-q"), root) == 0 else "FAIL",
        }
        for name, node in SCENARIOS
    ]
    source_commit = _git(root, "rev-parse", "HEAD")
    fixture = {
        "scenario_ids": [name for name, _ in SCENARIOS],
        "test_module_sha256": hashlib.sha256((root / TEST_MODULE).read_bytes()).hexdigest(),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "source_commit": source_commit,
        "fixture_digest": _digest(fixture),
        "evidence_scope": "mocked_in_process",
        "status": "PASS" if all(item["status"] == "PASS" for item in results) else "FAIL",
        "local_synthetic_certification": "UNVERIFIED",
        "live_external_certification": "UNVERIFIED",
        "scenarios": results,
    }


def _run_scenario(command: Sequence[str], root: Path) -> int:
    completed = subprocess.run(command, cwd=root, check=False, capture_output=True, text=True)
    return completed.returncode


def _require_clean_checkout(root: Path) -> None:
    if _git(root, "status", "--porcelain"):
        raise RuntimeError("mocked evidence requires a clean checkout")


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args),
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
