"""Validate immutable CI quality timing samples and reduce their p95 decision."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

SCHEMA_VERSION = 1
REQUIRED_SAMPLE_COUNT = 3
TARGET_SECONDS = 720.0
_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "evals/agent/ci-quality-timing-v1.schema.json"


@dataclass(frozen=True)
class TimingSample:
    """One successful, exact-head hosted quality execution."""

    head_sha: str
    run_id: int
    run_attempt: int
    critical_path_seconds: float
    status: str


def performance_decision(samples: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Return a conservative pass/fail decision from exactly three samples.

    Nearest-rank p95 for three observations is their maximum.  The closed input
    shape prevents a changed head, a duplicate rerun or a non-terminal result
    from being quietly substituted into the claim.
    """

    evidence = tuple(samples)
    materialized = tuple(_parse_sample(value) for value in evidence)
    if len(materialized) != REQUIRED_SAMPLE_COUNT:
        raise ValueError("exactly three timing samples are required")
    heads = {sample.head_sha for sample in materialized}
    if len(heads) != 1:
        raise ValueError("timing samples must bind one unchanged head SHA")
    identities = {(sample.run_id, sample.run_attempt) for sample in materialized}
    if len(identities) != REQUIRED_SAMPLE_COUNT:
        raise ValueError("timing samples must be distinct workflow run attempts")
    if any(sample.status != "PASS" for sample in materialized):
        raise ValueError("only PASS exact-head timing samples may support a decision")
    workflow_identities = {
        (value["repository"], value["workflow_sha256"], value["python_version"]) for value in evidence
    }
    if len(workflow_identities) != 1:
        raise ValueError("timing samples must bind one repository, workflow revision and Python context")
    durations = tuple(sample.critical_path_seconds for sample in materialized)
    p95_seconds = max(durations)
    return {
        "schema_version": SCHEMA_VERSION,
        "head_sha": next(iter(heads)),
        "sample_count": REQUIRED_SAMPLE_COUNT,
        "run_attempts": [{"run_id": sample.run_id, "run_attempt": sample.run_attempt} for sample in materialized],
        "critical_path_seconds": list(durations),
        "p95_seconds": p95_seconds,
        "target_seconds": TARGET_SECONDS,
        "status": "PASS" if p95_seconds < TARGET_SECONDS else "FAIL",
    }


def _parse_sample(value: Mapping[str, Any]) -> TimingSample:
    _validate_evidence(value)
    head_sha = value.get("head_sha")
    run_id, run_attempt = value.get("run_id"), value.get("run_attempt")
    duration, status = value.get("critical_path_seconds"), value.get("status")
    if not isinstance(head_sha, str) or len(head_sha) != 40 or any(char not in "0123456789abcdef" for char in head_sha):
        raise ValueError("sample head_sha must be a lowercase 40-character Git SHA")
    if not isinstance(run_id, int) or run_id <= 0 or not isinstance(run_attempt, int) or run_attempt <= 0:
        raise ValueError("sample run identity must contain positive integers")
    if not isinstance(duration, (int, float)) or isinstance(duration, bool) or duration < 0:
        raise ValueError("sample critical_path_seconds must be a non-negative number")
    if status not in {"PASS", "FAIL", "UNVERIFIED"}:
        raise ValueError("sample status is not a closed timing outcome")
    return TimingSample(head_sha, run_id, run_attempt, float(duration), status)


def _validate_evidence(value: Mapping[str, Any]) -> None:
    """Reject partial or structurally invalid hosted evidence before reduction."""

    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    errors = tuple(Draft202012Validator(schema).iter_errors(value))
    if errors:
        raise ValueError("timing sample does not satisfy ci-quality-timing-v1")


def main(argv: list[str] | None = None) -> int:
    """Read a JSON array of timing samples and write one decision artifact."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    payload = json.loads(args.samples.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        parser.error("--samples must contain a JSON array of timing objects")
    try:
        decision = performance_decision(payload)
    except ValueError as exc:
        parser.error(str(exc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if decision["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
