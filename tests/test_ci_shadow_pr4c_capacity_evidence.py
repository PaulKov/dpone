from __future__ import annotations

import hashlib
import json
import struct
from datetime import UTC, datetime
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.contracts.ci_shadow_reconciliation import AcquiredObservation, ReconciliationPolicyV1, ReconciliationPolicyV2
from dpone.services.ci.shadow_capacity import calibrate_capacity
from dpone.services.ci.shadow_capacity_evidence import render_capacity_evidence
from dpone.services.ci.shadow_capacity_source import CapacitySource
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget

ROOT = Path(__file__).resolve().parents[1]
_DOMAIN = "dpone.ci-shadow-reconciliation-observation-bundle.v1"


def _bundle_digest(entries: list[dict[str, str]], *, manifest_sha256: str) -> str:
    framed = []
    for entry in entries:
        path = entry["path"].encode("utf-8")
        value = (
            struct.pack(">I", len(path))
            + path
            + entry["mode"].encode("ascii")
            + bytes.fromhex(entry["blob_sha256"][7:])
        )
        framed.append(struct.pack(">I", len(value)) + value)
    manifest = struct.pack(">I", len(framed)) + b"".join(framed)
    return (
        "sha256:"
        + hashlib.sha256(
            _DOMAIN.encode()
            + b"\x00"
            + struct.pack(">Q", len(manifest))
            + manifest
            + bytes.fromhex(manifest_sha256[7:])
        ).hexdigest()
    )


def test_capacity_evidence_is_closed_schema_valid_and_never_pass() -> None:
    policy = ReconciliationPolicyV1.fixed()
    interval = policy.interval_for(datetime(2026, 8, 27, 3, 15, tzinfo=UTC))
    observation = AcquiredObservation(
        b"snapshot", True, interval.observation_started_at, interval.observation_started_at
    )
    calibration = calibrate_capacity(
        lambda: observation,
        budget=RequestBudget(policy=policy, monotonic_clock=lambda: 0.0),
        policy=policy,
        utc_clock=lambda: interval.observation_started_at,
    )
    entries = [
        {"path": "src/dpone/services/ci/shadow_capacity.py", "mode": "100644", "blob_sha256": "sha256:" + "a" * 64}
    ]
    payload = render_capacity_evidence(
        calibration,
        source=CapacitySource(1, 2, "b" * 40, 3, 1),
        policy=policy,
        observation_bundle_sha256=_bundle_digest(entries, manifest_sha256="sha256:" + "c" * 64),
        observation_bundle_manifest_sha256="sha256:" + "c" * 64,
        observation_bundle_entries=entries,
        execution_configuration={"runner_label": "ubuntu-24.04", "runner_arch": "X64", "python_version": "3.12.12"},
        interval={
            "scan_from": "2026-08-13T02:45:00Z",
            "safe_scan_through": "2026-08-27T02:45:00Z",
            "observation_started_at": "2026-08-27T03:15:00Z",
        },
    )
    schema = json.loads(
        (ROOT / "test_artifacts/agent-policy/dpone-ci-shadow-reconciliation-capacity.schema.json").read_text()
    )

    Draft202012Validator(schema).validate(payload)
    assert payload["decision"] == "UNVERIFIED"
    assert payload["code"] == "RECONCILIATION_CAPACITY_CALIBRATION_ONLY"


def test_v2_producer_path_emits_its_versioned_schema_digest_and_limits() -> None:
    policy = ReconciliationPolicyV2.fixed()
    interval = policy.interval_for(datetime(2026, 8, 27, 3, 15, tzinfo=UTC))
    observation = AcquiredObservation(
        b"snapshot", True, interval.observation_started_at, interval.observation_started_at
    )
    calibration = calibrate_capacity(
        lambda: observation,
        budget=RequestBudget(policy=policy, monotonic_clock=lambda: 0.0),
        policy=policy,
        utc_clock=lambda: interval.observation_started_at,
    )
    entries = [
        {"path": "src/dpone/services/ci/shadow_capacity.py", "mode": "100644", "blob_sha256": "sha256:" + "a" * 64}
    ]
    payload = render_capacity_evidence(
        calibration,
        source=CapacitySource(1, 2, "b" * 40, 3, 1),
        policy=policy,
        observation_bundle_sha256=_bundle_digest(entries, manifest_sha256="sha256:" + "c" * 64),
        observation_bundle_manifest_sha256="sha256:" + "c" * 64,
        observation_bundle_entries=entries,
        execution_configuration={"runner_label": "ubuntu-24.04", "runner_arch": "X64", "python_version": "3.12.12"},
        interval={
            "scan_from": "2026-08-13T02:45:00Z",
            "safe_scan_through": "2026-08-27T02:45:00Z",
            "observation_started_at": "2026-08-27T03:15:00Z",
        },
    )
    schema = json.loads(
        (ROOT / "test_artifacts/agent-policy/dpone-ci-shadow-reconciliation-capacity-v2.schema.json").read_text()
    )

    Draft202012Validator(schema).validate(payload)
    assert payload["schema"] == "dpone.ci-shadow-reconciliation-capacity.v2"
    assert payload["policy_sha256"] == policy.sha256
    assert payload["hard_maxima"]["total_http_requests"] == 3_000
    assert payload["approval_thresholds"]["total_http_requests"] == 1_500
