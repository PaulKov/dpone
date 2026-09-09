from __future__ import annotations

import copy
import hashlib
import json
import struct
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

from dpone.contracts.strict_json import StrictJsonError, strict_json_object
from tests.test_ci_shadow_spec_contracts import ROOT

POLICY = ROOT / "test_artifacts" / "agent-policy" / "dpone-ci-shadow-reconciliation-mvp.yml"
CAPACITY_SCHEMA = ROOT / "test_artifacts" / "agent-policy" / "dpone-ci-shadow-reconciliation-capacity-v2.schema.json"
CAPACITY_ARTIFACT_MAX_BYTES = 8_388_608
CAPACITY_ARTIFACT_MAX_DEPTH = 32
CAPACITY_ARTIFACT_MAX_NODES = 50_000


def _policy() -> dict[str, Any]:
    payload = yaml.safe_load(POLICY.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _capacity_validator() -> Draft202012Validator:
    schema = json.loads(CAPACITY_SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


_REQUEST_CLASSES = (
    "producer_list_page_requests",
    "auditor_list_page_requests",
    "exact_producer_run_requests",
    "attempt_run_requests",
    "jobs_page_requests",
    "artifact_metadata_requests",
    "artifact_download_requests",
    "redirect_requests",
    "git_object_requests",
    "pull_request_identity_requests",
    "polling_requests",
)
_BUNDLE_DOMAIN = "dpone.ci-shadow-reconciliation-observation-bundle.v1"
_BUNDLE_ENTRIES = [
    {
        "path": "pyproject.toml",
        "mode": "100644",
        "blob_sha256": "sha256:" + "1" * 64,
    },
    {
        "path": "src/dpone/contracts/ci_shadow_reconciliation.py",
        "mode": "100644",
        "blob_sha256": "sha256:" + "2" * 64,
    },
]
_EXECUTION_CONFIGURATION = {
    "runner_label": "ubuntu-24.04",
    "runner_arch": "X64",
    "python_version": "3.12.12",
}


def _observation_bundle_digest(entries: list[dict[str, str]], *, manifest_sha256: str) -> str:
    paths = [entry["path"].encode("utf-8") for entry in entries]
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise ValueError("observation bundle paths must be strictly sorted and unique")
    encoded_entries = []
    for entry, path_bytes in zip(entries, paths, strict=True):
        if len(path_bytes) > 1024:
            raise ValueError("observation bundle path exceeds 1024 UTF-8 bytes")
        if entry["mode"] not in {"100644", "100755"}:
            raise ValueError("observation bundle mode is invalid")
        digest_bytes = bytes.fromhex(entry["blob_sha256"].removeprefix("sha256:"))
        entry_bytes = struct.pack(">I", len(path_bytes)) + path_bytes + entry["mode"].encode("ascii") + digest_bytes
        encoded_entries.append(struct.pack(">I", len(entry_bytes)) + entry_bytes)
    manifest_bytes = struct.pack(">I", len(encoded_entries)) + b"".join(encoded_entries)
    digest_input = (
        _BUNDLE_DOMAIN.encode("utf-8")
        + b"\x00"
        + struct.pack(">Q", len(manifest_bytes))
        + manifest_bytes
        + bytes.fromhex(manifest_sha256.removeprefix("sha256:"))
    )
    return "sha256:" + hashlib.sha256(digest_input).hexdigest()


_BUNDLE_MANIFEST_SHA256 = "sha256:" + "c" * 64
_EXPECTED_OBSERVATION_BUNDLE_SHA256 = _observation_bundle_digest(
    _BUNDLE_ENTRIES, manifest_sha256=_BUNDLE_MANIFEST_SHA256
)


def _capacity_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _provider_transport_for(
    raw_bytes: bytes,
    *,
    repository_id: int = 1,
    workflow_run_id: int = 3,
    run_attempt: int = 1,
) -> dict[str, Any]:
    return {
        "repository_id": repository_id,
        "workflow_run_id": workflow_run_id,
        "run_attempt": run_attempt,
        "artifact_id": 4,
        "artifact_name": f"pr-gate-shadow-reconciliation-capacity-{workflow_run_id}-{run_attempt}.json",
        "provider_digest": "sha256:" + hashlib.sha256(raw_bytes).hexdigest(),
        "size_in_bytes": len(raw_bytes),
    }


def _validate_capacity_json_shape(payload: object) -> None:
    stack = [(payload, 1)]
    nodes = 0
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if nodes > CAPACITY_ARTIFACT_MAX_NODES:
            raise ValueError("capacity artifact exceeds the JSON node limit")
        if depth > CAPACITY_ARTIFACT_MAX_DEPTH:
            raise ValueError("capacity artifact exceeds the JSON depth limit")
        if isinstance(value, dict):
            stack.extend((child, depth + 1) for child in value.values())
        elif isinstance(value, list):
            stack.extend((child, depth + 1) for child in value)


def _validate_capacity_evidence(
    raw_bytes: bytes,
    *,
    provider_transport: dict[str, Any],
    expected_repository_id: int = 1,
    expected_workflow_id: int = 2,
    expected_workflow_sha: str = "a" * 40,
    expected_run_id: int = 3,
    expected_run_attempt: int = 1,
    expected_execution_configuration: dict[str, str] = _EXECUTION_CONFIGURATION,
    expected_policy_sha256: str = "sha256:" + "b" * 64,
    expected_observation_bundle_sha256: str = _EXPECTED_OBSERVATION_BUNDLE_SHA256,
    approval_time: str = "2026-08-08T04:00:00Z",
) -> None:
    expected_transport = (
        expected_repository_id,
        expected_run_id,
        expected_run_attempt,
        f"pr-gate-shadow-reconciliation-capacity-{expected_run_id}-{expected_run_attempt}.json",
    )
    actual_transport = (
        provider_transport["repository_id"],
        provider_transport["workflow_run_id"],
        provider_transport["run_attempt"],
        provider_transport["artifact_name"],
    )
    if actual_transport != expected_transport:
        raise ValueError("capacity provider transport does not match the source run and attempt")
    provider_digest = str(provider_transport["provider_digest"])
    try:
        decoded_provider_digest = bytes.fromhex(provider_digest.removeprefix("sha256:"))
    except ValueError as error:
        raise ValueError("capacity provider transport identity is incomplete") from error
    if (
        provider_transport["artifact_id"] < 1
        or provider_transport["size_in_bytes"] < 1
        or not provider_digest.startswith("sha256:")
        or len(decoded_provider_digest) != 32
    ):
        raise ValueError("capacity provider transport identity is incomplete")
    if provider_transport["size_in_bytes"] > CAPACITY_ARTIFACT_MAX_BYTES:
        raise ValueError("capacity artifact exceeds the provider-size limit")
    if len(raw_bytes) > CAPACITY_ARTIFACT_MAX_BYTES:
        raise ValueError("capacity artifact exceeds the downloaded-byte limit")
    if provider_transport["size_in_bytes"] != len(raw_bytes):
        raise ValueError("capacity provider size does not match the downloaded bytes")
    if decoded_provider_digest != hashlib.sha256(raw_bytes).digest():
        raise ValueError("capacity provider digest does not match the downloaded bytes")
    try:
        payload = strict_json_object(raw_bytes)
    except (StrictJsonError, RecursionError) as error:
        raise ValueError("capacity artifact bytes are not strict JSON") from error
    _validate_capacity_json_shape(payload)
    _capacity_validator().validate(payload)
    source = payload["source"]
    if (
        source["repository_id"],
        source["workflow_id"],
        source["workflow_sha"],
        source["run_id"],
        source["run_attempt"],
    ) != (
        expected_repository_id,
        expected_workflow_id,
        expected_workflow_sha,
        expected_run_id,
        expected_run_attempt,
    ):
        raise ValueError("capacity source does not match the approved workflow identity")
    if payload["execution_configuration"] != expected_execution_configuration:
        raise ValueError("capacity execution configuration does not match the reconciler configuration")
    if payload["policy_sha256"] != expected_policy_sha256:
        raise ValueError("capacity policy digest does not match the approved policy")
    manifest_sha256 = payload["observation_bundle_manifest"]["manifest_sha256"]
    computed_bundle_sha256 = _observation_bundle_digest(
        payload["observation_bundle_manifest"]["entries"], manifest_sha256=manifest_sha256
    )
    if payload["observation_bundle_sha256"] != computed_bundle_sha256:
        raise ValueError("capacity observation bundle digest does not match its canonical manifest")
    if computed_bundle_sha256 != expected_observation_bundle_sha256:
        raise ValueError("capacity observation bundle does not match the reconciler acquisition closure")

    interval = {name: datetime.fromisoformat(value) for name, value in payload["interval"].items()}
    if interval["safe_scan_through"] - interval["scan_from"] != timedelta(days=14):
        raise ValueError("capacity interval must cover exactly 14 days")
    if interval["observation_started_at"] - interval["safe_scan_through"] != timedelta(minutes=30):
        raise ValueError("capacity interval must preserve the 30-minute grace")
    observed_through = datetime.fromisoformat(payload["evidence_observed_through"])
    calibration_observed_at = datetime.fromisoformat(payload["calibration_observed_at"])
    if not interval["observation_started_at"] <= observed_through <= calibration_observed_at:
        raise ValueError("capacity observation timestamps are not monotonic")
    if calibration_observed_at - interval["observation_started_at"] > timedelta(
        seconds=payload["hard_maxima"]["execution_wall_seconds"]
    ):
        raise ValueError("capacity observation exceeds the wall-time budget")

    counters = payload["counters"]
    if counters["total_response_body_bytes"] != (
        counters["api_response_body_bytes"] + counters["artifact_download_body_bytes"]
    ):
        raise ValueError("total_response_body_bytes must equal API plus artifact body bytes")
    if counters["total_http_requests"] != sum(counters[field] for field in _REQUEST_CLASSES):
        raise ValueError("total_http_requests must equal the closed request-class sum")
    if counters["retry_requests"] > counters["total_http_requests"]:
        raise ValueError("retry_requests must be a subset of total_http_requests")
    canonical_crossed = [field for field in payload["hard_maxima"] if field in payload["limits_crossed"]]
    if payload["limits_crossed"] != canonical_crossed:
        raise ValueError("limits_crossed must use canonical hard-maximum order")
    if any(counters[field] != payload["hard_maxima"][field] for field in canonical_crossed):
        raise ValueError("crossed capacity counters must be saturated at their hard maxima")
    observed_at = calibration_observed_at
    valid_until = datetime.fromisoformat(payload["valid_until"])
    if valid_until - observed_at != timedelta(hours=24):
        raise ValueError("capacity evidence validity must be exactly 24 hours")
    approval_at = datetime.fromisoformat(approval_time)
    if not observed_at <= approval_at < valid_until:
        raise ValueError("capacity evidence is not current at approval time")
    for field, threshold in payload["approval_thresholds"].items():
        if counters[field] > threshold:
            raise ValueError(f"{field} exceeds approval threshold")
    if not (
        payload["complete"]
        and payload["two_observations_match"]
        and not payload["limits_crossed"]
        and payload["within_thresholds"]
    ):
        raise ValueError("capacity evidence is diagnostic only and cannot approve reconciliation")


def _capacity_evidence() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "schema": "dpone.ci-shadow-reconciliation-capacity.v2",
        "decision": "UNVERIFIED",
        "code": "RECONCILIATION_CAPACITY_CALIBRATION_ONLY",
        "complete": True,
        "source": {
            "repository_id": 1,
            "workflow_id": 2,
            "workflow_path": ".github/workflows/pr-gate-shadow-capacity.yml",
            "event": "workflow_dispatch",
            "ref": "refs/heads/master",
            "workflow_sha": "a" * 40,
            "run_id": 3,
            "run_attempt": 1,
        },
        "execution_configuration": copy.deepcopy(_EXECUTION_CONFIGURATION),
        "policy_sha256": "sha256:" + "b" * 64,
        "observation_bundle_sha256": _EXPECTED_OBSERVATION_BUNDLE_SHA256,
        "observation_bundle_manifest": {
            "schema": "dpone.ci-shadow-reconciliation-observation-bundle.v1",
            "domain": _BUNDLE_DOMAIN,
            "manifest_sha256": _BUNDLE_MANIFEST_SHA256,
            "entries": copy.deepcopy(_BUNDLE_ENTRIES),
        },
        "interval": {
            "scan_from": "2026-07-25T02:45:00Z",
            "safe_scan_through": "2026-08-08T02:45:00Z",
            "observation_started_at": "2026-08-08T03:15:00Z",
        },
        "evidence_observed_through": "2026-08-08T03:20:00Z",
        "calibration_observed_at": "2026-08-08T03:20:00Z",
        "valid_until": "2026-08-09T03:20:00Z",
        "two_observations_match": True,
        "limits_crossed": [],
        "counters": {
            "producer_runs": 10,
            "producer_attempts": 12,
            "auditor_runs": 12,
            "producer_list_page_requests": 2,
            "auditor_list_page_requests": 2,
            "exact_producer_run_requests": 12,
            "attempt_run_requests": 24,
            "jobs_page_requests": 24,
            "artifact_metadata_requests": 24,
            "artifact_download_requests": 24,
            "redirect_requests": 24,
            "git_object_requests": 8,
            "pull_request_identity_requests": 12,
            "polling_requests": 4,
            "retry_requests": 0,
            "api_response_body_bytes": 1000,
            "artifact_download_body_bytes": 2000,
            "total_response_body_bytes": 3000,
            "total_http_requests": 160,
            "execution_wall_seconds": 120,
        },
        "hard_maxima": {
            "total_http_requests": 3000,
            "total_response_body_bytes": 134217728,
            "execution_wall_seconds": 900,
        },
        "approval_thresholds": {
            "total_http_requests": 1500,
            "total_response_body_bytes": 67108864,
            "execution_wall_seconds": 450,
        },
        "within_thresholds": True,
        "counter_semantics": (
            "exact below limits; saturating lower bounds at a crossed hard limit; retry_requests is a subset "
            "and is not added to total_http_requests"
        ),
        "wall_time_scope": (
            "injected monotonic seconds from immediately before first provider dispatch through final "
            "observation evaluation; each dispatch timeout is min(adapter timeout, remaining wall budget)"
        ),
    }


def test_request_accounting_and_capacity_calibration_are_closed() -> None:
    policy = _policy()
    accounting = policy["request_accounting"]
    calibration = policy["capacity_calibration"]

    assert accounting == {
        "increment": "before every outbound HTTP dispatch",
        "includes": (
            "REST, Git object, pull-request identity, artifact download endpoint, redirect follow-up, polling, "
            "and retry dispatches"
        ),
        "redirect_rule": "initial download endpoint and each followed redirect are separate requests",
        "response_byte_rule": "count response-body octets delivered to the application before archive decompression",
        "retry_rule": "each retry increments total_http_requests and its functional request class",
        "request_class_conservation": (
            "total_http_requests equals the sum of all dispatch classes except retry_requests, which is an "
            "intersecting subset"
        ),
        "counter_semantics": "exact below limits; saturating lower bounds at a crossed hard limit",
        "wall_time_scope": (
            "injected monotonic seconds from immediately before first provider dispatch through final observation "
            "evaluation"
        ),
        "dispatch_timeout_rule": (
            "min(adapter timeout, remaining wall budget); do not dispatch when no budget remains"
        ),
    }
    assert calibration["probe_child_requires_prior_approval"] is True
    assert calibration["required_before_reconciler_child_approval"] is True
    assert calibration["current_limits_are_parent_hard_maxima"] is True
    assert calibration["complete_double_observation_required"] is True
    assert calibration["minimum_safety_factor"] == 2
    assert calibration["observation_bundle_digest_required"] is True
    assert calibration["observation_bundle_manifest_schema"] == _BUNDLE_DOMAIN
    assert calibration["artifact_max_bytes"] == CAPACITY_ARTIFACT_MAX_BYTES
    assert calibration["observation_bundle_max_path_utf8_bytes"] == 1024
    assert calibration["wrapper_source_bound_separately"] is False
    assert calibration["execution_configuration_must_match"] is True
    assert calibration["reconciler_bundle_must_match"] is True
    assert calibration["failure"] == "RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED"
    assert calibration["hard_maxima"] == {
        "total_http_requests": 800,
        "total_response_body_bytes": 134217728,
        "execution_wall_seconds": 900,
    }
    assert calibration["approval_thresholds"] == {
        "total_http_requests": 400,
        "total_response_body_bytes": 67108864,
        "execution_wall_seconds": 450,
    }
    assert all(
        calibration["hard_maxima"][field] == 2 * threshold
        for field, threshold in calibration["approval_thresholds"].items()
    )


def test_capacity_contract_files_stay_human_reviewable() -> None:
    assert len(CAPACITY_SCHEMA.read_text(encoding="utf-8").splitlines()) <= 240
    assert Path(CAPACITY_SCHEMA).suffix == ".json"
