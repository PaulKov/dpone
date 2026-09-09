"""Render closed diagnostic capacity evidence from captured PR4C observations."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final, Protocol

from dpone.contracts.ci_shadow_reconciliation import CapacityUsage, ReconciliationPolicy

_BUNDLE_SCHEMA: Final = "dpone.ci-shadow-reconciliation-observation-bundle.v1"


class CapacitySource(Protocol):
    """Authenticated source identity consumed by evidence rendering."""

    @property
    def repository_id(self) -> int: ...

    @property
    def workflow_id(self) -> int: ...

    @property
    def workflow_sha(self) -> str: ...

    @property
    def run_id(self) -> int: ...

    @property
    def run_attempt(self) -> int: ...


class CapacityCalibration(Protocol):
    """Completed or blocked observation projection accepted by the renderer."""

    @property
    def code(self) -> str: ...

    @property
    def complete(self) -> bool: ...

    @property
    def counters(self) -> dict[str, int | float]: ...

    @property
    def evidence_observed_through(self) -> datetime: ...

    @property
    def calibration_observed_at(self) -> datetime: ...

    @property
    def observation_started_at(self) -> datetime: ...

    @property
    def observations_match(self) -> bool: ...

    @property
    def limits_crossed(self) -> tuple[str, ...]: ...

    @property
    def usage(self) -> CapacityUsage: ...


def render_capacity_evidence(
    calibration: CapacityCalibration,
    *,
    source: CapacitySource,
    policy: ReconciliationPolicy,
    observation_bundle_sha256: str,
    observation_bundle_manifest_sha256: str,
    observation_bundle_entries: list[dict[str, str]],
    execution_configuration: dict[str, str],
    interval: dict[str, str],
) -> dict[str, object]:
    """Return the complete closed JSON shape; no field carries PASS authority."""

    _require_digest(observation_bundle_sha256, "observation bundle")
    _require_digest(observation_bundle_manifest_sha256, "observation bundle manifest")
    _require_execution_configuration(execution_configuration)
    counters = dict(calibration.counters)
    _require_counters(counters, calibration)
    return {
        "schema_version": 1,
        "schema": policy.capacity_schema,
        "decision": "UNVERIFIED",
        "code": calibration.code,
        "complete": calibration.complete,
        "source": {
            "repository_id": source.repository_id,
            "workflow_id": source.workflow_id,
            "workflow_path": ".github/workflows/pr-gate-shadow-capacity.yml",
            "event": "workflow_dispatch",
            "ref": "refs/heads/master",
            "workflow_sha": source.workflow_sha,
            "run_id": source.run_id,
            "run_attempt": source.run_attempt,
        },
        "execution_configuration": dict(execution_configuration),
        "policy_sha256": policy.sha256,
        "observation_bundle_sha256": observation_bundle_sha256,
        "observation_bundle_manifest": {
            "schema": _BUNDLE_SCHEMA,
            "domain": _BUNDLE_SCHEMA,
            "manifest_sha256": observation_bundle_manifest_sha256,
            "entries": observation_bundle_entries,
        },
        "interval": dict(interval),
        "evidence_observed_through": _timestamp(calibration.evidence_observed_through),
        "calibration_observed_at": _timestamp(calibration.calibration_observed_at),
        "valid_until": _timestamp(calibration.calibration_observed_at + timedelta(hours=24)),
        "two_observations_match": calibration.observations_match,
        "limits_crossed": list(calibration.limits_crossed),
        "counters": counters,
        "hard_maxima": {
            "total_http_requests": policy.hard_max_http_requests,
            "total_response_body_bytes": policy.hard_max_response_bytes,
            "execution_wall_seconds": policy.hard_max_wall_seconds,
        },
        "approval_thresholds": {
            "total_http_requests": policy.approval_thresholds.total_http_requests,
            "total_response_body_bytes": policy.approval_thresholds.total_response_body_bytes,
            "execution_wall_seconds": policy.approval_thresholds.execution_wall_seconds,
        },
        "within_thresholds": calibration.code == "RECONCILIATION_CAPACITY_CALIBRATION_ONLY",
        "counter_semantics": (
            "exact below limits; saturating lower bounds at a crossed hard limit; retry_requests is a subset "
            "and is not added to total_http_requests"
        ),
        "wall_time_scope": (
            "injected monotonic seconds from immediately before first provider dispatch through final observation "
            "evaluation; each dispatch timeout is min(adapter timeout, remaining wall budget)"
        ),
    }


def _timestamp(value: object) -> str:
    isoformat = getattr(value, "isoformat", None)
    if not callable(isoformat):
        raise ValueError("capacity timestamp is invalid")
    rendered = isoformat().replace("+00:00", "Z")
    if not rendered.endswith("Z") or "." in rendered:
        raise ValueError("capacity timestamp must be a whole UTC second")
    return rendered


def _require_digest(value: str, label: str) -> None:
    if not value.startswith("sha256:") or len(value) != 71 or any(char not in "0123456789abcdef" for char in value[7:]):
        raise ValueError(f"{label} digest is invalid")


def _require_execution_configuration(value: dict[str, str]) -> None:
    if set(value) != {"runner_label", "runner_arch", "python_version"} or not all(value.values()):
        raise ValueError("execution configuration is invalid")


def _require_counters(counters: dict[str, int | float], calibration: CapacityCalibration) -> None:
    required = {
        "producer_runs",
        "producer_attempts",
        "auditor_runs",
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
        "retry_requests",
        "api_response_body_bytes",
        "artifact_download_body_bytes",
        "total_response_body_bytes",
        "total_http_requests",
        "execution_wall_seconds",
    }
    if set(counters) != required or counters["total_http_requests"] != calibration.usage.total_http_requests:
        raise ValueError("capacity counters are incomplete")
    if counters["total_response_body_bytes"] != calibration.usage.total_response_body_bytes:
        raise ValueError("capacity response-byte counter is inconsistent")
    if counters["execution_wall_seconds"] != calibration.usage.execution_wall_seconds:
        raise ValueError("capacity wall-time counter is inconsistent")


__all__ = ["CapacitySource", "render_capacity_evidence"]
