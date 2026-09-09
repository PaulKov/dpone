"""Pure fail-closed policy for retained Airflow runtime Pods."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from dpone.contracts.airflow_runtime_pod_metadata import (
    TerminalPodMetadataSnapshot,
    iso_utc,
    require_aware_utc,
    runtime_pod_precondition_ref,
    runtime_pod_ref,
)

_CORRELATION_LABELS = ("dag_id", "task_id", "run_id")
_TERMINAL_PHASES = frozenset({"Succeeded", "Failed"})
pod_ref = runtime_pod_ref
precondition_ref = runtime_pod_precondition_ref


class RuntimePodOwnership(Protocol):
    """Structural ownership policy consumed by the pure classifier."""

    @property
    def managed_by_key(self) -> str: ...

    @property
    def managed_by_value(self) -> str: ...

    @property
    def runtime_contract_key(self) -> str: ...

    @property
    def runtime_contract_value(self) -> str: ...

    @property
    def workload_id_key(self) -> str: ...


@dataclass(frozen=True, slots=True)
class AirflowRuntimePodRetentionCandidate:
    pod: TerminalPodMetadataSnapshot
    age_seconds: int


@dataclass(frozen=True, slots=True)
class AirflowRuntimePodRetentionClassification:
    status: str
    inventory: dict[str, int]
    items: tuple[dict[str, Any], ...]
    candidates: tuple[AirflowRuntimePodRetentionCandidate, ...]
    warnings: tuple[str, ...]


def classify_runtime_pods(
    pods: tuple[TerminalPodMetadataSnapshot, ...],
    *,
    namespace: str,
    observed_at: datetime,
    minimum_age_seconds: int,
    ownership: RuntimePodOwnership,
) -> AirflowRuntimePodRetentionClassification:
    now = require_aware_utc(observed_at)
    uid_counts = Counter(pod.metadata.uid for pod in pods if pod.metadata.uid)
    items: list[dict[str, Any]] = []
    candidates: list[AirflowRuntimePodRetentionCandidate] = []
    quarantined = 0
    for pod in pods:
        action, reason, age = _classify_one(
            pod,
            now=now,
            minimum_age_seconds=minimum_age_seconds,
            uid_conflict=bool(pod.metadata.uid and uid_counts[pod.metadata.uid] > 1),
            ownership=ownership,
        )
        if action == "delete" and age is not None:
            candidates.append(AirflowRuntimePodRetentionCandidate(pod=pod, age_seconds=age))
        if action == "quarantine":
            quarantined += 1
        items.append(
            _item(
                pod,
                namespace=namespace,
                action=action,
                reason=reason,
                age_seconds=age,
                ownership=ownership,
            )
        )
    candidates.sort(key=lambda item: (-item.age_seconds, item.pod.metadata.name, item.pod.metadata.uid))
    status = "needs_attention" if quarantined else "needs_cleanup" if candidates else "ok"
    return AirflowRuntimePodRetentionClassification(
        status=status,
        inventory={
            "terminal_pods": len(pods),
            "succeeded_pods": sum(pod.phase == "Succeeded" for pod in pods),
            "failed_pods": sum(pod.phase == "Failed" for pod in pods),
            "quarantined": quarantined,
        },
        items=tuple(sorted(items, key=lambda item: (str(item["pod_name"]), str(item["pod_ref"])))),
        candidates=tuple(candidates),
        warnings=("terminal_age_uses_creation_timestamp_fallback",) if pods else (),
    )


def _classify_one(
    pod: TerminalPodMetadataSnapshot,
    *,
    now: datetime,
    minimum_age_seconds: int,
    uid_conflict: bool,
    ownership: RuntimePodOwnership,
) -> tuple[str, str, int | None]:
    metadata = pod.metadata
    if uid_conflict:
        return "quarantine", "inventory_phase_conflict", None
    if pod.phase not in _TERMINAL_PHASES:
        return "quarantine", "invalid_phase", None
    if not metadata.name or not metadata.uid or not metadata.resource_version:
        return "quarantine", "invalid_identity", None
    if metadata.deletion_timestamp is not None:
        return "protect", "terminating", _age(metadata.creation_timestamp, now)
    if not _owned_labels(metadata.labels, ownership=ownership):
        return "quarantine", "invalid_ownership", _age(metadata.creation_timestamp, now)
    if any(not metadata.labels.get(key, "").strip() for key in _CORRELATION_LABELS):
        return "quarantine", "missing_correlation", _age(metadata.creation_timestamp, now)
    if metadata.creation_timestamp is None:
        return "quarantine", "timestamp_missing", None
    age = _age(metadata.creation_timestamp, now)
    if age is None or age < 0:
        return "quarantine", "future_timestamp", age
    if age < minimum_age_seconds:
        return "protect", "minimum_age", age
    return "delete", "stale_terminal", age


def _owned_labels(labels: dict[str, str], *, ownership: RuntimePodOwnership) -> bool:
    return (
        labels.get(ownership.managed_by_key) == ownership.managed_by_value
        and labels.get(ownership.runtime_contract_key) == ownership.runtime_contract_value
        and bool(labels.get(ownership.workload_id_key, "").strip())
    )


def _age(created_at: datetime | None, now: datetime) -> int | None:
    if created_at is None:
        return None
    try:
        created = require_aware_utc(created_at)
    except ValueError:
        return None
    return int((now - created).total_seconds())


def _item(
    pod: TerminalPodMetadataSnapshot,
    *,
    namespace: str,
    action: str,
    reason: str,
    age_seconds: int | None,
    ownership: RuntimePodOwnership,
) -> dict[str, Any]:
    metadata = pod.metadata
    payload: dict[str, Any] = {
        "pod_ref": runtime_pod_ref(namespace, metadata.uid),
        "precondition_ref": runtime_pod_precondition_ref(namespace, metadata.uid, metadata.resource_version),
        "pod_name": metadata.name,
        "phase": pod.phase,
        "action": action,
        "reason": reason,
        "age_seconds": age_seconds,
        "age_basis": "creation_timestamp_fallback",
        "terminal_age_exact": False,
        "created_at": iso_utc(metadata.creation_timestamp),
        "workload_id": metadata.labels.get(ownership.workload_id_key),
        "dag_id": metadata.labels.get("dag_id"),
        "task_id": metadata.labels.get("task_id"),
        "run_id": metadata.labels.get("run_id"),
    }
    return payload


__all__ = [
    "AirflowRuntimePodRetentionCandidate",
    "AirflowRuntimePodRetentionClassification",
    "RuntimePodOwnership",
    "classify_runtime_pods",
    "iso_utc",
    "pod_ref",
    "precondition_ref",
    "require_aware_utc",
    "runtime_pod_precondition_ref",
    "runtime_pod_ref",
]
