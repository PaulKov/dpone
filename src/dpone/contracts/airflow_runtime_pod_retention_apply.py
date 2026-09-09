"""Canonical state projection for runtime Pod retention apply evidence."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Literal

EVIDENCE_DURABILITIES = frozenset({"process_ordered", "durable_acknowledged"})
EVIDENCE_FAILURE_CODE = "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_EVIDENCE_UNAVAILABLE"

_PARTIAL_REASONS = frozenset(
    {
        "batch_limit",
        "changed_since_plan",
        "not_attempted_after_failure",
        "inventory_phase_conflict",
        "invalid_phase",
        "invalid_identity",
        "invalid_ownership",
        "missing_correlation",
        "timestamp_missing",
        "future_timestamp",
    }
)


@dataclass(frozen=True, slots=True)
class AirflowRuntimePodRetentionApplyState:
    status: Literal["ok", "partial", "failed"]
    delete_accepted_pod_names: tuple[str, ...]
    skipped_pod_names: tuple[str, ...]
    failed_pod_names: tuple[str, ...]


def derive_runtime_pod_apply_state(
    items: Iterable[Mapping[str, object]],
    *,
    evidence_status: str,
) -> AirflowRuntimePodRetentionApplyState:
    """Derive all apply summaries from authoritative item outcomes."""

    materialized = tuple(items)
    accepted = _names(materialized, lambda item: item.get("action") == "delete_accepted")
    skipped = _names(materialized, lambda item: item.get("action") in {"skipped", "preserved"})
    failed = _names(
        materialized,
        lambda item: item.get("action") == "failed" or item.get("error_code") == EVIDENCE_FAILURE_CODE,
    )
    partial = any(item.get("reason") in _PARTIAL_REASONS for item in materialized)
    status: Literal["ok", "partial", "failed"]
    if evidence_status != "complete" or failed:
        status = "failed"
    elif partial:
        status = "partial"
    else:
        status = "ok"
    return AirflowRuntimePodRetentionApplyState(
        status=status,
        delete_accepted_pod_names=accepted,
        skipped_pod_names=skipped,
        failed_pod_names=failed,
    )


def _names(
    items: tuple[Mapping[str, object], ...],
    predicate: Callable[[Mapping[str, object]], bool],
) -> tuple[str, ...]:
    return tuple(sorted({str(item["pod_name"]) for item in items if predicate(item)}))


__all__ = [
    "AirflowRuntimePodRetentionApplyState",
    "EVIDENCE_DURABILITIES",
    "EVIDENCE_FAILURE_CODE",
    "derive_runtime_pod_apply_state",
]
