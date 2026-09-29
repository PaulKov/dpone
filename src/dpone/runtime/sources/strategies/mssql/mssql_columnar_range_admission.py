"""Fail-closed activation policy for MSSQL columnar range reads."""

from __future__ import annotations

from typing import Any

from dpone.runtime.partitioning_options import PartitioningOptionsResolver

RANGE_BYTE_ADMISSION_BLOCKER = "columnar_range_pre_read_byte_admission_unavailable"
RANGE_CONSISTENCY_BLOCKER = "columnar_range_parallelism_requires_explicit_consistency"
RANGE_SESSION_BLOCKER = "mssql_independent_range_sessions_unavailable"
RANGE_TERMINAL_BLOCKERS = (RANGE_BYTE_ADMISSION_BLOCKER, RANGE_CONSISTENCY_BLOCKER, RANGE_SESSION_BLOCKER)


def range_byte_admission_available() -> bool:
    """Return whether bytes can be reserved before ODBC materialization.

    The 0.87.0 producer acquires its byte reservation after ``fetchmany`` has
    materialized a batch and therefore cannot prove the approved hard bound.
    One adapter-local policy prevents composition paths from accidentally
    re-enabling that implementation.
    """

    return False


def validate_range_activation(source_options: dict[str, Any], connector: Any) -> tuple[Any, bool, bool]:
    """Validate activation-only inputs before metadata or row I/O."""

    partitioning = PartitioningOptionsResolver.resolve(source_options)
    policy = partitioning.range_parallelism
    raw_partitioning = source_options.get("partitioning")
    raw_partitioning = raw_partitioning if isinstance(raw_partitioning, dict) else {}
    parallelism = raw_partitioning.get("range_parallelism")
    parallelism = parallelism if isinstance(parallelism, dict) else {}
    if policy.mode != "off" and "consistency" not in parallelism:
        raise ValueError(RANGE_CONSISTENCY_BLOCKER)
    sessions_available = callable(getattr(connector, "open_session", None))
    if policy.mode == "required" and not sessions_available:
        raise RuntimeError(RANGE_SESSION_BLOCKER)
    byte_admission_available = range_byte_admission_available()
    if policy.mode == "required" and not byte_admission_available:
        raise RuntimeError(RANGE_BYTE_ADMISSION_BLOCKER)
    return partitioning, sessions_available, byte_admission_available


def range_decision_details(source_options: dict[str, Any], connector: Any) -> dict[str, object]:
    """Return truthful runtime-decision details for an automatic fallback."""

    policy = PartitioningOptionsResolver.resolve(source_options).range_parallelism
    if policy.mode != "auto" or range_byte_admission_available():
        return {}
    reason = RANGE_BYTE_ADMISSION_BLOCKER
    if not callable(getattr(connector, "open_session", None)):
        reason = RANGE_SESSION_BLOCKER
    return {"range_parallelism": {"status": "serial_fallback", "fallback_reason": reason}}


def admitted_range_parts(request: Any) -> tuple[Any, Any, tuple[Any, ...]]:
    """Return a validated plan and partitions only when byte admission exists."""

    if not range_byte_admission_available():
        raise RuntimeError(RANGE_BYTE_ADMISSION_BLOCKER)
    partitioner = request.range_partitioner
    plan = request.range_plan
    if partitioner is None or plan is None:
        raise ValueError("A canonical range partitioner and plan are required.")
    partitions = tuple(partitioner.partitions())
    if len(partitions) != len(plan.ranges):
        raise ValueError("Canonical range plan does not match the partitioner.")
    return partitioner, plan, partitions


__all__ = [
    "RANGE_BYTE_ADMISSION_BLOCKER",
    "RANGE_CONSISTENCY_BLOCKER",
    "RANGE_SESSION_BLOCKER",
    "RANGE_TERMINAL_BLOCKERS",
    "admitted_range_parts",
    "range_byte_admission_available",
    "range_decision_details",
    "validate_range_activation",
]
