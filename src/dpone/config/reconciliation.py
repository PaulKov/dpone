"""Public configuration contract for incremental key reconciliation.

The legacy ``reconciliation: true`` switch is intentionally preserved for one
compatibility window.  New workloads must use the typed ``key_snapshot``
object so the runtime can fail closed when it cannot prove snapshot
completeness or target-local atomicity.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.incremental_snapshot import KeySnapshotReconciliationPolicy


class ReconciliationConfigError(ValueError):
    """Raised when a public reconciliation block is ambiguous or unsafe."""


def normalize_reconciliation(value: object) -> tuple[bool, dict[str, Any] | None]:
    """Normalize the public boolean-or-object form.

    ``bool`` keeps the existing BigQuery-backed legacy behaviour.  A mapping is
    parsed strictly and returned as the runtime ``key_snapshot`` contract while
    the legacy boolean remains disabled.
    This distinction prevents the new MSSQL route from silently falling back
    to the unrelated legacy reconciliation service.
    """

    if value is None or value is False:
        return False, None
    if value is True:
        return True, None
    if not isinstance(value, Mapping):
        raise ReconciliationConfigError("reconciliation must be a boolean or an object")

    allowed = {
        "enabled",
        "mode",
        "cadence",
        "consistency",
        "delete_policy",
        "empty_snapshot",
        "guards",
    }
    unknown = sorted(str(key) for key in value if key not in allowed)
    if unknown:
        raise ReconciliationConfigError(f"unknown reconciliation fields: {', '.join(unknown)}")

    empty_raw = _mapping(value.get("empty_snapshot"), field_name="reconciliation.empty_snapshot")
    guard_raw = _mapping(value.get("guards"), field_name="reconciliation.guards")
    _reject_unknown(empty_raw, {"policy"}, field_name="reconciliation.empty_snapshot")
    _reject_unknown(
        guard_raw,
        {"max_delete_ratio", "max_delete_rows"},
        field_name="reconciliation.guards",
    )
    normalized = {
        "enabled": _boolean(value.get("enabled", True), field_name="reconciliation.enabled"),
        "mode": str(value.get("mode", "key_snapshot")),
        "cadence": str(value.get("cadence", "every_run")),
        "consistency": str(value.get("consistency", "same_source_snapshot")),
        "delete_policy": str(value.get("delete_policy", "soft_delete")),
        "empty_snapshot": {"policy": str(empty_raw.get("policy", "fail"))},
        "guards": {
            "max_delete_ratio": _ratio(guard_raw.get("max_delete_ratio", 0.05)),
            "max_delete_rows": _non_negative_int(guard_raw.get("max_delete_rows", 100_000)),
        },
    }
    try:
        policy = KeySnapshotReconciliationPolicy.from_runtime({"reconciliation": normalized})
    except ValueError as exc:
        raise ReconciliationConfigError(str(exc)) from exc
    # The first item controls only the deprecated BigQuery reconciliation
    # service.  Typed key_snapshot reconciliation is finalized target-locally
    # and must never activate that pre-load service.
    return False, policy.as_runtime_options()


def reconciliation_policy_from_options(
    options: Mapping[str, Any],
) -> KeySnapshotReconciliationPolicy:
    """Build the canonical policy from already-normalized runtime options."""

    try:
        return KeySnapshotReconciliationPolicy.from_runtime({"reconciliation": options})
    except ValueError as exc:
        raise ReconciliationConfigError(str(exc)) from exc


def _mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ReconciliationConfigError(f"{field_name} must be an object")
    return value


def _reject_unknown(value: Mapping[str, Any], allowed: set[str], *, field_name: str) -> None:
    unknown = sorted(str(key) for key in value if key not in allowed)
    if unknown:
        raise ReconciliationConfigError(f"unknown {field_name} fields: {', '.join(unknown)}")


def _boolean(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ReconciliationConfigError(f"{field_name} must be boolean")
    return value


def _ratio(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReconciliationConfigError("reconciliation.guards.max_delete_ratio must be numeric")
    return float(value)


def _non_negative_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReconciliationConfigError("reconciliation.guards.max_delete_rows must be an integer")
    return value


__all__ = [
    "ReconciliationConfigError",
    "normalize_reconciliation",
    "reconciliation_policy_from_options",
]
