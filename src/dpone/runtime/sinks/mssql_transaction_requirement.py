"""Fail-closed generic SQL Server transaction-governance requirement."""

from __future__ import annotations

from typing import Any

from dpone.contracts.incremental_snapshot import KeySnapshotReconciliationPolicy

MSSQL_GENERIC_TRANSACTION_CAPABILITY = "mssql_generic_receipt_fence_v1"
MSSQL_TRANSACTION_ADMISSION_OPTION = "__dpone_mssql_transaction_admission"


def is_snapshot_envelope_route(load_config: Any) -> bool:
    """Return whether the separate XMin snapshot finalizer owns atomicity."""

    policy = KeySnapshotReconciliationPolicy.from_runtime(
        getattr(load_config, "options", None),
        legacy_enabled=bool(getattr(load_config, "reconciliation", False)),
    )
    return policy.key_snapshot_enabled


def require_generic_transaction_state(load_config: Any, state_storage: Any) -> Any | None:
    """Require target-atomic state for every non-snapshot MSSQL strategy."""

    if is_snapshot_envelope_route(load_config):
        return None
    if state_storage is None:
        raise RuntimeError("mssql_transaction.state_storage_required")
    if str(getattr(state_storage, "atomicity", "") or "") != "target_atomic":
        raise RuntimeError("mssql_transaction.target_atomic_state_required")
    if str(getattr(state_storage, "provisioning", "") or "") != "external":
        raise RuntimeError("mssql_transaction.external_state_catalog_required")
    return state_storage


__all__ = [
    "MSSQL_GENERIC_TRANSACTION_CAPABILITY",
    "MSSQL_TRANSACTION_ADMISSION_OPTION",
    "is_snapshot_envelope_route",
    "require_generic_transaction_state",
]
