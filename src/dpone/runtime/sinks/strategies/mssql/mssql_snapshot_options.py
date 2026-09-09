"""Runtime-only safety options for MSSQL key-snapshot finalization."""

from __future__ import annotations

from typing import Any

from dpone.runtime.sinks.strategies.mssql.mssql_transaction_lock import (
    acquire_target_lock as _acquire_canonical_target_lock,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_lock import (
    target_lock_resource as _canonical_target_lock_resource,
)


class MssqlSnapshotOptionError(ValueError):
    """Raised when runtime-only snapshot options violate safe bounds."""


def require_safe_route_options(load_config: Any) -> None:
    """Keep unsafe legacy BCP bypasses unreachable for this public route."""

    options = getattr(load_config, "options", {}) or {}
    if options.get("allow_unsafe_raw_mssql_bulk_files") is True:
        raise MssqlSnapshotOptionError("mssql_snapshot_reconciliation.unsafe_raw_bulk_files_forbidden")


def lock_timeout_ms(load_config: Any) -> int:
    """Return a bounded wait so concurrent finalizers serialize safely."""

    options = getattr(load_config, "options", {}) or {}
    raw = options.get("mssql_snapshot_lock_timeout_ms", 300_000)
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise MssqlSnapshotOptionError("mssql_snapshot_reconciliation.lock_timeout_invalid") from exc
    if not 1 <= value <= 3_600_000:
        raise MssqlSnapshotOptionError("mssql_snapshot_reconciliation.lock_timeout_invalid")
    return value


def target_lock_resource(target_identity: bytes) -> str:
    """Bind serialization to the catalog-resolved binary target identity."""

    try:
        return _canonical_target_lock_resource(target_identity)
    except ValueError as exc:
        raise MssqlSnapshotOptionError("mssql_snapshot_reconciliation.target_lock_identity_invalid") from exc


def acquire_target_lock(connector: Any, resource: str, *, database: str, timeout_ms: int) -> None:
    """Acquire one database-scoped transaction-owned application lock."""

    try:
        _acquire_canonical_target_lock(
            connector,
            resource,
            database=database,
            timeout_ms=timeout_ms,
        )
    except (ValueError, RuntimeError) as exc:
        raise MssqlSnapshotOptionError("mssql_snapshot_reconciliation.lock_unavailable") from exc


__all__ = [
    "MssqlSnapshotOptionError",
    "acquire_target_lock",
    "lock_timeout_ms",
    "require_safe_route_options",
    "target_lock_resource",
]
