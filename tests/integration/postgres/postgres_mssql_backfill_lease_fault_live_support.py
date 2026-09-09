"""Vendor-backed lease evidence helpers for reviewed lifecycle faults."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from dpone.backfill.chunk_lifecycle import BackfillChunkLifecycleContext


def mssql_running_journal_rows(
    context: BackfillChunkLifecycleContext,
) -> tuple[dict[str, Any], ...]:
    """Read physical running-journal evidence from the real SQL state store."""

    store = context.store
    connector = getattr(store, "connector", None)
    table = getattr(store, "chunk_fq_table", None)
    if connector is None or not isinstance(table, str):
        raise AssertionError("lease-expiry journal evidence requires a SQL state store")
    rows = connector.get_records(
        f"SELECT journal_id, status, details_json, __dpone__loaded_at, "
        "sys.fn_PhysLocFormatter(%%physloc%%) AS physical_row_locator "
        f"FROM {table} WHERE run_key = ? AND chunk_index = ? "
        "AND status = N'running' ORDER BY journal_id DESC",
        (context.run_key, context.chunk_index),
        as_dict=True,
    )
    return tuple(
        {
            "journal_id": int(row["journal_id"]),
            "loaded_at": row["__dpone__loaded_at"].isoformat(),
            "physical_row_locator": str(row["physical_row_locator"]),
            "lease_owner": json.loads(str(row["details_json"]))["lease_owner"],
            "lease_expires_at": json.loads(str(row["details_json"]))["lease_expires_at"],
        }
        for row in rows
    )


def reacquire_with_transition_trace(
    context: BackfillChunkLifecycleContext,
    *,
    replacement_owner: str,
) -> tuple[bool, dict[str, Any]]:
    """Trace external-lock admission separately from the lease delegate."""

    store = context.store
    coordinator = getattr(store, "_state_coordinator", None)
    original = getattr(coordinator, "run_chunk_transition", None)
    if not callable(original):
        raise AssertionError("lease-expiry transition trace requires a SQL state coordinator")
    trace: dict[str, Any] = {
        "applock_acquired": False,
        "delegate_entered": False,
        "delegate_result": None,
    }

    def traced_transition(run_key: str, index: int, operation: Callable[[], Any]) -> Any:
        def traced_delegate() -> Any:
            trace["applock_acquired"] = True
            trace["delegate_entered"] = True
            decision_now = datetime.now(UTC)
            latest = store.load(run_key)
            if latest is None:
                raise AssertionError("lease-expiry transition trace lost its campaign")
            record = latest.chunk(index)
            expiry = datetime.fromisoformat(str(record.lease_expires_at))
            trace.update(
                {
                    "delegate_status": record.status,
                    "delegate_owner": record.lease_owner,
                    "delegate_lease_expires_at": record.lease_expires_at,
                    "delegate_decision_at": decision_now.isoformat(),
                    "delegate_expired": expiry <= decision_now,
                }
            )
            result = operation()
            trace["delegate_result"] = bool(result)
            return result

        return original(run_key, index, traced_delegate)

    setattr(coordinator, "run_chunk_transition", traced_transition)
    try:
        reacquired = store.acquire_chunk_lease(
            context.run_key,
            context.chunk_index,
            owner=replacement_owner,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
        )
    finally:
        setattr(coordinator, "run_chunk_transition", original)
    return bool(reacquired), trace


__all__ = ["mssql_running_journal_rows", "reacquire_with_transition_trace"]
