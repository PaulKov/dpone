"""Persist target-only recovery authority before source row I/O begins."""

from __future__ import annotations

import json
from typing import Any

from dpone.contracts.mssql_native_recovery_authority import build_mssql_native_recovery_authority


def persist_mssql_native_recovery_plan(
    store: Any,
    lease: Any,
    *,
    identity: Any,
    schema: tuple[tuple[str, str], ...],
    admission: Any,
    recovery_bindings: Any,
    window: Any,
) -> None:
    """CAS one immutable schema and operation authority before extraction."""

    if not callable(recovery_bindings):
        raise ValueError("mssql_native.recovery_bindings_required")
    key = f"mssql-native/recovery-plan-v1/{identity.invocation_key}"
    value = {
        "schema_version": 2,
        "kind": "dpone.mssql-native-recovery-plan.v2",
        "invocation_id": identity.invocation_key,
        "schema": [list(column) for column in schema],
        "window": (
            None
            if window is None
            else {
                "column": window.column,
                "start": window.start.isoformat(),
                "end": window.end.isoformat(),
            }
        ),
        "recovery_authority_v1": build_mssql_native_recovery_authority(
            admission,
            recovery_bindings(admission),
        ),
    }
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    current = store.load(key)
    if current is None:
        store.save(key, None, payload, lease)
    elif current.payload != payload:
        raise ValueError("mssql_native.recovery_plan_changed")


__all__ = ["persist_mssql_native_recovery_plan"]
