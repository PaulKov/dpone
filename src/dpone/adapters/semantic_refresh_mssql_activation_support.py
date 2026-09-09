"""Create-only activated-pack SQL and shared activation errors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class SemanticRefreshMssqlActivationError(RuntimeError):
    """Raised when create-only deployment or run authority differs."""


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...


@dataclass(frozen=True, slots=True)
class MssqlActivatedPackRow:
    """Exact protected values stored for one activated execution pack."""

    pack_fingerprint: str
    activation_authority_receipt_sha256: str
    authority_store_ref: str
    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    workflow_plan_sha256: str
    plan_bundle_sha256: str
    run_execution_bundle_sha256: str
    run_guard_closure_sha256: str
    workflow_guard_resource_id: str
    resource_guard_ids_json: str
    static_projection_identity_json: str


class MssqlActivatedPackQueries:
    """Persist activated-pack authority inside an owner transaction."""

    def __init__(self, control_schema: str) -> None:
        self._table = f"[{control_schema}].[semantic_refresh_activated_packs]"

    def register(self, cursor: _Cursor, row: MssqlActivatedPackRow) -> None:
        """Insert once, acknowledge exact ACTIVE replay, and reject binding reuse."""

        cursor.execute(
            f"""
SELECT activation_authority_receipt_sha256, authority_store_ref,
       workflow_execution_id, workflow_execution_binding_sha256,
       workflow_plan_sha256, plan_bundle_sha256,
       run_execution_bundle_sha256, run_guard_closure_sha256,
       workflow_guard_resource_id, resource_guard_ids_json,
       static_projection_identity_json, status
FROM {self._table} WITH (UPDLOCK, HOLDLOCK)
WHERE pack_fingerprint = ?;
""".strip(),
            row.pack_fingerprint,
        )
        existing = _row(cursor)
        expected = (
            row.activation_authority_receipt_sha256,
            row.authority_store_ref,
            row.workflow_execution_id,
            row.workflow_execution_binding_sha256,
            row.workflow_plan_sha256,
            row.plan_bundle_sha256,
            row.run_execution_bundle_sha256,
            row.run_guard_closure_sha256,
            row.workflow_guard_resource_id,
            row.resource_guard_ids_json,
            row.static_projection_identity_json,
            "ACTIVE",
        )
        if existing is not None:
            if existing != expected:
                raise SemanticRefreshMssqlActivationError("activated pack replay differs")
            return
        cursor.execute(
            f"""
SELECT pack_fingerprint
FROM {self._table} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
            row.workflow_execution_binding_sha256,
        )
        if _row(cursor) is not None:
            raise SemanticRefreshMssqlActivationError("activated pack execution binding already exists")
        cursor.execute(
            f"""
INSERT INTO {self._table} (
    pack_fingerprint, activation_authority_receipt_sha256, authority_store_ref,
    workflow_execution_id, workflow_execution_binding_sha256,
    workflow_plan_sha256, plan_bundle_sha256,
    run_execution_bundle_sha256, run_guard_closure_sha256,
    workflow_guard_resource_id, resource_guard_ids_json,
    static_projection_identity_json, status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, N'ACTIVE');
""".strip(),
            row.pack_fingerprint,
            row.activation_authority_receipt_sha256,
            row.authority_store_ref,
            row.workflow_execution_id,
            row.workflow_execution_binding_sha256,
            row.workflow_plan_sha256,
            row.plan_bundle_sha256,
            row.run_execution_bundle_sha256,
            row.run_guard_closure_sha256,
            row.workflow_guard_resource_id,
            row.resource_guard_ids_json,
            row.static_projection_identity_json,
        )


def _row(cursor: _Cursor) -> tuple[Any, ...] | None:
    value = cursor.fetchone()
    return None if value is None else tuple(value)


__all__ = [
    "MssqlActivatedPackQueries",
    "MssqlActivatedPackRow",
    "SemanticRefreshMssqlActivationError",
]
