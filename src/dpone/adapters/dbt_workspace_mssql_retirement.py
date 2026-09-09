"""Quiescent retirement transaction helpers for workspace activation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dpone.adapters.dbt_workspace_mssql_activation_connection import WorkspaceActivationCursor, row
from dpone.contracts.dbt_workspace_control import (
    DbtWorkspaceActivationError,
    DbtWorkspaceActivationReceipt,
    DbtWorkspaceActivationRequest,
    DbtWorkspaceGuardEpoch,
    canonical_fingerprint,
)

Table = Callable[[str], str]
ActivationRow = Callable[[WorkspaceActivationCursor, str], tuple[Any, ...] | None]
ExactReader = Callable[..., DbtWorkspaceActivationReceipt]


def activation_record(
    cursor: WorkspaceActivationCursor,
    activation_id: str,
    *,
    table: Table,
) -> tuple[Any, ...] | None:
    """Lock and return the durable occurrence row, when present."""

    cursor.execute(
        f"""
SELECT request_sha256, environment, release_id, deployment_id,
       previous_deployment_id, source_inventory_sha256, runtime_context_sha256, state
FROM {table("dbt_workspace_activations")} WITH (UPDLOCK, HOLDLOCK)
WHERE activation_id = ?;
""".strip(),
        activation_id,
    )
    return row(cursor)


def insert_activation_record(
    cursor: WorkspaceActivationCursor,
    request: DbtWorkspaceActivationRequest,
    *,
    table: Table,
) -> None:
    """Insert the immutable PREPARED occurrence identity."""

    cursor.execute(
        f"""
INSERT INTO {table("dbt_workspace_activations")} (
    activation_id, request_sha256, environment, release_id, deployment_id,
    previous_deployment_id, source_inventory_sha256, runtime_context_sha256, state
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, N'PREPARED');
""".strip(),
        request.activation_id,
        request.request_sha256,
        request.environment,
        request.release_id,
        request.deployment_id,
        request.previous_deployment_id,
        request.source_inventory_sha256,
        request.runtime_context_sha256,
    )


def read_exact_activation(
    cursor: WorkspaceActivationCursor,
    request: DbtWorkspaceActivationRequest,
    *,
    state: str,
    table: Table,
    existing: tuple[Any, ...] | None = None,
) -> DbtWorkspaceActivationReceipt:
    """Verify occurrence identity and its complete live guard ownership."""

    observed = existing if existing is not None else activation_record(cursor, request.activation_id, table=table)
    expected = (
        request.request_sha256,
        request.environment,
        request.release_id,
        request.deployment_id,
        request.previous_deployment_id,
        request.source_inventory_sha256,
        request.runtime_context_sha256,
        state,
    )
    if observed != expected:
        raise DbtWorkspaceActivationError("occurrence_mismatch")
    cursor.execute(
        f"""
SELECT ownership.guard_id, ownership.resource_sha256, ownership.fencing_epoch,
       guard.owner_id, guard.workflow_id, guard.operation_id, guard.status, guard.fencing_epoch
FROM {table("dbt_workspace_activation_guards")} AS ownership WITH (UPDLOCK, HOLDLOCK)
JOIN {table("semantic_refresh_guards")} AS guard WITH (UPDLOCK, HOLDLOCK)
  ON guard.resource_id = ownership.guard_id
WHERE ownership.activation_id = ?
ORDER BY ownership.guard_id;
""".strip(),
        request.activation_id,
    )
    rows = tuple(tuple(item) for item in cursor.fetchall())
    owner = f"dbt-workspace:{request.activation_id}"
    if len(rows) != len(request.resources):
        raise DbtWorkspaceActivationError("guard_readback")
    expected_rows = tuple(
        (
            resource.guard_id,
            canonical_fingerprint(resource.to_dict()),
            item[2],
            owner,
            request.activation_id,
            None,
            "HELD",
            item[2],
        )
        for resource, item in zip(request.resources, rows, strict=True)
    )
    if rows != expected_rows:
        raise DbtWorkspaceActivationError("guard_readback")
    return DbtWorkspaceActivationReceipt.build(
        activation_id=request.activation_id,
        request_sha256=request.request_sha256,
        state=state,
        guard_epochs=tuple(DbtWorkspaceGuardEpoch(str(item[0]), int(item[2])) for item in rows),
    )


def begin_retirement(
    cursor: WorkspaceActivationCursor,
    request: DbtWorkspaceActivationRequest,
    *,
    table: Table,
    activation_row: ActivationRow,
    read_exact: ExactReader,
) -> DbtWorkspaceActivationReceipt:
    """Close new admission while retaining all currently held epochs."""

    existing = activation_row(cursor, request.activation_id)
    if existing is not None and existing[-1] == "RETIRING":
        return read_exact(cursor, request, state="RETIRING", activation_row=existing)
    read_exact(cursor, request, state="ACTIVE", activation_row=existing)
    cursor.execute(
        f"""
UPDATE {table("dbt_workspace_activations")} WITH (UPDLOCK, HOLDLOCK)
SET state = N'RETIRING', updated_at_utc = SYSUTCDATETIME()
OUTPUT inserted.state
WHERE activation_id = ? AND request_sha256 = ? AND state = N'ACTIVE';
""".strip(),
        request.activation_id,
        request.request_sha256,
    )
    if row(cursor) != ("RETIRING",):
        raise DbtWorkspaceActivationError("occurrence_transition")
    return read_exact(cursor, request, state="RETIRING")


def finalize_retirement(
    cursor: WorkspaceActivationCursor,
    request: DbtWorkspaceActivationRequest,
    *,
    table: Table,
    activation_row: ActivationRow,
    read_exact: ExactReader,
) -> DbtWorkspaceActivationReceipt:
    """Release exact guards only after terminal attempt quiescence."""

    existing = activation_row(cursor, request.activation_id)
    if existing is not None and existing[-1] == "RETIRED":
        return read_retired(cursor, request, table=table, activation_row=activation_row, current=existing)
    retiring = read_exact(cursor, request, state="RETIRING", activation_row=existing)
    require_attempt_quiescence(cursor, request.activation_id, table=table)
    owner = f"dbt-workspace:{request.activation_id}"
    for guard in retiring.guard_epochs:
        cursor.execute(
            f"""
UPDATE {table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
SET owner_id = NULL, workflow_id = NULL, operation_id = NULL, status = N'AVAILABLE'
OUTPUT inserted.resource_id
WHERE resource_id = ? AND fencing_epoch = ? AND owner_id = ?
  AND workflow_id = ? AND operation_id IS NULL AND status = N'HELD';
""".strip(),
            guard.guard_id,
            guard.fencing_epoch,
            owner,
            request.activation_id,
        )
        if row(cursor) != (guard.guard_id,):
            raise DbtWorkspaceActivationError("guard_release")
    cursor.execute(
        f"""
UPDATE {table("dbt_workspace_activations")} WITH (UPDLOCK, HOLDLOCK)
SET state = N'RETIRED', updated_at_utc = SYSUTCDATETIME()
OUTPUT inserted.state
WHERE activation_id = ? AND request_sha256 = ? AND state = N'RETIRING';
""".strip(),
        request.activation_id,
        request.request_sha256,
    )
    if row(cursor) != ("RETIRED",):
        raise DbtWorkspaceActivationError("occurrence_transition")
    return read_retired(cursor, request, table=table, activation_row=activation_row)


def require_attempt_quiescence(cursor: WorkspaceActivationCursor, activation_id: str, *, table: Table) -> None:
    cursor.execute(
        f"""
SELECT TOP (1) attempt_id, state, terminal_receipt_sha256
FROM {table("dbt_workspace_attempts")} WITH (UPDLOCK, HOLDLOCK)
WHERE activation_id = ? AND (
    state IN (N'RUNNING', N'COMMIT_UNKNOWN')
    OR terminal_receipt_sha256 IS NULL
);
""".strip(),
        activation_id,
    )
    if row(cursor) is not None:
        raise DbtWorkspaceActivationError("terminal_quiescence_unavailable")


def read_retired(
    cursor: WorkspaceActivationCursor,
    request: DbtWorkspaceActivationRequest,
    *,
    table: Table,
    activation_row: ActivationRow,
    current: tuple[Any, ...] | None = None,
) -> DbtWorkspaceActivationReceipt:
    """Read durable retired ownership even after a successor claims the guards."""

    observed = current if current is not None else activation_row(cursor, request.activation_id)
    expected = (
        request.request_sha256,
        request.environment,
        request.release_id,
        request.deployment_id,
        request.previous_deployment_id,
        request.source_inventory_sha256,
        request.runtime_context_sha256,
        "RETIRED",
    )
    if observed != expected:
        raise DbtWorkspaceActivationError("occurrence_mismatch")
    cursor.execute(
        f"""
SELECT guard_id, fencing_epoch
FROM {table("dbt_workspace_activation_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE activation_id = ? ORDER BY guard_id;
""".strip(),
        request.activation_id,
    )
    guards = tuple(DbtWorkspaceGuardEpoch(str(item[0]), int(item[1])) for item in cursor.fetchall())
    if tuple(item.guard_id for item in guards) != tuple(item.guard_id for item in request.resources):
        raise DbtWorkspaceActivationError("guard_readback")
    return DbtWorkspaceActivationReceipt.build(
        activation_id=request.activation_id,
        request_sha256=request.request_sha256,
        state="RETIRED",
        guard_epochs=guards,
    )


__all__ = [
    "activation_record",
    "begin_retirement",
    "finalize_retirement",
    "insert_activation_record",
    "read_exact_activation",
    "read_retired",
]
