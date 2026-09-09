"""FAILED_PRE_COMMIT cleanup acknowledgement and successor-gate tests."""

from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.adapters.semantic_refresh_mssql_cleanup_ack import (
    MssqlSemanticRefreshFailedPrecommitCleanupAckStore,
    SemanticRefreshMssqlCleanupAckError,
)
from dpone.adapters.semantic_refresh_mssql_state import (
    MssqlSemanticRefreshStateAdapter,
    SemanticRefreshMssqlStateConflict,
)
from dpone.ports.semantic_refresh_clickhouse_resources import (
    ClickHouseFailedScratchCleanupReceipt,
    ClickHouseScratchRelationAbsence,
)
from dpone.ports.semantic_refresh_mssql_cleanup_ack import (
    MssqlFailedPrecommitCleanupAck,
)
from dpone.ports.semantic_refresh_mssql_resources import (
    MSSQL_RESOURCE_KINDS,
    MssqlProtectedResourceAllocation,
    MssqlProtectedResourceAllocationClosure,
    mssql_resource_allocation_id,
)

_BINDING = "sha256:" + "1" * 64
_OPERATION = "sha256:" + "2" * 64
_PLAN = "sha256:" + "3" * 64
_ATTEMPT = "sha256:" + "4" * 64
_TARGET_UUID = "0198f11c-6956-74f2-984b-4cfcb1653b88"


def _scratch() -> ClickHouseFailedScratchCleanupReceipt:
    return ClickHouseFailedScratchCleanupReceipt(
        workflow_execution_id="failed-run",
        workflow_execution_binding_sha256=_BINDING,
        operation_id=_OPERATION,
        operation_plan_sha256=_PLAN,
        attempt_binding_sha256=_ATTEMPT,
        fencing_epoch=7,
        target_uuid=_TARGET_UUID,
        relations=(
            ClickHouseScratchRelationAbsence("shadow", "events__shadow", _TARGET_UUID),
            ClickHouseScratchRelationAbsence("staging", "events__staging", None),
        ),
    )


def _resources(*, status: str = "RELEASED") -> MssqlProtectedResourceAllocationClosure:
    reservation_id = "reservation-failed-run"
    return MssqlProtectedResourceAllocationClosure(
        workflow_execution_binding_sha256=_BINDING,
        operation_id=_OPERATION,
        reservation_id=reservation_id,
        allocations=tuple(
            MssqlProtectedResourceAllocation(
                allocation_id=mssql_resource_allocation_id(reservation_id, _OPERATION, kind),
                reservation_id=reservation_id,
                workflow_execution_binding_sha256=_BINDING,
                operation_id=_OPERATION,
                resource_kind=kind,
                amount=1,
                status=status,
            )
            for kind in MSSQL_RESOURCE_KINDS
        ),
    )


class _Cursor:
    def __init__(
        self,
        *,
        ack_row=None,
        cleanup_count=(1, 1),
        guard_row=None,
        prior_terminal_row=None,
    ) -> None:
        self.ack_row = ack_row
        self.cleanup_count = cleanup_count
        self.guard_row = guard_row
        self.prior_terminal_row = prior_terminal_row
        self._row = None
        self.executions = []

    def execute(self, sql: str, *parameters: object):
        self.executions.append((sql, parameters))
        if "sp_getapplock" in sql:
            self._row = (0,)
        elif "semantic_refresh_failed_cleanup_acks" in sql and sql.startswith("SELECT workflow_execution_id"):
            self._row = self.ack_row
        elif "SUM(CASE WHEN ack.status" in sql:
            self._row = self.cleanup_count
        elif sql.startswith("SELECT fencing_epoch"):
            self._row = self.guard_row
        elif sql.startswith("SELECT execution.status"):
            self._row = self.prior_terminal_row
        elif "OUTPUT inserted.cleanup_receipt_sha256" in sql:
            self._row = (parameters[-1],)
        else:
            self._row = None
        return self

    def fetchone(self):
        row = self._row
        self._row = None
        return row

    def fetchall(self):
        return []

    def close(self) -> None:
        return None


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.autocommit = True
        self.cursor_instance = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


class _AckLossCursor(_Cursor):
    def __init__(self, durable_row: list[tuple[object, ...]]) -> None:
        super().__init__()
        self._durable_row = durable_row

    def execute(self, sql: str, *parameters: object):
        super().execute(sql, *parameters)
        if sql.startswith("SELECT workflow_execution_id"):
            if not self._durable_row:
                self._row = None
            else:
                row = self._durable_row[0]
                self._row = (
                    *row[:4],
                    str(row[4]).lower() if "LOWER(CONVERT(char(36), target_uuid))" in sql else row[4],
                    *row[5:],
                )
        elif sql.startswith("INSERT INTO"):
            values = (*parameters[2:], "COMPLETE")
            self._durable_row[:] = [(*values[:4], str(values[4]).upper(), *values[5:])]
        return self


class _CommitLossConnection(_Connection):
    def commit(self) -> None:
        super().commit()
        raise OSError("acknowledgement lost after durable commit")


def test_cleanup_ack_binds_exact_scratch_and_released_allocation_proofs() -> None:
    ack = MssqlFailedPrecommitCleanupAck.build(scratch=_scratch(), resources=_resources())

    assert ack.status == "COMPLETE"
    assert ack.cleanup_receipt_sha256.startswith("sha256:")
    assert ack.resources.resource_allocation_closure_sha256.startswith("sha256:")
    with pytest.raises(ValueError, match="RELEASED"):
        MssqlFailedPrecommitCleanupAck.build(
            scratch=_scratch(),
            resources=_resources(status="RESERVED"),
        )


def test_cleanup_ack_store_is_create_only_after_protected_state_recheck() -> None:
    connection = _Connection(_Cursor())
    store = MssqlSemanticRefreshFailedPrecommitCleanupAckStore(lambda: connection)
    store._require_failed_authority = lambda _cursor, _ack: object()
    store._require_allocations = lambda _cursor, _resources, _bundle: None

    ack = store.persist_exact(scratch=_scratch(), resources=_resources())

    assert ack.status == "COMPLETE"
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert any(sql.startswith("INSERT INTO") for sql, _ in connection.cursor_instance.executions)


def test_cleanup_ack_replays_exactly_after_commit_acknowledgement_loss() -> None:
    durable_row: list[tuple[object, ...]] = []
    first = _CommitLossConnection(_AckLossCursor(durable_row))
    second = _Connection(_AckLossCursor(durable_row))
    connections = iter((first, second))
    store = MssqlSemanticRefreshFailedPrecommitCleanupAckStore(lambda: next(connections))
    store._require_failed_authority = lambda _cursor, _ack: object()
    store._require_allocations = lambda _cursor, _resources, _bundle: None

    with pytest.raises(SemanticRefreshMssqlCleanupAckError, match="persistence failed"):
        store.persist_exact(scratch=_scratch(), resources=_resources())

    replay = store.persist_exact(scratch=_scratch(), resources=_resources())

    assert replay == MssqlFailedPrecommitCleanupAck.build(scratch=_scratch(), resources=_resources())
    assert second.commits == 1
    assert not any(sql.startswith("INSERT INTO") for sql, _ in second.cursor_instance.executions)
    assert any("LOWER(CONVERT(char(36), target_uuid))" in sql for sql, _ in second.cursor_instance.executions)


def test_successor_gate_rejects_missing_cleanup_ack_and_accepts_exact_closure() -> None:
    rejected = _Cursor(cleanup_count=(1, 0))
    state = MssqlSemanticRefreshStateAdapter(lambda: _Connection(rejected))
    with pytest.raises(SemanticRefreshMssqlStateConflict, match="cleanup acknowledgement"):
        state._require_failed_cleanup_closure(rejected, "failed-run")

    accepted = _Cursor(cleanup_count=(1, 1))
    state._require_failed_cleanup_closure(accepted, "failed-run")


def test_cleanup_ack_digest_rejects_cross_wired_resource_operation() -> None:
    resources = _resources()

    with pytest.raises(ValueError, match="identity"):
        changed = replace(resources, operation_id="sha256:" + "f" * 64)
        MssqlFailedPrecommitCleanupAck.build(scratch=_scratch(), resources=changed)


def test_released_failed_target_guard_requires_cleanup_ack_before_reacquisition() -> None:
    guard = (7, "old-owner", "failed-run", _OPERATION, "RELEASED")
    state = MssqlSemanticRefreshStateAdapter(lambda: None)  # type: ignore[arg-type,return-value]
    missing = _Cursor(
        guard_row=guard,
        prior_terminal_row=("FAILED_PRE_COMMIT", "FAILED_PRE_COMMIT", None, None),
    )

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="cleanup acknowledgement"):
        state.lock_admissible_guard_epoch(missing, "mssql://target")

    complete = _Cursor(
        guard_row=guard,
        prior_terminal_row=(
            "FAILED_PRE_COMMIT",
            "FAILED_PRE_COMMIT",
            "COMPLETE",
            "sha256:" + "a" * 64,
        ),
    )
    assert state.lock_admissible_guard_epoch(complete, "mssql://target") == 7


def test_released_dependency_guard_does_not_require_target_cleanup_ack() -> None:
    cursor = _Cursor(guard_row=(3, "old-owner", "failed-run", None, "RELEASED"))
    state = MssqlSemanticRefreshStateAdapter(lambda: None)  # type: ignore[arg-type,return-value]

    assert state.lock_admissible_guard_epoch(cursor, "dependency://source") == 3
    assert not any(sql.startswith("SELECT execution.status") for sql, _ in cursor.executions)


def test_released_failed_target_guard_rejects_noncanonical_cleanup_digest() -> None:
    cursor = _Cursor(
        guard_row=(7, "old-owner", "failed-run", _OPERATION, "RELEASED"),
        prior_terminal_row=(
            "FAILED_PRE_COMMIT",
            "FAILED_PRE_COMMIT",
            "COMPLETE",
            "sha256:" + "A" * 64,
        ),
    )
    state = MssqlSemanticRefreshStateAdapter(lambda: None)  # type: ignore[arg-type,return-value]

    with pytest.raises(SemanticRefreshMssqlStateConflict, match="digest is invalid"):
        state.lock_admissible_guard_epoch(cursor, "mssql://target")
