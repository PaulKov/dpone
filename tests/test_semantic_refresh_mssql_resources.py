"""Protected MSSQL resource-ledger capability tests."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.adapters.semantic_refresh_mssql_resource_authority import (
    MssqlSemanticRefreshProtectedResourceLedger,
    SemanticRefreshMssqlProtectedResourceError,
)
from dpone.ports.semantic_refresh_mssql_admission_authority import compose_admission
from dpone.ports.semantic_refresh_mssql_authority_records import (
    MssqlProtectedArtifactAuthority,
)
from dpone.ports.semantic_refresh_mssql_resources import (
    MSSQL_RESOURCE_KINDS,
    mssql_resource_allocation_id,
)
from tests.test_semantic_refresh_mssql_authority import _bundle

_BINDING = "sha256:" + "1" * 64
_OPERATION = "sha256:" + "2" * 64


class _Cursor:
    def __init__(self, allocations: dict[str, tuple[object, ...]]) -> None:
        self.allocations = allocations
        self.rowcount = 1
        self._row = None
        self._rows = []

    def execute(self, sql: str, *parameters: object):
        self._row = None
        self._rows = []
        self.rowcount = 1
        if sql.startswith("SELECT reservation_id, resource_kind"):
            self._row = self.allocations.get(str(parameters[0]))
        elif sql.startswith("SELECT allocation_id, reservation_id"):
            reservation_id = str(parameters[0])
            self._rows = sorted(
                (allocation_id, *row) for allocation_id, row in self.allocations.items() if row[0] == reservation_id
            )
        elif sql.startswith("INSERT INTO") and "resource_allocations" in sql:
            allocation_id, reservation_id, kind, amount = parameters
            self.allocations[str(allocation_id)] = (reservation_id, kind, amount, "RESERVED")
            self._row = (allocation_id,)
        elif sql.startswith("UPDATE") and "resource_allocations" in sql:
            allocation_id = str(parameters[0])
            reservation_id, kind, amount, _status = self.allocations[allocation_id]
            self.allocations[allocation_id] = (reservation_id, kind, amount, "RELEASED")
            self._row = (allocation_id,)
        elif sql.startswith("UPDATE") and "semantic_refresh_reservations" in sql:
            self._row = (parameters[2],)
        return self

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows

    def close(self) -> None:
        return None


class _NoCountCursor:
    """Model a driver that does not expose DML counts but does return OUTPUT rows."""

    rowcount = -1

    def __init__(self) -> None:
        self.sql = ""
        self.parameters: tuple[object, ...] = ()

    def execute(self, sql: str, *parameters: object):
        self.sql = sql
        self.parameters = parameters
        return self

    def fetchone(self):
        return (self.parameters[2],) if "OUTPUT inserted.reservation_id" in self.sql else None


class _Connection:
    def __init__(self, allocations: dict[str, tuple[object, ...]]) -> None:
        self.autocommit = True
        self.cursor_instance = _Cursor(allocations)
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


def _adapter():
    allocations: dict[str, tuple[object, ...]] = {}
    connections = []

    def factory():
        connection = _Connection(allocations)
        connections.append(connection)
        return connection

    adapter = MssqlSemanticRefreshProtectedResourceLedger(
        factory,
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC),
    )
    bundle = SimpleNamespace(
        execution_binding=SimpleNamespace(workflow_execution_binding_sha256=_BINDING),
        operation_plans=(
            SimpleNamespace(
                model_unique_id="model.test.events",
                operation_id=_OPERATION,
            ),
        ),
    )
    resource = SimpleNamespace(
        model_unique_id="model.test.events",
        resource_policy=SimpleNamespace(
            max_clickhouse_staging_bytes=20,
            max_clickhouse_retained_backup_bytes=40,
            max_clickhouse_shadow_bytes=30,
        ),
        artifact_authority=MssqlProtectedArtifactAuthority(
            provider="s3",
            provider_profile="local-minio",
            endpoint_authority_id="https://minio.test",
            bucket_or_container_authority_id="artifacts",
            kms_key_authority_id="local-kms",
            capability_evidence_sha256="sha256:" + "3" * 64,
            writer_scope="semantic-refresh/test",
            artifact_prefix="semantic-refresh/test",
            encryption_policy_sha256="sha256:" + "4" * 64,
            retention_policy_id="30d",
            retention_policy_sha256="sha256:" + "5" * 64,
            retention_days=30,
            retention_issued_at="2026-08-08T00:00:00Z",
            retention_until="2026-09-07T00:00:00Z",
            max_artifact_bytes=10,
        ),
    )
    bundle.model_resources = (resource,)
    request = SimpleNamespace(
        reservation_id="reservation-protected",
        workflow_execution_binding_sha256=_BINDING,
    )
    adapter._authority = lambda _cursor, _binding, _operation: (bundle, request, resource)
    adapter._state = lambda *_args: None
    return adapter, allocations, connections


def test_protected_resource_ledger_derives_exact_five_allocation_closure() -> None:
    adapter, allocations, connections = _adapter()

    assert (
        adapter.reserve_operation(
            workflow_execution_binding_sha256=_BINDING,
            operation_id=_OPERATION,
        )
        is None
    )

    assert tuple(sorted(str(row[1]) for row in allocations.values())) == MSSQL_RESOURCE_KINDS
    assert tuple(sorted(int(row[2]) for row in allocations.values())) == (1, 10, 20, 30, 40)
    assert all(row[3] == "RESERVED" for row in allocations.values())
    assert connections[-1].commits == 1

    adapter._clock = lambda: datetime(2030, 8, 9, tzinfo=UTC)
    assert (
        adapter.release_operation(
            workflow_execution_binding_sha256=_BINDING,
            operation_id=_OPERATION,
        )
        is None
    )
    released = adapter.assert_released(
        workflow_execution_binding_sha256=_BINDING,
        operation_id=_OPERATION,
    )
    assert all(item.status == "RELEASED" for item in released.allocations)


def test_aggregate_budget_cas_uses_output_identity_when_driver_rowcount_is_unavailable() -> None:
    adapter, _allocations, _connections = _adapter()
    cursor = _NoCountCursor()

    adapter._ledger._increase(cursor, "reservation-protected", "shadow_bytes", 30)

    assert "OUTPUT inserted.reservation_id" in cursor.sql
    assert cursor.parameters == (30, 30, "reservation-protected", 30, 30)


def test_protected_resource_ledger_rejects_missing_canonical_history() -> None:
    adapter, allocations, _connections = _adapter()
    adapter.reserve_operation(
        workflow_execution_binding_sha256=_BINDING,
        operation_id=_OPERATION,
    )
    adapter.release_operation(
        workflow_execution_binding_sha256=_BINDING,
        operation_id=_OPERATION,
    )
    del allocations[mssql_resource_allocation_id("reservation-protected", _OPERATION, "shadow_bytes")]

    with pytest.raises(SemanticRefreshMssqlProtectedResourceError, match="missing, extra, or conflicting"):
        adapter.assert_released(
            workflow_execution_binding_sha256=_BINDING,
            operation_id=_OPERATION,
        )


def test_protected_resource_ledger_rejects_extra_noncanonical_allocation() -> None:
    adapter, allocations, _connections = _adapter()
    adapter.reserve_operation(
        workflow_execution_binding_sha256=_BINDING,
        operation_id=_OPERATION,
    )
    adapter.release_operation(
        workflow_execution_binding_sha256=_BINDING,
        operation_id=_OPERATION,
    )
    allocations["sha256:" + "f" * 64] = (
        "reservation-protected",
        "shadow_bytes",
        30,
        "RELEASED",
    )

    with pytest.raises(SemanticRefreshMssqlProtectedResourceError, match="outside the canonical"):
        adapter.assert_released(
            workflow_execution_binding_sha256=_BINDING,
            operation_id=_OPERATION,
        )


def test_resource_allocation_identity_is_operation_and_kind_bound() -> None:
    assert mssql_resource_allocation_id("reservation-protected", _OPERATION, "prepared_models") != (
        mssql_resource_allocation_id("reservation-protected", _OPERATION, "shadow_bytes")
    )


class _StateCursor:
    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row

    def execute(self, _sql: str, *_parameters: object):
        return self

    def fetchall(self):
        return [self.row]


def _state_row(*, failed: bool = False) -> tuple[object, ...]:
    bundle = _bundle()
    request = compose_admission(bundle)
    operation = bundle.operation_plans[0]
    attempt = bundle.attempt_bindings[0]
    budget = request.resource_budget
    return (
        "FAILED_PRE_COMMIT" if failed else "PREPARING",
        "FAILED_PRE_COMMIT" if failed else "PREPARING",
        operation.operation_plan_sha256,
        attempt.attempt_binding_sha256,
        attempt.fencing_epoch,
        bundle.owner_id,
        None,
        "ROLLED_BACK" if failed else None,
        attempt.fencing_epoch,
        bundle.owner_id,
        bundle.workflow_execution_id,
        operation.operation_id,
        operation.operation_plan_sha256,
        attempt.attempt_binding_sha256,
        "RELEASED" if failed else "HELD",
        request.reservation_id,
        request.workflow_id,
        request.workflow_execution_binding_sha256,
        "FAILED_PRE_COMMIT" if failed else "PREPARING",
        budget.max_workflow_prepared_models,
        budget.max_workflow_sealed_extract_bytes,
        budget.max_workflow_clickhouse_staging_bytes,
        budget.max_workflow_shadow_bytes,
        budget.max_workflow_peak_bytes,
    )


def test_resource_state_accepts_failed_terminal_reservation_for_cleanup_release() -> None:
    bundle = _bundle()
    request = compose_admission(bundle)
    adapter = MssqlSemanticRefreshProtectedResourceLedger(
        lambda: None,  # type: ignore[arg-type,return-value]
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC),
    )

    adapter._state(
        _StateCursor(_state_row(failed=True)),
        bundle,
        request,
        bundle.operation_plans[0].operation_id,
        "release",
    )


@pytest.mark.parametrize("index", [9, 17, 20])
def test_resource_state_rejects_wrong_guard_or_reservation_authority(index: int) -> None:
    bundle = _bundle()
    request = compose_admission(bundle)
    row = list(_state_row())
    row[index] = "wrong" if index != 20 else int(row[index]) + 1
    adapter = MssqlSemanticRefreshProtectedResourceLedger(
        lambda: None,  # type: ignore[arg-type,return-value]
        clock=lambda: datetime(2026, 8, 9, tzinfo=UTC),
    )

    with pytest.raises(SemanticRefreshMssqlProtectedResourceError):
        adapter._state(
            _StateCursor(tuple(row)),
            bundle,
            request,
            bundle.operation_plans[0].operation_id,
            "reserve",
        )
