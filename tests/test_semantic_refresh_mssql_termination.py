"""SQL Server termination receipt persistence regressions."""

from __future__ import annotations

from typing import Any

from dpone.adapters.semantic_refresh_mssql_termination import (
    MssqlSemanticRefreshTerminationAdapter,
)
from dpone.contracts.semantic_refresh_container_termination import (
    SemanticRefreshContainerTermination,
)
from dpone.contracts.semantic_refresh_termination_receipt import (
    SemanticRefreshTrustedAttemptTerminationReceipt,
)

_A = "sha256:" + "a" * 64
_B = "sha256:" + "b" * 64
_C = "sha256:" + "c" * 64


def _receipt() -> SemanticRefreshTrustedAttemptTerminationReceipt:
    return SemanticRefreshTrustedAttemptTerminationReceipt.build(
        workflow_execution_id="scheduled__2026-08-10",
        workflow_execution_binding_sha256=_A,
        operation_ids=(_B,),
        attempt_binding_sha256=_C,
        dag_id="semantic_refresh_daily",
        run_id="scheduled__2026-08-10",
        task_id="semantic_refresh__events",
        map_index=-1,
        try_number=1,
        cluster_id="local",
        namespace="airflow",
        pod_name="semantic-refresh-events",
        pod_uid="0198f11c-6956-74f2-984b-4cfcb1653b87",
        pod_resource_version="103421",
        terminal_phase="Failed",
        container_terminations=(
            SemanticRefreshContainerTermination(
                name="base",
                container_id="containerd://abc",
                reason="Error",
                finished_at="2026-08-10T00:00:00Z",
                exit_code=1,
            ),
        ),
        observed_at="2026-08-10T00:00:01Z",
        observer_authority="kubernetes-controller/local",
        observer_policy_sha256=_A,
        observer_attestation_sha256=_B,
        observer_signature_sha256=_C,
    )


class _Cursor:
    def __init__(self, receipt_sha256: str) -> None:
        self._receipt_sha256 = receipt_sha256
        self._row: tuple[Any, ...] | None = None
        self.executions: list[str] = []

    def execute(self, sql: str, *parameters: object) -> _Cursor:
        del parameters
        self.executions.append(sql)
        if "sp_getapplock" in sql:
            self._row = (0,)
        elif sql.startswith("SELECT termination_receipt_sha256"):
            self._row = None
        elif "INSERT INTO" in sql:
            self._row = (self._receipt_sha256,)
        else:
            self._row = None
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        row = self._row
        self._row = None
        return row

    def close(self) -> None:
        return None


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.autocommit = True
        self._cursor = cursor
        self.committed = False

    def cursor(self) -> _Cursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


def test_store_uses_inserted_identity_instead_of_driver_rowcount() -> None:
    receipt = _receipt()
    cursor = _Cursor(receipt.termination_receipt_sha256)
    connection = _Connection(cursor)

    MssqlSemanticRefreshTerminationAdapter(lambda: connection).store(receipt)

    assert connection.committed is True
    insert = next(sql for sql in cursor.executions if "INSERT INTO" in sql)
    assert "OUTPUT inserted.termination_receipt_sha256" in insert
