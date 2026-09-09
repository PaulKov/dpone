"""Protected engine-quiescence observer for a later Airflow task try."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from dpone.contracts.semantic_refresh_termination_receipt import (
    SemanticRefreshTrustedAttemptTerminationReceipt,
)
from dpone.ports.semantic_refresh_attempt_quiescence import (
    ClickHouseAttemptQuiescenceProof,
    SemanticRefreshClickHouseAttemptQuiescencePort,
)
from dpone.ports.semantic_refresh_mssql_authority_models import (
    MssqlCanonicalAdmissionBundle,
)

_UTC = timezone.utc  # noqa: UP017 - package supports Python 3.10.


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...


class _OriginalAttempt(Protocol):
    @property
    def attempt_binding_sha256(self) -> str: ...

    @property
    def dag_run_id(self) -> str: ...

    @property
    def task_id(self) -> str: ...

    @property
    def try_number(self) -> int: ...

    @property
    def pod_uid(self) -> str: ...


class SemanticRefreshMssqlAttemptQuiescenceError(RuntimeError):
    """Raised when original-pod or engine quiescence is not exact."""


@dataclass(frozen=True, slots=True)
class MssqlSemanticRefreshAttemptQuiescenceClosure:
    """Typed exact proof inputs consumed by the durable continuation receipt."""

    termination: SemanticRefreshTrustedAttemptTerminationReceipt
    clickhouse: ClickHouseAttemptQuiescenceProof
    mssql_active_session_count: int = 0
    mssql_guard_lock_status: str = "EXCLUSIVE_ACQUIRED"


class MssqlSemanticRefreshAttemptQuiescenceObserver:
    """Authenticate original termination and prove both engines quiescent."""

    def __init__(
        self,
        *,
        control_schema: str,
        clickhouse: SemanticRefreshClickHouseAttemptQuiescencePort,
    ) -> None:
        self._control_schema = control_schema
        self._clickhouse = clickhouse

    def prove(
        self,
        cursor: _Cursor,
        *,
        bundle: MssqlCanonicalAdmissionBundle,
        operation_id: str,
        original: _OriginalAttempt,
        guard_resource_id: str,
        clickhouse_cluster_authority_id: str,
        observed_at: str,
    ) -> MssqlSemanticRefreshAttemptQuiescenceClosure:
        """Return the closed original-pod, MSSQL, and ClickHouse proof."""

        termination = self._locked_termination_receipt(
            cursor,
            bundle=bundle,
            operation_id=operation_id,
            original=original,
        )
        active_sessions = self._lock_and_require_mssql_quiescence(
            cursor,
            guard_resource_id=guard_resource_id,
            attempt_binding_sha256=original.attempt_binding_sha256,
        )
        clickhouse = self._clickhouse.prove_quiescent(
            workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
            operation_id=operation_id,
            original_attempt_binding_sha256=original.attempt_binding_sha256,
            clickhouse_cluster_authority_id=clickhouse_cluster_authority_id,
            observed_at=observed_at,
        )
        return MssqlSemanticRefreshAttemptQuiescenceClosure(
            termination=termination,
            clickhouse=clickhouse,
            mssql_active_session_count=active_sessions,
        )

    def _locked_termination_receipt(
        self,
        cursor: _Cursor,
        *,
        bundle: MssqlCanonicalAdmissionBundle,
        operation_id: str,
        original: _OriginalAttempt,
    ) -> SemanticRefreshTrustedAttemptTerminationReceipt:
        cursor.execute(
            f"""
SELECT workflow_execution_id, workflow_execution_binding_sha256,
       operation_ids_json, operation_set_sha256, attempt_binding_sha256,
       dag_id, run_id, task_id, map_index, try_number,
       cluster_id, namespace, pod_name, LOWER(CONVERT(char(36), pod_uid)),
       pod_resource_version, terminal_phase, container_terminations_json,
       observed_at_utc, observer_authority, observer_policy_sha256,
       observer_attestation_sha256, observer_signature_sha256,
       verification_status, termination_receipt_sha256
FROM [{self._control_schema}].[semantic_refresh_attempt_terminations] WITH (UPDLOCK, HOLDLOCK)
WHERE attempt_binding_sha256 COLLATE Latin1_General_100_BIN2 = ?;
""".strip(),
            original.attempt_binding_sha256,
        )
        row = cursor.fetchone()
        if row is None:
            raise SemanticRefreshMssqlAttemptQuiescenceError("trusted original attempt termination receipt is absent")
        try:
            receipt = SemanticRefreshTrustedAttemptTerminationReceipt.from_mapping(
                {
                    "schema": "dpone.semantic-refresh-trusted-attempt-termination-receipt.v1",
                    "workflow_execution_id": row[0],
                    "workflow_execution_binding_sha256": row[1],
                    "operation_ids": json.loads(str(row[2])),
                    "operation_set_sha256": row[3],
                    "attempt_binding_sha256": row[4],
                    "dag_id": row[5],
                    "run_id": row[6],
                    "task_id": row[7],
                    "map_index": row[8],
                    "try_number": row[9],
                    "cluster_id": row[10],
                    "namespace": row[11],
                    "pod_name": row[12],
                    "pod_uid": row[13],
                    "pod_resource_version": row[14],
                    "terminal_phase": row[15],
                    "container_terminations": json.loads(str(row[16])),
                    "observed_at": _stored_timestamp(row[17]),
                    "observer_authority": row[18],
                    "observer_policy_sha256": row[19],
                    "observer_attestation_sha256": row[20],
                    "observer_signature_sha256": row[21],
                    "verification_status": row[22],
                    "termination_receipt_sha256": row[23],
                }
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SemanticRefreshMssqlAttemptQuiescenceError(
                "trusted original attempt termination receipt is invalid"
            ) from exc
        if (
            receipt.workflow_execution_id != bundle.workflow_execution_id
            or receipt.workflow_execution_binding_sha256 != bundle.execution_binding.workflow_execution_binding_sha256
            or receipt.operation_ids != (operation_id,)
            or receipt.attempt_binding_sha256 != original.attempt_binding_sha256
            or receipt.run_id != original.dag_run_id
            or receipt.task_id != original.task_id
            or receipt.try_number != original.try_number
            or receipt.pod_uid != original.pod_uid
        ):
            raise SemanticRefreshMssqlAttemptQuiescenceError(
                "trusted termination receipt differs from original attempt"
            )
        return receipt

    @staticmethod
    def _lock_and_require_mssql_quiescence(
        cursor: _Cursor,
        *,
        guard_resource_id: str,
        attempt_binding_sha256: str,
    ) -> int:
        cursor.execute(
            """
DECLARE @dpone_continuation_lock_result int;
EXEC @dpone_continuation_lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
IF COALESCE(HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW SERVER STATE'), 0) <> 1
   AND COALESCE(HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW SERVER PERFORMANCE STATE'), 0) <> 1
    THROW 51028, 'DPONE_SEMANTIC_REFRESH_SESSION_QUIESCENCE_UNVERIFIED', 1;
SELECT @dpone_continuation_lock_result,
       (SELECT COUNT_BIG(*) FROM sys.dm_exec_sessions AS session_state
        WHERE session_state.session_id <> @@SPID
          AND session_state.context_info = HASHBYTES(
              'SHA2_256', CONVERT(varbinary(max), CONVERT(nvarchar(max), ?))
          ));
""".strip(),
            f"dpone:semantic-refresh:{guard_resource_id}",
            attempt_binding_sha256,
        )
        row = cursor.fetchone()
        if (
            row is None
            or isinstance(row[0], bool)
            or not isinstance(row[0], int)
            or row[0] < 0
            or isinstance(row[1], bool)
            or not isinstance(row[1], int)
            or row[1] != 0
        ):
            raise SemanticRefreshMssqlAttemptQuiescenceError(
                "original MSSQL session, transaction, or app lock is not quiescent"
            )
        return int(row[1])


def _stored_timestamp(value: object) -> str:
    if not isinstance(value, datetime):
        raise ValueError("stored termination timestamp is invalid")
    if value.tzinfo is None:
        value = value.replace(tzinfo=_UTC)
    if value.utcoffset() != _UTC.utcoffset(value):
        raise ValueError("stored termination timestamp is not UTC")
    return value.astimezone(_UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


__all__ = [
    "MssqlSemanticRefreshAttemptQuiescenceClosure",
    "MssqlSemanticRefreshAttemptQuiescenceObserver",
    "SemanticRefreshMssqlAttemptQuiescenceError",
]
