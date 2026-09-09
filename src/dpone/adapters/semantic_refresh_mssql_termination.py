"""SQL Server create-only adapter for Kubernetes termination receipts."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import datetime
from typing import Any, Protocol

from dpone.ports.semantic_refresh_termination import AttemptTerminationReceipt

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshTerminationConflict(RuntimeError):
    """Raised when an attempt already has different termination evidence."""


class MssqlSemanticRefreshTerminationAdapter:
    """Conditionally insert a receipt observed by a protected controller."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if not isinstance(control_schema, str) or _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._table = f"[{control_schema}].[semantic_refresh_attempt_terminations]"

    def store(self, receipt: AttemptTerminationReceipt) -> None:
        """Create once under a transaction-owned app lock; never update."""

        if not isinstance(receipt, AttemptTerminationReceipt):
            raise TypeError("receipt must be trusted termination evidence")
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            cursor.execute(
                """
DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @dpone_lock_result;
""".strip(),
                f"dpone:semantic-refresh:termination:{receipt.attempt_binding_sha256}",
            )
            lock = cursor.fetchone()
            if lock is None or not isinstance(lock[0], int) or isinstance(lock[0], bool) or lock[0] < 0:
                raise SemanticRefreshTerminationConflict("termination receipt lock was not acquired")
            cursor.execute(
                f"""
SELECT termination_receipt_sha256
FROM {self._table} WITH (UPDLOCK, HOLDLOCK)
WHERE attempt_binding_sha256 = ?;
""".strip(),
                receipt.attempt_binding_sha256,
            )
            existing = cursor.fetchone()
            if existing is not None:
                if tuple(existing) != (receipt.termination_receipt_sha256,):
                    raise SemanticRefreshTerminationConflict("termination receipt identity conflict")
                connection.commit()
                return
            cursor.execute(
                f"""
INSERT INTO {self._table} (
    attempt_binding_sha256, workflow_execution_id,
    workflow_execution_binding_sha256, operation_ids_json, operation_set_sha256,
    dag_id, run_id, task_id, map_index, try_number,
    cluster_id, namespace, pod_name, pod_uid, pod_resource_version,
    terminal_phase, container_terminations_json, observed_at_utc,
    observer_authority, observer_policy_sha256, observer_attestation_sha256,
    observer_signature_sha256, verification_status, termination_receipt_sha256
)
OUTPUT inserted.termination_receipt_sha256
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
""".strip(),
                receipt.attempt_binding_sha256,
                receipt.workflow_execution_id,
                receipt.workflow_execution_binding_sha256,
                json.dumps(list(receipt.operation_ids), separators=(",", ":")),
                receipt.operation_set_sha256,
                receipt.dag_id,
                receipt.run_id,
                receipt.task_id,
                receipt.map_index,
                receipt.try_number,
                receipt.cluster_id,
                receipt.namespace,
                receipt.pod_name,
                receipt.pod_uid,
                receipt.pod_resource_version,
                receipt.terminal_phase,
                json.dumps(
                    [item.to_dict() for item in receipt.container_terminations],
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                datetime.fromisoformat(receipt.observed_at.replace("Z", "+00:00")).replace(tzinfo=None),
                receipt.observer_authority,
                receipt.observer_policy_sha256,
                receipt.observer_attestation_sha256,
                receipt.observer_signature_sha256,
                receipt.verification_status,
                receipt.termination_receipt_sha256,
            )
            created = cursor.fetchone()
            if created is None or tuple(created) != (receipt.termination_receipt_sha256,):
                raise SemanticRefreshTerminationConflict("termination receipt was not created")
            connection.commit()
        except Exception:
            if connection is not None:
                try:
                    connection.rollback()
                except Exception:
                    pass
            raise
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


__all__ = [
    "MssqlSemanticRefreshTerminationAdapter",
    "SemanticRefreshTerminationConflict",
]
