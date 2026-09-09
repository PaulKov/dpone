"""Protected MSSQL reader for Kubernetes termination-observation authority."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any, Protocol

from dpone.ports.semantic_refresh_termination import MssqlTerminationObservationAuthority

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


class SemanticRefreshTerminationObservationAuthorityError(RuntimeError):
    """Raised when protected observation authority is absent or malformed."""


class MssqlSemanticRefreshTerminationObservationAuthority:
    """Load one ACTIVE scheduler/cluster observation authority under a lock."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if not isinstance(control_schema, str) or _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._table = f"[{control_schema}].[semantic_refresh_termination_observation_authorities]"

    def load_observation_authority(
        self,
        *,
        workflow_execution_binding_sha256: str,
        attempt_binding_sha256: str,
    ) -> MssqlTerminationObservationAuthority:
        """Return exact protected coordinates or fail closed."""

        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            cursor.execute(
                f"""
SELECT workflow_execution_id, operation_ids_json, operation_set_sha256,
       dag_id, run_id, task_id, map_index, try_number,
       cluster_id, namespace, pod_name, pod_uid,
       observer_authority, observer_policy_sha256, observer_attestation_sha256,
       observation_authority_sha256, status
FROM {self._table} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ? AND attempt_binding_sha256 = ?;
""".strip(),
                workflow_execution_binding_sha256,
                attempt_binding_sha256,
            )
            row = cursor.fetchone()
            if row is None or cursor.fetchone() is not None:
                raise SemanticRefreshTerminationObservationAuthorityError(
                    "termination observation authority is absent or ambiguous"
                )
            result = _authority(row, workflow_execution_binding_sha256, attempt_binding_sha256)
            connection.commit()
            return result
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, SemanticRefreshTerminationObservationAuthorityError):
                raise
            raise SemanticRefreshTerminationObservationAuthorityError(
                "termination observation authority read failed"
            ) from exc
        finally:
            cursor.close()
            connection.close()


def _authority(
    row: tuple[Any, ...],
    workflow_execution_binding_sha256: str,
    attempt_binding_sha256: str,
) -> MssqlTerminationObservationAuthority:
    try:
        operation_ids = json.loads(str(row[1]))
    except json.JSONDecodeError as exc:
        raise SemanticRefreshTerminationObservationAuthorityError("operation_ids_json is invalid") from exc
    if (
        not isinstance(operation_ids, list)
        or not operation_ids
        or any(not isinstance(item, str) for item in operation_ids)
        or operation_ids != sorted(set(operation_ids))
    ):
        raise SemanticRefreshTerminationObservationAuthorityError("operation_ids_json is not canonical")
    return MssqlTerminationObservationAuthority(
        workflow_execution_id=str(row[0]),
        workflow_execution_binding_sha256=workflow_execution_binding_sha256,
        operation_ids=tuple(operation_ids),
        operation_set_sha256=str(row[2]),
        attempt_binding_sha256=attempt_binding_sha256,
        dag_id=str(row[3]),
        run_id=str(row[4]),
        task_id=str(row[5]),
        map_index=_integer(row[6], "map_index", minimum=-1),
        try_number=_integer(row[7], "try_number", minimum=1),
        cluster_id=str(row[8]),
        namespace=str(row[9]),
        pod_name=str(row[10]),
        pod_uid=str(row[11]),
        observer_authority=str(row[12]),
        observer_policy_sha256=str(row[13]),
        observer_attestation_sha256=str(row[14]),
        observation_authority_sha256=str(row[15]),
        status=str(row[16]),
    )


def _integer(value: object, field_name: str, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise SemanticRefreshTerminationObservationAuthorityError(f"{field_name} is invalid")
    return value


__all__ = [
    "MssqlSemanticRefreshTerminationObservationAuthority",
    "SemanticRefreshTerminationObservationAuthorityError",
]
