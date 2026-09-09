"""Immutable MSSQL worker-pack and canonical run-binding authority."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_activation_identity import (
    activation_receipt_is_exact,
)
from dpone.adapters.semantic_refresh_mssql_worker_pack_identity import (
    mssql_worker_pack_projection_from_storage,
)
from dpone.ports.semantic_refresh_mssql_activation import (
    MssqlWorkerRunBindingAuthority,
    mssql_run_guard_closure_from_storage,
)
from dpone.ports.semantic_refresh_mssql_admission_authority import (
    compose_admission_from_record,
)
from dpone.ports.semantic_refresh_mssql_authority_codec import canonical_authority_record

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshMssqlWorkerRunBindingAuthorityError(RuntimeError):
    """Raised when immutable worker pack/canonical binding cannot be proven."""


class MssqlWorkerRunBindingQueries:
    """Load immutable worker pack and canonical authority on an owner transaction."""

    def __init__(self, control_schema: str = "dpone_control") -> None:
        if not isinstance(control_schema, str) or _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._control_schema = control_schema

    def load(
        self,
        cursor: _Cursor,
        workflow_plan_sha256: str,
        workflow_execution_id: str,
    ) -> tuple[MssqlWorkerRunBindingAuthority, Any, Any]:
        """Authenticate one pack/canonical binding in the current snapshot."""

        pack = self._pack(cursor, workflow_plan_sha256, workflow_execution_id)
        record = self._authority(cursor, workflow_execution_id, str(pack[3]))
        bundle, request = compose_admission_from_record(record)
        guard_closure = mssql_run_guard_closure_from_storage(
            run_guard_closure_sha256=str(pack[6]),
            workflow_guard_resource_id=str(pack[7]),
            resource_guard_ids_json=str(pack[8]),
        )
        try:
            projection_identity = mssql_worker_pack_projection_from_storage(
                pack_fingerprint=pack[0],
                activation_authority_receipt_sha256=pack[1],
                authority_store_ref=pack[2],
                plan_bundle_sha256=pack[4],
                workflow_plan_sha256=workflow_plan_sha256,
                run_execution_bundle_sha256=pack[5],
                run_guard_closure_sha256=pack[6],
                static_projection_identity_json=pack[9],
            )
        except (TypeError, ValueError) as exc:
            raise SemanticRefreshMssqlWorkerRunBindingAuthorityError(
                "worker pack identity differs from its protected projection"
            ) from exc
        self._require_activation_receipt(cursor, pack, projection_identity)
        if (
            bundle.workflow_plan.workflow_plan_sha256 != workflow_plan_sha256
            or bundle.workflow_execution_id != workflow_execution_id
            or bundle.workflow_guard.resource_id != guard_closure.workflow_guard_resource_id
            or tuple(item.resource_id for item in bundle.resource_guards) != guard_closure.resource_guard_ids
        ):
            raise SemanticRefreshMssqlWorkerRunBindingAuthorityError(
                "canonical worker authority differs from run locator"
            )
        return (
            MssqlWorkerRunBindingAuthority(
                record=record,
                workflow_plan_sha256=workflow_plan_sha256,
                pack_fingerprint=str(pack[0]),
                activation_authority_receipt_sha256=str(pack[1]),
                authority_store_ref=str(pack[2]),
                plan_bundle_sha256=str(pack[4]),
                run_execution_bundle_sha256=str(pack[5]),
                run_guard_closure=guard_closure,
                projection_identity=projection_identity,
            ),
            bundle,
            request,
        )

    def _require_activation_receipt(self, cursor: _Cursor, pack, projection_identity) -> None:
        if not activation_receipt_is_exact(
            cursor,
            activation_authority_table=self._table("semantic_refresh_activation_authorities"),
            deployment_id=projection_identity.deployment_id,
            release_id=projection_identity.release_id,
            plan_bundle_sha256=str(pack[4]),
            authority_store_ref=str(pack[2]),
            activation_authority_receipt_sha256=str(pack[1]),
        ):
            raise SemanticRefreshMssqlWorkerRunBindingAuthorityError(
                "worker activation receipt authority differs from its protected pack"
            )

    def _pack(
        self,
        cursor: _Cursor,
        workflow_plan_sha256: str,
        workflow_execution_id: str,
    ) -> tuple[Any, ...]:
        cursor.execute(
            f"""
SELECT pack_fingerprint, activation_authority_receipt_sha256, authority_store_ref,
       workflow_execution_binding_sha256, plan_bundle_sha256,
       run_execution_bundle_sha256, run_guard_closure_sha256,
       workflow_guard_resource_id, resource_guard_ids_json,
       static_projection_identity_json, status,
       workflow_execution_id, workflow_plan_sha256
FROM {self._table("semantic_refresh_activated_packs")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_plan_sha256 COLLATE Latin1_General_100_BIN2
          = CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2
  AND workflow_execution_id COLLATE Latin1_General_100_BIN2
          = CONVERT(nvarchar(512), ?) COLLATE Latin1_General_100_BIN2;
""".strip(),
            workflow_plan_sha256,
            workflow_execution_id,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if (
            len(rows) != 1
            or rows[0][10] != "ACTIVE"
            or rows[0][11] != workflow_execution_id
            or rows[0][12] != workflow_plan_sha256
        ):
            raise SemanticRefreshMssqlWorkerRunBindingAuthorityError(
                "worker activated-pack authority differs or is absent, ambiguous, or inactive"
            )
        return rows[0]

    def _authority(
        self,
        cursor: _Cursor,
        workflow_execution_id: str,
        expected_binding_sha256: str,
    ):
        cursor.execute(
            f"""
SELECT workflow_execution_binding_sha256, authority_sha256, authority_json, status,
       workflow_execution_id
FROM {self._table("semantic_refresh_canonical_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_id COLLATE Latin1_General_100_BIN2
          = CONVERT(nvarchar(512), ?) COLLATE Latin1_General_100_BIN2;
""".strip(),
            workflow_execution_id,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if (
            len(rows) != 1
            or rows[0][0] != expected_binding_sha256
            or rows[0][3] != "ACTIVE"
            or rows[0][4] != workflow_execution_id
        ):
            raise SemanticRefreshMssqlWorkerRunBindingAuthorityError(
                "worker canonical authority is absent, ambiguous, or inactive"
            )
        return canonical_authority_record(
            workflow_execution_binding_sha256=str(rows[0][0]),
            workflow_execution_id=workflow_execution_id,
            authority_sha256=str(rows[0][1]),
            authority_json=str(rows[0][2]),
            status=str(rows[0][3]),
        )

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


class MssqlSemanticRefreshWorkerRunBindingAuthority:
    """Resolve immutable worker binding for active work or terminal replay."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        self._connection_factory = connection_factory
        self._queries = MssqlWorkerRunBindingQueries(control_schema)

    def locate_binding(
        self,
        workflow_plan_sha256: str,
        workflow_execution_id: str,
    ) -> MssqlWorkerRunBindingAuthority:
        """Return immutable ACTIVE pack/canonical authority regardless of terminal state."""

        if _DIGEST.fullmatch(workflow_plan_sha256) is None:
            raise ValueError("workflow_plan_sha256 must be a lowercase sha256 digest")
        if not isinstance(workflow_execution_id, str) or not workflow_execution_id.strip():
            raise ValueError("workflow_execution_id must be non-empty text")
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            result, _bundle, _request = self._queries.load(
                cursor,
                workflow_plan_sha256,
                workflow_execution_id,
            )
            connection.commit()
            return result
        except SemanticRefreshMssqlWorkerRunBindingAuthorityError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlWorkerRunBindingAuthorityError(
                "protected MSSQL worker run binding lookup failed"
            ) from exc
        finally:
            _close(cursor)
            _close(connection)


def _rollback(connection: _Connection | None) -> None:
    if connection is not None:
        try:
            connection.rollback()
        except Exception:
            pass


def _close(resource: object | None) -> None:
    if resource is not None:
        try:
            resource.close()  # type: ignore[attr-defined]
        except Exception:
            pass


__all__ = [
    "MssqlSemanticRefreshWorkerRunBindingAuthority",
    "MssqlWorkerRunBindingQueries",
    "SemanticRefreshMssqlWorkerRunBindingAuthorityError",
]
