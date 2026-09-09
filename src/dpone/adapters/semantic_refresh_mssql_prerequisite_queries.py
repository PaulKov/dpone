"""Locked queries for current MSSQL activation prerequisite authority."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_activation_identity import (
    activation_receipt_is_exact,
)
from dpone.adapters.semantic_refresh_mssql_worker_pack_identity import (
    mssql_worker_pack_projection_from_storage,
)
from dpone.ports.semantic_refresh_mssql_prerequisite_validation import (
    validate_route_authority_json,
    validate_runtime_authority_json,
)


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...


class _RuntimeAssurance(Protocol):
    @property
    def assurance_kind(self) -> str: ...

    @property
    def receipt_sha256(self) -> str: ...

    @property
    def subject_json(self) -> str: ...


class _PrerequisiteAuthority(Protocol):
    @property
    def release_id(self) -> str: ...

    @property
    def deployment_id(self) -> str: ...

    @property
    def model_unique_id(self) -> str: ...

    @property
    def route_certification_receipt_sha256(self) -> str: ...

    @property
    def runtime_assurances(self) -> tuple[_RuntimeAssurance, ...]: ...


class MssqlPrerequisiteAuthorityConflict(RuntimeError):
    """Raised when locked prerequisite authority is absent or differs."""


class MssqlPrerequisiteAuthorityQueries:
    """Validate exact protected receipt rows inside an owner transaction."""

    def __init__(self, control_schema: str) -> None:
        self._control_schema = control_schema

    def require_current(
        self,
        cursor: _Cursor,
        claims: tuple[_PrerequisiteAuthority, ...],
    ) -> None:
        """Lock and validate exact current receipt authority for every model."""

        for claim in claims:
            cursor.execute(
                f"""
SELECT COUNT_BIG(*),
       COALESCE(SUM(CASE
           WHEN deployment_id COLLATE Latin1_General_100_BIN2
                    = CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2
            AND release_id COLLATE Latin1_General_100_BIN2
                    = CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2
            AND status COLLATE Latin1_General_100_BIN2
                    = N'ACTIVE' COLLATE Latin1_General_100_BIN2
           THEN CONVERT(bigint, 1) ELSE CONVERT(bigint, 0)
       END), 0)
FROM {self._table("semantic_refresh_activation_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE deployment_id COLLATE Latin1_General_100_BIN2
          = CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2;
""".strip(),
                claim.deployment_id,
                claim.release_id,
                claim.deployment_id,
            )
            activation_closure = _row(cursor)
            if (
                activation_closure is None
                or len(activation_closure) != 2
                or isinstance(activation_closure[0], bool)
                or not isinstance(activation_closure[0], int)
                or activation_closure[0] <= 0
                or activation_closure[1] != activation_closure[0]
            ):
                raise MssqlPrerequisiteAuthorityConflict("activation authority is absent or inactive")
            self._require_route(cursor, claim)
            self._require_runtime(cursor, claim)

    def require_current_for_admission(
        self,
        cursor: _Cursor,
        claims: tuple[_PrerequisiteAuthority, ...],
        *,
        workflow_execution_binding_sha256: str,
        workflow_execution_id: str,
        workflow_plan_sha256: str,
    ) -> None:
        """Bind prerequisites to the exact immutable worker plan receipt."""

        cursor.execute(
            f"""
SELECT pack_fingerprint, activation_authority_receipt_sha256, authority_store_ref,
       workflow_execution_id, workflow_plan_sha256, plan_bundle_sha256,
       run_execution_bundle_sha256, run_guard_closure_sha256,
       static_projection_identity_json, status
FROM {self._table("semantic_refresh_activated_packs")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
            workflow_execution_binding_sha256,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if len(rows) != 1 or len(rows[0]) != 10:
            raise MssqlPrerequisiteAuthorityConflict("exact activated worker plan is absent or ambiguous")
        pack = rows[0]
        try:
            projection = mssql_worker_pack_projection_from_storage(
                pack_fingerprint=pack[0],
                activation_authority_receipt_sha256=pack[1],
                authority_store_ref=pack[2],
                plan_bundle_sha256=pack[5],
                workflow_plan_sha256=workflow_plan_sha256,
                run_execution_bundle_sha256=pack[6],
                run_guard_closure_sha256=pack[7],
                static_projection_identity_json=pack[8],
            )
        except (TypeError, ValueError) as exc:
            raise MssqlPrerequisiteAuthorityConflict("activated worker plan identity is invalid") from exc
        releases = {claim.release_id for claim in claims}
        deployments = {claim.deployment_id for claim in claims}
        if (
            not claims
            or pack[9] != "ACTIVE"
            or pack[3] != workflow_execution_id
            or pack[4] != workflow_plan_sha256
            or releases != {projection.release_id}
            or deployments != {projection.deployment_id}
            or not activation_receipt_is_exact(
                cursor,
                activation_authority_table=self._table("semantic_refresh_activation_authorities"),
                deployment_id=projection.deployment_id,
                release_id=projection.release_id,
                plan_bundle_sha256=projection.plan_bundle_sha256,
                authority_store_ref=str(pack[2]),
                activation_authority_receipt_sha256=str(pack[1]),
            )
        ):
            raise MssqlPrerequisiteAuthorityConflict("exact activation authority receipt differs")
        self.require_current(cursor, claims)

    def _require_route(self, cursor: _Cursor, claim: _PrerequisiteAuthority) -> None:
        cursor.execute(
            f"""
SELECT route_certification_receipt_sha256, receipt_json, status
FROM {self._table("semantic_refresh_route_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE deployment_id = ? AND route_certification_receipt_sha256 = ?
  AND status = N'PASS'
  AND effective_from <= SYSUTCDATETIME() AND SYSUTCDATETIME() < expires_at
  AND revoked_receipt_sha256 IS NULL;
""".strip(),
            claim.deployment_id,
            claim.route_certification_receipt_sha256,
        )
        route = _row(cursor)
        if route is None or route[0] != claim.route_certification_receipt_sha256 or route[2] != "PASS":
            raise MssqlPrerequisiteAuthorityConflict("route certification is not current and protected")
        try:
            validate_route_authority_json(str(route[1]), claim.route_certification_receipt_sha256)
        except ValueError as exc:
            raise MssqlPrerequisiteAuthorityConflict("route certification document differs") from exc

    def _require_runtime(self, cursor: _Cursor, claim: _PrerequisiteAuthority) -> None:
        for assurance in claim.runtime_assurances:
            cursor.execute(
                f"""
SELECT runtime_assurance_receipt_sha256, subject_json, receipt_json, status
FROM {self._table("semantic_refresh_runtime_assurance_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE deployment_id = ? AND model_unique_id = ? AND assurance_kind = ?
  AND runtime_assurance_receipt_sha256 = ? AND subject_json = ?
  AND status = N'CERTIFIED'
  AND effective_from <= SYSUTCDATETIME() AND SYSUTCDATETIME() < expires_at
  AND revoked_receipt_sha256 IS NULL;
""".strip(),
                claim.deployment_id,
                claim.model_unique_id,
                assurance.assurance_kind,
                assurance.receipt_sha256,
                assurance.subject_json,
            )
            runtime = _row(cursor)
            if (
                runtime is None
                or runtime[:2] != (assurance.receipt_sha256, assurance.subject_json)
                or runtime[3] != "CERTIFIED"
            ):
                raise MssqlPrerequisiteAuthorityConflict("runtime assurance is not current, exact, and protected")
            try:
                validate_runtime_authority_json(
                    str(runtime[2]),
                    expected_digest=assurance.receipt_sha256,
                    expected_subject_json=assurance.subject_json,
                )
            except ValueError as exc:
                raise MssqlPrerequisiteAuthorityConflict("runtime assurance document differs") from exc

    def _table(self, table_name: str) -> str:
        return f"[{self._control_schema}].[{table_name}]"


def _row(cursor: _Cursor) -> tuple[Any, ...] | None:
    value = cursor.fetchone()
    return None if value is None else tuple(value)


__all__ = ["MssqlPrerequisiteAuthorityConflict", "MssqlPrerequisiteAuthorityQueries"]
