"""Atomic owned CREATE on one granted, continuously locked coordinator session.

The one-shot command never retries COMMIT or issues a compensating DROP. Validated
post-commit evidence is retained before any later check/cleanup can fail. Neither
this evidence nor a closed connection proves remote settlement or bulk quiescence.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from dpone.adapters.mssql_sqlclient_stage_catalog_sql import COLUMNS_SQL as _COLUMNS
from dpone.adapters.mssql_sqlclient_stage_catalog_sql import OBJECT_SQL as _OBJECT
from dpone.adapters.mssql_tds_coordinator_sql import TdsCoordinatorSql, _one
from dpone.contracts.mssql_tds_api import (
    TdsCoordinatorCommand,
    TdsCreateEvidence,
    TdsCreateObservedColumn,
    TdsCreateRequest,
    TdsCreateType,
    TdsRemoteSessionIdentity,
    authority_digest,
    create_command_digest,
    identifier,
    parse_stage_columns,
    parse_stage_object,
)
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorGrant,
    coordinator_grant_digest,
    coordinator_identity_digest,
)


class TdsCreateUnknown(RuntimeError):
    """Uncertain outcome retains any validated evidence; never implies rollback."""

    def __init__(self, evidence: TdsCreateEvidence | None) -> None:
        self.evidence = evidence
        super().__init__("mssql_native.tds_create_unknown")


def _quote(name: str) -> str:
    identifier(name)
    return "[" + name.replace("]", "]]") + "]"


_SQL_TYPES = {
    TdsCreateType.BIGINT: "BIGINT",
    TdsCreateType.FLOAT53: "FLOAT(53)",
    TdsCreateType.NVARCHARMAX: "NVARCHAR(MAX)",
    TdsCreateType.DATETIME2_6: "DATETIME2(6)",
}
_PROPERTY = """
EXEC sys.sp_addextendedproperty @name=?, @value=?,
 @level0type=N'SCHEMA',@level0name=?,@level1type=N'TABLE',@level1name=?;
"""


class TdsCoordinatorCreate:
    """Exactly one bound CREATE grant; effects remain inside the owned child."""

    def __init__(self, authority: TdsCoordinatorSql) -> None:
        self._sql = authority
        self._attempted = False
        self.evidence: TdsCreateEvidence | None = None

    def _validate(self, request: TdsCreateRequest, grant: TdsCoordinatorGrant) -> None:
        identity, receipt = self._sql.identity, self._sql.authority
        if type(request) is not TdsCreateRequest or type(grant) is not TdsCoordinatorGrant or receipt is None:
            raise ValueError("mssql_native.tds_create_binding_invalid")
        if (
            identity.command is not TdsCoordinatorCommand.CREATE
            or request.parent != identity.parent
            or create_command_digest(request) != identity.command_sha256
            or grant.operation_sha256 != coordinator_identity_digest(identity)
            or grant.ownership != self._sql.execution_owner
            or grant.ownership.fence != identity.original_fence
            or grant.process != self._sql.process
            or grant.session != receipt.session
            or grant.authority_sha256 != authority_digest(receipt)
        ):
            raise ValueError("mssql_native.tds_create_binding_invalid")

    def _observe(
        self, request: TdsCreateRequest, *, deadline: float
    ) -> tuple[Any, tuple[TdsCreateObservedColumn, ...]]:
        authority = self._sql.authority
        assert authority is not None
        self._sql.check_deadline(deadline=deadline)
        row = _one(self._sql.cursor, _OBJECT, (authority.schema_observation.schema_id, request.parent.table))
        observed = parse_stage_object(row)
        if (observed[1], observed[3], observed[4]) != (
            request.parent.table,
            request.parent.owner_binding,
            str(request.object_nonce),
        ):
            raise ValueError
        self._sql.check_deadline(deadline=deadline)
        self._sql.cursor.execute(_COLUMNS, row[0])
        rows = []
        for _ in range(101):
            column = self._sql.cursor.fetchone()
            if column is None:
                break
            rows.append(column)
        columns = parse_stage_columns(rows)
        if len(columns) != len(request.columns) or tuple(
            (col.name, col.type, col.nullable) for col in columns
        ) != tuple((col.name, col.type, col.nullable) for col in request.columns):
            raise ValueError
        return tuple(row[:5]), tuple(columns)

    def execute(self, request: TdsCreateRequest, grant: TdsCoordinatorGrant, *, deadline: float) -> TdsCreateEvidence:
        self._sql.connection.check_owner()
        if self._attempted:
            raise TdsCreateUnknown(self.evidence)
        self._attempted = True
        try:
            self._validate(request, grant)
            receipt = self._sql.require_authority(deadline=deadline)
            exists = _one(
                self._sql.cursor,
                "SELECT COUNT(*) FROM sys.objects WHERE schema_id=? AND name=?;",
                (receipt.schema_observation.schema_id, request.parent.table),
            )
            if len(exists) != 1 or type(exists[0]) is not int or exists[0] != 0:
                raise ValueError
            self._sql.require_authority(deadline=deadline)
            target = _quote(receipt.schema_observation.name) + "." + _quote(request.parent.table)
            ddl = ", ".join(
                _quote(col.name) + " " + _SQL_TYPES[col.type] + (" NULL" if col.nullable else " NOT NULL")
                for col in request.columns
            )
            self._sql.cursor.execute("BEGIN TRANSACTION;")
            self._sql.require_authority(deadline=deadline, in_transaction=True)
            self._sql.check_deadline(deadline=deadline)
            self._sql.cursor.execute("CREATE TABLE " + target + " (" + ddl + ");")
            for name, value in (
                ("dpone_native_owner", request.parent.owner_binding),
                ("dpone_tds_incarnation", str(request.object_nonce)),
            ):
                self._sql.check_deadline(deadline=deadline)
                self._sql.cursor.execute(_PROPERTY, name, value, receipt.schema_observation.name, request.parent.table)
            before = self._observe(request, deadline=deadline)
            self._sql.require_authority(deadline=deadline, in_transaction=True)
            self._sql.cursor.execute("COMMIT TRANSACTION;")
            self._sql.require_authority(deadline=deadline)
            observed, columns = self._observe(request, deadline=deadline)
            if (observed, columns) != before:
                raise ValueError
            self._sql.check_deadline(deadline=deadline)
            empty = _one(self._sql.cursor, "SELECT CASE WHEN EXISTS (SELECT 1 FROM " + target + ") THEN 0 ELSE 1 END;")
            if len(empty) != 1 or type(empty[0]) is not int or empty[0] != 1:
                raise ValueError
            if type(receipt.session) is not TdsRemoteSessionIdentity:
                raise ValueError
            evidence = TdsCreateEvidence(
                coordinator_identity_digest(self._sql.identity),
                create_command_digest(request),
                coordinator_grant_digest(grant),
                authority_digest(receipt),
                receipt.session,
                receipt.database,
                receipt.schema_observation,
                observed[1],
                observed[0],
                observed[2],
                observed[3],
                UUID(observed[4]),
                columns,
            )
            # Retain before the last authority/deadline check or any future cleanup.
            self.evidence = evidence
            self._sql.require_authority(deadline=deadline)
            return evidence
        except BaseException:
            raise TdsCreateUnknown(self.evidence) from None

    def close(self) -> None:
        """Keep any typed evidence even if child-owned driver teardown fails."""
        try:
            self._sql.close()
        except BaseException:
            raise TdsCreateUnknown(self.evidence) from None
