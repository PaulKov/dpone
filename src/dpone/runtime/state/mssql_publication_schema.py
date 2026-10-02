"""Explicit catalog plan/apply; never part of ordinary runtime bootstrap.

Composition supplies a fresh endpoint-admitted session for each invocation.
Only an absent catalog is provisioned, under a transaction-owned application
lock. Existing objects are never altered, dropped or silently repaired.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from dpone.adapters.mssql_publication_catalog_ddl import EVENT_TABLE, SLOT_TABLE, render_publication_catalog_ddl
from dpone.contracts.publication_schema import PublicationSchemaPlan
from dpone.runtime.state.mssql_publication_admission import require_publication_catalog
from dpone.runtime.state.mssql_publication_transaction import (
    PublicationTransactionUnknown,
    execute_publication_statement,
    run_publication_transaction,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dpone.ports.mssql_publication import (
        PublicationAuthorityBinding,
        PublicationSessionFactory,
        PublicationSqlCursor,
    )


@dataclass(frozen=True, slots=True)
class PublicationSchemaResult:
    """Catalog readiness only; not permission for a workload or authority cutover."""

    status: Literal["ready", "completed", "blocked", "outcome_unknown"]
    plan_digest: str
    reason_code: str


class MssqlPublicationSchema:
    def __init__(
        self,
        *,
        session_factory: PublicationSessionFactory,
        binding: PublicationAuthorityBinding,
        endpoint_identity: str,
    ) -> None:
        self._sessions, self._binding = session_factory, binding
        self._ddl = render_publication_catalog_ddl(database=binding.database, schema=binding.schema)
        self._plan = PublicationSchemaPlan(
            binding=binding,
            endpoint_identity=endpoint_identity,
            ddl_sha256=hashlib.sha256(self._ddl.encode()).hexdigest(),
        )

    def plan(self) -> PublicationSchemaPlan:
        """Render a reviewable immutable scope without constructing a connector."""
        return self._plan

    def inspect(self, plan: PublicationSchemaPlan) -> PublicationSchemaResult:
        """Resolve readiness or a lost apply ACK without executing provisioning SQL."""
        self._require_plan(plan)
        return self._run(apply=False)

    def apply(self, plan: PublicationSchemaPlan, *, confirmation_digest: str) -> PublicationSchemaResult:
        """Execute reviewed batches once and admit exact structure before commit.

        An unknown outcome requires explicit inspection; this method neither
        retries nor opens a second connection to guess that the write succeeded.
        A subsequent invocation always re-admits existing objects first.
        """
        self._require_plan(plan)
        if not plan.confirms(confirmation_digest):
            raise ValueError("exact publication schema confirmation required")
        return self._run(apply=True)

    def _require_plan(self, plan: PublicationSchemaPlan) -> None:
        if not isinstance(plan, PublicationSchemaPlan) or plan != self._plan:
            raise ValueError("publication schema scope or DDL differs")

    def _run(self, *, apply: bool) -> PublicationSchemaResult:
        def operation(cursor: PublicationSqlCursor) -> PublicationSchemaResult:
            execute_publication_statement(cursor, "SET XACT_ABORT ON; SET NOCOUNT ON;", rows_required=False)
            reader = _CursorCatalog(cursor)
            visibility = reader.get_records(
                "SELECT DB_NAME(),HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION'),SCHEMA_ID(?)",
                (self._binding.schema,),
            )
            if (
                len(visibility) != 1
                or len(visibility[0]) != 3
                or visibility[0][0] != self._binding.database
                or visibility[0][1] != 1
                or type(visibility[0][2]) is not int
                or visibility[0][2] <= 0
            ):
                return self._result("blocked", "catalog_visibility_or_location_unavailable")
            if apply:
                lock = reader.get_records(
                    "DECLARE @result int; EXEC @result=sys.sp_getapplock @Resource=?,"
                    "@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=0; SELECT @result;",
                    ("dpone.publication-schema.v1:" + self._binding.schema,),
                )
                if lock not in ([(0,)], [(1,)]):
                    return self._result("blocked", "catalog_provisioner_busy")
            objects = reader.get_records(
                "SELECT o.name,RTRIM(o.type) FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id "
                "WHERE s.name=? AND o.name IN (?,?,?)",
                (self._binding.schema, SLOT_TABLE, EVENT_TABLE, "dpone_publication_events_immutable"),
            )
            if objects:
                expected_objects = [(SLOT_TABLE, "U"), (EVENT_TABLE, "U"), ("dpone_publication_events_immutable", "TR")]
                if sorted(objects) != sorted(expected_objects):
                    return self._result("blocked", "catalog_not_exact")
                try:
                    require_publication_catalog(reader, binding=self._binding)
                except ValueError:
                    return self._result("blocked", "catalog_not_exact")
                return self._result("completed", "catalog_exact")
            if not apply:
                return self._result("ready", "catalog_absent")
            for batch in self._ddl.split("\nGO\n"):
                if batch.strip():
                    execute_publication_statement(cursor, batch, rows_required=False)
            # Failure after any DDL propagates through owned rollback/UNKNOWN;
            # it must not become a normal blocked result that commits a subset.
            require_publication_catalog(reader, binding=self._binding)
            return self._result("completed", "catalog_created")

        try:
            return run_publication_transaction(self._sessions, operation)
        except PublicationTransactionUnknown:
            return self._result("outcome_unknown", "catalog_requires_readback")

    def _result(
        self, status: Literal["ready", "completed", "blocked", "outcome_unknown"], reason: str
    ) -> PublicationSchemaResult:
        return PublicationSchemaResult(status, self._plan.digest, reason)


class _CursorCatalog:
    """Read exact catalog evidence using the same uncommitted DDL session."""

    def __init__(self, cursor: PublicationSqlCursor) -> None:
        self._cursor = cursor

    def get_records(self, query: str, params: Sequence[Any]) -> list[tuple[Any, ...]]:
        return execute_publication_statement(self._cursor, query, params)
