"""One fixed GRANT transaction on retained live SQL authority, never a bulk grant.

The caller supplies external process containment. Deadline checks cannot cancel a
blocked driver. UNKNOWN retains original resources and observations, not rollback
or remote closure. Successful return leaves the original session lock open.
"""

import math
import os
from copy import deepcopy
from threading import current_thread
from typing import NoReturn

from dpone.adapters.mssql_sqlclient_permission_grant_catalog import (
    MANAGEMENT_SQL as MANAGEMENT_SQL,
)
from dpone.adapters.mssql_sqlclient_permission_grant_catalog import OWNERS_SQL as OWNERS_SQL
from dpone.adapters.mssql_sqlclient_permission_grant_catalog import (
    PERMISSIONS_SQL as PERMISSIONS_SQL,
)
from dpone.adapters.mssql_sqlclient_permission_grant_catalog import WRITER_SQL as WRITER_SQL
from dpone.adapters.mssql_sqlclient_permission_grant_catalog import observe_permission_grant_catalog
from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.adapters.mssql_tds_coordinator_sql import TdsCoordinatorSql
from dpone.contracts.mssql_tds_api import (
    SqlClientPermissionGrantEvidence,
    SqlClientPermissionGrantRequest,
    TdsCoordinatorGrant,
    encode_permission_grant_evidence,
    quote_stage_identifier,
    validate_permission_binding,
)

ERROR = "mssql_native.sqlclient_permission_grant_unknown"


class SqlClientPermissionGrantUnknown(RuntimeError):
    def __init__(self, retained: "SqlClientPermissionGrant") -> None:
        self.retained = retained
        super().__init__(ERROR)


class SqlClientPermissionGrant:
    """Exact owner, one attempt, one immutable deadline, and one original close."""

    def __init__(self, sql: TdsCoordinatorSql) -> None:
        if type(sql) is not TdsCoordinatorSql or type(sql.connection) is not TdsSqlConnection or sql.authority is None:
            raise ValueError(ERROR)
        self.sql = self._sql = sql
        self._connection, self._cursor, self._session = sql.connection, sql.connection.cursor, sql._session
        self._raw_connection, self._clock = self._connection._connection, sql._clock
        self._identity, self._authority_record = sql.identity, sql.authority
        self._process, self._execution_owner = sql.process, sql.execution_owner
        self._values = deepcopy((sql.identity, sql.authority, sql.process, sql.execution_owner))
        self._local = os.getpid(), current_thread()
        self._attempted = self._busy = self._failed = self._closed = False
        self._deadline: float | None = None
        self.request: SqlClientPermissionGrantRequest | None = None
        self.grant: TdsCoordinatorGrant | None = None
        self._inputs: tuple | None = None
        self._input_values: tuple | None = None
        self.raw: list[tuple[str, str, list[tuple]]] = []
        self.phase = "unattempted"
        self.commit_attempted = False
        self.evidence: SqlClientPermissionGrantEvidence | None = None
        self._evidence_bytes: bytes | None = None

    def _reject(self) -> NoReturn:
        self._failed = True
        raise SqlClientPermissionGrantUnknown(self)

    def _original(self) -> None:
        sql = self.sql
        if (
            self._local != (os.getpid(), current_thread())
            or self._failed
            or self._closed
            or sql is not self._sql
            or sql.connection is not self._connection
            or sql.connection.cursor is not self._cursor
            or sql._session is not self._session
            or sql._clock is not self._clock
            or self._connection._connection is not self._raw_connection
            or sql.identity is not self._identity
            or sql.authority is not self._authority_record
            or sql.process is not self._process
            or sql.execution_owner is not self._execution_owner
            or (sql.identity, sql.authority, sql.process, sql.execution_owner) != self._values
        ):
            self._reject()
        if self._inputs is not None:
            if (
                self.request is not self._inputs[0]
                or self.grant is not self._inputs[1]
                or (self.request, self.grant) != self._input_values
            ):
                self._reject()
            assert self.request is not None and self.grant is not None
            validate_permission_binding(self.request, self._identity, self.grant, self._authority_record)
        if self.evidence is not None and encode_permission_grant_evidence(self.evidence) != self._evidence_bytes:
            self._reject()

    def _check(self, deadline: float) -> None:
        self._original()
        self._sql.check_deadline(deadline=deadline)
        self._original()

    def _authority(self, deadline: float, transaction: bool = False) -> None:
        self._check(deadline)
        value = self._sql.require_authority(deadline=deadline, in_transaction=transaction)
        if value is not self._authority_record:
            self._reject()
        self._check(deadline)

    def _query(self, text: str, args: tuple, deadline: float, *, limit: int = 1, statement: bool = False) -> tuple:
        self._check(deadline)
        self._cursor.execute(text, *args)
        rows: list[tuple] = []
        self.raw.append((self.phase, text, rows))
        self._check(deadline)
        if statement:
            if self._cursor.description is not None:
                self._reject()
        else:
            if self._cursor.description is None:
                self._reject()
            for _ in range(limit + 1):
                self._check(deadline)
                row = self._cursor.fetchone()
                if row is not None:
                    rows.append(tuple(row))
                self._check(deadline)
                if row is None:
                    break
            else:
                self._reject()
        self._check(deadline)
        completion = self._cursor.nextset()
        if completion is not None and completion is not False:
            self._reject()
        self._check(deadline)
        return tuple(rows)

    def _one(self, text: str, args: tuple, deadline: float) -> tuple:
        rows = self._query(text, args, deadline)
        if len(rows) != 1:
            self._reject()
        return rows[0]

    def _catalog(self, deadline: float, *, granted: bool) -> tuple:
        return observe_permission_grant_catalog(self, deadline, granted=granted)

    def execute(
        self, request: SqlClientPermissionGrantRequest, grant: TdsCoordinatorGrant, *, deadline: float
    ) -> SqlClientPermissionGrantEvidence:
        if self._busy or self._attempted or self._failed or self._closed:
            self._reject()
        self._attempted = self._busy = True
        try:
            if type(deadline) not in (int, float) or not math.isfinite(deadline) or deadline <= 0:
                self._reject()
            self._deadline = deadline
            validate_permission_binding(request, self._identity, grant, self._authority_record)
            self.request, self.grant = request, grant
            self._inputs, self._input_values = (request, grant), deepcopy((request, grant))
            self.phase = "before"
            self._authority(deadline)
            before = self._catalog(deadline, granted=False)
            self._query("BEGIN TRANSACTION;", (), deadline, statement=True)
            self.phase = "transaction"
            self._authority(deadline, True)
            if self._catalog(deadline, granted=False) != before:
                self._reject()
            q, r = quote_stage_identifier, request
            self.phase = "grant_attempted"
            self._query(
                "GRANT INSERT, SELECT, VIEW DEFINITION ON OBJECT::"
                + q(r.stage.schema_name)
                + "."
                + q(r.stage.table_name)
                + " TO "
                + q(r.writer.name)
                + " AS "
                + q(r.management.name)
                + ";",
                (),
                deadline,
                statement=True,
            )
            self.phase = "precommit"
            precommit = self._catalog(deadline, granted=True)
            self._authority(deadline, True)
            self.phase, self.commit_attempted = "commit_attempted", True
            self._query("COMMIT TRANSACTION;", (), deadline, statement=True)
            self.phase = "postcommit"
            self._authority(deadline)
            after = self._catalog(deadline, granted=True)
            if after != precommit:
                self._reject()
            self.evidence = SqlClientPermissionGrantEvidence(
                request=deepcopy(request),
                operation=deepcopy(self._identity),
                grant=deepcopy(grant),
                authority=deepcopy(self._authority_record),
                direct_permissions=after[-1],
            )
            self._evidence_bytes = encode_permission_grant_evidence(self.evidence)
            self.phase = "evidence_retained"
            self._authority(deadline)
            self._original()
            return self.evidence
        except BaseException:
            self._failed = True
            raise SqlClientPermissionGrantUnknown(self) from None
        finally:
            self._busy = False

    def require_held(self, *, deadline: float) -> SqlClientPermissionGrantEvidence:
        if self._busy or self._failed or self._closed or self.evidence is None:
            self._reject()
        self._busy = True
        try:
            evidence = self.evidence
            if (
                type(deadline) not in (int, float)
                or not math.isfinite(deadline)
                or self._deadline is None
                or not 0 < deadline <= self._deadline
                or evidence is None
            ):
                self._reject()
            self.phase = "held"
            self._authority(deadline)
            current = self._catalog(deadline, granted=True)
            if current[-1] != evidence.direct_permissions:
                self._reject()
            self._authority(deadline)
            self._original()
            return evidence
        except BaseException:
            self._failed = True
            raise SqlClientPermissionGrantUnknown(self) from None
        finally:
            self._busy = False

    def close(self) -> None:
        """Consume original resources once, including after field drift or UNKNOWN.

        Mirrors the retained TdsSqlConnection's close latch, but never follows its
        mutable cursor/raw connection fields to a substituted resource.
        """
        if self._local != (os.getpid(), current_thread()) or self._closed:
            self._reject()
        if self._busy:
            self._reject()
        self._closed = self._failed = True
        self._connection.check_owner()
        self._connection._closed = self._sql._poisoned = True
        failed = False
        for resource in (self._cursor, self._raw_connection):
            try:
                resource.close()
            except BaseException:
                failed = True
        if failed:
            raise SqlClientPermissionGrantUnknown(self) from None
