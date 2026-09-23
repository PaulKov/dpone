"""One-shot read-only VERIFY on one restricted, nonpooled SQL connection."""

from __future__ import annotations

import math
from copy import deepcopy
from time import monotonic
from typing import Any, NoReturn, Protocol

from dpone.adapters.mssql_sqlclient_restricted_writer_verify_sql import (
    CONTEXT_SQL,
    DATABASE_PERMISSIONS_SQL,
    DATABASE_SQL,
    LOGIN_TOKEN_SQL,
    SERVER_PERMISSIONS_SQL,
    STAGE_PERMISSIONS_SQL,
    USER_TOKEN_SQL,
)
from dpone.adapters.mssql_sqlclient_stage_catalog_sql import COLUMNS_SQL, OBJECT_SQL, SCHEMA_SQL
from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.adapters.mssql_tds_restricted_session import TdsRestrictedWriterSession

ERROR = "mssql_native.sqlclient_restricted_writer_verify_invalid"


class RestrictedWriterVerifySqlContract(Protocol):
    def validate_request(self, value: object) -> object: ...
    def encode_request(self, value: object) -> bytes: ...
    def context(self, session: object, row: tuple): ...
    def tokens(self, rows: tuple[tuple, ...]): ...
    def permissions(self, rows: tuple[tuple, ...]): ...
    def stage_target(self, request: object) -> str: ...
    def stage(
        self, request: object, database: tuple, schema: tuple, object_row: tuple, column_rows: tuple[tuple, ...]
    ): ...
    def result(self, **values): ...
    def validate_result(self, request: object, result: object) -> None: ...
    def encode_result(self, result: object) -> bytes: ...
    def opening(self, request: object, context: object) -> object: ...
    def encode_opening(self, opening: object) -> bytes: ...


class RestrictedWriterVerifyUnknown(RuntimeError):
    def __init__(self, retained: SqlClientRestrictedWriterVerify) -> None:
        self.retained = retained
        super().__init__(ERROR)


class SqlClientRestrictedWriterVerify:
    """Poison after any ambiguity; never reconnect, retry, mutate or transact."""

    def __init__(
        self, connection: TdsSqlConnection, contract: RestrictedWriterVerifySqlContract, *, clock=monotonic
    ) -> None:
        if type(connection) is not TdsSqlConnection:
            raise ValueError(ERROR)
        self._connection = connection
        self._cursor = connection.cursor
        self._clock = clock
        self._contract = contract
        self._attempted = False
        self._failed = False
        self._request: Any | None = None
        self._request_bytes: bytes | None = None
        self._session_guard: TdsRestrictedWriterSession | None = None
        self._session: Any | None = None
        self._opening: object | None = None

    def _unknown(self) -> NoReturn:
        self._failed = True
        raise RestrictedWriterVerifyUnknown(self)

    def _check(self, deadline: float) -> None:
        if (
            self._failed
            or type(deadline) is not float
            or not math.isfinite(deadline)
            or self._clock() >= deadline
            or self._connection.cursor is not self._cursor
        ):
            self._unknown()
        self._connection.check_owner()
        if self._request is not None and self._contract.encode_request(self._request) != self._request_bytes:
            self._unknown()

    def _query(self, sql: str, args: tuple, deadline: float, limit: int) -> tuple[tuple, ...]:
        self._check(deadline)
        self._cursor.execute(sql, *args)
        if self._cursor.description is None:
            self._unknown()
        rows = []
        for _ in range(limit + 1):
            self._check(deadline)
            row = self._cursor.fetchone()
            if row is None:
                break
            rows.append(tuple(row))
        else:
            self._unknown()
        if self._cursor.nextset() not in (None, False):
            self._unknown()
        self._check(deadline)
        return tuple(rows)

    def _one(self, sql: str, args: tuple, deadline: float) -> tuple:
        rows = self._query(sql, args, deadline, 1)
        if len(rows) != 1:
            self._unknown()
        return rows[0]

    def _context(self, session, deadline: float):
        row = self._one(CONTEXT_SQL, (), deadline)
        try:
            if len(row) != 18:
                raise ValueError
            values = list(row)
            for index in (10, 11, 12, 17):
                if type(values[index]) is not bool:
                    raise ValueError
            return self._contract.context(session, tuple(values))
        except (ValueError, TypeError, AttributeError, OverflowError):
            self._unknown()

    def _tokens(self, sql: str, deadline: float):
        rows = self._query(sql, (), deadline, 8)
        try:
            if any(len(row) != 5 for row in rows):
                raise ValueError
            return self._contract.tokens(rows)
        except (ValueError, TypeError, AttributeError, OverflowError):
            self._unknown()

    def _permissions(self, sql: str, deadline: float):
        rows = self._query(sql, (), deadline, 128)
        try:
            if any(len(row) != 3 for row in rows):
                raise ValueError
            return self._contract.permissions(rows)
        except (ValueError, TypeError, AttributeError, OverflowError):
            self._unknown()

    def _stage(self, request, deadline: float):
        stage = request.stage
        database = self._one(DATABASE_SQL, (), deadline)
        schema = self._one(SCHEMA_SQL, (stage.schema_id,), deadline)
        object_row = self._one(OBJECT_SQL, (stage.schema_id, stage.table_name), deadline)
        column_rows = self._query(COLUMNS_SQL, (stage.object_id,), deadline, 100)
        try:
            if database != (stage.database_id, stage.database_name, stage.database_guid) or schema != (
                stage.schema_id,
                stage.schema_name,
            ):
                raise ValueError
            return self._contract.stage(request, database, schema, object_row, column_rows)
        except (ValueError, TypeError, AttributeError, OverflowError):
            self._unknown()

    def _stage_permissions(self, request, deadline: float) -> tuple[str, ...]:
        target = self._contract.stage_target(request)
        rows = self._query(STAGE_PERMISSIONS_SQL, (target,), deadline, 3)
        if any(len(row) != 1 or type(row[0]) is not str for row in rows):
            self._unknown()
        return tuple(row[0] for row in rows)

    def _empty(self, request, deadline: float) -> int:
        target = self._contract.stage_target(request)
        row = self._one(f"SELECT COUNT_BIG(*) FROM {target};", (), deadline)
        if len(row) != 1 or type(row[0]) is not int or row[0] != 0:
            self._unknown()
        return row[0]

    def open_writer_session(self, request, nonce: bytes, *, deadline: float):
        if self._attempted:
            self._unknown()
        self._contract.validate_request(request)
        self._attempted = True
        self._request = request
        self._request_bytes = self._contract.encode_request(request)
        try:
            self._check(deadline)
            session_guard = TdsRestrictedWriterSession(self._connection)
            session = session_guard.initialize(nonce)
            context = self._context(session, deadline)
            opening = self._contract.opening(request, context)
            self._contract.encode_opening(opening)
            self._session_guard, self._session, self._opening = session_guard, session, opening
            return opening
        except RestrictedWriterVerifyUnknown:
            raise
        except BaseException:
            self._unknown()

    def execute_authorized(self, opening, *, deadline: float):
        if self._opening is None or opening is not self._opening:
            self._unknown()
        request = self._request
        session_guard = self._session_guard
        session = self._session
        if request is None or session_guard is None or session is None:
            self._unknown()
        try:
            self._check(deadline)
            login_token = self._tokens(LOGIN_TOKEN_SQL, deadline)
            user_token = self._tokens(USER_TOKEN_SQL, deadline)
            server = self._permissions(SERVER_PERMISSIONS_SQL, deadline)
            database = self._permissions(DATABASE_PERMISSIONS_SQL, deadline)
            stage_permissions = self._stage_permissions(request, deadline)
            stage = self._stage(request, deadline)
            empty = self._empty(request, deadline)
            session_guard.require_same(session)
            closing = self._context(session, deadline)
            closing_permissions = self._stage_permissions(request, deadline)
            closing_stage = self._stage(request, deadline)
            closing_empty = self._empty(request, deadline)
            result = self._contract.result(
                opening=opening.context,
                login_token=login_token,
                user_token=user_token,
                server_permissions=server,
                database_permissions=database,
                stage_permissions=stage_permissions,
                stage=stage,
                empty_count=empty,
                closing=closing,
                closing_stage_permissions=closing_permissions,
                closing_stage=closing_stage,
                closing_empty_count=closing_empty,
            )
            self._contract.validate_result(request, result)
            self._contract.encode_result(result)
            self._result = deepcopy(result)
            return result
        except RestrictedWriterVerifyUnknown:
            raise
        except BaseException:
            self._unknown()
