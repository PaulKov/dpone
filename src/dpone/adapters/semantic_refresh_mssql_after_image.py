"""Serializable SQL Server reader for a protected committed after-image."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from dpone.ports.semantic_refresh_mssql_after_image import (
    MssqlCommittedAfterImageSnapshot,
)
from dpone.ports.semantic_refresh_mssql_authority import (
    MssqlProtectedOperationAuthority,
    MssqlProtectedWritableColumn,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshMssqlAfterImageReadError(RuntimeError):
    """Raised when a committed after-image cannot be proven exactly."""


class MssqlSemanticRefreshAfterImageReader:
    """Verify relation/schema/digest and read ordered rows in one transaction."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        mssql_connection_authority_id: str,
    ) -> None:
        if not mssql_connection_authority_id.strip():
            raise ValueError("MSSQL connection authority ID must be non-empty")
        self._connection_factory = connection_factory
        self._connection_authority_id = mssql_connection_authority_id

    def read(self, operation: MssqlProtectedOperationAuthority) -> MssqlCommittedAfterImageSnapshot:
        """Return rows only when the current image still matches durable authority."""

        if not isinstance(operation, MssqlProtectedOperationAuthority):
            raise TypeError("operation must be protected MSSQL authority")
        if operation.mssql_connection_authority_id != self._connection_authority_id:
            raise SemanticRefreshMssqlAfterImageReadError("MSSQL connection authority differs")
        if operation.after_image_relation is None or operation.after_image_sha256 is None:
            raise SemanticRefreshMssqlAfterImageReadError("committed after-image authority is absent")
        relation = _relation(operation.after_image_relation)
        columns = operation.writable_columns
        if not columns:
            raise SemanticRefreshMssqlAfterImageReadError("protected writable schema is empty")
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            self._assert_columns(cursor, relation, columns)
            digest, row_count = self._observe_digest(cursor, relation, columns)
            if digest != operation.after_image_sha256:
                raise SemanticRefreshMssqlAfterImageReadError("committed after-image digest differs")
            if row_count > operation.resource_policy.max_after_image_rows:
                raise SemanticRefreshMssqlAfterImageReadError("committed after-image row budget exceeded")
            rows = self._read_rows(cursor, relation, columns)
            if len(rows) != row_count:
                raise SemanticRefreshMssqlAfterImageReadError("committed after-image changed while reading")
            connection.commit()
            return MssqlCommittedAfterImageSnapshot(
                relation_id=operation.after_image_relation,
                image_sha256=digest,
                row_count=row_count,
                columns=columns,
                rows=rows,
            )
        except SemanticRefreshMssqlAfterImageReadError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlAfterImageReadError("committed after-image read failed") from exc
        finally:
            _close(cursor)
            _close(connection)

    @staticmethod
    def _assert_columns(
        cursor: _Cursor,
        relation: _SqlRelation,
        columns: tuple[MssqlProtectedWritableColumn, ...],
    ) -> None:
        cursor.execute(
            f"SELECT name FROM {relation.columns_catalog} WHERE object_id = OBJECT_ID(?, N'U') ORDER BY column_id;",
            relation.quoted,
        )
        observed = tuple(str(row[0]) for row in cursor.fetchall())
        expected = tuple(column.name for column in columns)
        if observed != expected:
            raise SemanticRefreshMssqlAfterImageReadError("committed after-image schema differs")

    @staticmethod
    def _observe_digest(
        cursor: _Cursor,
        relation: _SqlRelation,
        columns: tuple[MssqlProtectedWritableColumn, ...],
    ) -> tuple[str, int]:
        column_sql = ", ".join(_quoted(column.name) for column in columns)
        key_sql = ", ".join(_order_expression(column) for column in _key_columns(columns))
        cursor.execute(
            f"""
DECLARE @dpone_image_json nvarchar(max);
SELECT @dpone_image_json = (
    SELECT {column_sql} FROM {relation.quoted} WITH (HOLDLOCK)
    ORDER BY {key_sql} FOR JSON PATH, INCLUDE_NULL_VALUES
);
SELECT 'sha256:' + LOWER(CONVERT(varchar(64), HASHBYTES(
           'SHA2_256', COALESCE(@dpone_image_json, N'[]')
       ), 2)),
       (SELECT COUNT_BIG(*) FROM {relation.quoted} WITH (HOLDLOCK));
""".strip()
        )
        row = cursor.fetchone()
        if row is None or not isinstance(row[0], str):
            raise SemanticRefreshMssqlAfterImageReadError("committed after-image digest is unavailable")
        count = row[1]
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise SemanticRefreshMssqlAfterImageReadError("committed after-image count is invalid")
        return str(row[0]), count

    @staticmethod
    def _read_rows(
        cursor: _Cursor,
        relation: _SqlRelation,
        columns: tuple[MssqlProtectedWritableColumn, ...],
    ) -> tuple[tuple[object, ...], ...]:
        column_sql = ", ".join(_quoted(column.name) for column in columns)
        key_sql = ", ".join(_order_expression(column) for column in _key_columns(columns))
        cursor.execute(f"SELECT {column_sql} FROM {relation.quoted} WITH (HOLDLOCK) ORDER BY {key_sql};")
        return tuple(tuple(row) for row in cursor.fetchall())


class _SqlRelation:
    def __init__(self, parts: tuple[str, ...]) -> None:
        self.quoted = ".".join(_quoted(part) for part in parts)
        self.columns_catalog = "sys.columns" if len(parts) == 2 else f"{_quoted(parts[0])}.sys.columns"


def _relation(value: str) -> _SqlRelation:
    if not isinstance(value, str) or not value.strip():
        raise SemanticRefreshMssqlAfterImageReadError("after-image relation is invalid")
    parts = tuple(_unquote(part.strip()) for part in value.split("."))
    if len(parts) not in {2, 3} or any(_IDENTIFIER.fullmatch(part) is None for part in parts):
        raise SemanticRefreshMssqlAfterImageReadError("after-image relation is outside the closed subset")
    return _SqlRelation(parts)


def _unquote(value: str) -> str:
    if len(value) >= 2 and (value[0], value[-1]) in {("[", "]"), ('"', '"')}:
        return value[1:-1]
    return value


def _quoted(value: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise SemanticRefreshMssqlAfterImageReadError("MSSQL identifier is outside the closed subset")
    return f"[{value}]"


def _key_columns(
    columns: tuple[MssqlProtectedWritableColumn, ...],
) -> tuple[MssqlProtectedWritableColumn, ...]:
    result = tuple(column for column in columns if column.writable_role != "MUTABLE_VALUE")
    if not result:
        raise SemanticRefreshMssqlAfterImageReadError("after-image effective-key closure is empty")
    return result


def _order_expression(column: MssqlProtectedWritableColumn) -> str:
    value = _quoted(column.name)
    return f"CONVERT(char(36), {value})" if column.source_type.lower() == "uniqueidentifier" else value


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
    "MssqlSemanticRefreshAfterImageReader",
    "SemanticRefreshMssqlAfterImageReadError",
]
