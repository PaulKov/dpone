"""Catalog identity guard for one ClickHouse native extraction."""

from __future__ import annotations

import re
from types import TracebackType
from typing import Any, Literal


class ClickHouseSchemaStabilityGuard:
    """Detect source relation replacement or schema drift around one SELECT.

    ClickHouse owns the in-query table lock and snapshot. This guard binds the
    surrounding dpone lifecycle to the exact UUID, engine, database engine and
    ordered column declarations observed before and after that query.
    """

    def __init__(self, connector: Any, database: str, table: str) -> None:
        if not database or not table:
            raise ValueError("mssql_native.source_identifier_invalid")
        self._connector = connector
        self._params = {"database": database, "table": table}
        self._before: tuple[tuple[str, str], ...] | None = None

    def __enter__(self) -> ClickHouseSchemaStabilityGuard:
        self._before = self._snapshot()
        return self

    def __exit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        del error_type, traceback
        try:
            changed = self._before is None or self._snapshot() != self._before
        except BaseException as probe_error:
            if error is not None:
                error.add_note(f"mssql_native.source_schema_guard_failed:{type(probe_error).__name__}")
                return False
            raise ValueError("mssql_native.source_schema_guard_unavailable") from None
        if changed:
            if error is not None:
                error.add_note("mssql_native.source_schema_changed")
                return False
            raise ValueError("mssql_native.source_schema_changed")
        return False

    def _snapshot(self) -> tuple[tuple[str, str], ...]:
        rows = self._connector.get_records(
            "SELECT toString(t.uuid) AS uuid, t.engine AS table_engine, "
            "d.engine AS database_engine, groupArray((c.position,c.name,c.type,c.default_kind)) AS columns "
            "FROM system.tables AS t INNER JOIN system.databases AS d ON d.name=t.database "
            "INNER JOIN system.columns AS c ON c.database=t.database AND c.table=t.name "
            "WHERE t.database=%(database)s AND t.name=%(table)s "
            "GROUP BY t.uuid,t.engine,d.engine",
            self._params,
            as_dict=True,
        )
        if len(rows) != 1 or not isinstance(rows[0], dict):
            raise ValueError("mssql_native.source_schema_guard_unavailable")
        return tuple(sorted((str(key), repr(value)) for key, value in rows[0].items()))


__all__ = ["ClickHouseSchemaStabilityGuard"]


def source_identifier(value: str) -> str:
    """Quote only the existing closed native source identifier grammar."""
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("mssql_native.source_identifier_invalid")
    return f"`{value}`"


def admit_source_type(value: str) -> None:
    """Retain the lossless native type allowlist for both source modes."""
    normalized = value
    if normalized.startswith("Nullable(") and normalized.endswith(")"):
        normalized = normalized[9:-1]
    if re.fullmatch(r"(?:U?Int(?:8|16|32|64)|Float(?:32|64)|String|UUID|Date|Date32|Bool)", normalized):
        return
    if re.fullmatch(r"Decimal\((?:[1-9]|[12][0-9]|3[0-8]),\s*\d+\)", normalized):
        return
    if re.fullmatch(r"DateTime(?:\('UTC'\))?|DateTime64\([0-6](?:,\s*'UTC')?\)", normalized):
        return
    raise ValueError("mssql_native.source_type_unsupported")
