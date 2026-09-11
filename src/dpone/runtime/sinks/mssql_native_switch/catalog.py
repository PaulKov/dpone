"""Sole SQL catalog producer for isolated SWITCH deployment snapshots."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING

from dpone.contracts.native_mssql_switch import (
    NativeSwitchBinding,
    NativeSwitchDatabase,
    NativeSwitchInterval,
    NativeSwitchObject,
    NativeSwitchRejected,
    NativeSwitchSnapshot,
    plan_native_switch,
)
from dpone.runtime.sinks.mssql_native_switch.catalog_parse import parse_table
from dpone.runtime.sinks.mssql_native_switch.catalog_sql import DATABASE_SQL, TABLE_SQL, TRANSACTION_SQL

if TYPE_CHECKING:
    from dpone.ports.native_mssql_switch import NativeSwitchSql


def quote(name: str) -> str:
    """Quote one catalog identifier, never accept a multipart caller SQL string."""
    if not isinstance(name, str) or not name or len(name) > 128 or "\x00" in name:
        raise NativeSwitchRejected("object_binding_mismatch")
    return "[" + name.replace("]", "]]") + "]"


def qualified(binding: NativeSwitchBinding, obj: NativeSwitchObject) -> str:
    """Use a fully qualified name after validating the current database binding."""
    return ".".join(quote(value) for value in (binding.database.name, obj.schema, obj.name))


def one(rows: Sequence[Mapping[str, object]], keys: set[str]) -> Mapping[str, object]:
    if len(rows) != 1 or set(rows[0]) != keys:
        raise NativeSwitchRejected("metadata_unknown")
    return rows[0]


def count(sql: NativeSwitchSql, query: str, parameters: tuple[object, ...] = ()) -> int:
    """Require an exact nonnegative COUNT_BIG observation, never catalog estimates."""
    value = one(sql.query(query, parameters), {"row_count"})["row_count"]
    if type(value) is not int or value < 0:
        raise NativeSwitchRejected("metadata_unknown")
    return value


def interval_parameters(interval: NativeSwitchInterval) -> tuple[object, ...]:
    """Bind UTC datetime2 values without driver-specific timezone coercion.

    Call only after pure admission proves exact boundaries and UTC. SQL Server
    datetime2 has no timezone; the feature's authored convention is UTC.
    """
    return tuple(
        value.replace(tzinfo=None) if isinstance(value, datetime) else value for value in (interval.start, interval.end)
    )


class NativeSwitchCatalog:
    """Strict snapshot reader; query exceptions propagate without retries.

    Initial inspection is diagnostic. The executor repeats it while holding
    transaction locks. It does not cache metadata, infer missing flags, create
    tables, or stamp ownership properties.
    """

    def __init__(self, sql: NativeSwitchSql) -> None:
        self._sql = sql

    def transaction_state(self) -> Mapping[str, object]:
        """Read one exact transaction observation without granting authority.

        The executor owns all value predicates and comparisons across reads.
        Each call queries the supplied session; nothing is cached or retried.
        """
        return one(
            self._sql.query(TRANSACTION_SQL),
            {"state", "depth", "session_id", "database_id", "xact_abort", "isolation"},
        )

    def database(self) -> NativeSwitchDatabase:
        row = one(
            self._sql.query(DATABASE_SQL),
            {
                "major",
                "edition",
                "visible",
                "server_visible",
                "database_triggers",
                "server_triggers",
                "database_id",
                "name",
                "generation",
            },
        )
        if row["major"] != 16 or row["edition"] not in (2, 3):
            raise NativeSwitchRejected("unsupported_server")
        if row["visible"] != 1 or row["server_visible"] != 1:
            raise NativeSwitchRejected("metadata_visibility_required")
        for key in ("database_triggers", "server_triggers"):
            triggers = row[key]
            if type(triggers) is not int or triggers < 0:
                raise NativeSwitchRejected("metadata_unknown")
            if triggers != 0:
                raise NativeSwitchRejected("unsupported_dependency")
        if type(row["database_id"]) is not int or row["database_id"] <= 0:
            raise NativeSwitchRejected("metadata_unknown")
        if (
            not isinstance(row["name"], str)
            or not row["name"]
            or not isinstance(row["generation"], str)
            or not row["generation"]
        ):
            raise NativeSwitchRejected("metadata_unknown")
        return NativeSwitchDatabase(row["database_id"], row["name"], row["generation"])

    def snapshot(self, owner_binding: NativeSwitchBinding, *, interval: NativeSwitchInterval) -> NativeSwitchSnapshot:
        """Read complete metadata before issuing any user-table content query."""
        database = self.database()
        if database != owner_binding.database:
            raise NativeSwitchRejected("database_binding_mismatch")
        objects = (owner_binding.target, owner_binding.prepared, owner_binding.switch_out)
        tables = tuple(
            parse_table(one(self._sql.query(TABLE_SQL, (obj.schema, obj.name)), {"metadata"})["metadata"])
            for obj in objects
        )
        snapshot = NativeSwitchSnapshot(database, tables[0], tables[1], tables[2], 0, 0, 0)
        # Admission with empty observations validates interval/types/identities
        # before identifiers are used in data SQL or datetime values are adapted.
        eligibility = plan_native_switch(snapshot, interval=interval, owner_binding=owner_binding)
        if eligibility.plan is None:
            raise NativeSwitchRejected(*eligibility.reasons)
        prepared = qualified(owner_binding, owner_binding.prepared)
        column = quote(interval.column)
        outside = count(
            self._sql,
            f"SELECT COUNT_BIG(*) AS row_count FROM {prepared} WHERE {column} IS NULL OR {column} < ? OR {column} >= ?",
            interval_parameters(interval),
        )
        prepared_rows = count(self._sql, f"SELECT COUNT_BIG(*) AS row_count FROM {prepared}")
        old_rows = count(
            self._sql, f"SELECT COUNT_BIG(*) AS row_count FROM {qualified(owner_binding, owner_binding.switch_out)}"
        )
        return NativeSwitchSnapshot(database, tables[0], tables[1], tables[2], prepared_rows, outside, old_rows)
