"""Narrow ClickHouse catalog and DDL adapter for full-refresh publication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ClickHousePublicationTable:
    """Catalog identity required by the publication state classifier."""

    name: str
    uuid: str
    engine: str
    comment: str


class ClickHouseFullRefreshCatalog:
    """Render publication SQL and project exact catalog observations."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def database_engine(self, database: str) -> str | None:
        rows = self._connector.get_records(
            f"SELECT engine FROM system.databases WHERE name = {_literal(database)} LIMIT 1"
        )
        return str(rows[0][0]) if rows else None

    def tables(self, database: str, names: tuple[str, ...]) -> dict[str, ClickHousePublicationTable]:
        if not names:
            return {}
        rendered_names = ", ".join(_literal(name) for name in names)
        rows = self._connector.get_records(
            "SELECT name, toString(uuid), engine, comment FROM system.tables "
            f"WHERE database = {_literal(database)} AND name IN ({rendered_names}) ORDER BY name"
        )
        return {
            str(name): ClickHousePublicationTable(
                name=str(name),
                uuid=str(uuid),
                engine=str(engine),
                comment=str(comment or ""),
            )
            for name, uuid, engine, comment in rows
        }

    def create_marker(self, database: str, marker: str, comment: str) -> None:
        self._connector.execute_query(
            f"CREATE TABLE {_qualified(database, marker)} (operation_id String) "
            f"ENGINE = TinyLog COMMENT {_literal(comment)}"
        )

    def exchange(self, database: str, target: str, candidate: str, *, query_id: str) -> None:
        self._connector.execute_query(
            f"EXCHANGE TABLES {_qualified(database, target)} AND {_qualified(database, candidate)}",
            query_id=query_id,
        )

    def rename(self, database: str, candidate: str, target: str, *, query_id: str) -> None:
        self._connector.execute_query(
            f"RENAME TABLE {_qualified(database, candidate)} TO {_qualified(database, target)}",
            query_id=query_id,
        )

    def publication_query_active(self, query_id: str) -> bool:
        rows = self._connector.get_records(
            f"SELECT query_id FROM system.processes WHERE query_id = {_literal(query_id)} LIMIT 1"
        )
        return bool(rows)

    def drop(self, database: str, table: str) -> None:
        self._connector.execute_query(f"DROP TABLE {_qualified(database, table)}")

    def count(self, database: str, table: str) -> int:
        rows = self._connector.get_records(f"SELECT count() FROM {_qualified(database, table)}")
        return int(rows[0][0]) if rows else 0


def _qualified(database: str, table: str) -> str:
    return f"`{database.replace('`', '``')}`.`{table.replace('`', '``')}`"


def _literal(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


__all__ = ["ClickHouseFullRefreshCatalog", "ClickHousePublicationTable"]
