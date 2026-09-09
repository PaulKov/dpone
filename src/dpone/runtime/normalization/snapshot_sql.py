"""SQL-backed child snapshot storage for nested reconciliation."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Protocol


class ChildSnapshotSqlExecutor(Protocol):
    """Minimal SQL executor needed by child snapshot stores."""

    def execute(self, statement: str, params: dict[str, object]) -> None: ...

    def fetch_value(self, statement: str, params: dict[str, object]) -> object: ...


class SqlChildSnapshotStore:
    """Durable child snapshot store backed by SQL state tables.

    The store intentionally depends on a tiny executor protocol instead of a
    concrete DB connector. MSSQL/Postgres/BigQuery state adapters can provide
    their native execution layer while nested normalization keeps one lifecycle
    contract: stage, commit, rollback and load committed keys.
    """

    def __init__(self, *, dialect: str, table: str, executor: ChildSnapshotSqlExecutor) -> None:
        self.dialect = str(dialect).lower()
        self.table = table
        self.executor = executor
        self._renderer = _SqlSnapshotRenderer(self.dialect, table)

    def load_committed(self, *, root_table: str, child_table: str) -> list[dict[str, object]]:
        payload = self.executor.fetch_value(
            self._renderer.select_committed(),
            {"root_table": root_table, "child_table": child_table},
        )
        if not payload:
            return []
        decoded = json.loads(str(payload))
        return [dict(item) for item in decoded if isinstance(item, dict)] if isinstance(decoded, list) else []

    def stage_snapshot(
        self,
        *,
        root_table: str,
        child_table: str,
        unique_key: Sequence[str],
        keys: Sequence[Mapping[str, object]],
        load_id: str,
    ) -> None:
        params = {
            "root_table": root_table,
            "child_table": child_table,
            "unique_key_json": json.dumps(list(unique_key), ensure_ascii=False),
            "keys_json": json.dumps([dict(item) for item in keys], ensure_ascii=False),
            "load_id": load_id,
        }
        for statement in self._renderer.stage():
            self.executor.execute(statement, params)

    def commit_snapshot(self, *, root_table: str, child_table: str, load_id: str) -> None:
        params = {"root_table": root_table, "child_table": child_table, "load_id": load_id}
        for statement in self._renderer.commit():
            self.executor.execute(statement, params)

    def rollback_staged(self, *, root_table: str, child_table: str, load_id: str) -> None:
        self.executor.execute(
            self._renderer.rollback(),
            {"root_table": root_table, "child_table": child_table, "load_id": load_id},
        )

    def ddl(self) -> str:
        return self._renderer.ddl()


class _SqlSnapshotRenderer:
    def __init__(self, dialect: str, table: str) -> None:
        self.dialect = dialect
        self.table = table

    def ddl(self) -> str:
        text_type = "NVARCHAR(MAX)" if self.dialect in {"mssql", "sqlserver"} else "TEXT"
        if self.dialect == "bigquery":
            text_type = "STRING"
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table} ("
            "root_table STRING, child_table STRING, unique_key_json "
            f"{text_type}, keys_json {text_type}, load_id STRING, status STRING, updated_at TIMESTAMP);"
        )

    def select_committed(self) -> str:
        return (
            f"SELECT keys_json FROM {self.table} WHERE root_table = %(root_table)s "
            "AND child_table = %(child_table)s AND status = 'committed'"
        )

    def stage(self) -> tuple[str, str]:
        return (
            f"DELETE FROM {self.table} WHERE root_table = %(root_table)s "
            "AND child_table = %(child_table)s AND status = 'staged'",
            f"INSERT INTO {self.table} "
            "(root_table, child_table, unique_key_json, keys_json, load_id, status, updated_at) "
            "VALUES (%(root_table)s, %(child_table)s, %(unique_key_json)s, %(keys_json)s, "
            "%(load_id)s, 'staged', CURRENT_TIMESTAMP)",
        )

    def commit(self) -> tuple[str, str]:
        return (
            f"DELETE FROM {self.table} WHERE root_table = %(root_table)s "
            "AND child_table = %(child_table)s AND status = 'committed'",
            f"UPDATE {self.table} SET status = 'committed', updated_at = CURRENT_TIMESTAMP "
            "WHERE root_table = %(root_table)s AND child_table = %(child_table)s "
            "AND load_id = %(load_id)s AND status = 'staged'",
        )

    def rollback(self) -> str:
        return (
            f"DELETE FROM {self.table} WHERE root_table = %(root_table)s "
            "AND child_table = %(child_table)s AND load_id = %(load_id)s AND status = 'staged'"
        )
