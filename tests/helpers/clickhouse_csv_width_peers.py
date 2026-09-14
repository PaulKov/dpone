"""External I/O peers for the public PostgreSQL CSV → ClickHouse regression.

Real connectors retain catalog access, COPY SQL/framing and file writing. Only
their public connection properties select these local driver peers. A child
process provides COPY bytes, including deliberate malformed-input fixtures.
"""

from __future__ import annotations

import io
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from dpone.runtime.connectors.clickhouse import ClickHouseConnector
from dpone.runtime.connectors.postgres import PostgresConnector


def render(statement: Any) -> str:
    return statement.as_string(None) if hasattr(statement, "as_string") else str(statement)


class CopyChild:
    def __init__(self, peer: PostgresPeer) -> None:
        self.peer = peer

    def __enter__(self) -> CopyChild:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import pathlib,sys;sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes())",
                str(self.peer.wire_path),
            ],
            capture_output=True,
            check=True,
            timeout=10,
        )
        self.peer.child_exit_codes.append(result.returncode)
        self.stream = io.BytesIO(result.stdout)
        return self

    def read(self) -> bytes:
        return self.stream.read(11)

    def __exit__(self, *_args: Any) -> bool:
        self.stream.close()
        return False


class PostgresCursor:
    rowcount = 0

    def __init__(self, peer: PostgresPeer) -> None:
        self.peer = peer

    def __enter__(self) -> PostgresCursor:
        return self

    def __exit__(self, *_args: Any) -> bool:
        return False

    def execute(self, query: Any, params: Any = None) -> None:
        self.query = render(query)
        self.peer.queries.append(self.query)

    def fetchall(self) -> list[dict[str, Any]]:
        if "information_schema.columns" in self.query:
            return [
                {
                    "column_name": name,
                    "data_type": dtype,
                    "catalog_declared_type": dtype,
                    "is_nullable": "YES",
                    "udt_schema": "pg_catalog",
                    "udt_name": "int4" if dtype == "integer" else "text",
                    "udt_kind": "b",
                    "udt_category": "N" if dtype == "integer" else "S",
                }
                for name, dtype in (("id", "integer"), ("value", "text"))
            ]
        if "txid_current_snapshot" in self.query:
            return [{"snapshot_token": "100:200:", "extraction_horizon": 200}]
        raise AssertionError(f"Unexpected PostgreSQL peer query: {self.query}")

    def copy(self, statement: Any, params: Any = ()) -> CopyChild:
        self.peer.copy_statements.append(render(statement))
        return CopyChild(self.peer)


class PostgresPeer:
    autocommit = True

    def __init__(self, wire_path: Path) -> None:
        self.wire_path = wire_path
        self.queries: list[str] = []
        self.copy_statements: list[str] = []
        self.child_exit_codes: list[int] = []
        self.transaction_events: list[str] = []

    def cursor(self) -> PostgresCursor:
        return PostgresCursor(self)

    def commit(self) -> None:
        self.transaction_events.append("commit")

    def rollback(self) -> None:
        self.transaction_events.append("rollback")


class LocalPostgresConnector(PostgresConnector):
    def __init__(self, peer: PostgresPeer) -> None:
        super().__init__("127.0.0.1", 5432, "synthetic", "synthetic", "")
        self.peer = peer

    @property
    def connection(self) -> PostgresPeer:
        return self.peer


class ClickHousePeer:
    """Minimal SQL peer for this two-column full-refresh route only."""

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.inserts: list[list[tuple[Any, ...]]] = []
        self.tables: dict[str, list[tuple[Any, ...]]] = {}
        self.load_statuses: list[str] = []

    def execute(self, query: Any, params: Any = None, **_kwargs: Any) -> list[Any]:
        query = str(query).strip()
        self.queries.append(query)
        if query.startswith("INSERT INTO"):
            if "`etl_state`." in query:
                if "__dpone__loads" in query:
                    self.load_statuses.extend(row[2] for row in params)
                return []
            assert "VALUES" in query
            rows = [tuple(row) for row in params]
            self.inserts.append(rows)
            self.tables[self.table_name(query)].extend(rows)
            return []
        if query.startswith("CREATE TABLE"):
            self.tables.setdefault(self.table_name(query), [])
            return []
        if query.startswith("CREATE DATABASE"):
            return []
        if query.startswith("ALTER TABLE `etl_state`."):
            return []
        if query.startswith("DROP TABLE"):
            self.tables.pop(self.table_name(query), None)
            return []
        if query.startswith("EXISTS TABLE"):
            return [(int(self.table_name(query) in self.tables),)]
        if query.startswith("RENAME TABLE"):
            for pair in query.removeprefix("RENAME TABLE ").split(", "):
                old, new = pair.split(" TO ")
                self.tables[new] = self.tables.pop(old)
            return []
        if "system.columns" in query:
            return [("id", "Int32"), ("value", "String")]
        if "count()" in query.lower():
            return [(len(self.tables[self.table_name(query)]),)]
        raise AssertionError(f"Unexpected ClickHouse peer query: {query}")

    @staticmethod
    def table_name(query: str) -> str:
        match = re.search(r"(`[^`]+`\.`[^`]+`)", query)
        assert match is not None, query
        return match.group(1)


class LocalClickHouseConnector(ClickHouseConnector):
    def __init__(self, peer: ClickHousePeer) -> None:
        super().__init__("127.0.0.1", 9000, "analytics", "synthetic", "")
        self.peer = peer

    @property
    def connection(self) -> ClickHousePeer:
        return self.peer
