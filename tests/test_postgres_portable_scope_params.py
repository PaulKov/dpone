"""Parameter forwarding proof for guarded PostgreSQL COPY scopes."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from psycopg import sql

from dpone.runtime.connectors.postgres import PostgresConnector
from dpone.runtime.sources.strategies.postgres.postgres_mssql_source_value_guard import (
    PostgresMssqlSourceValueGuard,
)


def test_fused_guarded_copy_preserves_composable_query_and_immutable_parameters(tmp_path) -> None:
    params = (17,)
    query = sql.SQL("SELECT timestamp_value FROM source WHERE partition_id = %s")
    observed: dict[str, Any] = {}

    class Copy:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def read():
            return b""

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def copy(statement, received_params):
            observed["copy_statement"] = statement
            observed["copy_params"] = received_params
            return Copy()

    class Connection:
        @staticmethod
        def cursor():
            return Cursor()

    connector = SimpleNamespace(connection=Connection())
    guarded = PostgresMssqlSourceValueGuard(connector).guard_copy_query(
        query,
        (("timestamp_value", "timestamp without time zone"),),
    )
    PostgresConnector.copy_to_file(
        connector,
        guarded.query_sql,
        str(tmp_path / "scope.csv"),
        compress=False,
        params=params,
    )

    assert observed["copy_params"] is params
    statement = observed["copy_statement"]
    render = getattr(statement, "as_string", None)
    rendered = render(None) if callable(render) else str(statement)
    assert "%s" in rendered
    assert "partition_id = 17" not in rendered
    assert "dpone_pg_mssql_copy_guard_v1_" in rendered
