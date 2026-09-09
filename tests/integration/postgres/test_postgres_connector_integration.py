from __future__ import annotations

# ruff: noqa: E402
import os

import pytest

pytestmark = [pytest.mark.integration]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

pytest.importorskip("psycopg")

from psycopg import sql


def test_postgres_connector_executes_queries_and_streams_rows(postgres_connector, postgres_schema: str) -> None:
    table = "customers"

    postgres_connector.execute_query(
        sql.SQL("CREATE TABLE {}.{} (id integer PRIMARY KEY, name text NOT NULL)").format(
            sql.Identifier(postgres_schema),
            sql.Identifier(table),
        )
    )

    inserted = postgres_connector.execute_query(
        sql.SQL("INSERT INTO {}.{} (id, name) VALUES (%s, %s), (%s, %s)").format(
            sql.Identifier(postgres_schema),
            sql.Identifier(table),
        ),
        (1, "alice", 2, "bob"),
    )
    assert inserted == 2

    rows = postgres_connector.get_records(
        sql.SQL("SELECT id, name FROM {}.{} ORDER BY id").format(
            sql.Identifier(postgres_schema),
            sql.Identifier(table),
        ),
        as_dict=True,
    )
    assert rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]

    streamed = list(
        postgres_connector.get_records_streaming(
            sql.SQL("SELECT id, name FROM {}.{} ORDER BY id").format(
                sql.Identifier(postgres_schema),
                sql.Identifier(table),
            ),
            batch_size=1,
            as_dict=False,
        )
    )
    assert streamed == [[(1, "alice")], [(2, "bob")]]


def test_postgres_connector_copy_from_iter_loads_csv_rows(postgres_connector, postgres_schema: str) -> None:
    table = "copy_target"
    postgres_connector.execute_query(
        sql.SQL("CREATE TABLE {}.{} (id integer PRIMARY KEY, city text NOT NULL)").format(
            sql.Identifier(postgres_schema),
            sql.Identifier(table),
        )
    )

    copied = postgres_connector.copy_from_iter(
        postgres_schema,
        table,
        ["id", "city"],
        [
            "1,Paris\n",
            "2,Berlin\n",
        ],
    )
    assert copied == 2

    count = postgres_connector.get_records(
        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(sql.Identifier(postgres_schema), sql.Identifier(table))
    )
    assert count[0][0] == 2
