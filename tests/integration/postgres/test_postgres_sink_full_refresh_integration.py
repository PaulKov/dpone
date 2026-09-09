from __future__ import annotations

# ruff: noqa: E402
import os

import pytest

pytestmark = [pytest.mark.integration]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

pytest.importorskip("psycopg")

from psycopg import sql

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.postgres import PostgresSink


class DummyStateStorage:
    def save_state(self, *_args, **_kwargs) -> None:
        return None


def _load_config(schema: str, table: str, technical_columns: str = "required") -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="public",
        source_table=table,
        target_schema=schema,
        target_table=table,
        staging_schema=schema,
        load_strategy=LoadStrategy.FULL_REFRESH,
        log_sample_rows=0,
        options={"technical_columns": technical_columns},
    )


def test_postgres_sink_full_refresh_replaces_target_rows_and_populates_technical_columns(
    postgres_connector,
    postgres_schema: str,
) -> None:
    sink = PostgresSink(postgres_connector, DummyStateStorage())
    load_config = _load_config(postgres_schema, "customers", technical_columns="required")
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact(
            [
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob"},
            ]
        ),
        schema=[("id", "integer"), ("name", "text")],
    )

    first = sink.load(load_config, payload)
    assert first.inserted_rows == 2
    assert first.total_rows == 2
    assert first.staging_rows == 2

    rows = postgres_connector.get_records(
        sql.SQL(
            "SELECT id, name, __dpone__loaded_at IS NOT NULL AS has_load_dtm, __dpone__deleted_at FROM {}.{} ORDER BY id"
        ).format(sql.Identifier(postgres_schema), sql.Identifier("customers")),
        as_dict=True,
    )
    assert rows == [
        {"id": 1, "name": "alice", "has_load_dtm": True, "__dpone__deleted_at": None},
        {"id": 2, "name": "bob", "has_load_dtm": True, "__dpone__deleted_at": None},
    ]

    second = sink.load(
        load_config,
        LoadPayload(
            artifact=InMemoryRowsArtifact(
                [
                    {"id": 3, "name": "carol"},
                ]
            ),
            schema=[("id", "integer"), ("name", "text")],
        ),
    )
    assert second.inserted_rows == 1
    rows_after_refresh = postgres_connector.get_records(
        sql.SQL("SELECT id, name FROM {}.{} ORDER BY id").format(
            sql.Identifier(postgres_schema),
            sql.Identifier("customers"),
        ),
        as_dict=True,
    )
    assert rows_after_refresh == [{"id": 3, "name": "carol"}]


def test_postgres_sink_can_disable_technical_columns(postgres_connector, postgres_schema: str) -> None:
    sink = PostgresSink(postgres_connector, DummyStateStorage())
    load_config = _load_config(postgres_schema, "events", technical_columns="forbidden")

    result = sink.load(
        load_config,
        LoadPayload(
            artifact=InMemoryRowsArtifact(
                [
                    {"id": 10, "name": "created"},
                ]
            ),
            schema=[("id", "integer"), ("name", "text")],
        ),
    )
    assert result.inserted_rows == 1

    columns = postgres_connector.get_records(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (postgres_schema, "events"),
        as_dict=True,
    )
    column_names = [row["column_name"] for row in columns]
    assert column_names == ["id", "name"]
