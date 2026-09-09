from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pytest
from psycopg import sql

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import StagingTableArtifact
from dpone.runtime.sources.postgres import PostgresSource

pytestmark = [pytest.mark.integration, pytest.mark.integration_postgres_xmin]


class MemoryXMinStateStorage:
    def __init__(self) -> None:
        self.states: dict[tuple[str, str], Any] = {}

    def load_state(self, source_schema: str, source_table: str) -> Any | None:
        return self.states.get((source_schema, source_table))

    def save_state(self, source_schema: str, source_table: str, state: Any) -> None:
        self.states[(source_schema, source_table)] = state


class CapturingLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.messages: list[str] = []

    def log_xmin_state_info(self, message: str, payload: dict[str, Any]) -> None:
        self.events.append((message, payload))

    def log_etl_progress(self, event: str, payload: dict[str, Any]) -> None:
        self.events.append((event, payload))

    def info(self, message: str) -> None:
        self.messages.append(message)

    def warning(self, message: str) -> None:
        self.messages.append(message)


@dataclass
class CapturingStagingManager:
    rows: list[Mapping[str, object]]

    def create(self, load_config: LoadConfig, schema: Sequence[tuple[str, str]]) -> StagingTableArtifact:
        return StagingTableArtifact(
            schema=load_config.staging_schema,
            table="xmin_delta_staging",
            columns=[column for column, _ in schema],
            staging_manager=self,  # type: ignore[arg-type]
        )

    def insert_rows(self, artifact: StagingTableArtifact, rows: Iterable[Mapping[str, object]]) -> int:
        del artifact
        batch = list(rows)
        self.rows.extend(batch)
        return len(batch)

    def drop(self, artifact: StagingTableArtifact) -> None:
        del artifact


def _load_config(schema: str, *, options: dict[str, Any] | None = None) -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="postgres_target",
        source_schema=schema,
        source_table="orders",
        target_schema=schema,
        target_table="orders_target",
        staging_schema=schema,
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key="id",
        batch_size=1000,
        export_format="csv",
        compress_export=False,
        options={"batch_commit_mode": "whole", **(options or {})},
    )


def test_postgres_xmin_selector_extracts_baseline_and_incremental_delta(
    postgres_connector, postgres_schema: str
) -> None:
    postgres_connector.execute_query(
        sql.SQL("CREATE TABLE {}.orders (id integer PRIMARY KEY, name text NOT NULL, amount numeric(18,2))").format(
            sql.Identifier(postgres_schema),
        )
    )
    postgres_connector.execute_query(
        sql.SQL(
            "CREATE TABLE {}.orders_target (id integer PRIMARY KEY, name text NOT NULL, amount numeric(18,2))"
        ).format(
            sql.Identifier(postgres_schema),
        )
    )
    postgres_connector.execute_query(
        sql.SQL("INSERT INTO {}.orders (id, name, amount) VALUES (1, 'alpha', 10.50)").format(
            sql.Identifier(postgres_schema),
        )
    )

    state_storage = MemoryXMinStateStorage()
    logger = CapturingLogger()
    source = PostgresSource(postgres_connector, state_storage, logger, sink_connector=postgres_connector)
    config = _load_config(postgres_schema, options={"incremental_strategy": "xmin", "delta_size_threshold": 999999})

    selected = source._resolve_strategy(config)
    baseline = source.extract(config, None)
    try:
        assert selected is source._xmin_extract
        assert baseline.state is not None
        assert baseline.schema == [("id", "integer"), ("name", "text"), ("amount", "numeric(18,2)")]
        assert baseline.force_full_refresh is False
        state_storage.save_state(config.source_schema, config.source_table, baseline.state)
    finally:
        baseline.artifact.cleanup()

    postgres_connector.execute_query(
        sql.SQL("UPDATE {}.orders SET name = 'beta', amount = 11.75 WHERE id = 1").format(
            sql.Identifier(postgres_schema),
        )
    )
    postgres_connector.execute_query(
        sql.SQL("INSERT INTO {}.orders (id, name, amount) VALUES (2, 'gamma', 21.00)").format(
            sql.Identifier(postgres_schema),
        )
    )

    delta = source.extract(config, state_storage.load_state(config.source_schema, config.source_table))
    staging = CapturingStagingManager(rows=[])
    handle = delta.artifact.materialize(staging, config, delta.schema)

    assert delta.state is not None
    assert delta.force_full_refresh is False
    assert handle.row_count == 2
    assert {row["id"] for row in staging.rows} == {1, 2}
    assert any("__dpone__xmin" in row for row in staging.rows)
    assert any(event == "INCREMENTAL_STREAMING" for event, _ in logger.events)
    delta.artifact.cleanup()


def test_postgres_xmin_selector_rejects_conflicting_incremental_column(
    postgres_connector, postgres_schema: str
) -> None:
    source = PostgresSource(
        postgres_connector, MemoryXMinStateStorage(), CapturingLogger(), sink_connector=postgres_connector
    )
    config = _load_config(postgres_schema, options={"incremental_strategy": "xmin", "incremental_column": "updated_at"})

    with pytest.raises(ValueError, match="conflicts with source.options.incremental_column"):
        source._resolve_strategy(config)
