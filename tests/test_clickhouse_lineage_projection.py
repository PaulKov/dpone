from __future__ import annotations

from dataclasses import replace

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.sinks.clickhouse_lineage_projection import ClickHouseSinkSideLineageProjector


def test_clickhouse_lineage_projector_adds_core_columns_before_finalization() -> None:
    connector = _Connector(columns=[("id", "Int32"), ("name", "String")])
    projector = ClickHouseSinkSideLineageProjector(
        connector=connector,
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        operation_table_name=lambda table, operation: f"{table}__{operation}",
        create_table_with_clickhouse_types=connector.create_table,
    )
    load_config = _cfg()
    source_config = replace(load_config, target_table="orders__staging")

    result = projector.project(load_config, source_config)

    assert result.finalization_config.target_table == "orders__lineage"
    assert result.cleanup_config == result.finalization_config
    assert connector.created_columns == [
        ("id", "Int32"),
        ("name", "String"),
        ("__dpone__run_id", "String"),
        ("__dpone__load_id", "String"),
        ("__dpone__loaded_at", "DateTime64(6, 'UTC')"),
        ("__dpone__extracted_at", "DateTime64(6, 'UTC')"),
    ]
    assert "INSERT INTO `raw`.`orders__lineage`" in connector.executed[-1]
    assert "'run-1' AS `__dpone__run_id`" in connector.executed[-1]
    assert "'load-1' AS `__dpone__load_id`" in connector.executed[-1]


def test_clickhouse_lineage_projector_skips_when_lineage_disabled() -> None:
    connector = _Connector(columns=[("id", "Int32")])
    projector = ClickHouseSinkSideLineageProjector(
        connector=connector,
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        operation_table_name=lambda table, operation: f"{table}__{operation}",
        create_table_with_clickhouse_types=connector.create_table,
    )
    load_config = replace(_cfg(), options={"lineage": False})
    source_config = replace(load_config, target_table="orders__staging")

    result = projector.project(load_config, source_config)

    assert result.finalization_config is source_config
    assert result.cleanup_config is None
    assert connector.created_columns == []
    assert connector.executed == []


def _cfg() -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "__dpone_load_identity": {
                "run_id": "run-1",
                "load_id": "load-1",
                "loaded_at": "2026-06-29T00:00:00+00:00",
                "extracted_at": "2026-06-29T00:00:00+00:00",
            }
        },
    )


class _Connector:
    def __init__(self, *, columns: list[tuple[str, str]]) -> None:
        self.columns = columns
        self.created_columns: list[tuple[str, str]] = []
        self.executed: list[str] = []

    def get_records(self, query: str):
        assert "system.columns" in query
        return self.columns

    def execute_query(self, query: str) -> None:
        self.executed.append(query)

    def create_table(self, load_config: LoadConfig, schema, *, if_not_exists: bool) -> None:
        assert load_config.target_table == "orders__lineage"
        assert if_not_exists is False
        self.created_columns = list(schema)
