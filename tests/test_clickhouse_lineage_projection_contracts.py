from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.lineage.audit import LoadAuditRecord
from dpone.runtime.lineage.options import LineageOptions
from dpone.runtime.sinks.clickhouse_lineage_projection import ClickHouseSinkSideLineageProjector


def test_clickhouse_sink_side_projection_defers_existing_target_lineage_columns() -> None:
    connector = _ClickHouseConnector()
    sink = _ClickHouseProjectionSink(connector, target_exists=True)
    load_config = _load_config(options={"lineage": {"enabled": True, "preset": "bulk_standard"}})
    handle = StagedLoadHandle(
        staging_config=replace(load_config, target_table="orders__dpone_staging_abcd"),
        payload_schema=(("id", "Int64"),),
        staged_rows=1,
    )

    result = ClickHouseSinkSideLineageProjector(sink).project(
        load_config=load_config,
        handle=handle,
        lineage_options=LineageOptions.from_config(load_config.options["lineage"]),
        load_record=_load_record(),
    )

    joined = "\n".join(connector.queries)
    assert "ALTER TABLE `landing`.`orders`" not in joined
    assert sink.target_lookups == 0
    assert result.handle.metadata["lineage_schema_columns"][:2] == (
        ("__dpone__run_id", "String"),
        ("__dpone__load_id", "String"),
    )


def test_clickhouse_sink_side_projection_uses_physical_types_for_payload_schema() -> None:
    connector = _ClickHouseConnector()
    sink = _ClickHouseProjectionSink(connector)
    load_config = _load_config(options={"lineage": {"enabled": True, "preset": "bulk_standard"}})
    handle = StagedLoadHandle(
        staging_config=replace(load_config, target_table="orders__dpone_staging_abcd"),
        payload_schema=(
            ("id", "bigint"),
            ("__dpone__loaded_at", "timestamp"),
            ("__dpone__extracted_at", "timestamp"),
        ),
        staged_rows=1,
    )

    ClickHouseSinkSideLineageProjector(sink).project(
        load_config=load_config,
        handle=handle,
        lineage_options=LineageOptions.from_config(load_config.options["lineage"]),
        load_record=_load_record(),
    )

    joined = "\n".join(connector.queries)
    assert "`id` Int64" in joined
    assert "`__dpone__loaded_at` DateTime64(6, 'UTC')" in joined
    assert "`__dpone__extracted_at` DateTime64(6, 'UTC')" in joined


def _load_config(**overrides: object) -> LoadConfig:
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={},
    )
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


def _load_record() -> LoadAuditRecord:
    return LoadAuditRecord(
        run_id="run_1",
        load_id="load_1",
        status="staged",
        process_name="orders",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        strategy="full_refresh",
        started_at=SimpleNamespace(),
    )


class _ClickHouseConnector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute_query(self, query: str, params: object = None) -> int:
        del params
        self.queries.append(query)
        return 0

    def get_records(self, query: str, params: object = None, as_dict: bool = False) -> list[tuple[str, str]]:
        del query, params, as_dict
        return [("__dpone__loaded_at", "DateTime64(6, 'UTC')")]


class _ClickHouseProjectionSink:
    def __init__(self, connector: _ClickHouseConnector, *, target_exists: bool = False) -> None:
        self.connector = connector
        self._target_exists = target_exists
        self.target_lookups = 0

    def _table(self, load_config: LoadConfig) -> str:
        return f"`{load_config.target_schema}`.`{load_config.target_table}`"

    def _table_exists(self, load_config: LoadConfig) -> bool:
        del load_config
        self.target_lookups += 1
        return self._target_exists

    def get_target_schema(self, load_config: LoadConfig) -> list[tuple[str, str]]:
        del load_config
        return [("__dpone__loaded_at", "DateTime64(6, 'UTC')")]

    def _operation_table_name(self, target_table: str, operation: str) -> str:
        return f"{target_table}__dpone_{operation}_test"

    @staticmethod
    def _map_type_for_config(dtype: str, load_config: LoadConfig) -> str:
        del load_config
        normalized = str(dtype).lower()
        if normalized in {"bigint", "int64"}:
            return "Int64"
        if normalized == "timestamp":
            return "DateTime64(6, 'UTC')"
        return str(dtype)

    def _create_table_with_clickhouse_types(
        self,
        load_config: LoadConfig,
        schema: tuple[tuple[str, str], ...],
        *,
        if_not_exists: bool,
    ) -> None:
        del if_not_exists
        columns_sql = ", ".join(f"`{column}` {dtype}" for column, dtype in schema)
        self.connector.execute_query(f"CREATE TABLE {self._table(load_config)} ({columns_sql}) ENGINE = Memory")
