from __future__ import annotations

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.mssql_physical_design import MssqlPhysicalDesignContract
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_state import PhysicalTableState
from dpone.runtime.etl.lifecycle import RuntimeLifecycleService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sinks.mssql_table_ddl import MssqlTableDdlRenderer, MssqlTableDesign


def test_mssql_table_ddl_renderer_inlines_page_compression_on_create() -> None:
    design = MssqlTableDesign(compression="PAGE")
    sql = MssqlTableDdlRenderer().render_create_table(
        table="[dwh_example].[ch].[marketing__sample_web_sync]",
        column_definitions=["[id] bigint NULL", "[created_at] datetime2(3) NULL"],
        design=design,
    )

    assert sql.startswith("CREATE TABLE [dwh_example].[ch].[marketing__sample_web_sync]")
    assert "WITH (DATA_COMPRESSION = PAGE)" in sql
    assert "REBUILD" not in sql


def test_mssql_table_ddl_renderer_supports_filegroup_and_fillfactor() -> None:
    design = MssqlTableDesign(
        compression="ROW",
        filegroup="DATA",
        textimage_filegroup="LOB",
        index_fillfactor=90,
    )
    statements = MssqlTableDdlRenderer().render_create_table_statements(
        table="[landing].[orders]",
        column_definitions=["[id] bigint NOT NULL"],
        design=design,
        primary_key=["id"],
    )

    assert "ON [DATA] TEXTIMAGE_ON [LOB]" in statements[0]
    assert "WITH (DATA_COMPRESSION = ROW)" in statements[0]
    assert "PRIMARY KEY CLUSTERED ([id])" in statements[1]
    assert "DATA_COMPRESSION = ROW, FILLFACTOR = 90" in statements[1]
    assert statements[1].endswith("ON [DATA];")


def test_physical_design_planner_emits_inline_mssql_compression() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="mssql",
        table="dwh_example.ch.marketing__sample_web_sync",
        source_schema=[("id", "bigint"), ("created_at", "datetime2(3)")],
        options=PhysicalDesignOptions.from_config(
            {"storage": {"mssql": {"compression": "page", "clustered_columnstore": False}}}
        ),
    )

    joined = "\n".join(plan.ddl)
    assert "CREATE TABLE [dwh_example].[ch].[marketing__sample_web_sync]" in joined
    assert "WITH (DATA_COMPRESSION = PAGE)" in joined
    assert "REBUILD" not in joined
    assert plan.risks == ()


def test_mssql_physical_state_from_plan_exposes_storage_settings() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="mssql",
        table="landing.orders",
        source_schema=[("id", "bigint")],
        options=PhysicalDesignOptions.from_config({"storage": {"mssql": {"compression": "page"}}}),
    )

    state = PhysicalTableState.from_physical_plan(plan)

    assert state.table_settings["compression"] == "PAGE"
    assert state.table_settings["clustered_columnstore"] is False


class _Connector:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self._exists = False

    def table_exists(self, schema: str, table: str, *, database: str | None = None) -> bool:
        del schema, table, database
        return self._exists

    def quote_identifier(self, name: str) -> str:
        return f"[{name}]"

    def execute_query(self, query: str, *params: object) -> None:
        del params
        self.queries.append(query)


def test_mssql_sink_delegates_missing_target_physical_ddl_to_strategy_owner() -> None:
    connector = _Connector()
    sink = MSSQLSink(connector)
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="default",
        source_table="sample_web_sync",
        target_schema="ch",
        target_table="marketing__sample_web_sync",
        target_database="DWH_Stage",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "sink_type": "mssql",
            "physical_design": {
                "apply_runtime": True,
                "storage": {"mssql": {"compression": "page"}},
            },
        },
    )
    payload = LoadPayload(
        artifact=object(),
        schema=[("id", "bigint"), ("created_at", "datetime2(3)")],
    )

    report = RuntimeLifecycleService()._apply_physical_design(load_config, sink, payload)

    assert report is not None
    assert report["applied"] is False
    assert report["delegated_to_strategy_target_creation"] is True
    assert report["table"] == "DWH_Stage.ch.marketing__sample_web_sync"
    assert report["blockers"] == []
    assert connector.queries == []


def test_mssql_load_strategy_create_shadow_table_uses_physical_design() -> None:
    from dpone.runtime.sinks.strategies.mssql.mssql_load_strategies import MSSQLFullRefreshStrategy

    connector = _Connector()
    strategy = MSSQLFullRefreshStrategy(connector, object(), object())
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="default",
        source_table="sample_web_sync",
        target_schema="ch",
        target_table="marketing__sample_web_sync",
        target_database="DWH_Stage",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"physical_design": {"storage": {"mssql": {"compression": "page"}}}},
    )

    shadow_table = strategy._create_shadow_table(
        load_config,
        [("id", "bigint"), ("created_at", "datetime2(3)")],
    )

    assert shadow_table.startswith("marketing__sample_web_sync__dpone_shadow_")
    assert connector.queries
    create_sql = connector.queries[-1]
    assert "CREATE TABLE" in create_sql
    assert "WITH (DATA_COMPRESSION = PAGE)" in create_sql


def test_mssql_table_design_rejects_invalid_compression() -> None:
    with pytest.raises(ValueError, match="compression must be one of"):
        MssqlTableDesign.from_options({"physical_design": {"storage": {"mssql": {"compression": "invalid"}}}})


def test_mssql_physical_design_does_not_consume_transfer_partitioning() -> None:
    contract = MssqlPhysicalDesignContract.from_options(
        {
            "partitioning": {
                "strategy": "range",
                "column": "id",
                "bounds": {"lower": 1, "upper": 100},
                "num_partitions": 4,
            },
            "bulk": {"mode": "bcp"},
        }
    )

    assert contract == MssqlPhysicalDesignContract()


def test_mssql_physical_design_rejects_explicit_partitioning() -> None:
    with pytest.raises(
        ValueError,
        match="physical_design.partitioning is not supported",
    ):
        MssqlPhysicalDesignContract.from_options({"physical_design": {"partitioning": {"column": "id"}}})


def test_mssql_physical_introspector_quotes_schema_literals_for_compact_labels() -> None:
    from dpone.runtime.sinks.mssql_physical_introspection import MssqlPhysicalIntrospector

    class Connector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def get_records_in_database(self, database: str, query: str) -> list[tuple[str]]:
            self.queries.append(query)
            return [("PAGE",)]

    connector = Connector()
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="marketing_datamarts",
        source_table="sample_web_sync",
        target_schema="dwh_example.ch",
        target_table="marketing__sample_web_sync",
        target_database="dwh_example",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )

    assert MssqlPhysicalIntrospector(connector)._compression(load_config) == "PAGE"
    assert "WHERE s.name = 'ch'" in connector.queries[0]
    assert "AND t.name = 'marketing__sample_web_sync'" in connector.queries[0]
    assert "WHERE s.name = ch" not in connector.queries[0]


def test_mssql_physical_introspector_qualifies_sys_catalogs_without_db_helper() -> None:
    """Production MSSQL connector has get_records only — qualify [db].sys.*."""
    from dpone.runtime.sinks.mssql_physical_introspection import MssqlPhysicalIntrospector

    class Connector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def get_records(self, query: str, params: object = None) -> list[tuple[str]]:
            self.queries.append(query)
            return [("PAGE",)]

    connector = Connector()
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="marketing_datamarts",
        source_table="sample_web_sync",
        target_schema="ch",
        target_table="marketing__sample_web_sync",
        target_database="DWH_Stage",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )

    assert MssqlPhysicalIntrospector(connector)._compression(load_config) == "PAGE"
    assert "[DWH_Stage].sys.partitions" in connector.queries[0]
    assert "FROM sys.partitions" not in connector.queries[0]
