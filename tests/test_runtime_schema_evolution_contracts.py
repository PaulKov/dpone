from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.base import LoadResult


class _Logger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []
        self.errors: list[tuple[str, dict]] = []

    def log_etl_start(self, payload):
        del payload

    def log_etl_progress(self, event, payload):
        self.events.append((event, payload))

    def log_etl_error(self, message, payload):
        self.errors.append((message, payload))

    def log_etl_end(self, payload):
        del payload

    def info(self, message):
        del message

    def warning(self, message):
        del message


class _Source:
    def __init__(self, rows, schema):
        self.connector = object()
        self._result = SimpleNamespace(
            artifact=InMemoryRowsArtifact(rows),
            schema=schema,
            state=None,
            force_full_refresh=False,
        )

    def get_incremental_state(self, load_config):
        del load_config
        return None

    def extract(self, load_config, state):
        del load_config, state
        return self._result


class _EvolutionAwareSink:
    def __init__(self, target_schema):
        self.connector = SimpleNamespace()
        self.target_schema = target_schema
        self.applied = []
        self.received_payload = None

    def get_target_schema(self, load_config):
        del load_config
        return self.target_schema

    def apply_schema_plan(self, load_config, plan):
        self.applied.extend(plan.ddl_sql("postgres", f"{load_config.target_schema}.{load_config.target_table}"))

    def load(self, load_config, payload):
        del load_config
        self.received_payload = payload
        return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1)


def _load_config(**options) -> LoadConfig:
    options.setdefault("lineage", False)
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options=options,
    )


def test_schema_comparator_supports_generated_new_column_for_incompatible_type_change() -> None:
    plan = SchemaComparator(SchemaEvolutionPolicy(mode="widening", on_type_change="new_column")).compare(
        source=[ColumnDef("amount", "nvarchar(100)")],
        target=[ColumnDef("amount", "int")],
    )

    assert plan.has_breaking_changes is False
    assert plan.column_mapping == {"amount": "__dpone__nc__amount"}
    assert plan.ddl_sql("mssql", "dbo.orders") == [
        "ALTER TABLE [dbo].[orders] ADD [__dpone__nc__amount] nvarchar(100) NULL"
    ]
    assert plan.to_dict()["generated_columns"] == {"amount": "__dpone__nc__amount"}


def test_schema_comparator_rejects_reserved_source_columns_by_default() -> None:
    plan = SchemaComparator(SchemaEvolutionPolicy()).compare(
        source=[ColumnDef("__dpone__user_value", "text")],
        target=[],
    )

    assert plan.has_breaking_changes is True
    assert plan.changes[0].change_type == "reserved_column"
    assert "__dpone__" in plan.changes[0].reason


def test_schema_comparator_allows_temporal_offset_companion_columns() -> None:
    plan = SchemaComparator(SchemaEvolutionPolicy()).compare(
        source=[
            ColumnDef("offset_at", "datetimeoffset(7)"),
            ColumnDef("__dpone__tz_offset_minutes__offset_at", "smallint"),
        ],
        target=[ColumnDef("offset_at", "datetimeoffset(7)")],
    )

    assert plan.has_breaking_changes is False
    assert plan.ddl_sql("clickhouse", "landing.events") == [
        "ALTER TABLE `landing`.`events` ADD COLUMN `__dpone__tz_offset_minutes__offset_at` Int16"
    ]


def test_schema_comparator_treats_expected_mssql_clickhouse_types_as_compatible() -> None:
    plan = SchemaComparator(SchemaEvolutionPolicy()).compare(
        source=[
            ColumnDef("doc_movement_id", "int", nullable=True),
            ColumnDef("dm_base_zone_name", "nvarchar(510) nullable", nullable=True),
            ColumnDef("created_at", "datetime", nullable=True),
            ColumnDef("created2_at", "datetime2(7)", nullable=True),
            ColumnDef("amount", "decimal(18,2)", nullable=True),
        ],
        target=[
            ColumnDef("doc_movement_id", "Nullable(Int32)", nullable=True),
            ColumnDef("dm_base_zone_name", "Nullable(String)", nullable=True),
            ColumnDef("created_at", "Nullable(DateTime64(3))", nullable=True),
            ColumnDef("created2_at", "Nullable(DateTime64(7))", nullable=True),
            ColumnDef("amount", "Nullable(Decimal(18,2))", nullable=True),
        ],
    )

    assert plan.changes == []
    assert plan.has_breaking_changes is False


def test_etl_processor_does_not_report_artem_mssql_clickhouse_false_type_changes() -> None:
    """Regression for MSSQL -> ClickHouse schema-evolution false positives."""

    source_schema = [
        ("doc_movement_id", "int"),
        ("dm_base_zone_name", "nvarchar(510) nullable"),
        ("created_at", "datetime"),
        ("created2_at", "datetime2(7)"),
        ("amount", "decimal(18,2)"),
    ]
    sink = _EvolutionAwareSink(
        target_schema=[
            ("doc_movement_id", "Nullable(Int32)"),
            ("dm_base_zone_name", "Nullable(String)"),
            ("created_at", "Nullable(DateTime64(3))"),
            ("created2_at", "Nullable(DateTime64(7))"),
            ("amount", "Nullable(Decimal(18,2))"),
        ]
    )
    source = _Source(
        rows=[
            {
                "doc_movement_id": 1,
                "dm_base_zone_name": "north",
                "created_at": "2026-06-11 12:34:56.123",
                "created2_at": "2026-06-11 12:34:56.1234567",
                "amount": "10.25",
            }
        ],
        schema=source_schema,
    )

    ETLProcessor(source, sink, etl_logger=_Logger()).run(_load_config())

    assert sink.applied == []
    assert sink.received_payload.schema == source_schema


def test_schema_comparator_blocks_temporal_offset_companion_type_conflict() -> None:
    plan = SchemaComparator(SchemaEvolutionPolicy()).compare(
        source=[ColumnDef("__dpone__tz_offset_minutes__offset_at", "smallint")],
        target=[ColumnDef("__dpone__tz_offset_minutes__offset_at", "String")],
    )

    assert plan.has_breaking_changes is True
    assert plan.changes[0].change_type == "type_change"


def test_schema_plan_renders_safe_ddl_for_supported_dialects() -> None:
    plan = SchemaComparator(SchemaEvolutionPolicy(mode="widening")).compare(
        source=[ColumnDef("id", "bigint", nullable=False), ColumnDef("name", "varchar(128)")],
        target=[ColumnDef("id", "int", nullable=False)],
    )

    assert "ALTER TABLE [dbo].[orders] ADD [name] varchar(128) NULL" in plan.ddl_sql("mssql", "dbo.orders")
    assert 'ALTER TABLE "public"."orders" ADD COLUMN "name" varchar(128)' in plan.ddl_sql("postgres", "public.orders")
    assert "ALTER TABLE `landing`.`orders` ADD COLUMN `name` String" in plan.ddl_sql("clickhouse", "landing.orders")
    assert "ALTER TABLE `project.landing.orders` ADD COLUMN `name` STRING" in plan.ddl_sql(
        "bigquery", "project.landing.orders"
    )


def test_etl_processor_applies_default_schema_evolution_and_remaps_rows_to_generated_column() -> None:
    sink = _EvolutionAwareSink(target_schema=[("id", "bigint"), ("amount", "int")])
    source = _Source(
        rows=[{"id": 1, "amount": "9.99"}],
        schema=[("id", "bigint"), ("amount", "nvarchar(100)")],
    )
    cfg = _load_config(schema_evolution={"on_type_change": "new_column"})

    ETLProcessor(source, sink, etl_logger=_Logger()).run(cfg)

    assert sink.applied == ['ALTER TABLE "landing"."orders" ADD COLUMN "__dpone__nc__amount" nvarchar(100)']
    assert sink.received_payload.schema == [("id", "bigint"), ("__dpone__nc__amount", "nvarchar(100)")]
    assert sink.received_payload.artifact._rows == [{"id": 1, "__dpone__nc__amount": "9.99"}]


def test_etl_processor_projects_temporal_fidelity_before_schema_evolution() -> None:
    sink = _EvolutionAwareSink(target_schema=[("id", "bigint"), ("occurred_at", "timestamp")])
    source = _Source(
        rows=[{"id": 1, "occurred_at": "2026-06-09T12:30:00+03:00"}],
        schema=[("id", "bigint"), ("occurred_at", "timestamptz")],
    )
    cfg = _load_config(
        type_fidelity={"temporal": {"offset_timestamp": {"mode": "preserve_offset"}}},
    )

    ETLProcessor(source, sink, etl_logger=_Logger()).run(cfg)

    assert sink.applied == [
        'ALTER TABLE "landing"."orders" ADD COLUMN "__dpone__tz_offset_minutes__occurred_at" smallint nullable'
    ]
    assert sink.received_payload.schema == [
        ("id", "bigint"),
        ("occurred_at", "timestamptz"),
        ("__dpone__tz_offset_minutes__occurred_at", "smallint nullable"),
    ]
    assert sink.received_payload.artifact._rows[0]["occurred_at"].isoformat() == "2026-06-09T09:30:00"
    assert sink.received_payload.artifact._rows[0]["__dpone__tz_offset_minutes__occurred_at"] == 180


def test_etl_processor_can_disable_schema_evolution() -> None:
    sink = _EvolutionAwareSink(target_schema=[("id", "bigint")])
    source = _Source(rows=[{"id": 1, "new_value": "x"}], schema=[("id", "bigint"), ("new_value", "text")])
    cfg = _load_config(schema_evolution={"enabled": False})

    ETLProcessor(source, sink, etl_logger=_Logger()).run(cfg)

    assert sink.applied == []
    assert sink.received_payload.schema == [("id", "bigint"), ("new_value", "text")]


def test_manifest_json_schemas_expose_schema_evolution_options() -> None:
    config_schema = json.loads(open("src/dpone/schema/etl-config.schema.json", encoding="utf-8").read())
    batch_schema = json.loads(open("src/dpone/schema/etl-batch-manifest.schema.json", encoding="utf-8").read())

    for schema in (config_schema, batch_schema):
        sink_options = (
            schema["properties"]["sink"]["properties"]["options"]["properties"]
            if "sink" in schema.get("properties", {})
            else schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["options"]["properties"]
        )
        assert sink_options["schema_evolution"]["properties"]["enabled"]["default"] is True
        assert sink_options["schema_evolution"]["properties"]["on_type_change"]["enum"] == ["fail", "new_column"]
        assert sink_options["schema_evolution"]["properties"]["target_nullability"] == {
            "type": "string",
            "description": (
                "Existing-target nullability policy. accept_existing_nullable is an explicit MSSQL legacy-target "
                "compatibility exception: a nullable target may accept a non-null source without ALTER TABLE."
            ),
            "enum": ["strict", "accept_existing_nullable"],
            "default": "strict",
        }
        assert sink_options["lineage"]["default"]["preset"] == "standard"
        assert sink_options["lineage"]["properties"]["preset"]["enum"] == [
            "off",
            "minimal",
            "bulk_standard",
            "standard",
            "debug",
            "hierarchical",
        ]

        strategy = (
            schema["properties"]["sink"]["properties"]["strategy"]["properties"]
            if "sink" in schema.get("properties", {})
            else schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["strategy"]["properties"]
        )
        assert "snapshot_diff" in strategy["mode"]["enum"]
        assert "scd2" in strategy["mode"]["enum"]
        assert "cdc_apply" in strategy["mode"]["enum"]
        assert "backfill" in strategy["mode"]["enum"]
        assert "scd2" in strategy
        assert "backfill" in strategy


def test_runtime_apply_safe_false_blocks_before_loading_when_safe_changes_exist() -> None:
    sink = _EvolutionAwareSink(target_schema=[("id", "bigint")])
    source = _Source(rows=[{"id": 1, "name": "Ada"}], schema=[("id", "bigint"), ("name", "text")])
    cfg = _load_config(schema_evolution={"apply_safe": False})

    with pytest.raises(
        RuntimeError,
        match=r"DPONE_SCHEMA_EVOLUTION_BLOCKED: schema_evolution\.apply_safe",
    ):
        ETLProcessor(source, sink, etl_logger=_Logger()).run(cfg)

    assert sink.applied == []
    assert sink.received_payload is None
