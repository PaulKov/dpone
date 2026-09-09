from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

from dpone.config import LoadConfig
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.sinks.clickhouse_physical_types import ClickHousePhysicalColumnTypeResolver
from dpone.runtime.sinks.clickhouse_row_values import ClickHouseRowValueCoercer
from dpone.runtime.sinks.clickhouse_sql_mixin import ClickHouseSqlMixin


@dataclass(frozen=True, slots=True)
class FakeDecision:
    target_type: str


class FailingSourceMapper:
    def resolve_column(self, column: str, source_type: str) -> FakeDecision:
        raise AssertionError(f"ClickHouse source type must not be remapped: {column} {source_type}")


def test_load_config_builder_preserves_source_and_sink_type_for_runtime_contracts() -> None:
    cfg = LoadConfigBuilder().build(
        {
            "source": {
                "type": "clickhouse",
                "connection_id": "clickhouse-src",
                "table": {"schema": "raw", "name": "orders"},
            },
            "sink": {
                "type": "clickhouse",
                "connection_id": "clickhouse-dst",
                "table": {"schema": "landing", "name": "orders"},
                "strategy": {"mode": "full_refresh"},
            },
        }
    )

    assert cfg.options["source_type"] == "clickhouse"
    assert cfg.options["sink_type"] == "clickhouse"


def test_clickhouse_source_physical_types_are_preserved_without_mssql_remapping() -> None:
    resolver = ClickHousePhysicalColumnTypeResolver()
    load_config = LoadConfig(
        source_conn_id="clickhouse-src",
        target_conn_id="clickhouse-dst",
        source_schema="raw",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={"source_type": "clickhouse"},
    )

    assert (
        resolver.resolve(
            load_config=load_config,
            type_mapper=FailingSourceMapper(),
            column="created_at",
            source_type="DateTime64(3)",
        )
        == "DateTime64(3)"
    )


def test_clickhouse_runtime_type_mapping_preserves_clickhouse_source_types_for_codec_paths() -> None:
    load_config = LoadConfig(
        source_conn_id="clickhouse-src",
        target_conn_id="clickhouse-dst",
        source_schema="raw",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={"source_type": "clickhouse"},
    )

    assert ClickHouseSqlMixin._map_type_for_config("Nullable(DateTime64(3))", load_config) == "Nullable(DateTime64(3))"


def test_clickhouse_row_value_coercer_converts_iso_strings_to_driver_native_values() -> None:
    coercer = ClickHouseRowValueCoercer()

    row = coercer.coerce_row(
        (
            "2026-06-19T09:01:06+00:00",
            "2026-06-19",
            "19.50",
            "true",
            42,
        ),
        (
            "DateTime64(6)",
            "Date",
            "Decimal(18,2)",
            "Bool",
            "String",
        ),
    )

    assert row == (
        datetime(2026, 6, 19, 9, 1, 6, tzinfo=UTC),
        date(2026, 6, 19),
        Decimal("19.50"),
        True,
        "42",
    )


def test_clickhouse_row_value_coercer_hex_encodes_bytes_for_string_columns() -> None:
    coercer = ClickHouseRowValueCoercer()
    assert coercer.coerce_value(b"\x01\x02\x03", "Nullable(String)") == "010203"
    assert coercer.coerce_value(memoryview(b"ab"), "String") == "6162"
