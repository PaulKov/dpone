from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig
from dpone.runtime.artifacts import InMemoryRowsArtifact, StreamingRowsArtifact
from dpone.runtime.connectors.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse_sql_mixin import ClickHouseSqlMixin
from dpone.runtime.sinks.clickhouse_staging_decoder import ClickHouseStagingDecoder
from dpone.runtime.support.temporal_fidelity import (
    TemporalFidelityPolicy,
    TemporalFidelityProjector,
    TemporalProjectionError,
    offset_minutes_column_name,
)
from dpone.runtime.support.type_mapping.mssql_clickhouse import (
    MssqlClickHouseTypeMapper,
    MssqlClickHouseTypePolicy,
    schema_with_temporal_companion_columns,
)
from dpone.strategy_intelligence.typed_reconciliation import TypedColumnSpec, TypedRowHashService


def test_mssql_clickhouse_type_mapper_preserves_exact_types() -> None:
    decisions = MssqlClickHouseTypeMapper().resolve_schema(
        [
            ("amount", "decimal(18, 2)"),
            ("trace_id", "uniqueidentifier"),
            ("legacy_created_at", "datetime"),
            ("created_at", "datetime2(7)"),
            ("rounded_at", "smalldatetime"),
            ("payload", "varbinary(max)"),
        ]
    )

    assert decisions["amount"].clickhouse_type == "Decimal(18,2)"
    assert decisions["amount"].lossless is True
    assert decisions["trace_id"].clickhouse_type == "UUID"
    assert decisions["legacy_created_at"].clickhouse_type == "DateTime64(3)"
    assert decisions["created_at"].clickhouse_type == "DateTime64(7)"
    assert decisions["rounded_at"].clickhouse_type == "DateTime64(0)"
    assert decisions["payload"].clickhouse_type == "String"
    assert decisions["payload"].lossless is False


@pytest.mark.parametrize("source_type", ("date", "datetime", "datetime2(7)"))
def test_mssql_clickhouse_temporal_mapping_requires_observed_target_range(source_type: str) -> None:
    decision = MssqlClickHouseTypeMapper().resolve_column("value", source_type)

    assert decision.lossless is False
    assert "range" in decision.reason


def test_mssql_clickhouse_type_mapper_honors_binary_and_time_policy() -> None:
    decisions = MssqlClickHouseTypeMapper(
        MssqlClickHouseTypePolicy(binary_encoding="hex", time_encoding="seconds_since_midnight")
    ).resolve_schema(
        [
            ("payload", "varbinary(max)"),
            ("business_time", "time(7)"),
        ]
    )

    assert decisions["payload"].clickhouse_type == "String"
    assert decisions["payload"].lossless is True
    assert "hex" in decisions["payload"].reason
    assert decisions["business_time"].clickhouse_type == "UInt32"
    assert decisions["business_time"].lossless is True


def test_mssql_clickhouse_type_mapper_treats_timestamp_as_rowversion_binary() -> None:
    decision = MssqlClickHouseTypeMapper(
        MssqlClickHouseTypePolicy(binary_encoding="hex", time_encoding="seconds_since_midnight")
    ).resolve_column(
        "row_version",
        "timestamp",
    )

    assert decision.clickhouse_type == "String"
    assert decision.lossless is True
    assert "binary values encoded as hex" == decision.reason


def test_typed_row_hash_treats_mssql_timestamp_as_rowversion_binary() -> None:
    columns = (TypedColumnSpec("row_version", "timestamp"),)
    service = TypedRowHashService(
        columns,
        policy=MssqlClickHouseTypePolicy(binary_encoding="hex", time_encoding="seconds_since_midnight"),
    )

    source_hash = service.hash_rows([(bytes.fromhex("000000000001DEE2"),)])
    target_hash = service.hash_rows([("000000000001DEE2",)])

    assert source_hash == target_hash


def test_clickhouse_staging_decoder_uses_load_config_type_policy() -> None:
    class FakeConnector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> None:
            self.queries.append(query)

    class FakeCodec:
        def clickhouse_decode_expression(self, value_sql: str) -> str:
            return f"decode({value_sql})"

    connector = FakeConnector()
    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={"type_fidelity": {"time_encoding": "seconds_since_midnight", "binary_encoding": "hex"}},
    )
    staging_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders__staging",
    )
    decoded_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders__decoded",
    )
    decoder = ClickHouseStagingDecoder(
        connector=connector,
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        create_staging_table=lambda _cfg, _schema: decoded_config,
        map_type=ClickHouseSqlMixin._map_type_for_config,
    )

    decoder.prepare(
        load_config,
        staging_config,
        LoadPayload(
            artifact=SimpleNamespace(bulk_text_codec=FakeCodec()),
            schema=[("business_time", "time(7)"), ("description", "nvarchar(max)")],
        ),
    )

    query = connector.queries[-1]
    assert "`business_time`, decode(`description`) AS `description`" in query
    assert "decode(`business_time`)" not in query


def test_clickhouse_runtime_map_type_uses_lossless_mssql_decimal() -> None:
    assert ClickHouseSqlMixin._map_type("decimal(18, 2)") == "Decimal(18,2)"
    assert ClickHouseSqlMixin._map_type("numeric(38, 9)") == "Decimal(38,9)"
    assert ClickHouseSqlMixin._map_type("uniqueidentifier") == "UUID"
    assert ClickHouseSqlMixin._map_type("boolean") == "Bool"
    assert ClickHouseSqlMixin._map_type("numeric") == "Decimal(38,9)"


def test_typed_row_hash_is_stable_across_decimal_and_datetime_representations() -> None:
    columns = (
        TypedColumnSpec("id", "int"),
        TypedColumnSpec("amount", "decimal(18,2)"),
        TypedColumnSpec("created_at", "datetime2(7)"),
    )
    service = TypedRowHashService(columns)

    source_hash = service.hash_rows([(1, Decimal("1.1400"), "2026-06-09 10:00:00.1200000")])
    target_hash = service.hash_rows([(1, 1.1400000000000001, "2026-06-09T10:00:00.12")])

    assert source_hash == target_hash


def test_typed_row_hash_supports_binary_and_time_policy() -> None:
    columns = (
        TypedColumnSpec("payload", "varbinary(max)"),
        TypedColumnSpec("business_time", "time(7)"),
    )
    policy = MssqlClickHouseTypePolicy(binary_encoding="hex", time_encoding="seconds_since_midnight")
    service = TypedRowHashService(columns, policy=policy)

    source_hash = service.hash_rows([(b"\x00\xff", "01:02:03.0000000")])
    target_hash = service.hash_rows([("00ff", 3723)])

    assert source_hash == target_hash


def test_typed_row_hash_places_fraction_before_timezone_suffix() -> None:
    columns = (TypedColumnSpec("offset_at", "datetimeoffset(7)"),)
    service = TypedRowHashService(columns)

    source_hash = service.hash_rows([("2025-12-31 21:00:01.0000000",)])
    target_hash = service.hash_rows([(datetime(2025, 12, 31, 21, 0, 1, tzinfo=UTC),)])

    assert source_hash == target_hash


def test_clickhouse_tsv_codec_exports_binary_and_time_policy_for_bcp_queryout() -> None:
    policy = MssqlClickHouseTypePolicy(binary_encoding="hex", time_encoding="seconds_since_midnight")
    codec = ClickHouseTabSeparatedCodec(type_policy=policy)

    binary_expression = codec.mssql_select_expression(
        "dpone_src.[payload]",
        text_column=False,
        source_type="varbinary(max)",
    )
    time_expression = codec.mssql_select_expression(
        "dpone_src.[business_time]",
        text_column=False,
        source_type="time(7)",
    )
    offset_expression = codec.mssql_select_expression(
        "dpone_src.[offset_at]",
        text_column=False,
        source_type="datetimeoffset(7)",
    )

    assert (
        "CONVERT(NVARCHAR(MAX), CONVERT(VARCHAR(MAX), CONVERT(VARBINARY(MAX), dpone_src.[payload]), 2))"
        in binary_expression
    )
    assert "DATEDIFF(SECOND" in time_expression
    assert "SWITCHOFFSET" in offset_expression
    assert "+00:00" in offset_expression


def test_clickhouse_tsv_codec_exports_naive_timestamps_as_epoch_ticks_by_default() -> None:
    codec = ClickHouseTabSeparatedCodec()

    datetime_expression = codec.mssql_select_expression(
        "dpone_src.[created_at]",
        text_column=False,
        source_type="datetime",
    )
    datetime2_expression = codec.mssql_select_expression(
        "dpone_src.[created_at]",
        text_column=False,
        source_type="datetime2(7)",
    )

    assert "DATEDIFF_BIG(SECOND" in datetime_expression
    assert "* 1000" in datetime_expression
    assert "DATEPART(NANOSECOND" in datetime_expression
    assert "'T'" not in datetime_expression
    assert "DATEDIFF_BIG(SECOND" in datetime2_expression
    assert "* 10000000" in datetime2_expression
    assert "/ 100" in datetime2_expression


def test_clickhouse_tsv_codec_can_export_naive_timestamps_as_text_without_iso_t() -> None:
    codec = ClickHouseTabSeparatedCodec(
        MssqlClickHouseTypePolicy.from_config({"temporal": {"naive_timestamp": {"transfer_encoding": "text"}}})
    )

    expression = codec.mssql_select_expression(
        "dpone_src.[created_at]",
        text_column=False,
        source_type="datetime2(7)",
    )

    assert "CONVERT(VARCHAR(MAX), CAST(dpone_src.[created_at] AS datetime2(7)), 126)" in expression
    assert "REPLACE(" in expression
    assert "'T'" in expression
    assert "' '" in expression


def test_clickhouse_runtime_create_table_uses_configured_order_by() -> None:
    class FakeConnector:
        host = "localhost"
        port = 9000
        database = "default"
        user = "default"
        password = ""
        application_name = "test"
        secure = False
        compression = False
        connect_timeout = 1
        send_receive_timeout = 1
        settings = {}

        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> None:
            self.queries.append(query)

    class Sink(ClickHouseSqlMixin):
        def __init__(self) -> None:
            self.connector = FakeConnector()
            self.logger = SimpleNamespace()

    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        options={
            "physical_design": {
                "storage": {
                    "clickhouse": {
                        "partition_by": "toYYYYMM(created_at)",
                        "order_by": ["created_at", "order_id"],
                    }
                }
            }
        },
    )
    sink = Sink()

    sink._create_table(load_config, [("order_id", "int"), ("created_at", "datetime")], if_not_exists=True)

    ddl = sink.connector.queries[-1]
    assert "PARTITION BY toYYYYMM(created_at)" in ddl
    assert "ORDER BY (`created_at`, `order_id`)" in ddl
    assert "ORDER BY tuple()" not in ddl


def test_temporal_fidelity_policy_parses_canonical_and_aliases() -> None:
    canonical = TemporalFidelityPolicy.from_config(
        {"temporal": {"offset_timestamp": {"mode": "fixed_timezone", "timezone": "Europe/Moscow"}}}
    )
    alias = TemporalFidelityPolicy.from_config({"datetimeoffset": {"mode": "preserve_offset"}})
    legacy = TemporalFidelityPolicy.from_config(
        {"datetimeoffset_mode": "fixed_timezone", "datetimeoffset_timezone": "Europe/Moscow"}
    )

    assert canonical.offset_timestamp_mode == "fixed_timezone"
    assert canonical.target_timezone == "Europe/Moscow"
    assert alias.offset_timestamp_mode == "preserve_offset"
    assert alias.warnings
    assert legacy.offset_timestamp_mode == "fixed_timezone"
    assert legacy.warnings


def test_mssql_clickhouse_datetimeoffset_modes_map_explicitly() -> None:
    modes = {
        "utc_instant": ("DateTime64(7, 'UTC')", False),
        "fixed_timezone": ("DateTime64(7, 'Europe/Moscow')", False),
        "preserve_offset": ("DateTime64(7, 'UTC')", False),
        "preserve_text": ("String", True),
    }
    for mode, (expected_type, expected_lossless) in modes.items():
        policy = MssqlClickHouseTypePolicy.from_config(
            {"temporal": {"offset_timestamp": {"mode": mode, "timezone": "Europe/Moscow"}}}
        )
        decision = MssqlClickHouseTypeMapper(policy).resolve_column("offset_at", "datetimeoffset(7)")
        assert decision.clickhouse_type == expected_type
        assert decision.lossless is expected_lossless


def test_mssql_clickhouse_preserve_offset_adds_companion_column() -> None:
    policy = MssqlClickHouseTypePolicy.from_config({"temporal": {"offset_timestamp": {"mode": "preserve_offset"}}})

    schema = schema_with_temporal_companion_columns((("offset_at", "datetimeoffset(7)"),), policy)
    nullable_schema = schema_with_temporal_companion_columns((("offset_at", "datetimeoffset(7) nullable"),), policy)

    assert schema == [("offset_at", "datetimeoffset(7)"), ("__dpone__tz_offset_minutes__offset_at", "smallint")]
    assert nullable_schema[-1] == ("__dpone__tz_offset_minutes__offset_at", "smallint nullable")
    assert offset_minutes_column_name("offset_at") == "__dpone__tz_offset_minutes__offset_at"


def test_clickhouse_tsv_codec_renders_datetimeoffset_modes() -> None:
    fixed = ClickHouseTabSeparatedCodec(
        MssqlClickHouseTypePolicy.from_config(
            {"temporal": {"offset_timestamp": {"mode": "fixed_timezone", "timezone": "Europe/Moscow"}}}
        )
    ).mssql_select_expression("dpone_src.[offset_at]", text_column=False, source_type="datetimeoffset(7)")
    preserve_text = ClickHouseTabSeparatedCodec(
        MssqlClickHouseTypePolicy.from_config({"temporal": {"offset_timestamp": {"mode": "preserve_text"}}})
    ).mssql_select_expression("dpone_src.[offset_at]", text_column=False, source_type="datetimeoffset(7)")
    preserve_offset_codec = ClickHouseTabSeparatedCodec(
        MssqlClickHouseTypePolicy.from_config({"temporal": {"offset_timestamp": {"mode": "preserve_offset"}}})
    )

    assert "AT TIME ZONE N'Russian Standard Time'" in fixed
    assert "datetime2(7)" in fixed
    assert "CONVERT(VARCHAR(MAX), CAST(dpone_src.[offset_at] AS datetimeoffset), 126)" in preserve_text
    fixed_offset = ClickHouseTabSeparatedCodec(
        MssqlClickHouseTypePolicy.from_config(
            {"temporal": {"offset_timestamp": {"mode": "fixed_timezone", "timezone": "+03:00"}}}
        )
    ).mssql_select_expression("dpone_src.[offset_at]", text_column=False, source_type="datetimeoffset(7)")
    generated = preserve_offset_codec.mssql_generated_select_expressions(
        "offset_at", "dpone_src.[offset_at]", "datetimeoffset(7)"
    )
    assert "SWITCHOFFSET" in fixed_offset
    assert generated[0][0] == "__dpone__tz_offset_minutes__offset_at"
    assert "DATEPART(TZOFFSET" in generated[0][1]


def test_typed_row_hash_supports_offset_timestamp_modes() -> None:
    columns = (TypedColumnSpec("offset_at", "datetimeoffset(7)"),)
    fixed_policy = MssqlClickHouseTypePolicy.from_config(
        {"temporal": {"offset_timestamp": {"mode": "fixed_timezone", "timezone": "Europe/Moscow"}}}
    )
    preserve_policy = MssqlClickHouseTypePolicy.from_config(
        {"temporal": {"offset_timestamp": {"mode": "preserve_offset"}}}
    )

    fixed_service = TypedRowHashService(columns, policy=fixed_policy)
    preserve_service = TypedRowHashService(columns, policy=preserve_policy)
    source_value = datetime(2026, 6, 9, 12, 30, tzinfo=timezone(timedelta(hours=3)))

    assert fixed_service.hash_rows([(source_value,)]) == fixed_service.hash_rows([("2026-06-09T12:30:00",)])
    assert preserve_service.hash_rows([(source_value,)]) == preserve_service.hash_rows([(source_value,)])


def test_typed_row_hash_supports_generic_offset_timestamp_source_types() -> None:
    columns = (
        TypedColumnSpec("pg_ts", "timestamptz"),
        TypedColumnSpec("api_ts", "iso8601_offset_timestamp"),
    )
    policy = MssqlClickHouseTypePolicy.from_config({"temporal": {"offset_timestamp": {"mode": "preserve_offset"}}})
    service = TypedRowHashService(columns, policy=policy)

    source_hash = service.hash_rows([("2026-06-09T12:30:00+03:00", "2026-06-09T06:00:00-05:30")])
    target_hash = service.hash_rows(
        [
            (
                datetime(2026, 6, 9, 12, 30, tzinfo=timezone(timedelta(hours=3))),
                datetime(2026, 6, 9, 6, 0, tzinfo=timezone(timedelta(hours=-5, minutes=-30))),
            )
        ]
    )

    assert source_hash == target_hash


def test_temporal_projector_materializes_preserve_offset_for_in_memory_rows() -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact(
            [
                {"id": 1, "occurred_at": "2026-06-09T12:30:00+03:00"},
                {
                    "id": 2,
                    "occurred_at": datetime(2026, 6, 9, 1, 15, tzinfo=timezone(timedelta(hours=-5, minutes=-30))),
                },
                {"id": 3, "occurred_at": None},
            ]
        ),
        schema=[("id", "bigint"), ("occurred_at", "timestamptz")],
    )
    policy = TemporalFidelityPolicy.from_config({"temporal": {"offset_timestamp": {"mode": "preserve_offset"}}})

    projected = TemporalFidelityProjector(policy).project_payload(payload)

    assert projected.schema == [
        ("id", "bigint"),
        ("occurred_at", "timestamptz"),
        ("__dpone__tz_offset_minutes__occurred_at", "smallint nullable"),
    ]
    rows = projected.artifact._rows
    assert rows[0]["occurred_at"] == datetime(2026, 6, 9, 9, 30)
    assert rows[0]["__dpone__tz_offset_minutes__occurred_at"] == 180
    assert rows[1]["occurred_at"] == datetime(2026, 6, 9, 6, 45)
    assert rows[1]["__dpone__tz_offset_minutes__occurred_at"] == -330
    assert rows[2]["occurred_at"] is None
    assert rows[2]["__dpone__tz_offset_minutes__occurred_at"] is None


def test_temporal_projector_applies_fixed_timezone_to_streaming_rows() -> None:
    payload = LoadPayload(
        artifact=StreamingRowsArtifact(iter([{"occurred_at": "2026-06-09T12:30:00+03:00"}])),
        schema=[("occurred_at", "iso8601_offset_timestamp")],
    )
    policy = TemporalFidelityPolicy.from_config(
        {"temporal": {"offset_timestamp": {"mode": "fixed_timezone", "timezone": "Europe/Moscow"}}}
    )

    projected = TemporalFidelityProjector(policy).project_payload(payload)

    assert projected.schema == [("occurred_at", "timestamp")]
    assert list(projected.artifact._iterator) == [{"occurred_at": datetime(2026, 6, 9, 12, 30)}]


def test_temporal_projector_preserve_text_keeps_original_offset_text() -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"occurred_at": datetime(2026, 6, 9, 12, 30, tzinfo=UTC)}]),
        schema=[("occurred_at", "timestamptz")],
    )
    policy = TemporalFidelityPolicy.from_config({"temporal": {"offset_timestamp": {"mode": "preserve_text"}}})

    projected = TemporalFidelityProjector(policy).project_payload(payload)

    assert projected.schema == [("occurred_at", "string")]
    assert projected.artifact._rows == [{"occurred_at": "2026-06-09T12:30:00+00:00"}]


def test_temporal_projector_applies_per_column_policy_overrides() -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact(
            [
                {
                    "created_at": "2026-06-09T12:30:00+03:00",
                    "business_at": "2026-06-09T12:30:00+03:00",
                    "raw_at": "2026-06-09T12:30:00+03:00",
                }
            ]
        ),
        schema=[
            ("created_at", "timestamptz"),
            ("business_at", "timestamptz"),
            ("raw_at", "iso8601_offset_timestamp"),
        ],
    )
    policy = TemporalFidelityPolicy.from_config(
        {
            "temporal": {
                "offset_timestamp": {
                    "mode": "utc_instant",
                    "columns": {
                        "business_at": {"mode": "fixed_timezone", "timezone": "Europe/Moscow"},
                        "raw_at": {"mode": "preserve_text"},
                    },
                }
            }
        }
    )

    projected = TemporalFidelityProjector(policy).project_payload(payload)

    assert projected.schema == [
        ("created_at", "timestamp"),
        ("business_at", "timestamp"),
        ("raw_at", "string"),
    ]
    row = projected.artifact._rows[0]
    assert row["created_at"] == datetime(2026, 6, 9, 9, 30)
    assert row["business_at"] == datetime(2026, 6, 9, 12, 30)
    assert row["raw_at"] == "2026-06-09T12:30:00+03:00"


def test_temporal_projector_malformed_fail_and_preserve_text_modes() -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"occurred_at": "not-a-timestamp"}]),
        schema=[("occurred_at", "iso8601_offset_timestamp")],
    )
    fail_policy = TemporalFidelityPolicy.from_config({"temporal": {"offset_timestamp": {"mode": "utc_instant"}}})
    text_policy = TemporalFidelityPolicy.from_config(
        {"temporal": {"offset_timestamp": {"mode": "utc_instant", "malformed": "preserve_text"}}}
    )

    with pytest.raises(TemporalProjectionError, match="occurred_at"):
        TemporalFidelityProjector(fail_policy).project_payload(payload)

    projected = TemporalFidelityProjector(text_policy).project_payload(payload)
    assert projected.schema == [("occurred_at", "string")]
    assert projected.artifact._rows == [{"occurred_at": "not-a-timestamp"}]
