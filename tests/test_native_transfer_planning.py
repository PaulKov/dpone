from __future__ import annotations

from datetime import UTC, datetime

from dpone.runtime.lineage.partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointStatus,
    build_transfer_partition_id,
)
from dpone.strategy_intelligence.native_transfer import (
    NativeTransferPlanBuilder,
    NativeTransferRequest,
)


def test_mssql_clickhouse_native_plan_prefers_bcp_to_direct_tsv() -> None:
    request = NativeTransferRequest(
        source_type="mssql",
        sink_type="clickhouse",
        source_table="dbo.orders",
        target_table="analytics.orders",
        strategy="incremental_merge",
        unique_key=("order_id",),
        source_options={
            "extract_mode": "bcp_queryout",
            "partitioning": {
                "strategy": "auto",
                "column": "order_id",
                "bounds": "auto",
                "target_rows_per_partition": 1_000_000,
                "max_partitions": 64,
                "export_workers": 8,
                "load_workers": 6,
            },
        },
        sink_options={
            "clickhouse_bulk": {
                "mode": "auto",
                "insert_settings": {
                    "async_insert": 1,
                    "max_insert_block_size": 1_000_000,
                    "input_format_parallel_parsing": 1,
                },
            },
        },
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.fast_path_id == "mssql_bcp_queryout_to_clickhouse_direct_tsv"
    assert plan.export_method == "bcp_queryout"
    assert plan.ingest_method == "clickhouse_direct_tsv"
    assert plan.finalizer == "lightweight_delete_insert"
    assert plan.native_ingest_settings["clickhouse_bulk"]["insert_settings"]["async_insert"] == 1
    assert plan.native_ingest_settings["clickhouse_bulk"]["insert_settings"]["wait_for_async_insert"] == 1
    assert plan.partitioning.column == "order_id"
    assert plan.partitioning.bounds_mode == "auto"
    assert plan.partitioning.load_workers == 6
    assert plan.retry_boundaries == ("export", "load_staging", "finalize")
    assert plan.partitioning.adaptive["enabled"] is False


def test_native_transfer_plan_includes_adaptive_partitioning_recommendations() -> None:
    request = NativeTransferRequest(
        source_type="mssql",
        sink_type="clickhouse",
        source_table="dbo.orders",
        target_table="analytics.orders",
        strategy="incremental_append",
        source_options={
            "partitioning": {
                "column": "order_id",
                "bounds": "auto",
                "target_rows_per_partition": 5000,
                "max_partitions": 16,
                "export_workers": 4,
                "load_workers": 4,
                "adaptive": {
                    "enabled": True,
                    "max_skew_ratio": 2.0,
                    "retry_split_factor": 4,
                    "observations": [
                        {"partition_id": "p0", "row_count": 1000, "bytes_count": 10_000, "duration_seconds": 1.0},
                        {"partition_id": "p1", "row_count": 9000, "bytes_count": 90_000, "duration_seconds": 7.0},
                    ],
                },
            },
        },
        sink_options={"clickhouse_bulk": {"mode": "http"}},
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.partitioning.adaptive["enabled"] is True
    assert plan.partitioning.adaptive["recommended_target_rows_per_partition"] == 2500
    assert plan.partitioning.adaptive["actions_by_partition"]["p1"] == "split_skewed_partition"
    assert any("skew" in warning for warning in plan.warnings)


def test_mssql_clickhouse_native_plan_reports_type_fidelity_decisions() -> None:
    request = NativeTransferRequest(
        source_type="mssql",
        sink_type="clickhouse",
        source_table="dbo.orders",
        target_table="analytics.orders",
        strategy="full_refresh",
        source_options={
            "source_schema": [
                ("amount", "decimal(18,2)"),
                ("trace_id", "uniqueidentifier"),
                ("payload", "varbinary(max)"),
            ],
            "partitioning": {"column": "id", "bounds": {"lower": 1, "upper": 10}, "num_partitions": 1},
        },
        sink_options={"clickhouse_bulk": {"mode": "http"}},
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.type_fidelity["profile"] == "mssql_to_clickhouse_lossless_v1"
    assert plan.type_fidelity["columns"]["amount"]["clickhouse_type"] == "Decimal(18,2)"
    assert plan.type_fidelity["columns"]["trace_id"]["clickhouse_type"] == "UUID"
    assert plan.type_fidelity["columns"]["payload"]["lossless"] is False


def test_mssql_clickhouse_native_plan_applies_type_fidelity_policy() -> None:
    request = NativeTransferRequest(
        source_type="mssql",
        sink_type="clickhouse",
        source_table="dbo.events",
        target_table="analytics.events",
        strategy="full_refresh",
        source_options={
            "source_schema": [("payload", "varbinary(max)"), ("business_time", "time(7)")],
            "type_fidelity": {"binary_encoding": "hex", "time_encoding": "seconds_since_midnight"},
            "partitioning": {"column": "id", "bounds": {"lower": 1, "upper": 10}, "num_partitions": 1},
        },
        sink_options={"clickhouse_bulk": {"mode": "http"}},
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.type_fidelity["policy"]["binary_encoding"] == "hex"
    assert plan.type_fidelity["policy"]["time_encoding"] == "seconds_since_midnight"
    assert plan.type_fidelity["policy"]["temporal"]["offset_timestamp"]["offset_timestamp_mode"] == "utc_instant"
    assert plan.type_fidelity["columns"]["payload"]["lossless"] is True
    assert plan.type_fidelity["columns"]["business_time"]["clickhouse_type"] == "UInt32"


def test_mssql_clickhouse_native_plan_reports_temporal_alias_warnings() -> None:
    request = NativeTransferRequest(
        source_type="mssql",
        sink_type="clickhouse",
        source_table="dbo.events",
        target_table="analytics.events",
        strategy="full_refresh",
        source_options={
            "source_schema": [("offset_at", "datetimeoffset(7)")],
            "type_fidelity": {"datetimeoffset": {"mode": "fixed_timezone", "timezone": "Europe/Moscow"}},
            "partitioning": {"column": "id", "bounds": {"lower": 1, "upper": 10}, "num_partitions": 1},
        },
        sink_options={"clickhouse_bulk": {"mode": "http"}},
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.type_fidelity["policy"]["temporal"]["offset_timestamp"]["offset_timestamp_mode"] == "fixed_timezone"
    assert plan.type_fidelity["columns"]["offset_at"]["clickhouse_type"] == "DateTime64(7, 'Europe/Moscow')"
    assert any("type_fidelity.datetimeoffset" in warning for warning in plan.warnings)


def test_generic_native_plan_reports_temporal_offset_policy() -> None:
    request = NativeTransferRequest(
        source_type="postgres",
        sink_type="clickhouse",
        source_table="public.events",
        target_table="analytics.events",
        strategy="incremental_append",
        source_options={
            "source_schema": [("event_id", "bigint"), ("occurred_at", "timestamptz")],
            "type_fidelity": {
                "temporal": {
                    "offset_timestamp": {
                        "mode": "preserve_offset",
                    }
                }
            },
            "partitioning": {"column": "event_id", "bounds": {"lower": 1, "upper": 10}, "num_partitions": 1},
        },
        sink_options={"clickhouse_bulk": {"mode": "http"}},
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.type_fidelity["profile"] == "temporal_offset_timestamp_v1"
    assert plan.type_fidelity["columns"]["occurred_at"]["target_type"] == "DateTime64(6, 'UTC')"
    assert plan.type_fidelity["columns"]["occurred_at"]["generated_column"] == (
        "__dpone__tz_offset_minutes__occurred_at"
    )
    assert plan.type_fidelity["generated_columns"] == {"occurred_at": "__dpone__tz_offset_minutes__occurred_at"}
    assert plan.type_fidelity["lossless"] is True
    assert any("preserve_offset" in warning for warning in plan.warnings)


def test_generic_native_plan_reports_fixed_timezone_as_non_lossless() -> None:
    request = NativeTransferRequest(
        source_type="api",
        sink_type="postgres",
        source_table="api/events",
        target_table="landing.events",
        strategy="incremental_append",
        source_options={
            "columns": [{"name": "occurred_at", "type": "iso8601_offset_timestamp"}],
            "type_fidelity": {
                "temporal": {
                    "offset_timestamp": {
                        "mode": "fixed_timezone",
                        "timezone": "Europe/Moscow",
                    }
                }
            },
            "partitioning": {"strategy": "single"},
        },
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.type_fidelity["columns"]["occurred_at"]["target_type"] == "timestamptz"
    assert plan.type_fidelity["columns"]["occurred_at"]["timezone"] == "Europe/Moscow"
    assert plan.type_fidelity["lossless"] is False
    assert any("fixed_timezone" in warning for warning in plan.warnings)


def test_generic_native_plan_reports_per_column_temporal_policy() -> None:
    request = NativeTransferRequest(
        source_type="api",
        sink_type="clickhouse",
        source_table="api/events",
        target_table="landing.events",
        strategy="incremental_append",
        source_options={
            "columns": [
                {"name": "created_at", "type": "iso8601_offset_timestamp"},
                {"name": "source_event_at", "type": "iso8601_offset_timestamp"},
            ],
            "type_fidelity": {
                "temporal": {
                    "offset_timestamp": {
                        "mode": "utc_instant",
                        "columns": {
                            "source_event_at": {"mode": "preserve_offset"},
                        },
                    }
                }
            },
            "partitioning": {"strategy": "single"},
        },
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.type_fidelity["columns"]["created_at"]["mode"] == "utc_instant"
    assert plan.type_fidelity["columns"]["source_event_at"]["mode"] == "preserve_offset"
    assert plan.type_fidelity["generated_columns"] == {"source_event_at": "__dpone__tz_offset_minutes__source_event_at"}


def test_generic_native_plan_reports_malformed_preserve_text_target_type() -> None:
    request = NativeTransferRequest(
        source_type="api",
        sink_type="clickhouse",
        source_table="api/events",
        target_table="landing.events",
        strategy="incremental_append",
        source_options={
            "columns": [{"name": "occurred_at", "type": "iso8601_offset_timestamp"}],
            "type_fidelity": {
                "temporal": {
                    "offset_timestamp": {
                        "mode": "utc_instant",
                        "malformed": "preserve_text",
                    }
                }
            },
            "partitioning": {"strategy": "single"},
        },
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.type_fidelity["columns"]["occurred_at"]["target_type"] == "String"
    assert plan.type_fidelity["columns"]["occurred_at"]["malformed"] == "preserve_text"


def test_mssql_clickhouse_native_plan_applies_optimizer_profile_to_both_legs() -> None:
    request = NativeTransferRequest(
        source_type="mssql",
        sink_type="clickhouse",
        source_table="dbo.orders",
        target_table="analytics.orders",
        strategy="full_refresh",
        source_options={
            "native_transfer": {"optimizer_profile": "high_throughput_safe"},
            "partitioning": {
                "column": "order_id",
                "bounds": {"lower": 1, "upper": 100},
                "num_partitions": 4,
            },
        },
        sink_options={
            "native_transfer": {"optimizer_profile": "high_throughput_safe"},
            "bulk": {"mode": "bcp"},
            "clickhouse_bulk": {"mode": "http"},
        },
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.native_ingest_settings["bulk"]["bcp"]["packet_size"] == 16_384
    assert plan.native_ingest_settings["bulk"]["bcp"]["batch_size"] == 250_000
    assert plan.native_ingest_settings["clickhouse_bulk"]["insert_settings"]["async_insert"] == 1
    assert plan.native_ingest_settings["clickhouse_bulk"]["http"]["chunk_size"] == 4 * 1024 * 1024


def test_native_transfer_plan_uses_single_partition_when_no_safe_key() -> None:
    request = NativeTransferRequest(
        source_type="mssql",
        sink_type="clickhouse",
        source_table="dbo.events",
        target_table="analytics.events",
        strategy="full_refresh",
        source_options={"partitioning": {"strategy": "auto"}},
        sink_options={},
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.partitioning.strategy == "single"
    assert plan.partitioning.max_partitions == 1
    assert any("No safe partition column" in warning for warning in plan.warnings)


def test_transfer_partition_id_is_deterministic_and_hash_sensitive() -> None:
    first = build_transfer_partition_id(
        source_table="dbo.orders",
        target_table="analytics.orders",
        strategy="incremental_merge",
        query_hash="query-a",
        schema_hash="schema-a",
        partition_bounds={"lower": 0, "upper": 1000},
    )
    second = build_transfer_partition_id(
        source_table="dbo.orders",
        target_table="analytics.orders",
        strategy="incremental_merge",
        query_hash="query-a",
        schema_hash="schema-a",
        partition_bounds={"lower": 0, "upper": 1000},
    )
    changed = build_transfer_partition_id(
        source_table="dbo.orders",
        target_table="analytics.orders",
        strategy="incremental_merge",
        query_hash="query-b",
        schema_hash="schema-a",
        partition_bounds={"lower": 0, "upper": 1000},
    )

    assert first == second
    assert len(first) == 64
    assert first != changed


def test_committed_partition_checkpoint_matches_only_same_hashes() -> None:
    checkpoint = PartitionCheckpoint(
        transfer_partition_id="a" * 64,
        status=PartitionCheckpointStatus.COMMITTED,
        query_hash="query-a",
        schema_hash="schema-a",
        source_table="dbo.orders",
        target_table="analytics.orders",
        partition_bounds={"lower": 0, "upper": 1000},
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        completed_at=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert checkpoint.can_skip(query_hash="query-a", schema_hash="schema-a")
    assert not checkpoint.can_skip(query_hash="query-b", schema_hash="schema-a")
    assert not checkpoint.can_skip(query_hash="query-a", schema_hash="schema-b")


def test_postgres_mssql_native_plan_uses_copy_to_bcp_and_delete_insert_default() -> None:
    request = NativeTransferRequest(
        source_type="postgres",
        sink_type="mssql",
        source_table="public.orders",
        target_table="landing.orders",
        strategy="incremental_merge",
        unique_key=("order_id",),
        source_options={
            "export_format": "mssql-delimited",
            "partitioning": {
                "strategy": "auto",
                "column": "order_id",
                "bounds": "auto",
                "target_rows_per_partition": 1_000_000,
                "max_partitions": 64,
                "export_workers": 8,
                "load_workers": 4,
            },
        },
        sink_options={
            "bulk": {"mode": "bcp", "bcp": {"batch_size": 100_000, "packet_size": 65_535}},
        },
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.fast_path_id == "postgres_copy_to_mssql_bcp"
    assert plan.export_method == "copy_to_stdout"
    assert plan.ingest_method == "mssql_bcp"
    assert plan.finalizer == "delete_insert"
    assert plan.partitioning.load_workers == 4
    assert plan.native_ingest_settings["bulk"]["mode"] == "bcp"
    assert plan.native_ingest_settings["bulk"]["bcp"]["packet_size"] == 16_384
    assert plan.transport_contract["route"] == "postgres_to_mssql"
    assert plan.transport_contract["wire_format"] == "mssql-delimited"
    assert plan.transport_contract["text_codec"] == "BulkTextCodec"
    assert plan.transport_contract["null_policy"] == "empty_bcp_field_is_null"
    assert plan.transport_contract["empty_string_policy"] == "encoded_marker_roundtrip"
    assert plan.transport_contract["lossless"] is True
    assert plan.to_dict()["transport_contract"]["lossless"] is True


def test_postgres_mssql_native_plan_warns_when_contract_is_not_lossless() -> None:
    request = NativeTransferRequest(
        source_type="postgres",
        sink_type="mssql",
        source_table="public.orders",
        target_table="dbo.orders",
        strategy="full_refresh",
        source_options={
            "export_format": "csv",
            "compress_export": True,
            "partitioning": {
                "column": "order_id",
                "bounds": {"lower": 1, "upper": 10},
                "num_partitions": 2,
                "load_workers": 2,
            },
        },
        sink_options={"bulk": {"mode": "pyodbc"}},
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.transport_contract["route"] == "postgres_to_mssql"
    assert plan.transport_contract["lossless"] is False
    assert plan.transport_contract["wire_format"] == "csv"
    assert "Postgres -> MSSQL native fast path expects source.options.export_format=mssql-delimited." in plan.warnings
    assert "Postgres -> MSSQL bcp native fast path cannot load gzip artifacts directly." in plan.warnings
    assert "Postgres -> MSSQL native fast path expects sink.options.bulk.mode=bcp." in plan.warnings


def test_native_transfer_plan_reports_deprecated_sink_parallel_load_workers() -> None:
    request = NativeTransferRequest(
        source_type="mssql",
        sink_type="clickhouse",
        source_table="dbo.orders",
        target_table="analytics.orders",
        strategy="incremental_merge",
        source_options={"partitioning": {"column": "id", "bounds": {"lower": 1, "upper": 10}, "num_partitions": 2}},
        sink_options={"parallel_load_workers": 2},
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.partitioning.load_workers == 2
    assert any("parallel_load_workers is deprecated" in warning for warning in plan.warnings)
