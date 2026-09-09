from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.runtime.bulk_options import BulkOptionsResolver, ClickHouseBulkOptionsResolver
from dpone.runtime.partitioning import RangePartitioner
from dpone.runtime.partitioning_options import PartitioningOptionsResolver


def test_partitioning_options_prefer_nested_canonical_workers() -> None:
    resolved = PartitioningOptionsResolver.resolve(
        {
            "partition_column": "legacy_id",
            "partition_workers": 99,
            "parallel_load_workers": 99,
            "partitioning": {
                "column": "order_id",
                "bounds": {"lower": 1, "upper": 100},
                "num_partitions": 4,
                "export_workers": 3,
                "load_workers": 2,
            },
        }
    )

    assert resolved.column == "order_id"
    assert resolved.export_workers == 3
    assert resolved.load_workers == 2


def test_partitioning_options_accept_statistics_planner_policy() -> None:
    resolved = PartitioningOptionsResolver.resolve(
        {
            "partitioning": {
                "strategy": "stats",
                "column": "id",
                "planner": {
                    "mode": "statistics",
                    "stats_source": "dmv",
                    "skew_policy": "split_hot_ranges",
                    "max_hot_partition_factor": 2.5,
                    "min_partition_rows": 10_000,
                    "null_bucket": "separate",
                },
            }
        }
    )

    assert resolved.strategy == "stats"
    assert resolved.planner.mode == "statistics"
    assert resolved.planner.stats_source == "dmv"
    assert resolved.planner.max_hot_partition_factor == 2.5
    assert resolved.planner.min_partition_rows == 10_000
    assert resolved.deprecated_aliases == ()


def test_partitioning_options_support_legacy_aliases_with_warnings() -> None:
    resolved = PartitioningOptionsResolver.resolve(
        {
            "partition_column": "id",
            "lower_bound": 1,
            "upper_bound": 10,
            "num_partitions": 3,
            "partition_workers": 2,
            "parallel_load_workers": 2,
        }
    )

    assert resolved.column == "id"
    assert resolved.export_workers == 2
    assert resolved.load_workers == 2
    assert "source.options.partition_workers" in resolved.deprecated_aliases
    assert any("partitioning.load_workers" in warning for warning in resolved.warnings)


def test_range_partitioner_uses_canonical_load_workers_and_clamps_to_partitions() -> None:
    partitioner = RangePartitioner.from_options(
        {
            "partitioning": {
                "column": "id",
                "bounds": {"lower": 1, "upper": 10},
                "num_partitions": 3,
                "export_workers": 8,
                "load_workers": 7,
            }
        }
    )

    assert partitioner.max_workers == 3
    assert partitioner.load_workers == 3


def test_bulk_options_prefer_nested_bcp_config() -> None:
    resolved = BulkOptionsResolver.resolve(
        {
            "bulk_mode": "legacy",
            "bcp_batch_size": 1,
            "bulk": {
                "mode": "bcp",
                "bcp": {
                    "batch_size": 100_000,
                    "packet_size": 65_535,
                    "table_lock": True,
                    "timeout_seconds": 3600,
                },
            },
        }
    )

    assert resolved.mode == "bcp"
    assert resolved.bcp.batch_size == 100_000
    assert resolved.bcp.packet_size == 16_384
    assert resolved.bcp.table_lock is True
    assert resolved.bcp.timeout_seconds == 3600
    assert resolved.deprecated_aliases == ()


def test_bulk_options_support_flat_legacy_aliases_with_warnings() -> None:
    resolved = BulkOptionsResolver.resolve(
        {
            "bulk_mode": "bcp",
            "bcp_batch_size": 50_000,
            "bcp_packet_size": 16_384,
            "bcp_table_lock": "yes",
        }
    )

    assert resolved.mode == "bcp"
    assert resolved.bcp.batch_size == 50_000
    assert resolved.bcp.packet_size == 16_384
    assert resolved.bcp.table_lock is True
    assert "sink.options.bcp_batch_size" in resolved.deprecated_aliases
    assert any("bulk.bcp.batch_size" in warning for warning in resolved.warnings)


def test_bulk_options_give_ordinary_mssql_bcp_a_finite_process_deadline() -> None:
    resolved = BulkOptionsResolver.resolve({})
    explicit_none = BulkOptionsResolver.resolve({"bulk": {"bcp": {"timeout_seconds": None}}})

    assert resolved.bcp.timeout_seconds == 3600
    assert explicit_none.bcp.timeout_seconds == 3600
    assert (
        resolved.bcp.to_bcp_options(
            bcp_path="bcp",
            trust_server_certificate=False,
        ).timeout_seconds
        == 3600
    )


@pytest.mark.parametrize("value", [0, -1, True, False])
def test_bulk_options_reject_invalid_explicit_bcp_deadline(value: object) -> None:
    with pytest.raises(ValueError, match="sink.options.bulk.bcp.timeout_seconds must be a positive integer"):
        BulkOptionsResolver.resolve({"bulk": {"bcp": {"timeout_seconds": value}}})


def test_bulk_options_apply_high_throughput_native_transfer_profile() -> None:
    resolved = BulkOptionsResolver.resolve(
        {
            "native_transfer": {"optimizer_profile": "high_throughput_safe"},
            "bulk": {"mode": "bcp"},
        }
    )

    assert resolved.bcp.batch_size == 250_000
    assert resolved.bcp.packet_size == 16_384
    assert resolved.bcp.timeout_seconds == 3600


def test_bulk_options_profile_never_overrides_explicit_bcp_values() -> None:
    resolved = BulkOptionsResolver.resolve(
        {
            "native_transfer": {"optimizer_profile": "high_throughput_safe"},
            "bulk": {
                "mode": "bcp",
                "bcp": {
                    "batch_size": 10_000,
                    "packet_size": 32_768,
                    "timeout_seconds": 120,
                },
            },
        }
    )

    assert resolved.bcp.batch_size == 10_000
    assert resolved.bcp.packet_size == 16_384
    assert resolved.bcp.timeout_seconds == 120


def test_clickhouse_bulk_options_prefer_nested_config_over_legacy_aliases() -> None:
    resolved = ClickHouseBulkOptionsResolver.resolve(
        {
            "clickhouse_bulk_mode": "python",
            "clickhouse_insert_settings": {"async_insert": 0},
            "clickhouse_bulk": {
                "mode": "http",
                "http": {"host": "clickhouse.local", "port": 8123, "chunk_size": 2_097_152},
                "insert_settings": {"async_insert": 1},
            },
        }
    )

    assert resolved.mode == "http"
    assert resolved.http.host == "clickhouse.local"
    assert resolved.http.chunk_size == 2_097_152
    assert resolved.insert_settings == {"async_insert": 1, "wait_for_async_insert": 1}
    assert resolved.deprecated_aliases == ()


def test_clickhouse_bulk_options_apply_high_throughput_profile() -> None:
    resolved = ClickHouseBulkOptionsResolver.resolve(
        {
            "native_transfer": {"optimizer_profile": "high_throughput_safe"},
            "clickhouse_bulk": {"mode": "http"},
        }
    )

    assert resolved.insert_settings["async_insert"] == 1
    assert resolved.insert_settings["wait_for_async_insert"] == 1
    assert resolved.insert_settings["input_format_parallel_parsing"] == 1
    assert resolved.insert_settings["max_insert_block_size"] == 1_000_000
    assert resolved.http.chunk_size == 4 * 1024 * 1024


def test_clickhouse_bulk_options_profile_never_overrides_explicit_values() -> None:
    resolved = ClickHouseBulkOptionsResolver.resolve(
        {
            "native_transfer": {"optimizer_profile": "high_throughput_safe"},
            "clickhouse_bulk": {
                "mode": "http",
                "http": {"chunk_size": 512},
                "insert_settings": {"async_insert": 0, "max_insert_block_size": 10_000},
            },
        }
    )

    assert resolved.insert_settings["async_insert"] == 0
    assert resolved.insert_settings["max_insert_block_size"] == 10_000
    assert resolved.http.chunk_size == 512


def test_clickhouse_bulk_options_support_legacy_aliases_with_warnings() -> None:
    resolved = ClickHouseBulkOptionsResolver.resolve(
        {
            "clickhouse_bulk_mode": "http",
            "clickhouse_http_host": "localhost",
            "clickhouse_http_chunk_size": 1024,
            "clickhouse_insert_settings": {"max_threads": 4},
        }
    )

    assert resolved.mode == "http"
    assert resolved.http.host == "localhost"
    assert resolved.http.chunk_size == 1024
    assert resolved.insert_settings == {"max_threads": 4}
    assert "sink.options.clickhouse_bulk_mode" in resolved.deprecated_aliases
    assert any("clickhouse_bulk.http.host" in warning for warning in resolved.warnings)


def test_manifest_schemas_accept_canonical_transfer_option_namespaces() -> None:
    for schema_path in (
        Path("src/dpone/schema/etl-config.schema.json"),
        Path("src/dpone/schema/etl-batch-manifest.schema.json"),
    ):
        schema = json.loads(schema_path.read_text())
        serialized = json.dumps(schema)

        assert '"partitioning"' in serialized
        assert '"bulk"' in serialized
        assert '"clickhouse_bulk"' in serialized
        assert '"nullability"' in serialized
