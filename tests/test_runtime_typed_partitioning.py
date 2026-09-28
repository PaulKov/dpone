from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from uuid import UUID

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import PartitionedFileExportArtifact
from dpone.runtime.native_transfer_execution import NativeTransferResourcePolicy
from dpone.runtime.native_transfer_slicing import RangeSlicePlanner, TransferSlice
from dpone.runtime.partitioning import RangePartition, RangePartitioner
from dpone.runtime.partitioning_bounds import PartitionBoundaryTypeResolver, PartitionBoundKind
from dpone.runtime.partitioning_options import PartitioningOptionsResolver
from dpone.runtime.partitioning_predicates import MssqlPartitionPredicateRenderer
from dpone.runtime.sources.strategies.mssql import MSSQLFullExtractStrategy


class CapturingLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def info(self, message: str) -> None:
        del message

    def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
        self.events.append((event, payload))


def test_range_partitioner_auto_bounds_supports_date_values_without_strategy_override() -> None:
    partitioner = RangePartitioner.from_options(
        {
            "partitioning": {
                "strategy": "auto",
                "column": "doc_date",
                "bounds": "auto",
                "num_partitions": 3,
            }
        },
        bounds_resolver=lambda _column: (date(2024, 1, 1), date(2024, 1, 10), 9),
    )

    partitions = partitioner.partitions()

    assert partitioner.enabled
    assert [(p.lower_bound, p.upper_bound, p.include_upper) for p in partitions] == [
        (date(2024, 1, 1), date(2024, 1, 4), False),
        (date(2024, 1, 4), date(2024, 1, 7), False),
        (date(2024, 1, 7), date(2024, 1, 10), True),
    ]


def test_range_partitioner_manual_bounds_can_use_spark_stride_edges() -> None:
    partitions = RangePartitioner.from_options(
        {
            "partitioning": {
                "strategy": "range",
                "column": "order_id",
                "bounds": {"lower": 10, "upper": 20},
                "num_partitions": 2,
                "planner": {"bounds_role": "stride"},
            }
        }
    ).partitions()

    assert [(p.lower_bound, p.upper_bound, p.include_lower, p.include_upper) for p in partitions] == [
        (None, 15, True, False),
        (15, None, True, False),
    ]
    assert partitions[0].predicate('"order_id"') == '"order_id" < 15'
    assert partitions[1].predicate('"order_id"') == '"order_id" >= 15'


def test_range_partitioner_creates_separate_null_partition_when_bounds_report_nulls() -> None:
    partitions = RangePartitioner.from_options(
        {
            "partitioning": {
                "strategy": "auto",
                "column": "order_id",
                "bounds": "auto",
                "num_partitions": 2,
                "planner": {"null_bucket": "separate"},
            }
        },
        bounds_resolver=lambda _column: (1, 10, 10, 2),
    ).partitions()

    assert partitions[0].is_null_partition
    assert partitions[0].predicate('"order_id"') == '"order_id" IS NULL'
    assert [(p.index, p.lower_bound, p.upper_bound) for p in partitions[1:]] == [(1, 1, 6), (2, 6, 10)]


def test_transfer_slice_serializes_temporal_bounds_for_json_evidence() -> None:
    item = TransferSlice(
        partition_index=0,
        slice_index=0,
        lower_bound=date(2024, 1, 1),
        upper_bound=datetime(2024, 1, 2, 3, 4, 5),
        include_upper=True,
        is_null_partition=False,
    )

    assert item.to_dict() == {
        "partition_index": 0,
        "slice_index": 0,
        "lower_bound": "2024-01-01",
        "upper_bound": "2024-01-02T03:04:05",
        "include_upper": True,
        "estimated_rows": None,
        "is_null_partition": False,
    }


def test_range_slice_planner_marks_temporal_slice_unsplittable_without_type_error() -> None:
    planner = RangeSlicePlanner(NativeTransferResourcePolicy(min_slice_rows=1, max_slice_rows=10))
    item = TransferSlice(
        partition_index=0,
        slice_index=0,
        lower_bound=date(2024, 1, 1),
        upper_bound=date(2024, 1, 2),
    )

    assert planner.split(item) == ()


def test_mssql_partition_predicate_renders_typed_temporal_literals_without_column_functions() -> None:
    boundary = PartitionBoundaryTypeResolver.resolve(boundary_type="datetime2")
    partition = RangePartition(
        index=0,
        lower_bound=datetime(2024, 1, 1),
        upper_bound=datetime(2024, 1, 2),
        include_upper=True,
        boundary=boundary,
    )

    predicate = partition.predicate("[event_ts]", renderer=MssqlPartitionPredicateRenderer())

    assert "[event_ts] >= CONVERT(datetime2(7), '2024-01-01T00:00:00.0000000', 126)" in predicate
    assert "[event_ts] <= CONVERT(datetime2(7), '2024-01-02T00:00:00.0000000', 126)" in predicate
    assert "CONVERT(datetime2(7), [event_ts]" not in predicate
    assert "REPLACE(" not in predicate


def test_mssql_timestamp_metadata_resolves_to_rowversion_not_temporal() -> None:
    resolved = PartitionBoundaryTypeResolver.resolve(source_type="timestamp", source_system="mssql")

    assert resolved.kind == PartitionBoundKind.ROWVERSION
    assert resolved.warning_code == "mssql_timestamp_is_rowversion"


def test_partitioning_options_resolver_exposes_typed_planner_options() -> None:
    resolved = PartitioningOptionsResolver.resolve(
        {
            "partitioning": {
                "column": "doc_date",
                "planner": {
                    "boundary_type": "date",
                    "bounds_role": "stride",
                    "temporal_granularity": "day",
                    "low_confidence_policy": "single_partition",
                    "null_bucket": "separate",
                },
            }
        }
    )

    assert resolved.planner.boundary_type == "date"
    assert resolved.planner.bounds_role == "stride"
    assert resolved.planner.temporal_granularity == "day"
    assert resolved.planner.low_confidence_policy == "single_partition"
    assert resolved.planner.null_bucket == "separate"


def test_explicit_numeric_ranges_validate_gap_overlap_and_null_routing() -> None:
    options = {
        "partitioning": {
            "column": "record_id",
            "num_partitions": 2,
            "planner": {"boundary_type": "numeric", "null_bucket": "separate"},
            "range_parallelism": {"mode": "required", "gap_policy": "reject", "consistency": "immutable"},
            "ranges": [
                {"lower": 0, "upper": 10, "include_lower": True, "include_upper": False, "null": True},
                {"lower": 10, "upper": 20, "include_lower": True, "include_upper": True},
            ],
        }
    }

    partitions = RangePartitioner.from_options(options).partitions()

    assert len(partitions) == 3
    assert partitions[0].is_null_partition
    assert [(item.lower_bound, item.upper_bound, item.include_upper) for item in partitions[1:]] == [
        (0, 10, False),
        (10, 20, True),
    ]

    broken = options["partitioning"].copy()
    broken["ranges"] = [
        {"lower": 0, "upper": 11},
        {"lower": 10, "upper": 20, "include_upper": True},
    ]
    with pytest.raises(ValueError, match="overlap"):
        RangePartitioner.from_options({"partitioning": broken})

    broken["ranges"] = [{"lower": 0, "upper": 9}, {"lower": 10, "upper": 20, "include_upper": True}]
    with pytest.raises(ValueError, match="gap"):
        RangePartitioner.from_options({"partitioning": broken})


def test_explicit_gap_may_be_declared_but_not_silently_inferred() -> None:
    partitioner = RangePartitioner.from_options(
        {
            "partitioning": {
                "column": "record_id",
                "num_partitions": 2,
                "planner": {"boundary_type": "numeric"},
                "range_parallelism": {
                    "mode": "required",
                    "gap_policy": "allow_explicit",
                    "consistency": "immutable",
                },
                "ranges": [{"lower": 0, "upper": 5}, {"lower": 10, "upper": 20, "include_upper": True}],
            }
        }
    )

    assert len(partitioner.partitions()) == 2


def test_include_first_null_policy_renders_null_once_with_first_range() -> None:
    partitioner = RangePartitioner.from_options(
        {
            "partitioning": {
                "column": "record_id",
                "num_partitions": 2,
                "planner": {"boundary_type": "numeric", "null_bucket": "include_first"},
                "range_parallelism": {"mode": "required", "consistency": "immutable"},
                "ranges": [
                    {"lower": 0, "upper": 10, "null": True},
                    {"lower": 10, "upper": 20, "include_upper": True},
                ],
            }
        }
    )

    predicates = [
        item.predicate("[record_id]", renderer=MssqlPartitionPredicateRenderer()) for item in partitioner.partitions()
    ]

    assert predicates[0] == "([record_id] IS NULL OR ([record_id] >= 0 AND [record_id] < 10))"
    assert all("IS NULL" not in predicate for predicate in predicates[1:])


def test_mssql_uuid_ranges_are_explicit_only_and_use_source_ordering() -> None:
    with pytest.raises(ValueError, match="explicit ranges"):
        RangePartitioner.from_options(
            {
                "partitioning": {
                    "column": "record_id",
                    "bounds": {
                        "lower": "00000000-0000-0000-0000-000000000001",
                        "upper": "ffffffff-ffff-ffff-ffff-ffffffffffff",
                    },
                    "num_partitions": 2,
                    "planner": {"boundary_type": "uniqueidentifier"},
                }
            }
        )

    partitioner = RangePartitioner.from_options(
        {
            "partitioning": {
                "column": "record_id",
                "num_partitions": 2,
                "planner": {"boundary_type": "uniqueidentifier"},
                "range_parallelism": {"mode": "required", "consistency": "immutable"},
                "ranges": [
                    {
                        "lower": "ffffffff-ffff-ffff-ffff-000000000000",
                        "upper": "00000000-0000-0000-0000-000000000001",
                    },
                    {
                        "lower": "00000000-0000-0000-0000-000000000001",
                        "upper": "00000000-0000-0000-0000-000000000002",
                        "include_upper": True,
                    },
                ],
            }
        }
    )
    partitions = partitioner.partitions()
    predicate = partitions[0].predicate("[record_id]", renderer=MssqlPartitionPredicateRenderer())

    assert partitions[0].lower_bound == UUID("ffffffff-ffff-ffff-ffff-000000000000")
    assert "CONVERT(uniqueidentifier, 'ffffffff-ffff-ffff-ffff-000000000000')" in predicate
    assert "CONVERT(uniqueidentifier, '00000000-0000-0000-0000-000000000001')" in predicate


def test_range_parallelism_options_are_configurable_and_validate_positive_budgets() -> None:
    resolved = PartitioningOptionsResolver.resolve(
        {
            "partitioning": {
                "column": "record_id",
                "num_partitions": 7,
                "export_workers": 3,
                "load_workers": 2,
                "range_parallelism": {
                    "mode": "required",
                    "upload_workers": 5,
                    "max_inflight_ranges": 6,
                    "max_inflight_rows": 123,
                    "max_inflight_bytes": 456,
                    "consistency": "write_exclusion",
                    "consistency_authority": {"write_exclusion_ref": "synthetic_write_exclusion"},
                    "staging_topology": "per_partition",
                },
            }
        }
    )

    assert resolved.range_parallelism.reader_workers == 3
    assert resolved.range_parallelism.upload_workers == 5
    assert resolved.range_parallelism.load_workers == 2
    assert resolved.range_parallelism.max_inflight_ranges == 6
    assert resolved.range_parallelism.max_inflight_rows == 123
    assert resolved.range_parallelism.max_inflight_bytes == 456
    assert resolved.range_parallelism.staging_topology == "per_partition"

    with pytest.raises(ValueError, match="max_inflight_bytes"):
        PartitioningOptionsResolver.resolve(
            {
                "partitioning": {
                    "range_parallelism": {"mode": "required", "max_inflight_bytes": 0},
                }
            }
        )


def test_route_local_reader_worker_conflict_and_boolean_values_fail_closed() -> None:
    with pytest.raises(ValueError, match="conflicts"):
        PartitioningOptionsResolver.resolve(
            {
                "partitioning": {
                    "export_workers": 2,
                    "range_parallelism": {"mode": "required", "reader_workers": 3},
                }
            }
        )

    with pytest.raises(ValueError, match="integer"):
        PartitioningOptionsResolver.resolve(
            {"partitioning": {"range_parallelism": {"mode": "required", "reader_workers": True}}}
        )


def test_mssql_full_extract_uses_typed_date_partition_predicates_for_auto_bounds(tmp_path: Path) -> None:
    class FakeConnector:
        bcp_path = "bcp"
        trust_server_certificate = "yes"

        def __init__(self) -> None:
            self.queries: list[str] = []

        def fetch_schema(self, schema: str, table: str):
            assert (schema, table) == ("reporting", "account_sales_history")
            return [("doc_date", "date"), ("amount", "decimal(18,2)")]

        def build_select_query(self, schema: str, table: str, columns: list[str]) -> str:
            return f"SELECT {', '.join(f'[{column}]' for column in columns)} FROM [{schema}].[{table}]"

        def quote_identifier(self, name: str) -> str:
            return f"[{name}]"

        def get_records(self, query: str):
            self.queries.append(query)
            return [(date(2024, 1, 1), date(2024, 1, 10), 9)]

        def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
            del options
            self.queries.append(query)
            Path(output_path).write_text("2024-01-01\t1.00\n", encoding="utf-8")
            return 1

    config = LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="reporting",
        source_table="account_sales_history",
        target_schema="Example_Datamarts",
        target_table="account_sales_history",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "partitioning": {
                "strategy": "auto",
                "column": "doc_date",
                "bounds": "auto",
                "num_partitions": 3,
                "export_workers": 1,
            },
            "partition_tmp_dir": str(tmp_path),
        },
    )

    connector = FakeConnector()
    extract = MSSQLFullExtractStrategy(connector, CapturingLogger(), sink_connector=object()).extract(config, None)

    assert isinstance(extract.artifact, PartitionedFileExportArtifact)
    assert any("dpone_null_count" in query for query in connector.queries)
    bcp_queries = [query for query in connector.queries if "dpone_partitioned" in query]
    assert any("[doc_date] >= CONVERT(date, '2024-01-01', 23)" in query for query in bcp_queries)
    assert any("[doc_date] < CONVERT(date, '2024-01-04', 23)" in query for query in bcp_queries)
    assert all("CONVERT(date, [doc_date]" not in query for query in bcp_queries)
    assert all("REPLACE(" not in query for query in bcp_queries)

    extract.artifact.cleanup()
