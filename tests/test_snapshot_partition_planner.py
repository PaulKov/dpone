from __future__ import annotations

from dpone.runtime.snapshot_partition_planner import (
    HistogramStep,
    SnapshotPartitionPlanner,
    SnapshotPartitionPlannerPolicy,
    SourceStatistic,
)
from dpone.runtime.sources.strategies.mssql.mssql_statistics_inspector import (
    MssqlSourceStatisticsInspector,
    MssqlStatisticsQueryBuilder,
)


def test_histogram_planner_builds_balanced_ranges_with_null_bucket() -> None:
    statistic = SourceStatistic(
        source_type="mssql",
        table="dbo.orders",
        column="id",
        total_rows=1_000,
        null_rows=25,
        confidence="high",
        steps=(
            HistogramStep(lower_key=0, range_hi_key=100, equal_rows=10, range_rows=240, distinct_range_rows=90),
            HistogramStep(lower_key=100, range_hi_key=200, equal_rows=10, range_rows=240, distinct_range_rows=90),
            HistogramStep(lower_key=200, range_hi_key=300, equal_rows=10, range_rows=240, distinct_range_rows=90),
            HistogramStep(lower_key=300, range_hi_key=400, equal_rows=10, range_rows=240, distinct_range_rows=90),
        ),
    )

    plan = SnapshotPartitionPlanner().plan(
        statistic,
        SnapshotPartitionPlannerPolicy(target_rows=250, max_partitions=8, null_bucket="separate"),
    )

    evidence = plan.to_evidence()

    assert evidence["schema_version"] == "dpone.native_transfer.snapshot_partition_plan.v1"
    assert plan.boundary_column == "id"
    assert plan.stats_confidence == "high"
    assert [partition.kind for partition in plan.partitions][:1] == ["null"]
    assert len([partition for partition in plan.partitions if partition.kind == "range"]) == 4
    assert all(partition.estimated_rows <= 260 for partition in plan.partitions if partition.kind == "range")


def test_histogram_planner_splits_hot_ranges_when_allowed() -> None:
    statistic = SourceStatistic(
        source_type="mssql",
        table="dbo.orders",
        column="id",
        total_rows=1_000,
        confidence="high",
        steps=(HistogramStep(lower_key=0, range_hi_key=1000, equal_rows=0, range_rows=900, distinct_range_rows=900),),
    )

    plan = SnapshotPartitionPlanner().plan(
        statistic,
        SnapshotPartitionPlannerPolicy(
            target_rows=200,
            max_partitions=10,
            skew_policy="split_hot_ranges",
            max_hot_partition_factor=2.0,
        ),
    )

    ranges = [partition for partition in plan.partitions if partition.kind == "range"]
    assert len(ranges) > 1
    assert "snapshot_hot_range_split" in plan.warnings
    assert ranges[0].upper_bound < ranges[-1].upper_bound


def test_low_confidence_stats_reduce_parallelism_and_warn() -> None:
    statistic = SourceStatistic(
        source_type="mssql",
        table="dbo.v_orders",
        column="id",
        total_rows=0,
        confidence="low",
        steps=(),
        source_is_view=True,
    )

    plan = SnapshotPartitionPlanner().plan(
        statistic,
        SnapshotPartitionPlannerPolicy(target_rows=200, max_partitions=8),
    )

    assert plan.stats_confidence == "low"
    assert plan.max_parallel_exports == 1
    assert "source_stats_low_confidence" in plan.warnings
    assert "source_stats_view_route" in plan.warnings


def test_mssql_statistics_query_builder_uses_histogram_dmv_without_full_scan() -> None:
    sql = MssqlStatisticsQueryBuilder().histogram_query(
        database="analytics_reporting",
        schema="reporting",
        table="account_orders",
        column="id",
    )

    assert "sys.dm_db_stats_histogram" in sql
    assert "COUNT(" not in sql.upper()
    assert "MIN(" not in sql.upper()
    assert "MAX(" not in sql.upper()
    assert "CONVERT(nvarchar(4000), hist.range_high_key) AS range_high_key" in sql
    assert "analytics_reporting.sys.stats" in sql
    assert "reporting" in sql
    assert "account_orders" in sql


def test_mssql_statistics_inspector_maps_histogram_rows_to_generic_statistic() -> None:
    class FakeConnector:
        def get_records(self, query, as_dict=False):
            assert "sys.dm_db_stats_histogram" in query
            assert as_dict is True
            return [
                {"range_high_key": 100, "equal_rows": 10, "range_rows": 90, "distinct_range_rows": 90},
                {"range_high_key": 200, "equal_rows": 10, "range_rows": 90, "distinct_range_rows": 90},
            ]

    statistic = MssqlSourceStatisticsInspector(FakeConnector()).inspect_histogram(
        database="DWH",
        schema="dbo",
        table="orders",
        column="id",
    )

    assert statistic.source_type == "mssql"
    assert statistic.table == "dbo.orders"
    assert statistic.column == "id"
    assert statistic.total_rows == 200
    assert statistic.confidence == "high"
    assert statistic.steps[1].lower_key == 100


def test_mssql_statistics_inspector_normalizes_histogram_key_strings() -> None:
    class FakeConnector:
        def get_records(self, query, as_dict=False):
            return [
                {"range_high_key": "100", "equal_rows": 10, "range_rows": 90, "distinct_range_rows": 90},
                {"range_high_key": "200.5", "equal_rows": 10, "range_rows": 90, "distinct_range_rows": 90},
            ]

    statistic = MssqlSourceStatisticsInspector(FakeConnector()).inspect_histogram(
        database="DWH",
        schema="dbo",
        table="orders",
        column="id",
    )

    assert statistic.steps[0].range_hi_key == 100
    assert statistic.steps[1].range_hi_key == 200.5
