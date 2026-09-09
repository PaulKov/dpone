from __future__ import annotations

from dpone.strategy_intelligence.adaptive_partitioning import (
    AdaptivePartitioningOptions,
    AdaptivePartitioningService,
    PartitionRuntimeObservation,
)


def test_adaptive_partitioning_recommends_split_for_skew_and_failed_partition() -> None:
    observations = (
        PartitionRuntimeObservation(partition_id="p0", row_count=1000, bytes_count=10_000, duration_seconds=1.0),
        PartitionRuntimeObservation(partition_id="p1", row_count=9000, bytes_count=90_000, duration_seconds=7.0),
        PartitionRuntimeObservation(
            partition_id="p2",
            row_count=1000,
            bytes_count=10_000,
            duration_seconds=1.0,
            retry_count=1,
            status="failed",
        ),
    )

    plan = AdaptivePartitioningService().plan(
        options=AdaptivePartitioningOptions(
            enabled=True,
            target_rows_per_partition=5000,
            max_partitions=16,
            export_workers=4,
            load_workers=4,
            max_skew_ratio=2.0,
            retry_split_factor=4,
        ),
        observations=observations,
    )

    assert plan.enabled is True
    assert plan.recommended_target_rows_per_partition == 1250
    assert plan.recommended_max_partitions == 16
    assert plan.recommended_export_workers == 4
    assert plan.recommended_load_workers == 4
    assert plan.actions_by_partition["p1"] == "split_skewed_partition"
    assert plan.actions_by_partition["p2"] == "split_retry_partition"
    assert any("skew" in warning for warning in plan.warnings)


def test_adaptive_partitioning_collects_observability_when_no_measurements_exist() -> None:
    plan = AdaptivePartitioningService().plan(
        options=AdaptivePartitioningOptions(enabled=True, target_rows_per_partition=1_000_000),
        observations=(),
    )

    assert plan.actions == ("collect_partition_observability",)
    assert plan.recommended_target_rows_per_partition == 1_000_000
