from __future__ import annotations

from pathlib import Path

from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy
from dpone.runtime.native_transfer_slicing import (
    AdaptiveSliceSizer,
    RangeSlicePlanner,
    TransferPartition,
    TransferSlice,
)


def test_execution_policy_expands_safe_worker_profile() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping({"profile": "safe_worker"})

    assert policy.mode == "auto"
    assert policy.resource.max_active_files == 1
    assert policy.resource.target_file_bytes == 64 * 1024 * 1024
    assert policy.resource.max_file_bytes == 128 * 1024 * 1024


def test_execution_policy_keeps_custom_resource_overrides() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping(
        {
            "profile": "balanced",
            "resource_policy": {
                "target_file_bytes": "32MiB",
                "max_slice_rows": 5000,
            },
        }
    )

    assert policy.resource.target_file_bytes == 32 * 1024 * 1024
    assert policy.resource.max_slice_rows == 5000
    assert policy.resource.max_active_files == 2


def test_adaptive_slice_sizer_clamps_rows_by_policy() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping(
        {
            "resource_policy": {
                "target_file_bytes": "128MiB",
                "min_slice_rows": 1000,
                "max_slice_rows": 250000,
            }
        }
    )
    sizer = AdaptiveSliceSizer(policy.resource)

    assert sizer.initial_rows(estimated_bytes_per_row=2048) == 65536
    assert sizer.initial_rows(estimated_bytes_per_row=1) == 250000
    assert sizer.initial_rows(estimated_bytes_per_row=10**9) == 1000


def test_adaptive_slice_sizer_shrinks_after_large_file_observation() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping(
        {"resource_policy": {"target_file_bytes": "100MiB", "min_slice_rows": 100, "max_slice_rows": 100000}}
    )
    sizer = AdaptiveSliceSizer(policy.resource)

    next_rows = sizer.next_rows(previous_rows=10000, observed_bytes=200 * 1024 * 1024)

    assert 4500 <= next_rows <= 5500


def test_range_slice_planner_splits_logical_partition_into_physical_slices() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping(
        {"resource_policy": {"target_file_bytes": "10MiB", "min_slice_rows": 10, "max_slice_rows": 100}}
    )
    partition = TransferPartition(index=0, lower_bound=0, upper_bound=350, include_upper=False, estimated_rows=350)
    slices = RangeSlicePlanner(policy.resource).plan(partition, estimated_bytes_per_row=1024)

    assert [(item.lower_bound, item.upper_bound, item.slice_index) for item in slices] == [
        (0, 100, 0),
        (100, 200, 1),
        (200, 300, 2),
        (300, 350, 3),
    ]


def test_range_slice_planner_splits_oversized_slice_for_retry() -> None:
    policy = NativeTransferExecutionPolicy.from_mapping()
    original = TransferPartition(index=1, lower_bound=10, upper_bound=20, include_upper=False, estimated_rows=10)
    first_slice = RangeSlicePlanner(policy.resource).plan(original, estimated_bytes_per_row=1)[0]

    children = RangeSlicePlanner(policy.resource).split(first_slice)

    assert [(item.lower_bound, item.upper_bound, item.slice_index) for item in children] == [
        (10, 15, 0),
        (15, 20, 1),
    ]


def test_partitioned_transfer_plan_exports_loads_and_cleans_each_slice(tmp_path: Path) -> None:
    policy = NativeTransferExecutionPolicy.from_mapping(
        {"resource_policy": {"target_file_bytes": "1MiB", "max_file_bytes": "2MiB"}}
    )
    slices = (TransferSlice(0, 0, 0, 10), TransferSlice(0, 1, 10, 20))
    loaded_existing_files: list[bool] = []

    def export_slice(item: TransferSlice) -> FileExportArtifact:
        path = tmp_path / f"slice_{item.slice_index}.tsv"
        path.write_text(f"{item.lower_bound}\n", encoding="utf-8")
        return FileExportArtifact(str(path), ["id"], format="mssql-delimited", estimated_rows=1)

    artifact = PartitionedTransferPlanArtifact(slices, ["id"], exporter=export_slice, resource_policy=policy.resource)
    rows = artifact.load_with(
        lambda file_artifact: loaded_existing_files.append(Path(file_artifact.file_path).exists()) or 1
    )

    assert rows == 2
    assert loaded_existing_files == [True, True]
    assert not list(tmp_path.glob("*.tsv"))


def test_partitioned_transfer_plan_splits_and_retries_oversized_slice(tmp_path: Path) -> None:
    policy = NativeTransferExecutionPolicy.from_mapping(
        {
            "resource_policy": {
                "target_file_bytes": "1B",
                "max_file_bytes": "4B",
                "min_slice_rows": 1,
                "max_slice_rows": 10,
            }
        }
    )
    exported: list[tuple[int, int]] = []

    def export_slice(item: TransferSlice) -> FileExportArtifact:
        exported.append((item.lower_bound, item.upper_bound))
        path = tmp_path / f"slice_{item.lower_bound}_{item.upper_bound}.tsv"
        payload = "oversized\n" if item.upper_bound - item.lower_bound > 1 else "1\n"
        path.write_text(payload, encoding="utf-8")
        return FileExportArtifact(str(path), ["id"], format="mssql-delimited", estimated_rows=1)

    artifact = PartitionedTransferPlanArtifact(
        (TransferSlice(0, 0, 0, 2),),
        ["id"],
        exporter=export_slice,
        resource_policy=policy.resource,
    )

    assert artifact.load_with(lambda _file_artifact: 1) == 2
    assert exported == [(0, 2), (0, 1), (1, 2)]
    assert not list(tmp_path.glob("*.tsv"))
