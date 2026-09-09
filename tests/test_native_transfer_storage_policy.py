from __future__ import annotations

from pathlib import Path

from dpone.runtime.artifact_lifecycle import ArtifactLease
from dpone.runtime.disk_budget import DiskBudget
from dpone.runtime.storage_policy import RuntimeStoragePolicy, StoragePreflightService, parse_byte_size
from dpone.runtime.transfer_workspace import TransferWorkspace


def test_parse_byte_size_accepts_binary_and_decimal_units() -> None:
    assert parse_byte_size("512MiB") == 512 * 1024 * 1024
    assert parse_byte_size("2GiB") == 2 * 1024 * 1024 * 1024
    assert parse_byte_size("10MB") == 10 * 1000 * 1000
    assert parse_byte_size(4096) == 4096


def test_runtime_storage_policy_prefers_explicit_work_dir_over_legacy_and_env(tmp_path: Path) -> None:
    explicit = tmp_path / "explicit"
    legacy = tmp_path / "legacy"
    policy = RuntimeStoragePolicy.from_sources(
        runtime={"storage": {"work_dir": str(explicit), "evidence_dir": ".dpone/custom-runs"}},
        source_options={"partition_tmp_dir": str(legacy)},
        env={"DPONE_EXPORT_TMP_DIR": str(tmp_path / "env")},
    )

    assert policy.work_dir == explicit
    assert policy.evidence_dir == Path(".dpone/custom-runs")
    assert policy.warnings == ()


def test_runtime_storage_policy_keeps_legacy_alias_with_warning(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy"
    policy = RuntimeStoragePolicy.from_sources(
        runtime={},
        source_options={"partition_tmp_dir": str(legacy)},
        env={"DPONE_EXPORT_TMP_DIR": str(tmp_path / "env")},
    )

    assert policy.work_dir == legacy
    assert "partition_tmp_dir" in policy.warnings[0]


def test_runtime_storage_policy_parses_transfer_store_contract(tmp_path: Path) -> None:
    policy = RuntimeStoragePolicy.from_sources(
        runtime={
            "storage": {
                "profile": "object_backed",
                "work_dir": str(tmp_path / "scratch"),
                "transfer_store": {
                    "type": "s3",
                    "uri": "s3://dpone-stage/native-transfer",
                    "connection_id": "dpone_transfer_store",
                    "connection_type": "env",
                    "cleanup": {
                        "temp_objects": "on_success",
                        "failed_objects": "keep",
                    },
                    "multipart": {
                        "enabled": True,
                        "part_size": "64MiB",
                    },
                },
            }
        }
    )

    assert policy.profile == "object_backed"
    assert policy.transfer_store is not None
    assert policy.transfer_store.store_type == "s3"
    assert policy.transfer_store.uri == "s3://dpone-stage/native-transfer"
    assert policy.transfer_store.connection_id == "dpone_transfer_store"
    assert policy.transfer_store.cleanup.temp_objects == "on_success"
    assert policy.transfer_store.multipart.part_size == 64 * 1024 * 1024
    assert policy.to_dict()["transfer_store"]["uri"] == "s3://dpone-stage/native-transfer"


def test_transfer_workspace_isolates_run_paths(tmp_path: Path) -> None:
    policy = RuntimeStoragePolicy.from_sources(runtime={"storage": {"work_dir": str(tmp_path / "work")}})
    workspace = TransferWorkspace.from_policy(policy, pipeline="orders", run_id="run-1")

    assert workspace.transfer_dir == tmp_path / "work" / "orders" / "run-1" / "transfer"
    assert workspace.lease_dir == tmp_path / "work" / "orders" / "run-1" / "leases"
    assert workspace.slice_path(partition_index=2, slice_index=7, suffix=".tsv").name == "p0002_s0007.tsv"


def test_storage_preflight_creates_and_validates_writable_directories(tmp_path: Path) -> None:
    policy = RuntimeStoragePolicy.from_sources(
        runtime={"storage": {"work_dir": str(tmp_path / "work"), "min_free_bytes": "1MiB"}}
    )
    result = StoragePreflightService().check(policy)

    assert result.passed is True
    assert result.blockers == ()
    assert (tmp_path / "work").is_dir()


def test_storage_preflight_blocks_when_min_free_bytes_exceeds_filesystem_capacity(tmp_path: Path) -> None:
    policy = RuntimeStoragePolicy.from_sources(
        runtime={"storage": {"work_dir": str(tmp_path / "work"), "min_free_bytes": "100000000000TiB"}}
    )
    result = StoragePreflightService().check(policy)

    assert result.passed is False
    assert "work_dir_low_space" in result.blockers


def test_disk_budget_tracks_active_files_and_bytes() -> None:
    budget = DiskBudget(max_active_files=1, max_active_bytes=10)

    assert budget.try_acquire(expected_bytes=8) is True
    assert budget.try_acquire(expected_bytes=1) is False
    budget.release(actual_bytes=8)
    assert budget.try_acquire(expected_bytes=10) is True


def test_artifact_lease_cleans_successful_file_and_retains_failed_debug_copy(tmp_path: Path) -> None:
    source = tmp_path / "work" / "slice.tsv"
    source.parent.mkdir()
    debug_dir = tmp_path / "debug"
    source.write_text("1\talpha\n", encoding="utf-8")

    lease = ArtifactLease(path=source, debug_dir=debug_dir, failed_files="keep")
    lease.close(success=False)

    assert not source.exists()
    assert (debug_dir / "slice.tsv").read_text(encoding="utf-8") == "1\talpha\n"


def test_artifact_lease_removes_successful_file(tmp_path: Path) -> None:
    source = tmp_path / "slice.tsv"
    source.write_text("1\talpha\n", encoding="utf-8")

    ArtifactLease(path=source).close(success=True)

    assert not source.exists()
