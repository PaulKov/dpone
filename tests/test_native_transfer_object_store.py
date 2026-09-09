from __future__ import annotations

from pathlib import Path

import pytest

from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_execution import NativeTransferExecutionPolicy
from dpone.runtime.native_transfer_slicing import TransferSlice
from dpone.runtime.native_transfer_transport import StreamCapability, TransferTransportResolver
from dpone.runtime.transfer_store_models import TransferObjectRef, TransferStorePolicy
from dpone.runtime.transfer_store_service import SliceTransferStore, TransferStorePreflightService
from dpone.storage import LocalObjectStorageClient, ObjectStorageUri


def test_transfer_store_preflight_round_trips_probe_object(tmp_path: Path) -> None:
    policy = TransferStorePolicy.from_mapping(
        {
            "type": "s3",
            "uri": "s3://dpone-stage/native-transfer",
            "cleanup": {"temp_objects": "on_success"},
        }
    )
    client = LocalObjectStorageClient(root_dir=tmp_path / "store")

    result = TransferStorePreflightService(client).check(policy, scratch_dir=tmp_path / "scratch")

    assert result.passed is True
    assert result.blockers == ()
    assert result.details["prefix"] == "s3://dpone-stage/native-transfer"
    assert not client.exists(ObjectStorageUri.parse("s3://dpone-stage/native-transfer/.preflight/probe.txt"))


def test_partitioned_transfer_plan_uploads_slice_to_store_and_cleans_local_file(tmp_path: Path) -> None:
    policy = NativeTransferExecutionPolicy.from_mapping(
        {"resource_policy": {"target_file_bytes": "1MiB", "max_file_bytes": "2MiB"}}
    )
    transfer_store = SliceTransferStore(
        client=LocalObjectStorageClient(root_dir=tmp_path / "store"),
        policy=TransferStorePolicy.from_mapping({"type": "s3", "uri": "s3://dpone-stage/native-transfer"}),
        run_id="run-1",
        dataset="mssql",
        table="orders",
    )
    slice_item = TransferSlice(0, 0, 0, 10)

    def export_slice(item: TransferSlice) -> FileExportArtifact:
        path = tmp_path / f"slice_{item.slice_index}.tsv"
        path.write_text("1\talpha\n", encoding="utf-8")
        return FileExportArtifact(str(path), ["id", "name"], format="mssql-delimited", estimated_rows=1)

    artifact = PartitionedTransferPlanArtifact(
        (slice_item,),
        ["id", "name"],
        exporter=export_slice,
        resource_policy=policy.resource,
        transfer_store=transfer_store,
    )

    rows = artifact.load_with(lambda file_artifact: Path(file_artifact.file_path).exists() and 1)

    assert rows == 1
    assert not list(tmp_path.glob("slice_*.tsv"))
    [evidence] = artifact.slice_evidence
    assert evidence["transfer_object"]["uri"].endswith("/run-1/p0000_s0000.tsv")
    assert evidence["transfer_object"]["sha256"].startswith("sha256:")


def test_partitioned_transfer_plan_hydrates_reusable_object_without_export(tmp_path: Path) -> None:
    client = LocalObjectStorageClient(root_dir=tmp_path / "store")
    store_policy = TransferStorePolicy.from_mapping({"type": "s3", "uri": "s3://dpone-stage/native-transfer"})
    transfer_store = SliceTransferStore(
        client=client, policy=store_policy, run_id="retry-run", dataset="mssql", table="orders"
    )
    source = tmp_path / "already-exported.tsv"
    source.write_text("7\tready\n", encoding="utf-8")
    reusable = transfer_store.stage_file(
        source, partition_index=0, slice_index=0, content_type="text/tab-separated-values"
    )
    source.unlink()
    policy = NativeTransferExecutionPolicy.from_mapping()
    loaded_payloads: list[str] = []

    def export_slice(_item: TransferSlice) -> FileExportArtifact:
        raise AssertionError("source export must not run when verified object is reusable")

    artifact = PartitionedTransferPlanArtifact(
        (TransferSlice(0, 0, 0, 10),),
        ["id", "name"],
        exporter=export_slice,
        resource_policy=policy.resource,
        transfer_store=transfer_store,
        reusable_objects={(0, 0): reusable},
    )

    rows = artifact.load_with(
        lambda file_artifact: loaded_payloads.append(Path(file_artifact.file_path).read_text(encoding="utf-8")) or 1
    )

    assert rows == 1
    assert loaded_payloads == ["7\tready\n"]
    assert not list((tmp_path / "scratch").glob("*.tsv"))
    assert artifact.slice_evidence[0]["transfer_object"]["reused"] is True


def test_slice_transfer_store_rejects_checksum_mismatch(tmp_path: Path) -> None:
    client = LocalObjectStorageClient(root_dir=tmp_path / "store")
    transfer_store = SliceTransferStore(
        client=client,
        policy=TransferStorePolicy.from_mapping({"type": "s3", "uri": "s3://dpone-stage/native-transfer"}),
        run_id="run-1",
        dataset="mssql",
        table="orders",
    )
    source = tmp_path / "payload.tsv"
    source.write_text("1\talpha\n", encoding="utf-8")
    uploaded = transfer_store.stage_file(source, partition_index=0, slice_index=0)
    bad_ref = TransferObjectRef(
        uri=uploaded.uri,
        provider=uploaded.provider,
        size_bytes=uploaded.size_bytes,
        sha256="sha256:" + "0" * 64,
        content_type=uploaded.content_type,
        metadata=uploaded.metadata,
    )

    with pytest.raises(RuntimeError, match="native_transfer_object_checksum_mismatch"):
        transfer_store.hydrate(bad_ref, tmp_path / "hydrated.tsv")


def test_partitioned_transfer_plan_uses_stream_transport_without_local_files(tmp_path: Path) -> None:
    policy = NativeTransferExecutionPolicy.from_mapping({"transport": {"mode": "stream"}})
    transport_plan = TransferTransportResolver().resolve(
        policy.transport,
        source=StreamCapability.supported("postgres_copy_stdout"),
        sink=StreamCapability.supported("clickhouse_http_body"),
        codec=StreamCapability.supported("tabseparated_codec"),
    )
    loaded: list[bytes] = []

    def export_file(_item: TransferSlice) -> FileExportArtifact:
        raise AssertionError("file exporter must not run for stream transport")

    def export_stream(_item: TransferSlice) -> ByteStreamArtifact:
        return ByteStreamArtifact(
            lambda: iter((b"1\talpha\n", b"2\tbeta\n")),
            columns=("id", "name"),
            format="mssql-delimited",
            estimated_rows=2,
        )

    artifact = PartitionedTransferPlanArtifact(
        (TransferSlice(0, 0, 0, 10),),
        ["id", "name"],
        exporter=export_file,
        stream_exporter=export_stream,
        transport_plan=transport_plan,
        resource_policy=policy.resource,
    )

    rows = artifact.load_with(
        lambda _file_artifact: 0,
        stream_loader=lambda stream_artifact: loaded.append(b"".join(stream_artifact.iter_bytes())) or 2,
    )

    assert rows == 2
    assert loaded == [b"1\talpha\n2\tbeta\n"]
    assert list(tmp_path.glob("*.bcp")) == []
    assert artifact.slice_evidence[0]["transport"] == "stream"
    assert artifact.slice_evidence[0]["bytes"] == 15
    assert artifact.slice_evidence[0]["sha256"].startswith("sha256:")
