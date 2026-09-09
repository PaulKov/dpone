"""Columnar object-storage chunked export authority for quality gates (SS-76)."""

from __future__ import annotations

from types import SimpleNamespace

from dpone.governance.quality import QualityGatePolicy, QualityGateRunner
from dpone.runtime.columnar_fast_path_models import ObjectStorageChunk
from dpone.runtime.columnar_object_storage_windows import (
    ObjectStorageChunkWindow,
    ObjectStorageColumnarChunkedArtifact,
)
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.native_transfer_quality_scope import NativeTransferQualityScope
from dpone.runtime.object_storage_access_models import ObjectStorageReadContract


def test_columnar_chunked_windows_publish_slice_export_evidence() -> None:
    artifact = ObjectStorageColumnarChunkedArtifact(
        provider=_WindowProvider(_windows()),
        request=object(),
        columns=("id",),
        schema_hash="schema-window",
        estimated_rows=6,
    )

    windows = list(artifact.iter_windows())
    assert len(windows) == 2
    assert [window.row_count for window in windows] == [2, 4]
    assert artifact.slice_evidence == [
        {
            "partition_index": 0,
            "slice_index": 0,
            "rows_exported": 2,
            "transport": "columnar_object_storage_window",
            "uri_prefix": "s3://dpone-stage/msql/run-1/window-000001/",
            "bytes": 0,
        },
        {
            "partition_index": 0,
            "slice_index": 1,
            "rows_exported": 4,
            "transport": "columnar_object_storage_window",
            "uri_prefix": "s3://dpone-stage/msql/run-1/window-000002/",
            "bytes": 0,
        },
    ]


def test_columnar_chunked_quality_scope_uses_export_rows_after_windows_materialize() -> None:
    artifact = ObjectStorageColumnarChunkedArtifact(
        provider=_WindowProvider(_windows()),
        request=object(),
        columns=("id",),
        schema_hash="schema-window",
        estimated_rows=6,
    )
    scope = NativeTransferQualityScope.from_slice_evidence_artifact(artifact)
    assert scope is not None

    before_export = scope.projected_snapshots(staged_rows=6)
    assert before_export.source.row_count is None
    assert before_export.target.row_count == 6

    list(artifact.iter_windows())
    snapshots = scope.projected_snapshots(staged_rows=6)

    assert snapshots.source.row_count == 6
    assert snapshots.target.row_count == 6


def test_columnar_chunked_source_probe_sums_slice_export_rows() -> None:
    artifact = ObjectStorageColumnarChunkedArtifact(
        provider=_WindowProvider(_windows()),
        request=object(),
        columns=("id",),
        schema_hash="schema-window",
        estimated_rows=6,
    )
    list(artifact.iter_windows())
    extract_result = SimpleNamespace(artifact=artifact, typed_hash=None)
    load_result = SimpleNamespace(staging_rows=6, total_rows=6, typed_hash=None)

    source, target = LoadGovernanceService().quality_probe_snapshots(
        extract_result=extract_result,
        load_result=load_result,
    )

    assert source.row_count == 6
    assert target.row_count == 6

    report = QualityGateRunner().run(
        QualityGatePolicy.from_config({"gates": [{"id": "rows", "type": "row_count_reconciliation"}]}),
        source=source,
        target=target,
    )
    assert report.passed is True


class _WindowProvider:
    def __init__(self, windows: list[ObjectStorageChunkWindow]) -> None:
        self.windows = windows

    def iter_object_storage_windows(self, request: object):
        del request
        yield from self.windows


def _windows() -> list[ObjectStorageChunkWindow]:
    contract = ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage")
    return [
        ObjectStorageChunkWindow(
            uri_prefix="s3://dpone-stage/msql/run-1/window-000001/",
            columns=("id",),
            chunks=(
                ObjectStorageChunk(
                    uri="s3://dpone-stage/msql/run-1/window-000001/chunk-00000.parquet",
                    index=0,
                    row_count=2,
                    size_bytes=0,
                    sha256="a" * 64,
                    schema_hash="schema-window",
                ),
            ),
            read_contract=contract,
            schema_hash="schema-window",
            estimated_rows=2,
        ),
        ObjectStorageChunkWindow(
            uri_prefix="s3://dpone-stage/msql/run-1/window-000002/",
            columns=("id",),
            chunks=(
                ObjectStorageChunk(
                    uri="s3://dpone-stage/msql/run-1/window-000002/chunk-00000.parquet",
                    index=0,
                    row_count=4,
                    size_bytes=0,
                    sha256="b" * 64,
                    schema_hash="schema-window",
                ),
            ),
            read_contract=contract,
            schema_hash="schema-window",
            estimated_rows=4,
        ),
    ]
