"""PreparedSourceArtifact must expose inner export authority for quality gates."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dpone.governance.quality import QualityGatePolicy, QualityGateRunner
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.native_transfer_quality_scope import NativeTransferQualityScope, has_slice_evidence
from dpone.runtime.source_materialization import (
    PreparedSourceArtifact,
    SourceMaterializationDecision,
    SourceMaterializedSnapshot,
)


def test_prepared_source_artifact_forwards_rows_exported_to_quality_probe(tmp_path: Path) -> None:
    path = tmp_path / "data.bcp"
    path.write_bytes(b"1810556 rows")
    inner = FileExportArtifact(str(path), ["id"], format="mssql-native")
    inner.rows_exported = 1810556
    wrapped = _wrap(inner)

    source, target = LoadGovernanceService().quality_probe_snapshots(
        extract_result=SimpleNamespace(artifact=wrapped, typed_hash=None),
        load_result=SimpleNamespace(staging_rows=1810556, total_rows=1810556, typed_hash=None),
    )

    assert source.row_count == 1810556
    assert target.row_count == 1810556
    report = QualityGateRunner().run(
        QualityGatePolicy.from_config({"gates": [{"id": "rows", "type": "row_count_reconciliation"}]}),
        source=source,
        target=target,
    )
    assert report.passed is True


def test_prepared_source_artifact_forwards_live_slice_evidence_for_quality_scope() -> None:
    inner = SimpleNamespace(slice_evidence=[], estimated_rows=6)
    wrapped = _wrap(inner)
    assert has_slice_evidence(wrapped) is True

    scope = NativeTransferQualityScope.from_slice_evidence_artifact(wrapped)
    assert scope is not None
    before = scope.projected_snapshots(staged_rows=6)
    assert before.source.row_count is None

    inner.slice_evidence.append(
        {
            "partition_index": 0,
            "slice_index": 0,
            "rows_exported": 2,
            "transport": "file",
        }
    )
    inner.slice_evidence.append(
        {
            "partition_index": 0,
            "slice_index": 1,
            "rows_exported": 4,
            "transport": "file",
        }
    )

    after = scope.projected_snapshots(staged_rows=6)
    assert after.source.row_count == 6
    assert after.target.row_count == 6
    assert wrapped.slice_evidence is inner.slice_evidence


def _wrap(inner: object) -> PreparedSourceArtifact:
    return PreparedSourceArtifact(
        inner,
        snapshot=SourceMaterializedSnapshot(
            qualified_name="[clickhouse].[__dpone_snapshot_wms]",
            cleanup=lambda: None,
            evidence={"work_table": "[clickhouse].[__dpone_snapshot_wms]"},
        ),
        decision=SourceMaterializationDecision(selected=True, release_gate="green"),
        cleanup_policy="eager",
    )
