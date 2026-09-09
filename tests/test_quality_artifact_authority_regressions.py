"""Regressions for exact source-row authority at quality-gate boundaries."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.governance.quality import (
    QualityGatePolicy,
    QualityGateRunner,
    QualityProbeSnapshot,
)
from dpone.runtime.columnar_fast_path_models import (
    LocalColumnarChunk,
    LocalColumnarStagingManifest,
    ObjectStorageChunk,
    ObjectStorageStagingManifest,
)
from dpone.runtime.columnar_object_storage_windows import ObjectStorageChunkWindow
from dpone.runtime.etl.contract_artifacts import (
    ContractEnforcedStreamingArtifact,
    ContractValidatedFileArtifact,
    PartitionedContractValidationArtifact,
)
from dpone.runtime.governance.quality_probe_snapshots import source_snapshot
from dpone.runtime.object_storage_access_models import ObjectStorageReadContract

ArtifactFactory = Callable[[int], Any]
WrapperFactory = Callable[[Any], object]

_ROW_COUNT_POLICY = QualityGatePolicy.from_config(
    {"gates": [{"id": "source_target_count", "type": "row_count_reconciliation"}]}
)


@pytest.mark.parametrize(
    "factory",
    (
        pytest.param(lambda rows: _local_manifest(rows), id="local_manifest"),
        pytest.param(
            lambda rows: _object_storage_manifest(rows),
            id="object_storage_manifest",
        ),
        pytest.param(
            lambda rows: _object_storage_window(rows),
            id="object_storage_window",
        ),
    ),
)
def test_zero_row_columnar_artifacts_never_promote_estimates_to_exact_counts(
    factory: ArtifactFactory,
) -> None:
    artifact = factory(11)

    assert artifact.row_count == 0
    assert artifact.to_evidence()["row_count"] == 0

    snapshot = source_snapshot(SimpleNamespace(artifact=artifact, typed_hash=None))
    assert snapshot.row_count == 0
    _assert_row_count_gate(snapshot, target_rows=11, expected_status="failed")


@pytest.mark.parametrize(
    "wrapper",
    (
        pytest.param(
            lambda artifact: _streaming_contract_wrapper(artifact),
            id="streaming_contract",
        ),
        pytest.param(
            lambda artifact: _file_contract_wrapper(artifact),
            id="file_contract",
        ),
        pytest.param(
            lambda artifact: _partitioned_contract_wrapper(artifact),
            id="partitioned_contract",
        ),
    ),
)
def test_contract_wrappers_recover_inner_exact_authority_without_reading_rows(
    wrapper: WrapperFactory,
) -> None:
    artifact = wrapper(_SensitiveExactAuthorityArtifact(row_count=7, estimated_rows=19))

    snapshot = source_snapshot(SimpleNamespace(artifact=artifact, typed_hash=None))

    assert snapshot.row_count == 7
    assert snapshot.metrics == {}
    _assert_row_count_gate(snapshot, target_rows=7, expected_status="passed")


@pytest.mark.parametrize(
    "wrapper",
    (
        pytest.param(
            lambda artifact: _streaming_contract_wrapper(artifact),
            id="streaming_contract",
        ),
        pytest.param(
            lambda artifact: _file_contract_wrapper(artifact),
            id="file_contract",
        ),
        pytest.param(
            lambda artifact: _partitioned_contract_wrapper(artifact),
            id="partitioned_contract",
        ),
    ),
)
def test_estimate_only_contract_wrappers_fail_closed_without_reading_rows(
    wrapper: WrapperFactory,
) -> None:
    artifact = wrapper(_SensitiveEstimateOnlyArtifact(estimated_rows=19))

    snapshot = source_snapshot(SimpleNamespace(artifact=artifact, typed_hash=None))

    assert snapshot.row_count is None
    _assert_row_count_gate(snapshot, target_rows=19, expected_status="failed")


def test_unrelated_wrappers_are_not_unwrapped_for_quality_authority() -> None:
    artifact = SimpleNamespace(
        _artifact=_SensitiveExactAuthorityArtifact(row_count=7, estimated_rows=19),
        estimated_rows=19,
    )

    snapshot = source_snapshot(SimpleNamespace(artifact=artifact, typed_hash=None))

    assert snapshot.row_count is None


@pytest.mark.parametrize("invalid_rows", [True, -1, 1.5, "1"])
def test_malformed_columnar_chunk_authority_fails_closed(invalid_rows: object) -> None:
    chunk = ObjectStorageChunk(
        uri="s3://dpone-stage/quality/run-1/chunk-00000.parquet",
        index=0,
        row_count=invalid_rows,  # type: ignore[arg-type]
        size_bytes=0,
        sha256="a" * 64,
        schema_hash="schema-authority",
    )
    artifact = ObjectStorageStagingManifest(
        uri_prefix="s3://dpone-stage/quality/run-1/",
        columns=("id",),
        chunks=(chunk,),
        read_contract=_read_contract(),
        schema_hash="schema-authority",
        estimated_rows=1,
    )

    snapshot = source_snapshot(SimpleNamespace(artifact=artifact, typed_hash=None))

    assert snapshot.row_count is None
    _assert_row_count_gate(snapshot, target_rows=1, expected_status="failed")


def test_completed_authority_wrapper_cycle_fails_closed() -> None:
    artifact = _CyclicAuthorityArtifact()

    snapshot = source_snapshot(SimpleNamespace(artifact=artifact, typed_hash=None))

    assert snapshot.row_count is None


class _SensitiveExactAuthorityArtifact:
    def __init__(self, *, row_count: int, estimated_rows: int) -> None:
        self.rows_exported = row_count
        self.estimated_rows = estimated_rows

    @property
    def _rows(self) -> object:
        raise AssertionError("quality authority must not read wrapped row values")

    def materialize(
        self,
        staging_manager: object,
        load_config: object,
        schema: Sequence[tuple[str, str]],
    ) -> object:
        del staging_manager, load_config, schema
        raise AssertionError("regression test does not materialize the artifact")

    def cleanup(self) -> None:
        return None


class _SensitiveEstimateOnlyArtifact:
    def __init__(self, *, estimated_rows: int) -> None:
        self.estimated_rows = estimated_rows

    @property
    def _rows(self) -> object:
        raise AssertionError("quality authority must not read wrapped row values")

    def materialize(
        self,
        staging_manager: object,
        load_config: object,
        schema: Sequence[tuple[str, str]],
    ) -> object:
        del staging_manager, load_config, schema
        raise AssertionError("regression test does not materialize the artifact")

    def cleanup(self) -> None:
        return None


class _CyclicAuthorityArtifact:
    @property
    def completed_source_authority_artifact(self) -> object:
        return self


def _local_manifest(estimated_rows: int) -> LocalColumnarStagingManifest:
    return LocalColumnarStagingManifest(
        base_dir=Path("unused-columnar"),
        columns=("id",),
        chunks=(
            LocalColumnarChunk(
                path=Path("unused-columnar/chunk-00000.parquet"),
                index=0,
                row_count=0,
                size_bytes=0,
                sha256="a" * 64,
                schema_hash="schema-authority",
            ),
        ),
        schema_hash="schema-authority",
        estimated_rows=estimated_rows,
    )


def _object_storage_manifest(estimated_rows: int) -> ObjectStorageStagingManifest:
    return ObjectStorageStagingManifest(
        uri_prefix="s3://dpone-stage/quality/run-1/",
        columns=("id",),
        chunks=(_zero_row_object_chunk(),),
        read_contract=_read_contract(),
        schema_hash="schema-authority",
        estimated_rows=estimated_rows,
    )


def _object_storage_window(estimated_rows: int) -> ObjectStorageChunkWindow:
    return ObjectStorageChunkWindow(
        uri_prefix="s3://dpone-stage/quality/run-1/window-000001/",
        columns=("id",),
        chunks=(_zero_row_object_chunk(),),
        read_contract=_read_contract(),
        schema_hash="schema-authority",
        estimated_rows=estimated_rows,
    )


def _zero_row_object_chunk() -> ObjectStorageChunk:
    return ObjectStorageChunk(
        uri="s3://dpone-stage/quality/run-1/chunk-00000.parquet",
        index=0,
        row_count=0,
        size_bytes=0,
        sha256="a" * 64,
        schema_hash="schema-authority",
    )


def _read_contract() -> ObjectStorageReadContract:
    return ObjectStorageReadContract(
        mode="named_collection",
        named_collection="dpone_stage",
    )


def _streaming_contract_wrapper(artifact: Any) -> ContractEnforcedStreamingArtifact:
    return ContractEnforcedStreamingArtifact(
        artifact,
        contract=object(),
        run_id="quality-authority-run",
        load_id="quality-authority-load",
    )


def _file_contract_wrapper(artifact: Any) -> ContractValidatedFileArtifact:
    return ContractValidatedFileArtifact(
        artifact,
        contract=object(),
        run_id="quality-authority-run",
        load_id="quality-authority-load",
        prevalidated=True,
    )


def _partitioned_contract_wrapper(
    artifact: Any,
) -> PartitionedContractValidationArtifact:
    return PartitionedContractValidationArtifact(artifact, prevalidated=True)


def _assert_row_count_gate(
    source: QualityProbeSnapshot,
    *,
    target_rows: int,
    expected_status: str,
) -> None:
    report = QualityGateRunner().run(
        _ROW_COUNT_POLICY,
        source=source,
        target=QualityProbeSnapshot(row_count=target_rows),
    )

    assert report.results[0].status == expected_status
    assert report.passed is (expected_status == "passed")
