"""Cross-artifact source-row authority contract for quality gates."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.governance.quality import (
    QualityGatePolicy,
    QualityGateRunner,
    QualityProbeSnapshot,
)
from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.columnar_fast_path_models import ObjectStorageChunk
from dpone.runtime.columnar_object_storage_windows import (
    ObjectStorageChunkWindow,
    ObjectStorageColumnarChunkedArtifact,
)
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.governance.quality_probe_snapshots import source_snapshot
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.object_storage_access_models import ObjectStorageReadContract
from dpone.runtime.source_materialization import (
    PreparedSourceArtifact,
    SourceMaterializationDecision,
    SourceMaterializedSnapshot,
)
from dpone.runtime.sql_query_artifact import SqlQueryArtifact
from dpone.runtime.streaming_rows import StreamingRowsArtifact

ArtifactFactory = Callable[[int], object]
InvalidArtifactFactory = Callable[[object], object]

_ARTIFACT_DIRECTORY: Path | None = None

_ROW_COUNT_POLICY = QualityGatePolicy.from_config(
    {"gates": [{"id": "source_target_count", "type": "row_count_reconciliation"}]}
)


class _CountingStagingManager:
    def create(
        self,
        load_config: object,
        schema: Sequence[tuple[str, str]],
    ) -> SimpleNamespace:
        del load_config
        return SimpleNamespace(
            row_count=0,
            columns=[name for name, _ in schema],
            qualified_name=lambda: "analytics.orders__staging",
        )

    def insert_rows(
        self,
        handle: object,
        rows: Sequence[Mapping[str, object]],
    ) -> int:
        del handle
        return len(rows)


class _WindowProvider:
    def __init__(self, windows: Sequence[ObjectStorageChunkWindow]) -> None:
        self._windows = tuple(windows)

    def iter_object_storage_windows(
        self,
        request: object,
    ) -> Iterator[ObjectStorageChunkWindow]:
        del request
        yield from self._windows


@pytest.fixture(autouse=True)
def _owned_artifact_directory(tmp_path: Path) -> Iterator[None]:
    """Give every file-artifact case a real runtime-owned filesystem scope."""

    global _ARTIFACT_DIRECTORY
    _ARTIFACT_DIRECTORY = tmp_path
    try:
        yield
    finally:
        _ARTIFACT_DIRECTORY = None


def _artifact_path(name: str) -> str:
    directory = _ARTIFACT_DIRECTORY
    assert directory is not None
    path = directory / name
    path.write_bytes(b"")
    return str(path)


def _completed_in_memory(row_count: int) -> InMemoryRowsArtifact:
    return InMemoryRowsArtifact({"id": index} for index in range(row_count))


def _completed_streaming(row_count: int) -> StreamingRowsArtifact:
    artifact = StreamingRowsArtifact(
        iter({"id": index} for index in range(row_count)),
        batch_size=2,
        estimated_rows=row_count + 7,
    )
    artifact.materialize(
        _CountingStagingManager(),
        load_config=SimpleNamespace(),
        schema=[("id", "Int64")],
    )
    return artifact


def _completed_prepared_source(row_count: int) -> PreparedSourceArtifact:
    return _wrap_prepared_source(_completed_file(row_count))


def _completed_file(row_count: int) -> FileExportArtifact:
    artifact = FileExportArtifact(
        _artifact_path("completed-native-export.bcp"),
        ["id"],
        format="mssql-native",
        estimated_rows=row_count + 7,
    )
    artifact.rows_exported = row_count
    return artifact


def _completed_columnar_windows(
    row_count: int,
) -> ObjectStorageColumnarChunkedArtifact:
    artifact = _columnar_artifact(
        estimated_rows=row_count + 7,
        windows=(_columnar_window(row_count),),
    )
    list(artifact.iter_windows())
    return artifact


def _completed_sql_query(row_count: int) -> SqlQueryArtifact:
    artifact = SqlQueryArtifact(
        sql="SELECT id FROM source.orders",
        dialect="clickhouse",
        sql_hash="sha256:quality-authority",
        estimated_rows=row_count + 7,
    )
    artifact.rows_exported = row_count
    return artifact


def _estimated_streaming(row_count: int) -> StreamingRowsArtifact:
    return StreamingRowsArtifact(iter(()), estimated_rows=row_count)


def _estimated_prepared_source(row_count: int) -> PreparedSourceArtifact:
    return _wrap_prepared_source(_estimated_file(row_count))


def _estimated_file(row_count: int) -> FileExportArtifact:
    return FileExportArtifact(
        _artifact_path("planned-native-export.bcp"),
        ["id"],
        format="mssql-native",
        estimated_rows=row_count,
    )


def _estimated_columnar_windows(
    row_count: int,
) -> ObjectStorageColumnarChunkedArtifact:
    return _columnar_artifact(estimated_rows=row_count, windows=())


def _estimated_sql_query(row_count: int) -> SqlQueryArtifact:
    return SqlQueryArtifact(
        sql="SELECT id FROM source.orders",
        dialect="clickhouse",
        sql_hash="sha256:quality-estimate",
        estimated_rows=row_count,
    )


def _invalid_in_memory(value: object) -> InMemoryRowsArtifact:
    artifact = _completed_in_memory(3)
    artifact.rows_exported = value
    return artifact


def _invalid_streaming(value: object) -> StreamingRowsArtifact:
    artifact = _completed_streaming(3)
    artifact.rows_exported = value
    return artifact


def _invalid_prepared_source(value: object) -> PreparedSourceArtifact:
    inner = _estimated_file(3)
    inner.rows_exported = value
    return _wrap_prepared_source(inner)


def _invalid_file(value: object) -> FileExportArtifact:
    artifact = _estimated_file(3)
    artifact.rows_exported = value
    return artifact


def _invalid_columnar_windows(
    value: object,
) -> ObjectStorageColumnarChunkedArtifact:
    artifact = _estimated_columnar_windows(3)
    artifact.slice_evidence = [
        {
            "partition_index": 0,
            "slice_index": 0,
            "rows_exported": value,
            "transport": "columnar_object_storage_window",
        }
    ]
    return artifact


def _invalid_sql_query(value: object) -> SqlQueryArtifact:
    artifact = _estimated_sql_query(3)
    artifact.rows_exported = value
    return artifact


def _wrap_prepared_source(inner: object) -> PreparedSourceArtifact:
    return PreparedSourceArtifact(
        inner,
        snapshot=SourceMaterializedSnapshot(
            qualified_name="[source].[__dpone_snapshot_orders]",
            cleanup=lambda: None,
            evidence={"work_table": "[source].[__dpone_snapshot_orders]"},
        ),
        decision=SourceMaterializationDecision(
            selected=True,
            release_gate="green",
        ),
        cleanup_policy="eager",
    )


def _columnar_artifact(
    *,
    estimated_rows: int,
    windows: Sequence[ObjectStorageChunkWindow],
) -> ObjectStorageColumnarChunkedArtifact:
    return ObjectStorageColumnarChunkedArtifact(
        provider=_WindowProvider(windows),
        request=object(),
        columns=("id",),
        schema_hash="schema-quality-authority",
        estimated_rows=estimated_rows,
    )


def _columnar_window(row_count: int) -> ObjectStorageChunkWindow:
    return ObjectStorageChunkWindow(
        uri_prefix="s3://dpone-stage/quality/run-1/window-000001/",
        columns=("id",),
        chunks=(
            ObjectStorageChunk(
                uri="s3://dpone-stage/quality/run-1/window-000001/chunk-00000.parquet",
                index=0,
                row_count=row_count,
                size_bytes=0,
                sha256="a" * 64,
                schema_hash="schema-quality-authority",
            ),
        ),
        read_contract=ObjectStorageReadContract(
            mode="named_collection",
            named_collection="dpone_stage",
        ),
        schema_hash="schema-quality-authority",
        estimated_rows=row_count,
    )


_COMPLETED_FAMILIES: tuple[tuple[str, ArtifactFactory], ...] = (
    ("in_memory_rows", _completed_in_memory),
    ("streaming_rows", _completed_streaming),
    ("prepared_source_wrapper", _completed_prepared_source),
    ("file_native_transfer", _completed_file),
    ("slice_evidence_columnar_windows", _completed_columnar_windows),
    ("sql_query", _completed_sql_query),
)

# In-memory rows are materialized at construction, so that family has no
# supported estimate-only state.
_ESTIMATE_ONLY_FAMILIES: tuple[tuple[str, ArtifactFactory], ...] = (
    ("streaming_rows", _estimated_streaming),
    ("prepared_source_wrapper", _estimated_prepared_source),
    ("file_native_transfer", _estimated_file),
    ("slice_evidence_columnar_windows", _estimated_columnar_windows),
    ("sql_query", _estimated_sql_query),
)

_INVALID_AUTHORITY_FAMILIES: tuple[tuple[str, InvalidArtifactFactory], ...] = (
    ("in_memory_rows", _invalid_in_memory),
    ("streaming_rows", _invalid_streaming),
    ("prepared_source_wrapper", _invalid_prepared_source),
    ("file_native_transfer", _invalid_file),
    ("slice_evidence_columnar_windows", _invalid_columnar_windows),
    ("sql_query", _invalid_sql_query),
)

_INVALID_ROW_COUNTS: tuple[tuple[str, object], ...] = (
    ("boolean", True),
    ("negative", -1),
    ("float", 3.0),
    ("string", "3"),
    ("mapping", {"rows": 3}),
    ("sequence", [3]),
)


@pytest.mark.parametrize(
    ("family", "factory"),
    _COMPLETED_FAMILIES,
    ids=[family for family, _ in _COMPLETED_FAMILIES],
)
@pytest.mark.parametrize("row_count", [0, 3], ids=["zero_rows", "nonzero_rows"])
def test_completed_artifact_family_projects_exact_source_rows_idempotently(
    family: str,
    factory: ArtifactFactory,
    row_count: int,
) -> None:
    artifact = factory(row_count)
    extract_result = SimpleNamespace(artifact=artifact, typed_hash=None)

    first = source_snapshot(extract_result)
    replay = source_snapshot(extract_result)

    assert first.row_count == row_count, family
    assert replay == first
    assert first.metrics == {}
    _assert_row_count_gate(first, target_rows=row_count, expected_status="passed")


@pytest.mark.parametrize(
    ("family", "factory"),
    _ESTIMATE_ONLY_FAMILIES,
    ids=[family for family, _ in _ESTIMATE_ONLY_FAMILIES],
)
def test_estimate_only_artifact_family_cannot_certify_row_count(
    family: str,
    factory: ArtifactFactory,
) -> None:
    snapshot = source_snapshot(SimpleNamespace(artifact=factory(3), typed_hash=None))

    assert snapshot.row_count is None, family
    _assert_row_count_gate(snapshot, target_rows=3, expected_status="failed")


@pytest.mark.parametrize(
    ("authority_name", "invalid_row_count"),
    _INVALID_ROW_COUNTS,
    ids=[name for name, _ in _INVALID_ROW_COUNTS],
)
@pytest.mark.parametrize(
    ("family", "factory"),
    _INVALID_AUTHORITY_FAMILIES,
    ids=[family for family, _ in _INVALID_AUTHORITY_FAMILIES],
)
def test_malformed_artifact_authority_fails_closed_without_coercion(
    family: str,
    factory: InvalidArtifactFactory,
    authority_name: str,
    invalid_row_count: object,
) -> None:
    try:
        artifact = factory(invalid_row_count)
    except ArtifactIntegrityError as exc:
        assert family in {"prepared_source_wrapper", "file_native_transfer"}
        assert exc.code == "artifact_integrity.rows_exported_invalid"
        return

    snapshot = source_snapshot(SimpleNamespace(artifact=artifact, typed_hash=None))

    assert snapshot.row_count is None, (family, authority_name)
    _assert_row_count_gate(snapshot, target_rows=3, expected_status="failed")


def test_prepared_wrapper_observes_retry_published_authority_without_copying_rows() -> None:
    inner = _estimated_file(5)
    wrapped = _wrap_prepared_source(inner)
    extract_result = SimpleNamespace(artifact=wrapped, typed_hash=None)

    assert source_snapshot(extract_result).row_count is None

    inner.rows_exported = 5

    assert source_snapshot(extract_result).row_count == 5
    assert wrapped.rows_exported == 5
    assert wrapped.artifact is inner
    assert "rows_exported" not in wrapped.__dict__
    assert "row_count" not in wrapped.__dict__
    assert "_rows" not in wrapped.__dict__
    assert "rows_exported" not in wrapped.source_materialization
    assert "row_count" not in wrapped.source_materialization


def test_legacy_row_count_only_authority_remains_compatible_and_replay_safe() -> None:
    artifact = _estimated_file(99)
    artifact.row_count = 4
    extract_result = SimpleNamespace(artifact=artifact, typed_hash=None)

    first = source_snapshot(extract_result)
    replay = source_snapshot(extract_result)

    assert first.row_count == 4
    assert replay == first
    _assert_row_count_gate(first, target_rows=4, expected_status="passed")


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
