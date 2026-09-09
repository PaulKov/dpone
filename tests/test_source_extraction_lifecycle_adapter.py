"""Compatibility matrix for systemic source extraction timing evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.etl.source_extraction_lifecycle import (
    SourceExtractionLifecycleError,
    SourceExtractionLifecycleService,
)
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.internal_query_artifact import InternalQueryArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class _Clock:
    def __init__(self, *values: datetime) -> None:
        self._values = iter(values)

    def __call__(self) -> datetime:
        return next(self._values)


class _Staging:
    def __init__(self) -> None:
        self.rows: list[object] = []

    def create(self, _config, schema) -> StagingTableArtifact:
        return StagingTableArtifact("stage", "payload", tuple(name for name, _ in schema), self)  # type: ignore[arg-type]

    def insert_rows(self, _handle, rows) -> int:
        values = list(rows)
        self.rows.extend(values)
        return len(values)

    def insert_from_query(self, _handle, _query, _schema, _params) -> int:
        return 2

    def drop(self, _handle) -> None:
        return None


def test_eager_file_source_gets_completed_invocation_window(tmp_path: Path) -> None:
    acquired = datetime(2026, 8, 15, 10, 0, tzinfo=UTC)
    completed = datetime(2026, 8, 15, 10, 1, tzinfo=UTC)
    path = tmp_path / "source.tsv"
    path.write_bytes(b"1\n")
    artifact = FileExportArtifact(str(path), ("id",), rows_exported=1)
    service = SourceExtractionLifecycleService(clock=_Clock(acquired, completed))

    result = service.capture(lambda: ExtractResult(artifact, (("id", "int"),)))

    receipt = result.extraction_lifecycle.require_completed()
    assert receipt.extraction_started_at == acquired
    assert receipt.extraction_completed_at == completed
    assert receipt.clock_authority == "dpone.orchestrator.utc"
    assert receipt.snapshot_acquired_at is None
    assert receipt.snapshot_authority is None
    assert receipt.source_token is None
    assert artifact.extraction_lifecycle is result.extraction_lifecycle


def test_stream_source_completes_only_after_verified_consumption() -> None:
    acquired = datetime(2026, 8, 15, 10, 0, tzinfo=UTC)
    completed = datetime(2026, 8, 15, 10, 2, tzinfo=UTC)
    artifact = StreamingRowsArtifact(iter(({"id": 1}, {"id": 2})), batch_size=1)
    service = SourceExtractionLifecycleService(clock=_Clock(acquired, completed))

    result = service.capture(lambda: ExtractResult(artifact, (("id", "int"),)))

    assert result.extraction_lifecycle.receipt is not None
    assert result.extraction_lifecycle.receipt.complete is False
    artifact.materialize(_Staging(), object(), (("id", "int"),))
    assert result.extraction_lifecycle.require_completed().extraction_completed_at == completed


def test_internal_query_acquires_at_target_session_execution_boundary() -> None:
    acquired = datetime(2026, 8, 15, 10, 3, tzinfo=UTC)
    completed = datetime(2026, 8, 15, 10, 4, tzinfo=UTC)
    artifact = InternalQueryArtifact("SELECT id FROM source")
    service = SourceExtractionLifecycleService(
        clock=_Clock(datetime(2026, 8, 15, 9, 0, tzinfo=UTC), acquired, completed)
    )

    result = service.capture(lambda: ExtractResult(artifact, (("id", "int"),)))

    assert result.extraction_lifecycle.receipt is None
    artifact.materialize(_Staging(), object(), (("id", "int"),))
    receipt = result.extraction_lifecycle.require_completed()
    assert receipt.extraction_started_at == acquired
    assert receipt.snapshot_acquired_at is None
    assert receipt.extraction_completed_at == completed


@pytest.mark.parametrize(
    ("source_kind", "lazy"),
    (
        pytest.param("mssql", False, id="mssql-eager-file"),
        pytest.param("mysql", False, id="mysql-eager-file"),
        pytest.param("kafka", True, id="kafka-lazy-stream"),
        pytest.param("rest_api", False, id="rest-eager-memory"),
    ),
)
def test_generic_source_windows_never_claim_native_snapshot(
    source_kind: str,
    lazy: bool,
    tmp_path: Path,
) -> None:
    started = datetime(2026, 8, 15, 11, 0, tzinfo=UTC)
    completed = datetime(2026, 8, 15, 11, 1, tzinfo=UTC)
    if lazy:
        artifact = StreamingRowsArtifact(iter(({"id": 1},)), batch_size=1)
    else:
        path = tmp_path / f"{source_kind}.tsv"
        path.write_bytes(b"1\n")
        artifact = FileExportArtifact(str(path), ("id",), rows_exported=1)
    service = SourceExtractionLifecycleService(clock=_Clock(started, completed))

    result = service.capture(lambda: ExtractResult(artifact, (("id", "int"),)))
    if lazy:
        artifact.materialize(_Staging(), object(), (("id", "int"),))
    receipt = result.extraction_lifecycle.require_completed()

    assert source_kind
    assert receipt.extraction_started_at == started
    assert receipt.extraction_completed_at == completed
    assert receipt.clock_authority == "dpone.orchestrator.utc"
    assert receipt.snapshot_acquired_at is None
    assert receipt.snapshot_authority is None
    assert receipt.source_token is None


def test_explicitly_unprovable_source_fails_before_extract() -> None:
    class _UnsupportedSource:
        extracted = False

        def extraction_lifecycle_capability(self, _load_config) -> str:
            return "unprovable"

        def extract(self) -> None:
            self.extracted = True

    source = _UnsupportedSource()
    service = SourceExtractionLifecycleService()

    with pytest.raises(SourceExtractionLifecycleError, match="unsupported"):
        service.assert_supported(source, object())

    assert source.extracted is False


def test_in_memory_api_style_source_is_finalizer_ready_at_handoff() -> None:
    service = SourceExtractionLifecycleService()
    artifact = InMemoryRowsArtifact(({"id": 1},))

    result = service.capture(lambda: ExtractResult(artifact, (("id", "int"),)))

    assert result.extraction_lifecycle.require_completed().complete is True
