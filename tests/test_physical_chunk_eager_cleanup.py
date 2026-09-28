"""Bounded disk must not discard source authority or ambiguous load evidence."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.physical_chunking import (
    PhysicalChunkedFileExportArtifact,
    PhysicalChunkPolicy,
    RowBoundaryChunkWriter,
)
from dpone.runtime.sinks.clickhouse_staged_evidence import SourceByteBudgetError, enforce_source_byte_budget


def artifact_for(tmp_path, rows):
    writer = RowBoundaryChunkWriter(
        policy=PhysicalChunkPolicy(target_chunk_bytes=4, max_chunk_bytes=8),
        columns=("value",),
        directory=tmp_path,
        format="mssql-delimited",
    )
    return PhysicalChunkedFileExportArtifact(
        chunk_generator=lambda: writer.write(rows),
        columns=("value",),
        evidence_path=tmp_path / "evidence.json",
        cleanup_policy="eager",
    )


def test_eager_cleanup_bounds_peak_disk_and_preserves_measured_budget(tmp_path):
    artifact = artifact_for(tmp_path, (b"abc\n" for _ in range(40)))
    peaks = []

    def load(chunk):
        assert Path(chunk.file_path).read_bytes() == b"abc\n"
        peaks.append(sum(p.stat().st_size for p in tmp_path.glob("*.bcp")))
        return 1

    assert artifact.load_with(load) == 40
    assert max(peaks) <= 8
    assert not list(tmp_path.glob("*.bcp"))
    evidence = enforce_source_byte_budget(SimpleNamespace(artifact=artifact), maximum_bytes=160, full_refresh=True)
    assert evidence.observed_bytes == 160
    assert evidence.unique_parts == 40
    with pytest.raises(SourceByteBudgetError, match="EXCEEDED"):
        enforce_source_byte_budget(SimpleNamespace(artifact=artifact), maximum_bytes=159, full_refresh=True)
    with pytest.raises(ValueError, match="already_materialized"):
        artifact.rebind_generator(lambda: ())
    with pytest.raises(ValueError, match="already_materialized"):
        artifact.load_with(load)
    artifact.terminate(ArtifactTerminalOutcome.SUCCESS)


def test_failed_chunk_is_retained_without_completed_source_authority(tmp_path):
    artifact = artifact_for(tmp_path, (b"abc\n" for _ in range(3)))
    calls = 0

    def load(chunk):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise TimeoutError("staging acknowledgement unavailable")
        return 1

    with pytest.raises(TimeoutError, match="acknowledgement"):
        artifact.load_with(load)
    assert len(list(tmp_path.glob("*.bcp"))) == 1
    assert not artifact.source_byte_measurement_complete
    with pytest.raises(SourceByteBudgetError, match="UNMEASURABLE"):
        enforce_source_byte_budget(SimpleNamespace(artifact=artifact), maximum_bytes=160, full_refresh=True)
    artifact.terminate(ArtifactTerminalOutcome.ABORT)
    assert not list(tmp_path.glob("*.bcp"))


def test_empty_completed_source_is_measurable_and_not_replayable(tmp_path):
    artifact = artifact_for(tmp_path, ())
    assert artifact.load_with(lambda _: pytest.fail("empty source has no chunks")) == 0
    evidence = enforce_source_byte_budget(SimpleNamespace(artifact=artifact), maximum_bytes=1, full_refresh=True)
    assert evidence.observed_bytes == 0
    with pytest.raises(ValueError, match="already_materialized"):
        artifact.rebind_generator(lambda: ())


def test_late_producer_error_cannot_certify_released_chunks(tmp_path):
    def rows():
        yield b"abc\n"
        raise RuntimeError("producer exited nonzero")

    artifact = artifact_for(tmp_path, rows())
    with pytest.raises(RuntimeError, match="producer exited"):
        artifact.load_with(lambda _: 1)
    assert not list(tmp_path.glob("*.bcp"))
    assert not artifact.source_byte_measurement_complete
    assert artifact.rows_exported is None


def test_unlink_failure_stops_before_next_chunk(tmp_path, monkeypatch):
    artifact = artifact_for(tmp_path, (b"abc\n" for _ in range(20)))
    real_unlink = Path.unlink
    attempts = []

    def fail_chunk_unlink(path, *args, **kwargs):
        if path.suffix == ".bcp":
            attempts.append(path)
            raise PermissionError("release unavailable")
        return real_unlink(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", fail_chunk_unlink)
        with pytest.raises(PermissionError, match="release unavailable"):
            artifact.load_with(lambda _: 1)
    assert len(attempts) == 1
    assert len(list(tmp_path.glob("*.bcp"))) == 1
    assert not artifact.source_byte_measurement_complete
    artifact.terminate(ArtifactTerminalOutcome.ABORT)
    assert not list(tmp_path.glob("*.bcp"))


def test_eager_release_does_not_touch_unrelated_files_and_unknown_commit_keeps_receipts(tmp_path):
    unrelated = tmp_path / "unrelated.bcp"
    unrelated.write_bytes(b"unrelated")
    artifact = artifact_for(tmp_path, (b"abc\n",))
    assert artifact.load_with(lambda _: 999) == 1
    assert artifact.rows_exported == 1
    artifact.terminate(ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN)
    assert unrelated.read_bytes() == b"unrelated"
    assert (tmp_path / "evidence.json").exists()
    assert len(list(tmp_path.glob("*.bcp"))) == 1


@pytest.mark.parametrize("error", [KeyboardInterrupt(), TimeoutError("unknown load outcome")])
def test_interrupted_load_keeps_current_file_until_owner_decides(tmp_path, error):
    artifact = artifact_for(tmp_path, (b"abc\n",))

    def load(_):
        raise error

    with pytest.raises(type(error)):
        artifact.load_with(load)
    assert not artifact.source_byte_measurement_complete
    assert artifact.rows_exported is None
    artifact.terminate(ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN)
    assert len(list(tmp_path.glob("*.bcp"))) == 1
