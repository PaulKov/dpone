from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.runtime.physical_chunking import (
    PhysicalChunkedFileExportArtifact,
    PhysicalChunkLimitExceeded,
    PhysicalChunkPolicy,
    PhysicalTransferChunk,
    RowBoundaryChunkWriter,
)


def test_physical_chunked_artifact_records_safe_generation_failure(tmp_path: Path) -> None:
    artifact, evidence_path = _artifact(tmp_path, [b"safe\nSECRET-ROW\n"])

    with pytest.raises(PhysicalChunkLimitExceeded):
        artifact.load_with(lambda _artifact: 0)

    evidence_text = evidence_path.read_text(encoding="utf-8")
    evidence = json.loads(evidence_text)
    failure = evidence["generation_failure"]
    assert failure["chunk_index"] == 0
    assert failure["status"] == "generation_failed"
    assert failure["error_code"] == "physical_chunk_row_exceeds_max_bytes"
    assert failure["row_bytes"] == 11
    assert failure["max_chunk_bytes"] == 8
    assert failure["timestamp"]
    assert "SECRET-ROW" not in evidence_text
    assert evidence["chunks"] == []


def test_physical_chunked_artifact_never_persists_loader_error_contents(tmp_path: Path) -> None:
    artifact, evidence_path = _artifact(tmp_path, [b"safe\n"])

    with pytest.raises(RuntimeError, match="SECRET-ROW"):
        artifact.load_with(lambda _artifact: (_ for _ in ()).throw(RuntimeError("SECRET-ROW")))

    evidence_text = evidence_path.read_text(encoding="utf-8")
    failed = next(event for event in json.loads(evidence_text)["chunks"] if event["status"] == "failed")
    assert failed["error_code"] == "physical_chunk_staging_load_failed"
    assert "error" not in failed
    assert "SECRET-ROW" not in evidence_text


def test_physical_chunked_artifact_whitelists_generation_failure_codes(tmp_path: Path) -> None:
    class UnsafeGenerationError(RuntimeError):
        code = "SECRET-ROW"

    def fail_generation():
        raise UnsafeGenerationError("SECRET-ROW")

    evidence_path = tmp_path / "physical_chunks.json"
    artifact = PhysicalChunkedFileExportArtifact(
        chunk_generator=fail_generation,
        columns=("value",),
        evidence_path=evidence_path,
    )

    with pytest.raises(UnsafeGenerationError):
        artifact.load_with(lambda _artifact: 0)

    evidence_text = evidence_path.read_text(encoding="utf-8")
    assert json.loads(evidence_text)["generation_failure"]["error_code"] == "physical_chunk_generation_failed"
    assert "SECRET-ROW" not in evidence_text


def test_physical_chunked_artifact_does_not_trust_limit_exception_subclass_evidence(tmp_path: Path) -> None:
    class UnsafeLimitError(PhysicalChunkLimitExceeded):
        def to_evidence(self):
            return {"error_code": "SECRET-CODE", "source_row": "SECRET-ROW"}

    def fail_generation():
        raise UnsafeLimitError(chunk_index=0, row_bytes=9, max_chunk_bytes=8)

    evidence_path = tmp_path / "physical_chunks.json"
    artifact = PhysicalChunkedFileExportArtifact(
        chunk_generator=fail_generation,
        columns=("value",),
        evidence_path=evidence_path,
    )

    with pytest.raises(UnsafeLimitError):
        artifact.load_with(lambda _artifact: 0)

    evidence_text = evidence_path.read_text(encoding="utf-8")
    failure = json.loads(evidence_text)["generation_failure"]
    assert failure["error_code"] == "physical_chunk_generation_failed"
    assert "source_row" not in failure
    assert "SECRET" not in evidence_text


def test_physical_chunked_artifact_snapshots_only_valid_base_exception_scalars(tmp_path: Path) -> None:
    error = PhysicalChunkLimitExceeded(chunk_index=0, row_bytes=9, max_chunk_bytes=8)
    error.row_bytes = "SECRET-ROW"  # type: ignore[assignment]

    def fail_generation():
        raise error

    evidence_path = tmp_path / "physical_chunks.json"
    artifact = PhysicalChunkedFileExportArtifact(
        chunk_generator=fail_generation,
        columns=("value",),
        evidence_path=evidence_path,
    )

    with pytest.raises(PhysicalChunkLimitExceeded):
        artifact.load_with(lambda _artifact: 0)

    evidence_text = evidence_path.read_text(encoding="utf-8")
    failure = json.loads(evidence_text)["generation_failure"]
    assert failure["error_code"] == "physical_chunk_generation_failed"
    assert "row_bytes" not in failure
    assert "SECRET" not in evidence_text


def test_physical_chunked_artifact_cleanup_never_deletes_another_artifacts_chunk(tmp_path: Path) -> None:
    owned_path = tmp_path / "dpone_physical_chunk_owned.bcp"
    foreign_path = tmp_path / "dpone_physical_chunk_foreign.bcp"
    owned_path.write_bytes(b"owned\n")
    foreign_path.write_bytes(b"foreign\n")
    owned_chunk = PhysicalTransferChunk(
        chunk_index=0,
        file_path=str(owned_path),
        byte_count=owned_path.stat().st_size,
        checksum="owned-checksum",
        row_count=1,
        format="mssql-delimited",
    )
    artifact = PhysicalChunkedFileExportArtifact(
        chunk_generator=lambda: iter((owned_chunk,)),
        columns=("value",),
        evidence_path=tmp_path / "physical_chunks.json",
    )

    with pytest.raises(RuntimeError):
        artifact.load_with(lambda _artifact: (_ for _ in ()).throw(RuntimeError("load failed")))
    artifact.cleanup()

    assert not owned_path.exists()
    assert foreign_path.read_bytes() == b"foreign\n"


def _artifact(tmp_path: Path, frames: list[bytes]) -> tuple[PhysicalChunkedFileExportArtifact, Path]:
    writer = RowBoundaryChunkWriter(
        policy=PhysicalChunkPolicy(target_chunk_bytes=8, max_chunk_bytes=8),
        columns=("value",),
        directory=tmp_path,
        format="mssql-delimited",
    )
    evidence_path = tmp_path / "physical_chunks.json"
    return (
        PhysicalChunkedFileExportArtifact(
            chunk_generator=lambda: writer.write(frames),
            columns=("value",),
            evidence_path=evidence_path,
        ),
        evidence_path,
    )
