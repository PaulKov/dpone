"""Single-pass PostgreSQL baseline projection for MSSQL reconciliation."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from dpone.runtime.file_artifacts import ArtifactIntegrityError, CompletedFileWrite, FileExportArtifact, FileIdentity

_SEQUENTIAL_BUFFER_BYTES = 16 * 1024 * 1024


def build_baseline_snapshot_files(
    raw: FileExportArtifact,
    *,
    delta_schema: Sequence[tuple[str, str]],
    key_columns: Sequence[str],
    delta_hash_column: str,
    key_hash_column: str,
) -> tuple[FileExportArtifact, FileExportArtifact]:
    """Project row- and key-checksummed outputs with one sequential raw read.

    Relational key semantics are intentionally validated set-wise in MSSQL
    native staging. Snapshot/scoping receipts are added by the composition
    layer so this producer remains independent from reconciliation wrappers.
    """

    _require_safe_file(raw)
    source_columns = tuple(str(name) for name, _dtype in delta_schema)
    keys = tuple(str(name) for name in key_columns)
    try:
        key_indexes = tuple(source_columns.index(key) for key in keys)
    except ValueError as exc:
        raise ValueError("key_snapshot unique_key is absent from the source schema") from exc

    delta_path = _temporary_path(raw.file_path, prefix="dpone_delta_snapshot_hashed_")
    key_path = _temporary_path(raw.file_path, prefix="dpone_key_snapshot_hashed_")
    delta_artifact: FileExportArtifact | None = None
    key_artifact: FileExportArtifact | None = None
    try:
        evidence = _project_once(
            raw,
            delta_path=delta_path,
            key_path=key_path,
            source_column_count=len(source_columns),
            key_indexes=key_indexes,
        )
        delta_artifact = FileExportArtifact(
            delta_path,
            [*source_columns, delta_hash_column],
            compressed=False,
            format="mssql-delimited",
            bulk_text_codec=raw.bulk_text_codec,
            _completed_write=CompletedFileWrite(
                sha256=evidence.delta_sha256,
                size_bytes=evidence.delta_size_bytes,
                rows_exported=evidence.row_count,
            ),
        )
        key_artifact = FileExportArtifact(
            key_path,
            [*keys, key_hash_column],
            compressed=False,
            format="mssql-delimited",
            bulk_text_codec=raw.bulk_text_codec,
            _completed_write=CompletedFileWrite(
                sha256=evidence.key_sha256,
                size_bytes=evidence.key_size_bytes,
                rows_exported=evidence.row_count,
            ),
        )
        result = (delta_artifact, key_artifact)
        delta_artifact = None
        key_artifact = None
        return result
    except BaseException:
        if delta_artifact is not None:
            delta_artifact.cleanup()
        else:
            Path(delta_path).unlink(missing_ok=True)
        if key_artifact is not None:
            key_artifact.cleanup()
        else:
            Path(key_path).unlink(missing_ok=True)
        raise
    finally:
        raw.cleanup()


def _project_once(
    raw: FileExportArtifact,
    *,
    delta_path: str,
    key_path: str,
    source_column_count: int,
    key_indexes: tuple[int, ...],
) -> _ProjectionEvidence:
    receipt = raw.integrity_receipt
    if receipt is None:
        raise ArtifactIntegrityError("artifact_integrity.receipt_missing")
    if receipt.wire_contract_sha256 != raw.wire_contract().sha256:
        raise ArtifactIntegrityError("artifact_integrity.wire_contract_mismatch")
    receipt.owned_scope.verify(raw.file_path)
    _require_captured_identity(raw.file_path, receipt.identity)

    digest = hashlib.sha256()
    delta_digest = hashlib.sha256()
    key_digest = hashlib.sha256()
    delta_size = 0
    key_size = 0
    row_count = 0
    try:
        with (
            open(raw.file_path, "rb", buffering=_SEQUENTIAL_BUFFER_BYTES) as source,
            open(delta_path, "wb", buffering=_SEQUENTIAL_BUFFER_BYTES) as delta,
            open(key_path, "wb", buffering=_SEQUENTIAL_BUFFER_BYTES) as keys,
        ):
            opened = FileIdentity.from_stat(os.fstat(source.fileno()))
            if opened != receipt.identity:
                raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")
            for row_number, line in enumerate(source, start=1):
                digest.update(line)
                row = line.removesuffix(b"\n")
                values = row.split(b"\t")
                if len(values) != source_column_count:
                    raise ValueError(f"delta artifact row {row_number} has an invalid field count")
                key_row = b"\t".join(values[index] for index in key_indexes)
                delta_line = row + b"\t" + _wire_hash(row) + b"\n"
                key_line = key_row + b"\t" + _wire_hash(key_row) + b"\n"
                delta.write(delta_line)
                keys.write(key_line)
                delta_digest.update(delta_line)
                key_digest.update(key_line)
                delta_size += len(delta_line)
                key_size += len(key_line)
                row_count = row_number
            closed_identity = FileIdentity.from_stat(os.fstat(source.fileno()))
    except OSError as exc:
        raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc

    _require_captured_identity(raw.file_path, receipt.identity)
    if closed_identity != receipt.identity:
        raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")
    if digest.hexdigest() != receipt.sha256:
        raise ArtifactIntegrityError("artifact_integrity.sha256_mismatch")
    if receipt.rows_exported is not None and receipt.rows_exported != row_count:
        raise ArtifactIntegrityError("artifact_integrity.rows_exported_mismatch")
    return _ProjectionEvidence(
        row_count=row_count,
        delta_sha256=delta_digest.hexdigest(),
        delta_size_bytes=delta_size,
        key_sha256=key_digest.hexdigest(),
        key_size_bytes=key_size,
    )


@dataclass(frozen=True, slots=True)
class _ProjectionEvidence:
    row_count: int
    delta_sha256: str
    delta_size_bytes: int
    key_sha256: str
    key_size_bytes: int


def _require_captured_identity(path: str, expected: FileIdentity) -> None:
    try:
        details = os.lstat(path)
    except OSError as exc:
        raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
    if not stat.S_ISREG(details.st_mode):
        raise ArtifactIntegrityError("artifact_integrity.regular_file_required")
    if FileIdentity.from_stat(details) != expected:
        raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")


def _temporary_path(source_path: str, *, prefix: str) -> str:
    descriptor, path = tempfile.mkstemp(prefix=prefix, suffix=".bcp", dir=str(Path(source_path).parent))
    os.close(descriptor)
    return path


def _wire_hash(row: bytes) -> bytes:
    return hashlib.sha256(row.decode("utf-8").encode("utf-16le")).hexdigest().encode("ascii")


def _require_safe_file(artifact: FileExportArtifact) -> None:
    if artifact.compressed or artifact.format != "mssql-delimited" or artifact.bulk_text_codec is None:
        raise ValueError(
            "baseline snapshot requires an uncompressed dpone mssql-delimited artifact with BulkTextCodec metadata"
        )


__all__ = ["build_baseline_snapshot_files"]
