"""Complete-key file artifacts for PostgreSQL snapshot reconciliation."""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dpone.runtime.artifact_models import (
    BaseExtractionArtifact,
    bind_artifact_resource_view,
    release_artifact_resource_view,
)
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.incremental_snapshot import KEY_HASH_COLUMN, KeySnapshotReceipt


class PostgresKeySnapshotFileArtifact(BaseExtractionArtifact):
    """A validated, uncompressed character file containing only source keys."""

    def __init__(
        self,
        artifact: FileExportArtifact,
        *,
        snapshot_token: str,
        scope_hash: str,
        key_columns: Sequence[str],
        text_key_columns: Sequence[str] = (),
    ) -> None:
        super().__init__()
        _require_safe_file(artifact)
        # SQL Server native staging is the relational authority for NULL,
        # duplicate and padded text keys.  Keep this parameter for source
        # compatibility, but do not build a row-at-a-time local index.
        del text_key_columns
        try:
            prepared = _append_key_hashes(artifact, key_columns)
        finally:
            artifact.cleanup()
        self._bind_prepared(
            prepared,
            snapshot_token=snapshot_token,
            scope_hash=scope_hash,
            key_columns=key_columns,
        )

    @classmethod
    def from_hashed_artifact(
        cls,
        artifact: FileExportArtifact,
        *,
        snapshot_token: str,
        scope_hash: str,
        key_columns: Sequence[str],
    ) -> PostgresKeySnapshotFileArtifact:
        """Bind a producer-created checksummed key file without rescanning it."""

        instance = cls.__new__(cls)
        BaseExtractionArtifact.__init__(instance)
        instance._bind_prepared(
            artifact,
            snapshot_token=snapshot_token,
            scope_hash=scope_hash,
            key_columns=key_columns,
        )
        return instance

    def _bind_prepared(
        self,
        artifact: FileExportArtifact,
        *,
        snapshot_token: str,
        scope_hash: str,
        key_columns: Sequence[str],
    ) -> None:
        _require_safe_file(artifact)
        expected_columns = (*tuple(key_columns), KEY_HASH_COLUMN)
        if tuple(artifact.columns) != expected_columns:
            artifact.cleanup()
            raise ValueError("key_snapshot prepared artifact columns do not match")
        self._artifact = artifact
        try:
            self.receipt = _receipt_from_artifact(
                artifact,
                snapshot_token=snapshot_token,
                scope_hash=scope_hash,
                key_columns=key_columns,
            )
            bind_artifact_resource_view(self, artifact)
        except BaseException:
            artifact.cleanup()
            raise

    @property
    def estimated_rows(self) -> int:
        return self.receipt.row_count

    @property
    def columns(self) -> Sequence[str]:
        return self._artifact.columns

    @property
    def file_path(self) -> str:
        """Expose the attempt-local file path for integrity diagnostics."""

        return self._artifact.file_path

    @property
    def size_bytes(self) -> int:
        receipt = self._artifact.integrity_receipt
        if receipt is None:  # pragma: no cover - construction is fail-closed
            raise ValueError("key_snapshot artifact integrity receipt is missing")
        return receipt.size_bytes

    def materialize(self, staging_manager: Any, load_config: Any, schema: Sequence[tuple[str, str]]) -> Any:
        self._artifact.require_integrity_receipt()
        return self._artifact.materialize(staging_manager, load_config, schema)

    def cleanup(self) -> None:
        self.terminate(ArtifactTerminalOutcome.ABORT)

    def _release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> None:
        release_artifact_resource_view(self, self._artifact, outcome)

    def _should_release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> bool:
        del outcome
        return True


def derive_key_artifact_from_delta(
    delta: FileExportArtifact,
    *,
    delta_schema: Sequence[tuple[str, str]],
    key_columns: Sequence[str],
    snapshot_token: str,
    scope_hash: str,
    text_key_columns: Sequence[str] = (),
) -> PostgresKeySnapshotFileArtifact:
    """Project keys locally so a full baseline performs one source scan."""

    _require_safe_file(delta)
    source_columns = [name for name, _dtype in delta_schema]
    try:
        key_indexes = [source_columns.index(key) for key in key_columns]
    except ValueError as exc:
        raise ValueError("key_snapshot unique_key is absent from the source schema") from exc
    fd, path = tempfile.mkstemp(
        prefix="dpone_key_snapshot_",
        suffix=".bcp",
        dir=str(Path(delta.file_path).parent),
    )
    os.close(fd)
    try:
        with open(delta.file_path, "rb") as source, open(path, "wb") as target:
            for row_number, raw_line in enumerate(source, start=1):
                values = raw_line.removesuffix(b"\n").split(b"\t")
                if len(values) != len(source_columns):
                    raise ValueError(f"delta artifact row {row_number} has an invalid field count")
                target.write(b"\t".join(values[index] for index in key_indexes) + b"\n")
        artifact = FileExportArtifact(
            path,
            list(key_columns),
            compressed=False,
            format="mssql-delimited",
            bulk_text_codec=delta.bulk_text_codec,
        )
        return PostgresKeySnapshotFileArtifact(
            artifact,
            snapshot_token=snapshot_token,
            scope_hash=scope_hash,
            key_columns=key_columns,
            text_key_columns=text_key_columns,
        )
    except BaseException:
        try:
            os.remove(path)
        except OSError:
            pass
        raise


def _receipt_from_artifact(
    artifact: FileExportArtifact,
    *,
    snapshot_token: str,
    scope_hash: str,
    key_columns: Sequence[str],
) -> KeySnapshotReceipt:
    integrity = artifact.integrity_receipt
    if integrity is None:
        raise ValueError("key_snapshot artifact integrity receipt is missing")
    return KeySnapshotReceipt(
        snapshot_token=snapshot_token,
        scope_hash=scope_hash,
        key_columns=tuple(key_columns),
        row_count=integrity.require_rows_exported(),
        checksum=f"sha256:{integrity.sha256}",
        complete=True,
    )


def _require_safe_file(artifact: FileExportArtifact) -> None:
    if artifact.compressed or artifact.format != "mssql-delimited" or artifact.bulk_text_codec is None:
        raise ValueError(
            "key_snapshot requires an uncompressed dpone mssql-delimited artifact with BulkTextCodec metadata"
        )


def _append_key_hashes(
    artifact: FileExportArtifact,
    key_columns: Sequence[str],
) -> FileExportArtifact:
    fd, path = tempfile.mkstemp(
        prefix="dpone_key_snapshot_hashed_",
        suffix=".bcp",
        dir=str(Path(artifact.file_path).parent),
    )
    os.close(fd)
    try:
        row_count = 0
        with open(artifact.file_path, "rb") as source, open(path, "wb") as target:
            for row_number, line in enumerate(source, start=1):
                row = line.removesuffix(b"\n")
                values = row.split(b"\t")
                if len(values) != len(key_columns):
                    raise ValueError(f"key_snapshot row {row_number} has an invalid field count")
                target.write(row + b"\t" + _key_hash(row).encode("ascii") + b"\n")
                row_count = row_number
        return FileExportArtifact(
            path,
            [*key_columns, KEY_HASH_COLUMN],
            compressed=False,
            format="mssql-delimited",
            bulk_text_codec=artifact.bulk_text_codec,
            rows_exported=row_count,
        )
    except BaseException:
        try:
            os.remove(path)
        except OSError:
            pass
        raise


def _key_hash(row: bytes) -> str:
    canonical = row.decode("utf-8").encode("utf-16le")
    return hashlib.sha256(canonical).hexdigest()


__all__ = [
    "KEY_HASH_COLUMN",
    "PostgresKeySnapshotFileArtifact",
    "derive_key_artifact_from_delta",
]
