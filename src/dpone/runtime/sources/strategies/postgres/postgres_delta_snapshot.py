"""Immutable PostgreSQL delta artifacts for XMin reconciliation."""

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
from dpone.runtime.incremental_snapshot import DELTA_HASH_COLUMN, DeltaSnapshotReceipt


class PostgresDeltaSnapshotFileArtifact(BaseExtractionArtifact):
    """Own a complete delta file with immutable count and checksum evidence."""

    def __init__(
        self,
        artifact: FileExportArtifact,
        *,
        snapshot_token: str,
        scope_hash: str,
        columns: Sequence[str],
    ) -> None:
        super().__init__()
        _require_safe_file(artifact)
        try:
            prepared = _append_row_hashes(artifact, columns)
        finally:
            artifact.cleanup()
        self._bind_prepared(
            prepared,
            snapshot_token=snapshot_token,
            scope_hash=scope_hash,
            columns=columns,
        )

    @classmethod
    def from_hashed_artifact(
        cls,
        artifact: FileExportArtifact,
        *,
        snapshot_token: str,
        scope_hash: str,
        columns: Sequence[str],
    ) -> PostgresDeltaSnapshotFileArtifact:
        """Bind a producer-created checksummed file without rescanning it."""

        instance = cls.__new__(cls)
        BaseExtractionArtifact.__init__(instance)
        instance._bind_prepared(
            artifact,
            snapshot_token=snapshot_token,
            scope_hash=scope_hash,
            columns=columns,
        )
        return instance

    def _bind_prepared(
        self,
        artifact: FileExportArtifact,
        *,
        snapshot_token: str,
        scope_hash: str,
        columns: Sequence[str],
    ) -> None:
        _require_safe_file(artifact)
        expected_columns = (*tuple(columns), DELTA_HASH_COLUMN)
        if tuple(artifact.columns) != expected_columns:
            artifact.cleanup()
            raise ValueError("delta_snapshot prepared artifact columns do not match")
        self._artifact = artifact
        try:
            self.receipt = _receipt_from_artifact(
                artifact,
                snapshot_token=snapshot_token,
                scope_hash=scope_hash,
                columns=columns,
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
        return self._artifact.file_path

    @property
    def size_bytes(self) -> int:
        receipt = self._artifact.integrity_receipt
        if receipt is None:  # pragma: no cover - construction is fail-closed
            raise ValueError("delta_snapshot artifact integrity receipt is missing")
        return receipt.size_bytes

    def materialize(self, staging_manager: Any, load_config: Any, schema: Sequence[tuple[str, str]]) -> Any:
        self._artifact.require_integrity_receipt()
        staged = self._artifact.materialize(staging_manager, load_config, schema)
        if staged.row_count != self.receipt.row_count:
            staged.cleanup()
            raise ValueError("delta_snapshot staged row count does not match receipt")
        return staged

    def cleanup(self) -> None:
        self.terminate(ArtifactTerminalOutcome.ABORT)

    def _release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> None:
        release_artifact_resource_view(self, self._artifact, outcome)

    def _should_release_for_terminal_outcome(self, outcome: ArtifactTerminalOutcome) -> bool:
        del outcome
        return True


def _receipt_from_artifact(
    artifact: FileExportArtifact,
    *,
    snapshot_token: str,
    scope_hash: str,
    columns: Sequence[str],
) -> DeltaSnapshotReceipt:
    integrity = artifact.integrity_receipt
    if integrity is None:
        raise ValueError("delta_snapshot artifact integrity receipt is missing")
    return DeltaSnapshotReceipt(
        snapshot_token=snapshot_token,
        scope_hash=scope_hash,
        columns=(*tuple(columns), DELTA_HASH_COLUMN),
        row_count=integrity.require_rows_exported(),
        checksum=f"sha256:{integrity.sha256}",
        complete=True,
    )


def _append_row_hashes(artifact: FileExportArtifact, columns: Sequence[str]) -> FileExportArtifact:
    fd, path = tempfile.mkstemp(
        prefix="dpone_delta_snapshot_hashed_",
        suffix=".bcp",
        dir=str(Path(artifact.file_path).parent),
    )
    os.close(fd)
    try:
        row_count = 0
        with open(artifact.file_path, "rb") as source, open(path, "wb") as target:
            for row_number, line in enumerate(source, start=1):
                row = line.removesuffix(b"\n")
                if len(row.split(b"\t")) != len(columns):
                    raise ValueError(f"delta_snapshot row {row_number} has an invalid field count")
                target.write(row + b"\t" + _row_hash(row).encode("ascii") + b"\n")
                row_count = row_number
        return FileExportArtifact(
            path,
            [*columns, DELTA_HASH_COLUMN],
            compressed=False,
            format="mssql-delimited",
            bulk_text_codec=artifact.bulk_text_codec,
            rows_exported=row_count,
        )
    except BaseException:
        Path(path).unlink(missing_ok=True)
        raise


def _row_hash(row: bytes) -> str:
    return hashlib.sha256(row.decode("utf-8").encode("utf-16le")).hexdigest()


def _require_safe_file(artifact: FileExportArtifact) -> None:
    if artifact.compressed or artifact.format != "mssql-delimited" or artifact.bulk_text_codec is None:
        raise ValueError(
            "delta_snapshot requires an uncompressed dpone mssql-delimited artifact with BulkTextCodec metadata"
        )


__all__ = ["PostgresDeltaSnapshotFileArtifact"]
