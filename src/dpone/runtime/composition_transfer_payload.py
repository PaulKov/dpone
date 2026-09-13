"""Retain exact eager transfer source bytes before executor-owned cleanup.

The invocation-scoped supervisor owns this directory. Captures are exclusive,
fsynced and never overwritten. Reopening rehashes the retained original; a later
source query is deliberately not an input. Only the certified uncompressed
PostgreSQL delimited file profile is accepted in this execution cell.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.runtime.consumed_payload_evidence import (
    ConsumedPayloadEvidence,
    canonical_native_contract_sha256,
    canonical_source_provenance_sha256,
)
from dpone.runtime.etl.source_extraction_lifecycle import SourceExtractionLifecycleService
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.mssql_native_chunks_files import native_multiset_digest
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.staging_managers.mssql_typed_values import MssqlTypedValueDecoder
from dpone.runtime.support.bulk_text_codec import BulkTextCodec


@dataclass(frozen=True, slots=True)
class RetainedTransferPayload:
    """Reopened source original and its captured schema/provenance authority."""

    artifact: FileExportArtifact
    schema: tuple[tuple[str, str], ...]
    source_provenance_sha256: str

    def evidence(self) -> ConsumedPayloadEvidence:
        receipt = self.artifact.require_integrity_receipt()
        return ConsumedPayloadEvidence.empty().append_verified_file(
            self.artifact,
            validated_schema=self.schema,
            source_provenance_sha256=self.source_provenance_sha256,
            actual_raw_rows=receipt.require_rows_exported(),
        )

    def reconcile_native(
        self, columns: Any, target_rows: Any, *, max_rows: int, max_row_bytes: int
    ) -> TransferPayloadReconciliation:
        """Compare retained wire and observed target through the same production codec.

        This produces factual content evidence; the caller independently checks
        receipt authority and transaction continuity before deciding an outcome.
        """
        evidence = self.evidence()
        native = canonical_native_contract_sha256(
            columns, source_wire_contract_sha256s=tuple(part.wire_contract_sha256 for part in evidence.parts)
        )
        complete = evidence.with_native_rows(evidence.actual_raw_rows, native_contract_sha256=native)
        if tuple(self.artifact.columns) != tuple(column["target_name"] for column in columns):
            raise CompositionAdmissionError("transfer_payload_columns")
        schema = tuple(
            (column["target_name"], column["target_type"] + (" nullable" if column["nullable"] else ""))
            for column in columns
        )
        contract = build_mssql_bcp_native_contract(
            schema=schema, query="composition-observation", target_format="mssql_native"
        )
        encoder = MssqlNativeEncoder(contract, max_row_bytes=max_row_bytes)
        source_count, source_digest = _digest(_source_rows(self, columns, max_row_bytes), encoder, max_rows)
        target_count, target_digest = _digest(target_rows, encoder, max_rows)
        return TransferPayloadReconciliation(complete, source_count, target_count, source_digest, target_digest)


@dataclass(frozen=True, slots=True)
class TransferPayloadReconciliation:
    """Immutable facts about decoded content, without a commit or authority claim."""

    evidence: ConsumedPayloadEvidence
    source_rows: int
    target_rows: int
    source_digest: str
    target_digest: str


class CompositionTransferPayloadStore:
    """One attempt, one immutable extraction original; no reuse or replacement."""

    def __init__(self, root: Path, *, max_bytes: int = 64 * 1024 * 1024) -> None:
        if type(max_bytes) is not int or max_bytes <= 0:
            raise CompositionAdmissionError("transfer_payload_budget")
        info = root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise CompositionAdmissionError("transfer_payload_root")
        self.root, self.max_bytes = root, max_bytes

    def capture(self, attempt: Any, result: Any) -> None:
        """Verify and copy bounded source bytes before returning them to the sink."""
        attempt.__post_init__()
        artifact = result.artifact
        if type(artifact) is not FileExportArtifact:
            raise CompositionAdmissionError("transfer_payload_profile")
        wire = artifact.wire_contract()
        if (
            wire.format not in {"mssql-delimited", "tsv"}
            or wire.compressed
            or wire.has_header
            or artifact.bulk_text_codec != BulkTextCodec()
        ):
            raise CompositionAdmissionError("transfer_payload_profile")
        receipt = artifact.require_integrity_receipt()
        if receipt.size_bytes > self.max_bytes:
            raise CompositionAdmissionError("transfer_payload_budget")
        folder = self.root / attempt.attempt_sha256[7:]
        folder.mkdir(mode=0o700)
        target = folder / "source.bin"
        source_fd = os.open(artifact.file_path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            with os.fdopen(source_fd, "rb") as source, target.open("xb") as output:
                digest, count = sha256(), 0
                while chunk := source.read(min(1024 * 1024, self.max_bytes + 1 - count)):
                    count += len(chunk)
                    if count > self.max_bytes:
                        raise CompositionAdmissionError("transfer_payload_budget")
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            if count != receipt.size_bytes or digest.hexdigest() != receipt.sha256:
                raise CompositionAdmissionError("transfer_payload_changed")
            artifact.require_integrity_receipt()
            schema = tuple(tuple(row) for row in result.schema)
            metadata = {
                "attempt_sha256": attempt.attempt_sha256,
                "sha256": receipt.sha256,
                "bytes": receipt.size_bytes,
                "rows": receipt.require_rows_exported(),
                "format": wire.format,
                "schema": schema,
                "columns": wire.columns,
                "source_provenance_sha256": canonical_source_provenance_sha256(
                    relation_dialect=getattr(result, "relation_dialect", None),
                    relation_schema=getattr(result, "relation_schema", None),
                    relation_metadata=getattr(result, "relation_metadata", None),
                    fallback_schema=schema,
                ),
            }
            with (folder / "capture.json").open("xb") as output:
                output.write(canonical_json_bytes(metadata))
                output.flush()
                os.fsync(output.fileno())
            for path in (target, folder / "capture.json"):
                path.chmod(0o400)
            for directory in (folder, self.root):
                descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        except Exception:
            raise CompositionAdmissionError("transfer_payload_capture") from None

    def read(self, attempt: Any) -> RetainedTransferPayload:
        """Reopen retained bytes and verify their immutable capture before decoding."""
        import json

        attempt.__post_init__()
        folder = self.root / attempt.attempt_sha256[7:]
        raw = _read_owned(folder / "capture.json", 1024 * 1024)
        metadata = json.loads(raw)
        if canonical_json_bytes(metadata) != raw or metadata["attempt_sha256"] != attempt.attempt_sha256:
            raise CompositionAdmissionError("transfer_payload_identity")
        content = _read_owned(folder / "source.bin", self.max_bytes)
        if len(content) != metadata["bytes"] or sha256(content).hexdigest() != metadata["sha256"]:
            raise CompositionAdmissionError("transfer_payload_changed")
        artifact = FileExportArtifact(
            str(folder / "source.bin"),
            columns=tuple(metadata["columns"]),
            format=metadata["format"],
            rows_exported=metadata["rows"],
            bulk_text_codec=BulkTextCodec(),
        )
        if artifact.require_integrity_receipt().sha256 != metadata["sha256"]:
            raise CompositionAdmissionError("transfer_payload_changed")
        return RetainedTransferPayload(
            artifact, tuple(tuple(row) for row in metadata["schema"]), metadata["source_provenance_sha256"]
        )


class CompositionTransferCaptureLifecycle:
    """Delegate lifecycle semantics, then retain completed source originals."""

    def __init__(self, store: CompositionTransferPayloadStore, attempt: Any, delegate: Any | None = None) -> None:
        self._store, self._attempt = store, attempt
        self._delegate = delegate or SourceExtractionLifecycleService()

    def assert_supported(self, source: Any, load_config: Any) -> None:
        self._delegate.assert_supported(source, load_config)

    def capture(self, extract: Any) -> Any:
        result = self._delegate.capture(extract)
        try:
            self._store.capture(self._attempt, result)
        except BaseException:
            result.artifact.cleanup()
            raise
        return result


def _read_owned(path: Path, limit: int) -> bytes:
    for parent in (path.parent, path.parent.parent):
        info = parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise CompositionAdmissionError("transfer_payload_root")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o377:
            raise CompositionAdmissionError("transfer_payload_permissions")
        content = stream.read(limit + 1)
        if len(content) > limit:
            raise CompositionAdmissionError("transfer_payload_budget")
        return content


def _digest(rows: Any, encoder: MssqlNativeEncoder, max_rows: int) -> tuple[int, str]:
    count, total = 0, 0
    for row in rows:
        count += 1
        if count > max_rows:
            raise CompositionAdmissionError("transfer_observation_budget")
        total = (total + int.from_bytes(sha256(encoder.encode_row(tuple(row))).digest(), "big")) % (1 << 256)
    return count, native_multiset_digest(count, total)


def _source_rows(payload: RetainedTransferPayload, columns: Any, max_row_bytes: int) -> Any:
    """Decode the retained certified wire with the production scalar codec."""
    artifact = payload.artifact
    codec = artifact.bulk_text_codec
    if codec is None:
        raise CompositionAdmissionError("transfer_payload_profile")
    decoder = MssqlTypedValueDecoder(codec)
    artifact.require_integrity_receipt()
    with open(artifact.file_path, "rb") as source:
        while line := source.readline(max_row_bytes + 1):
            if len(line) > max_row_bytes or not line.endswith(b"\n"):
                raise CompositionAdmissionError("transfer_payload_row")
            values = line[:-1].split(b"\t")
            if len(values) != len(columns):
                raise CompositionAdmissionError("transfer_payload_columns")
            yield tuple(
                decoder.decode(raw, column=column["target_name"], target_type=column["target_type"])
                for raw, column in zip(values, columns, strict=True)
            )
    artifact.require_integrity_receipt()
