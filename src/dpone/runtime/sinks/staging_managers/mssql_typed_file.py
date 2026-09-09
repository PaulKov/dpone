"""One-pass immutable-file ingestion into native MSSQL staging."""

from __future__ import annotations

import hashlib
import hmac
import math
import os
import stat
import tempfile
import time
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from threading import RLock
from types import TracebackType
from typing import IO, Any, Literal

from dpone.runtime.artifact_integrity import ArtifactIntegrityError, FileArtifactReceipt, FileIdentity
from dpone.runtime.bulk_options import BulkOptionsResolver
from dpone.runtime.consumed_payload_evidence import (
    ConsumedPayloadEvidence,
    canonical_native_contract_sha256,
)
from dpone.runtime.incremental_snapshot import DELTA_HASH_COLUMN, KEY_HASH_COLUMN
from dpone.runtime.sinks.staging_managers.mssql_staging_evidence import (
    ensure_staging_evidence_authority,
    file_order_key,
    require_source_provenance,
)
from dpone.runtime.sinks.staging_managers.mssql_typed_bcp import (
    write_format_file,
    write_length_prefixed_row,
)
from dpone.runtime.sinks.staging_managers.mssql_typed_values import (
    MssqlTypedValueDecoder,
)

_HASH_COLUMNS = frozenset({DELTA_HASH_COLUMN, KEY_HASH_COLUMN})
_BUFFER_BYTES = 16 * 1024 * 1024


class VerifiedArtifactLineReader:
    """Stream exact artifact bytes once and prove their receipt at EOF."""

    def __init__(self, artifact: Any) -> None:
        receipt = getattr(artifact, "integrity_receipt", None)
        if not isinstance(receipt, FileArtifactReceipt):
            raise ArtifactIntegrityError("artifact_integrity.receipt_missing")
        if receipt.wire_contract_sha256 != artifact.wire_contract().sha256:
            raise ArtifactIntegrityError("artifact_integrity.wire_contract_mismatch")
        receipt.require_rows_exported()
        self._artifact = artifact
        self._receipt = receipt
        self._handle: IO[bytes] | None = None
        self._digest = hashlib.sha256()
        self._bytes_read = 0
        self._rows_read = 0
        self._completed = False

    def __enter__(self) -> VerifiedArtifactLineReader:
        path = self._artifact.file_path
        self._receipt.owned_scope.verify(path)
        if _path_identity(path) != self._receipt.identity:
            raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_BINARY", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
        try:
            if FileIdentity.from_stat(os.fstat(descriptor)) != self._receipt.identity:
                raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")
            self._handle = os.fdopen(descriptor, "rb", buffering=_BUFFER_BYTES, closefd=True)
        except BaseException:
            os.close(descriptor)
            raise
        return self

    def __iter__(self):
        for line in self._require_handle():
            self._digest.update(line)
            self._bytes_read += len(line)
            self._rows_read += 1
            yield line

    def require_complete(self) -> FileArtifactReceipt:
        handle = self._require_handle()
        if handle.read(1):
            raise ArtifactIntegrityError("artifact_integrity.consumer_did_not_reach_eof")
        descriptor_identity = FileIdentity.from_stat(os.fstat(handle.fileno()))
        if descriptor_identity != self._receipt.identity:
            raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")
        if self._bytes_read != self._receipt.size_bytes:
            raise ArtifactIntegrityError("artifact_integrity.byte_count_mismatch")
        if self._digest.hexdigest() != self._receipt.sha256:
            raise ArtifactIntegrityError("artifact_integrity.sha256_mismatch")
        if self._rows_read != self._receipt.require_rows_exported():
            raise ArtifactIntegrityError("artifact_integrity.rows_exported_mismatch")
        self._receipt.owned_scope.verify(self._artifact.file_path)
        if _path_identity(self._artifact.file_path) != descriptor_identity:
            raise ArtifactIntegrityError("artifact_integrity.file_identity_mismatch")
        self._completed = True
        return self._receipt

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        del exc_type, exc, traceback
        handle, self._handle = self._handle, None
        if handle is not None:
            handle.close()
        return False

    def _require_handle(self) -> IO[bytes]:
        if self._handle is None:
            raise RuntimeError("artifact_integrity.reader_not_open")
        if self._completed:
            raise RuntimeError("artifact_integrity.reader_already_completed")
        return self._handle


def _path_identity(path: str) -> FileIdentity:
    try:
        details = os.lstat(path)
    except OSError as exc:
        raise ArtifactIntegrityError("artifact_integrity.file_unavailable") from exc
    if not stat.S_ISREG(details.st_mode):
        raise ArtifactIntegrityError("artifact_integrity.regular_file_required")
    return FileIdentity.from_stat(details)


class MssqlTypedFileIngestor:
    """Decode and BCP-load one immutable artifact in a single file pass."""

    def __init__(self, connector: Any, logger: Any) -> None:
        self._connector = connector
        self._logger = logger
        self._lock = RLock()

    def ingest(self, staging: Any, artifact: Any) -> int:
        if not staging.typed_file_ingestion:
            raise RuntimeError("mssql_typed_ingest.staging_contract_required")
        if artifact.compressed or artifact.format != "mssql-delimited":
            raise ArtifactIntegrityError("mssql_typed_ingest.wire_format_unsupported")
        codec = artifact.bulk_text_codec
        if codec is None or not callable(getattr(codec, "decode", None)):
            raise ArtifactIntegrityError("mssql_typed_ingest.codec_required")
        columns = tuple(str(value) for value in artifact.columns)
        wire_columns = (
            tuple(str(column) for column, _dtype in staging.wire_schema)
            if staging.wire_schema
            else tuple(str(column) for column in staging.columns)
        )
        if wire_columns != columns or tuple(staging.columns[: len(columns)]) != columns:
            raise ArtifactIntegrityError("mssql_typed_ingest.column_alignment_mismatch")
        types = tuple(str(staging.column_types[column]) for column in columns)
        validate_row_hash = bool(staging.typed_file_row_hash_validation)
        if validate_row_hash and (not columns or columns[-1] not in _HASH_COLUMNS):
            raise ArtifactIntegrityError("mssql_typed_ingest.row_hash_column_required")
        batch_size = _batch_size(staging)
        decoder = MssqlTypedValueDecoder(codec)
        started = time.perf_counter()
        with tempfile.TemporaryDirectory(
            prefix=".dpone-typed-bcp-",
            dir=str(Path(artifact.file_path).resolve().parent),
        ) as work_dir:
            data_path = Path(work_dir) / "payload.bin"
            format_path = Path(work_dir) / "payload.fmt"
            reject_path = Path(work_dir) / "rejects.log"
            write_format_file(format_path, types)
            with data_path.open("xb", buffering=16 * 1024 * 1024) as typed_file:
                with VerifiedArtifactLineReader(artifact) as reader:
                    for row_number, line in enumerate(reader, start=1):
                        write_length_prefixed_row(
                            typed_file,
                            _decode_row(
                                line,
                                row_number=row_number,
                                columns=columns,
                                target_types=types,
                                decoder=decoder,
                                validate_row_hash=validate_row_hash,
                            ),
                        )
                    receipt = reader.require_complete()
            expected = receipt.require_rows_exported()
            with self._lock:
                before = _count_rows(self._connector, staging)
                copied = self._bcp_import(
                    staging,
                    data_path=data_path,
                    format_path=format_path,
                    reject_path=reject_path,
                    expected_rows=expected,
                )
                actual = _count_rows(self._connector, staging) - before
            if copied != expected:
                raise ArtifactIntegrityError("mssql_typed_ingest.bcp_row_count_mismatch")
            if actual != expected:
                raise ArtifactIntegrityError("mssql_typed_ingest.staging_row_count_mismatch")
            if reject_path.exists() and reject_path.stat().st_size:
                raise ArtifactIntegrityError("mssql_typed_ingest.bcp_rejected_rows")
        batches = math.ceil(expected / batch_size) if expected else 0
        staging.typed_transport = "mssql.bcp.length_prefixed_utf8.v1"
        staging.typed_batch_count = batches
        self._record_evidence(
            staging,
            artifact,
            receipt=receipt,
            rows=actual,
            defer_native=bool(staging.typed_file_deferred_native_evidence),
        )
        self._log_complete(
            rows=actual,
            batches=batches,
            batch_size=batch_size,
            elapsed=time.perf_counter() - started,
        )
        return actual

    def _bcp_import(
        self,
        staging: Any,
        *,
        data_path: Path,
        format_path: Path,
        reject_path: Path,
        expected_rows: int,
    ) -> int:
        if expected_rows == 0:
            return 0
        bulk = staging.bulk_options or BulkOptionsResolver.resolve({})
        options = bulk.bcp.to_bcp_options(
            bcp_path=getattr(self._connector, "bcp_path", "bcp"),
            trust_server_certificate=self._connector.trust_server_certificate == "yes",
        )
        options = replace(options, error_file=str(reject_path))
        copied = self._connector.bcp_import_format(
            staging.schema,
            staging.table,
            str(data_path),
            str(format_path),
            options=options,
            database=staging.database,
        )
        if isinstance(copied, bool) or not isinstance(copied, int) or copied < 0:
            raise ArtifactIntegrityError("mssql_typed_ingest.bcp_row_count_invalid")
        return copied

    @staticmethod
    def _record_evidence(
        staging: Any,
        artifact: Any,
        *,
        receipt: Any,
        rows: int,
        defer_native: bool,
    ) -> None:
        ensure_staging_evidence_authority(staging)
        evidence = staging.consumed_payload_evidence or ConsumedPayloadEvidence.empty()
        evidence = evidence.append_verified_file(
            artifact,
            validated_schema=staging.wire_schema,
            source_provenance_sha256=require_source_provenance(staging),
            actual_raw_rows=rows,
            order_key=file_order_key(artifact, evidence),
            verified_integrity_receipt=receipt,
        )
        if defer_native:
            staging.consumed_payload_evidence = evidence
            return
        columns = [
            {
                "wire_name": column,
                "target_name": column,
                "target_type": staging.column_types[column],
                "nullable": staging.target_column_nullability.get(column, True),
                "collation": staging.target_column_collations.get(column),
                "generation_contract": None,
            }
            for column in staging.columns
        ]
        staging.consumed_payload_evidence = evidence.with_native_rows(
            rows,
            native_contract_sha256=canonical_native_contract_sha256(
                columns,
                source_wire_contract_sha256s=tuple(part.wire_contract_sha256 for part in evidence.parts),
            ),
        )

    def _log_complete(self, *, rows: int, batches: int, batch_size: int, elapsed: float) -> None:
        logger = getattr(self._logger, "log_etl_progress", None)
        if callable(logger):
            logger(
                "MSSQL_TYPED_STAGING_COMPLETE",
                {
                    "Rows": rows,
                    "Batches": batches,
                    "Batch Size": batch_size,
                    "Duration": f"{elapsed:.1f}s",
                    "Rows Per Second": round(rows / elapsed) if elapsed > 0 else rows,
                    "Transport": "mssql.bcp.length_prefixed_utf8.v1",
                },
            )


def _decode_row(
    line: bytes,
    *,
    row_number: int,
    columns: Sequence[str],
    target_types: Sequence[str],
    decoder: MssqlTypedValueDecoder,
    validate_row_hash: bool,
) -> tuple[object | None, ...]:
    if not line.endswith(b"\n"):
        raise ArtifactIntegrityError("mssql_typed_ingest.row_terminator_missing")
    wire_row = line[:-1]
    values = wire_row.split(b"\t")
    if len(values) != len(columns):
        raise ArtifactIntegrityError("mssql_typed_ingest.field_count_mismatch")
    if validate_row_hash:
        payload = b"\t".join(values[:-1])
        try:
            expected = hashlib.sha256(payload.decode("utf-8").encode("utf-16le")).hexdigest().encode("ascii")
        except UnicodeDecodeError as exc:
            raise ArtifactIntegrityError(f"mssql_typed_ingest.utf8_invalid:row={row_number}") from exc
        provided = values[-1]
        if len(provided) != 64 or not hmac.compare_digest(provided.lower(), expected):
            code = "delta_checksum_mismatch" if columns[-1] == DELTA_HASH_COLUMN else "key_checksum_mismatch"
            raise ArtifactIntegrityError(f"mssql_snapshot_reconciliation.{code}")
    return tuple(
        decoder.decode(raw, column=column, target_type=target_type)
        for raw, column, target_type in zip(values, columns, target_types, strict=True)
    )


def _count_rows(connector: Any, staging: Any) -> int:
    qualified = connector.qualified_name(staging.schema, staging.table, database=staging.database)
    rows = connector.get_records(f"SELECT COUNT_BIG(*) AS row_count FROM {qualified}", as_dict=True)
    if not rows:
        raise ArtifactIntegrityError("mssql_typed_ingest.staging_row_count_unavailable")
    value = rows[0]["row_count"] if isinstance(rows[0], dict) else rows[0][0]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ArtifactIntegrityError("mssql_typed_ingest.staging_row_count_unavailable")
    return value


def _batch_size(staging: Any) -> int:
    raw = getattr(getattr(staging, "bulk_options", None), "bcp", None)
    value = getattr(raw, "batch_size", 10_000)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 100_000:
        raise ValueError("mssql_typed_ingest.batch_size_invalid")
    return value


__all__ = ["MssqlTypedFileIngestor", "VerifiedArtifactLineReader"]
