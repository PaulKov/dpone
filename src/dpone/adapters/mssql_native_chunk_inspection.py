"""Thin parent-journal and immutable-evidence inspection adapters."""

from __future__ import annotations

from hashlib import sha256
from typing import Any, Protocol

from dpone.adapters.mssql_sqlclient_native_receipt_contracts import (
    SCHEMA,
    native,
    validate_native_chunk_receipt,
)
from dpone.ports.evidence import ExactEvidenceReaderV1
from dpone.ports.mssql_native_chunk_inspection import NativeChunkInspectionReference

ERROR = "mssql_native.sqlclient_chunk_inspection_unknown"


class NativeParentJournalSnapshot(Protocol):
    """Existing parent journal's detached, read-only snapshot surface."""

    @property
    def data(self) -> dict[str, Any] | None: ...


class NativeParentJournalInspectionIndex:
    """Validate and enumerate exact coordinates from one schema-v4 parent."""

    def __init__(self, journal: NativeParentJournalSnapshot) -> None:
        self._journal = journal

    def references(self) -> tuple[NativeChunkInspectionReference, ...]:
        try:
            data = self._journal.data
            if (
                not isinstance(data, dict)
                or data.get("version") != 4
                or data.get("phase") != "stage_complete"
                or not isinstance(data.get("chunks"), dict)
                or data.get("identity", {}).get("transport", {}).get("backend") != "mssql_sqlclient"
            ):
                raise ValueError(ERROR)
            chunks = data["chunks"]
            if set(chunks) != {str(index) for index in range(len(chunks))}:
                raise ValueError(ERROR)
            run_id = data["identity"].get("run_id")
            window_fingerprint = data["identity"].get("window_fingerprint")
            parent_plan_sha256 = native.canonical_digest(data["identity"])
            if type(run_id) is not str or not run_id or type(window_fingerprint) is not str:
                raise ValueError(ERROR)
            return tuple(
                self._reference(
                    ordinal,
                    chunks[str(ordinal)],
                    run_id,
                    parent_plan_sha256,
                    window_fingerprint,
                )
                for ordinal in range(len(chunks))
            )
        except (AttributeError, KeyError, TypeError, ValueError, OverflowError, RecursionError):
            raise ValueError(ERROR) from None

    @staticmethod
    def _reference(
        ordinal: int,
        chunk: object,
        run_id: str,
        parent_plan_sha256: str,
        window_fingerprint: str,
    ) -> NativeChunkInspectionReference:
        if (
            not isinstance(chunk, dict)
            or chunk.get("phase") != "verified"
            or not isinstance(chunk.get("receipt"), dict)
        ):
            raise ValueError(ERROR)
        raw_receipt = dict(chunk["receipt"])
        receipt = native.NativeChunkReceipt(**raw_receipt)
        if (
            receipt.ordinal != ordinal
            or type(chunk.get("attempt")) is not int
            or receipt.attempt_id != f"{run_id}-{ordinal}-{chunk['attempt']}"
        ):
            raise ValueError(ERROR)
        evidence = validate_native_chunk_receipt(receipt)
        if evidence.plan_sha256 != parent_plan_sha256 or evidence.window_fingerprint != window_fingerprint:
            raise ValueError(ERROR)
        projection_sha256 = evidence.projection_sha256
        registration = evidence.registration_receipt
        verification = evidence.verification_receipt
        if registration.attempt_sha256 != verification.attempt_sha256:
            raise ValueError(ERROR)
        return NativeChunkInspectionReference(
            receipt,
            projection_sha256,
            evidence.lifecycle_verification_sha256,
            evidence.lifecycle_revision,
            registration,
            verification,
        )


class ExactNativeChunkInspectionEvidence:
    """Bind a generic exact reader to one validated SqlClient receipt."""

    def __init__(self, reader: ExactEvidenceReaderV1) -> None:
        self._reader = reader

    def read(self, receipt: native.SqlClientEvidenceReceipt) -> bytes:
        try:
            receipt.__post_init__()
            payload = self._reader.read(receipt.relative_name, receipt.byte_count, receipt.payload_sha256)
            if (
                type(payload) is not bytes
                or len(payload) != receipt.byte_count
                or sha256(payload).hexdigest() != receipt.payload_sha256
            ):
                raise ValueError(ERROR)
            return payload
        except (AttributeError, OSError, TypeError, ValueError, OverflowError, RecursionError):
            raise ValueError(ERROR) from None


__all__ = (
    "ERROR",
    "SCHEMA",
    "ExactNativeChunkInspectionEvidence",
    "NativeParentJournalInspectionIndex",
)
