"""Read-only capabilities for source-free SqlClient chunk inspection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.mssql_native_chunks import NativeChunkReceipt
from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceReceipt
from dpone.contracts.mssql_sqlclient_native_chunk import SqlClientNativeChunkProjection
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits


@dataclass(frozen=True, slots=True)
class NativeChunkInspectionReference:
    """Parent-owned coordinate and immutable evidence locators for one chunk."""

    receipt: NativeChunkReceipt
    projection_sha256: str
    lifecycle_verification_sha256: str
    lifecycle_revision: int
    registration_receipt: SqlClientEvidenceReceipt
    verification_receipt: SqlClientEvidenceReceipt


class NativeChunkInspectionIndex(Protocol):
    """Enumerate verified chunks from the durable v4 parent journal."""

    def references(self) -> tuple[NativeChunkInspectionReference, ...]: ...


class NativeChunkInspectionEvidence(Protocol):
    """Read exact acknowledged evidence without create or repair authority."""

    def read(self, receipt: SqlClientEvidenceReceipt) -> bytes: ...


class NativeChunkInspector(Protocol):
    """Reproduce an immutable P10f projection without source or effects."""

    def inspect(self, ordinal: int, limits: TdsDirectoryLimits) -> SqlClientNativeChunkProjection: ...


__all__ = (
    "NativeChunkInspectionEvidence",
    "NativeChunkInspectionIndex",
    "NativeChunkInspectionReference",
    "NativeChunkInspector",
)
