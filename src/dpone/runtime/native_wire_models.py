"""Connector-neutral source-native wire contracts and evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any

NATIVE_WIRE_SCHEMA_VERSION = "dpone.native_transfer.native_wire.v1"


@dataclass(frozen=True, slots=True)
class NativeWireColumnLayout:
    """Physical layout for one source-native column."""

    name: str
    source_type: str
    target_type: str
    nullable: bool
    storage_type: str
    prefix_width: int = 0
    fixed_length: int | None = None
    precision: int | None = None
    scale: int | None = None
    encoding: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SourceNativeWireContract:
    """Contract required to decode a source-native bulk artifact."""

    schema_version: str
    source_system: str
    source_format: str
    target_format: str
    columns: tuple[NativeWireColumnLayout, ...]
    schema_hash: str
    query_hash: str
    type_layout_hash: str
    bcp_version: str | None = None
    warnings: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "source_system": self.source_system,
            "source_format": self.source_format,
            "target_format": self.target_format,
            "columns": [column.to_dict() for column in self.columns],
            "schema_hash": self.schema_hash,
            "query_hash": self.query_hash,
            "type_layout_hash": self.type_layout_hash,
            "bcp_version": self.bcp_version,
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
        }

    def to_evidence(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "source_system": self.source_system,
            "source_format": self.source_format,
            "target_format": self.target_format,
            "schema_hash": self.schema_hash,
            "query_hash": self.query_hash,
            "type_layout_hash": self.type_layout_hash,
            "bcp_version": self.bcp_version,
            "column_count": len(self.columns),
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
        }


@dataclass(slots=True)
class NativeWireEvidence:
    """Runtime evidence for one native-wire transcode."""

    source_format: str
    target_format: str
    schema_hash: str
    query_hash: str
    type_layout_hash: str
    rows: int = 0
    decoded_bytes: int = 0
    encoded_bytes: int = 0
    checksum: str = "sha256:" + "0" * 64
    failure_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": NATIVE_WIRE_SCHEMA_VERSION,
            "source_format": self.source_format,
            "target_format": self.target_format,
            "schema_hash": self.schema_hash,
            "query_hash": self.query_hash,
            "type_layout_hash": self.type_layout_hash,
            "rows": self.rows,
            "decoded_bytes": self.decoded_bytes,
            "encoded_bytes": self.encoded_bytes,
            "checksum": self.checksum,
            "failure_code": self.failure_code,
        }


def stable_hash(payload: Any) -> str:
    """Return a stable sha256 hash for evidence payloads."""

    return "sha256:" + sha256(repr(payload).encode("utf-8")).hexdigest()


__all__ = [
    "NATIVE_WIRE_SCHEMA_VERSION",
    "NativeWireColumnLayout",
    "NativeWireEvidence",
    "SourceNativeWireContract",
    "stable_hash",
]
