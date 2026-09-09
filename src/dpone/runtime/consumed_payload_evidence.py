"""Cryptographic evidence for bytes consumed by MSSQL native staging.

The generic MSSQL transaction receipt must identify the exact source bytes,
their ordered wire/provenance interpretation, and the native projection that
was committed.  Row counts alone (and SQL Server ``CHECKSUM_AGG`` in
particular) are not an identity authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.runtime.artifact_integrity import ArtifactIntegrityError

_SHA256 = re.compile(r"[0-9a-f]{64}")
_CANONICALIZATION_CONTRACT = "dpone.mssql.native-canonical.v2"


@dataclass(frozen=True, slots=True)
class ConsumedPayloadPartEvidence:
    """One verified immutable file part loaded into raw staging."""

    order_key: str
    artifact_sha256: str
    artifact_size_bytes: int
    wire_contract_sha256: str
    wire_schema_sha256: str
    source_provenance_sha256: str
    validated_schema_sha256: str
    contract_validation_sha256: str | None
    declared_rows: int
    actual_raw_rows: int
    version: int = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            _raise("consumed_payload.part_version_unsupported")
        if not self.order_key:
            _raise("consumed_payload.order_key_required")
        for value in (
            self.artifact_sha256,
            self.wire_contract_sha256,
            self.wire_schema_sha256,
            self.source_provenance_sha256,
            self.validated_schema_sha256,
        ):
            _require_sha256(value)
        if self.contract_validation_sha256 is not None:
            _require_sha256(self.contract_validation_sha256)
        _require_count(self.artifact_size_bytes, "artifact_size")
        _require_count(self.declared_rows, "declared_rows")
        _require_count(self.actual_raw_rows, "actual_raw_rows")
        if self.declared_rows != self.actual_raw_rows:
            _raise("consumed_payload.raw_row_count_mismatch")

    @property
    def part_sha256(self) -> str:
        return _digest(self.to_payload())

    def to_payload(self) -> dict[str, object]:
        return {
            "actual_raw_rows": self.actual_raw_rows,
            "artifact_sha256": self.artifact_sha256,
            "artifact_size_bytes": self.artifact_size_bytes,
            "contract_validation_sha256": self.contract_validation_sha256,
            "declared_rows": self.declared_rows,
            "order_key": self.order_key,
            "source_provenance_sha256": self.source_provenance_sha256,
            "validated_schema_sha256": self.validated_schema_sha256,
            "version": self.version,
            "wire_contract_sha256": self.wire_contract_sha256,
            "wire_schema_sha256": self.wire_schema_sha256,
        }


@dataclass(frozen=True, slots=True)
class ConsumedPayloadEvidence:
    """Ordered source-part manifest plus an exact native projection receipt."""

    parts: tuple[ConsumedPayloadPartEvidence, ...] = ()
    actual_native_rows: int | None = None
    native_contract_sha256: str | None = None
    version: int = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            _raise("consumed_payload.version_unsupported")
        keys = tuple(part.order_key for part in self.parts)
        if any(not key for key in keys) or len(set(keys)) != len(keys):
            _raise("consumed_payload.order_key_duplicate")
        if keys != tuple(sorted(keys)):
            _raise("consumed_payload.parts_not_canonical")
        if (self.actual_native_rows is None) != (self.native_contract_sha256 is None):
            _raise("consumed_payload.native_receipt_incomplete")
        if self.actual_native_rows is not None:
            _require_count(self.actual_native_rows, "actual_native_rows")
            _require_sha256(str(self.native_contract_sha256))
            if self.actual_native_rows != self.actual_raw_rows:
                _raise("consumed_payload.native_row_count_mismatch")

    @classmethod
    def empty(cls) -> ConsumedPayloadEvidence:
        return cls()

    @property
    def declared_rows(self) -> int:
        return sum(part.declared_rows for part in self.parts)

    @property
    def actual_raw_rows(self) -> int:
        return sum(part.actual_raw_rows for part in self.parts)

    @property
    def manifest_sha256(self) -> str:
        return _digest(
            {
                "actual_native_rows": self.actual_native_rows,
                "actual_raw_rows": self.actual_raw_rows,
                "declared_rows": self.declared_rows,
                "native_contract_sha256": self.native_contract_sha256,
                "part_sha256": [part.part_sha256 for part in self.parts],
                "version": self.version,
            }
        )

    def append_verified_file(
        self,
        file_artifact: Any,
        *,
        validated_schema: Sequence[tuple[str, str]],
        source_provenance_sha256: str,
        actual_raw_rows: int,
        order_key: str | None = None,
        verified_integrity_receipt: Any | None = None,
    ) -> ConsumedPayloadEvidence:
        """Append one consumer-verified part and deterministically reorder it."""

        if self.actual_native_rows is not None:
            _raise("consumed_payload.already_native")
        integrity = verified_integrity_receipt
        if integrity is None:
            integrity = file_artifact.require_integrity_receipt()
        elif integrity != getattr(file_artifact, "integrity_receipt", None):
            _raise("consumed_payload.integrity_receipt_mismatch")
        integrity.require_rows_exported()
        wire_contract = file_artifact.wire_contract()
        frozen_schema = tuple((str(name), str(dtype)) for name, dtype in validated_schema)
        if tuple(wire_contract.columns) != tuple(name for name, _dtype in frozen_schema):
            _raise("consumed_payload.wire_schema_alignment")
        _require_sha256(source_provenance_sha256)
        wire_schema_sha256 = canonical_schema_sha256(frozen_schema)
        validation_sha256 = _contract_validation_sha256(
            file_artifact,
            wire_schema_sha256=wire_schema_sha256,
            integrity=integrity,
        )
        resolved_order_key = order_key or _next_order_key(self.parts)
        part = ConsumedPayloadPartEvidence(
            order_key=resolved_order_key,
            artifact_sha256=integrity.sha256,
            artifact_size_bytes=integrity.size_bytes,
            wire_contract_sha256=wire_contract.sha256,
            wire_schema_sha256=wire_schema_sha256,
            source_provenance_sha256=source_provenance_sha256,
            validated_schema_sha256=_digest(
                {
                    "source_provenance_sha256": source_provenance_sha256,
                    "wire_schema_sha256": wire_schema_sha256,
                }
            ),
            contract_validation_sha256=validation_sha256,
            declared_rows=integrity.require_rows_exported(),
            actual_raw_rows=actual_raw_rows,
        )
        return ConsumedPayloadEvidence(tuple(sorted((*self.parts, part), key=lambda item: item.order_key)))

    def with_native_rows(
        self,
        actual_native_rows: int,
        *,
        native_contract_sha256: str,
    ) -> ConsumedPayloadEvidence:
        if not self.parts:
            _raise("consumed_payload.parts_required")
        return ConsumedPayloadEvidence(
            self.parts,
            actual_native_rows=actual_native_rows,
            native_contract_sha256=native_contract_sha256,
        )

    def require_complete(self, *, native: bool = True) -> ConsumedPayloadEvidence:
        if not self.parts:
            _raise("consumed_payload.parts_required")
        if native and (self.actual_native_rows is None or self.native_contract_sha256 is None):
            _raise("consumed_payload.native_receipt_required")
        _require_sha256(self.manifest_sha256)
        return self


def canonical_schema_sha256(schema: Sequence[tuple[str, str]]) -> str:
    """Hash one exact ordered wire schema."""

    return _digest([[str(name), str(dtype)] for name, dtype in schema])


def canonical_source_provenance_sha256(
    *,
    relation_dialect: Any | None,
    relation_schema: Sequence[tuple[str, str]] | None,
    relation_metadata: Sequence[Any] | None,
    fallback_schema: Sequence[tuple[str, str]],
) -> str:
    """Bind the wire schema to its immutable declared source authority."""

    dialect = getattr(relation_dialect, "value", relation_dialect)
    declared = relation_schema if relation_schema is not None else fallback_schema
    metadata = [str(getattr(column, "identity_token", "")) for column in (relation_metadata or ())]
    if relation_metadata is not None and any(not token for token in metadata):
        _raise("consumed_payload.source_provenance_invalid")
    return _digest(
        {
            "authority": "catalog" if relation_schema is not None else "runtime_declared_schema",
            "dialect": None if dialect is None else str(dialect),
            "metadata": metadata,
            "relation_schema": [[str(name), str(dtype)] for name, dtype in declared],
        }
    )


def canonical_native_contract_sha256(
    columns: Sequence[Mapping[str, object]],
    *,
    source_wire_contract_sha256s: Sequence[str],
) -> str:
    """Hash native conversion semantics, physical shape and codec identities."""

    wire_hashes = tuple(str(value) for value in source_wire_contract_sha256s)
    if not wire_hashes or any(not _SHA256.fullmatch(value) for value in wire_hashes):
        _raise("consumed_payload.native_wire_contract_invalid")
    normalized = [
        {
            "collation": value.get("collation"),
            "nullable": bool(value["nullable"]),
            "target_name": str(value["target_name"]),
            "target_type": str(value["target_type"]),
            "wire_name": str(value["wire_name"]),
            "generation_contract": (
                None if value.get("generation_contract") is None else str(value["generation_contract"])
            ),
        }
        for value in columns
    ]
    if not normalized or any(not value["wire_name"] or not value["target_name"] for value in normalized):
        _raise("consumed_payload.native_contract_invalid")
    return _digest(
        {
            "canonicalization_contract": _CANONICALIZATION_CONTRACT,
            "columns": normalized,
            "source_wire_contract_sha256s": wire_hashes,
            "version": 1,
        }
    )


def _contract_validation_sha256(
    artifact: Any,
    *,
    wire_schema_sha256: str,
    integrity: Any,
) -> str | None:
    receipt = getattr(artifact, "contract_validation_receipt", None)
    if receipt is None:
        return None
    required = (
        "artifact_sha256",
        "artifact_size_bytes",
        "contract_sha256",
        "schema_sha256",
        "wire_contract_sha256",
        "rows_validated",
        "validator_version",
    )
    if any(not hasattr(receipt, name) for name in required):
        _raise("consumed_payload.contract_validation_receipt_invalid")
    if (
        receipt.artifact_sha256 != integrity.sha256
        or receipt.artifact_size_bytes != integrity.size_bytes
        or receipt.schema_sha256 != wire_schema_sha256
        or receipt.wire_contract_sha256 != artifact.wire_contract().sha256
        or receipt.rows_validated != integrity.require_rows_exported()
    ):
        _raise("consumed_payload.contract_validation_receipt_mismatch")
    return _digest({name: getattr(receipt, name) for name in required})


def _next_order_key(parts: Sequence[ConsumedPayloadPartEvidence]) -> str:
    return f"sequential:{len(parts):020d}"


def _require_sha256(value: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        _raise("consumed_payload.sha256_invalid")


def _require_count(value: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _raise(f"consumed_payload.{label}_invalid")


def _digest(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _raise(code: str) -> None:
    raise ArtifactIntegrityError(code)


__all__ = [
    "ArtifactIntegrityError",
    "canonical_native_contract_sha256",
    "canonical_schema_sha256",
    "canonical_source_provenance_sha256",
    "ConsumedPayloadEvidence",
    "ConsumedPayloadPartEvidence",
]
