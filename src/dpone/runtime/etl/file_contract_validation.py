"""Source-side validation receipts for immutable transfer files."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.connectors.bulk_text_codec import (
    BulkTextCodec,
    BulkTextFileReadError,
    decode_wire_value,
    iter_wire_rows,
)
from dpone.runtime.file_artifact_authority import FileVerificationBudget


class FileContractValidationError(RuntimeError):
    """Stable failure raised before a file can reach target staging."""

    code = "DPONE_FILE_CONTRACT_VALIDATION_BLOCKED"

    def __init__(self, blocker: str) -> None:
        self.blocker = blocker
        super().__init__(f"{self.code}:{blocker}")


@dataclass(frozen=True, slots=True)
class FileContractValidationReceipt:
    """Proof bound to exact bytes, schema positions, and logical contract."""

    artifact_sha256: str
    artifact_size_bytes: int
    contract_sha256: str
    schema_sha256: str
    validated_schema: tuple[tuple[str, str], ...]
    wire_contract_sha256: str
    rows_validated: int
    validator_version: int = 2

    def verify(
        self,
        artifact: Any,
        contract: SchemaContract,
        *,
        schema: tuple[tuple[str, str], ...] | None = None,
        verification_budget: FileVerificationBudget | None = None,
    ) -> None:
        integrity = (
            artifact.require_integrity_receipt()
            if verification_budget is None
            else artifact.require_integrity_receipt(verification_budget=verification_budget)
        )
        if integrity.sha256 != self.artifact_sha256 or integrity.size_bytes != self.artifact_size_bytes:
            raise FileContractValidationError("file_contract_receipt.artifact_identity_mismatch")
        frozen_schema = _normalized_schema(self.validated_schema)
        if _schema_digest(frozen_schema) != self.schema_sha256:
            raise FileContractValidationError("file_contract_receipt.schema_identity_mismatch")
        if schema is not None and _normalized_schema(schema) != frozen_schema:
            raise FileContractValidationError("file_contract_receipt.schema_identity_mismatch")
        wire_contract = artifact.wire_contract()
        if wire_contract.sha256 != self.wire_contract_sha256:
            raise FileContractValidationError("file_contract_receipt.wire_identity_mismatch")
        if tuple(wire_contract.columns) != tuple(name for name, _dtype in frozen_schema):
            raise FileContractValidationError("file_contract_receipt.schema_alignment")
        if _contract_digest(contract) != self.contract_sha256:
            raise FileContractValidationError("file_contract_receipt.contract_identity_mismatch")
        if integrity.require_rows_exported() != self.rows_validated:
            raise FileContractValidationError("file_contract_receipt.row_count_mismatch")


def project_key_snapshot_schema_contract(
    contract: SchemaContract,
    *,
    key_columns: Sequence[str],
    source_schema: Sequence[tuple[str, str]],
) -> SchemaContract:
    """Project one full relation contract to its authoritative snapshot keys.

    This is deliberately narrower than a generic subset operation.  The
    caller must supply the complete observed source schema and the exact
    reconciliation key.  Unknown, duplicate, or case-ambiguous identities
    fail before any key file can be exported; ordinary delta/full exports
    continue to validate against the unprojected contract.
    """

    keys = tuple(str(value) for value in key_columns)
    if not keys or any(not value for value in keys):
        raise FileContractValidationError("key_contract_projection.key_columns_required")
    duplicate_key = _first_duplicate_casefold(keys)
    if duplicate_key is not None:
        raise FileContractValidationError(f"key_contract_projection.key_column_duplicate:{duplicate_key}")

    source_columns = tuple(str(name) for name, _dtype in source_schema)
    duplicate_source = _first_duplicate_casefold(source_columns)
    if duplicate_source is not None:
        raise FileContractValidationError(f"key_contract_projection.source_column_ambiguous:{duplicate_source}")
    source_by_identity = {name.casefold(): name for name in source_columns}

    declared = contract.columns or {}
    declared_names = tuple(str(name) for name in declared)
    duplicate_contract = _first_duplicate_casefold(declared_names)
    if duplicate_contract is not None:
        raise FileContractValidationError(f"key_contract_projection.contract_column_ambiguous:{duplicate_contract}")
    contract_by_identity = {name.casefold(): name for name in declared_names}

    projected = {}
    for key in keys:
        source_name = source_by_identity.get(key.casefold())
        if source_name is None:
            raise FileContractValidationError(f"key_contract_projection.key_column_missing_from_source:{key}")
        if source_name != key:
            raise FileContractValidationError(f"key_contract_projection.key_column_case_mismatch:{key}")
        contract_name = contract_by_identity.get(key.casefold())
        if contract_name is None:
            raise FileContractValidationError(f"key_contract_projection.key_column_missing_from_contract:{key}")
        if contract_name != key:
            raise FileContractValidationError(f"key_contract_projection.contract_column_case_mismatch:{key}")
        projected[key] = declared[contract_name]
    return SchemaContract(enforcement=contract.enforcement, columns=projected)


_OPAQUE_NATIVE_FORMATS = frozenset({"mssql-bcp-native", "mssql-native"})
OPAQUE_NATIVE_VALIDATOR_VERSION = 3


def admit_mssql_native_file_contract(
    artifact: Any,
    *,
    schema: tuple[tuple[str, str], ...],
    contract: SchemaContract,
) -> FileContractValidationReceipt:
    """Bind opaque SQL Server native bytes to a contract without scanning cells.

    A character wire is scanned by ``validate_mssql_delimited_file_contract``.
    Native BCP is not that wire. This receipt proves artifact identity, column
    alignment, contract identity, and the exporter row count. It does not prove
    cell values.
    """

    wire = str(getattr(artifact, "format", "")).replace("_", "-").lower()
    if wire not in _OPAQUE_NATIVE_FORMATS:
        raise FileContractValidationError("file_contract_receipt.unsupported_wire")
    if bool(getattr(artifact, "compressed", False)):
        raise FileContractValidationError("file_contract_receipt.compressed_wire_unsupported")
    integrity = artifact.require_integrity_receipt()
    frozen_schema = _normalized_schema(schema)
    names = tuple(name for name, _dtype in frozen_schema)
    if tuple(str(name) for name in getattr(artifact, "columns", ())) != names:
        raise FileContractValidationError("file_contract_receipt.schema_alignment")
    if len({name.casefold() for name in names}) != len(names):
        raise FileContractValidationError("file_contract_receipt.schema_identity_ambiguous")
    declared = contract.columns or {}
    indexes = {name: index for index, name in enumerate(names)}
    missing = tuple(name for name in declared if name not in indexes)
    if missing:
        raise FileContractValidationError(f"file_contract_receipt.contract_column_missing:{missing[0]}")
    rows = integrity.require_rows_exported()
    receipt = FileContractValidationReceipt(
        artifact_sha256=integrity.sha256,
        artifact_size_bytes=integrity.size_bytes,
        contract_sha256=_contract_digest(contract),
        schema_sha256=_schema_digest(frozen_schema),
        validated_schema=frozen_schema,
        wire_contract_sha256=artifact.wire_contract().sha256,
        rows_validated=rows,
        validator_version=OPAQUE_NATIVE_VALIDATOR_VERSION,
    )
    artifact.contract_validation_receipt = receipt
    return receipt


def attach_mssql_export_contract(
    load_config: Any,
    artifact: Any,
    schema: Sequence[tuple[str, str]],
) -> None:
    """Issue the receipt for one completed MSSQL export when a contract exists."""

    options = getattr(load_config, "options", None) or {}
    raw = options.get("schema_contract") if isinstance(options, Mapping) else None
    if not isinstance(raw, Mapping) or not raw:
        return
    contract = SchemaContract.from_config(dict(raw))
    if not contract.columns:
        return
    frozen = tuple((str(name), str(dtype)) for name, dtype in schema)
    wire = str(getattr(artifact, "format", "")).replace("_", "-").lower()
    if wire == "mssql-delimited":
        validate_mssql_delimited_file_contract(artifact, schema=frozen, contract=contract)
        return
    if wire in _OPAQUE_NATIVE_FORMATS:
        admit_mssql_native_file_contract(artifact, schema=frozen, contract=contract)


def validate_mssql_delimited_file_contract(
    artifact: Any,
    *,
    schema: tuple[tuple[str, str], ...],
    contract: SchemaContract,
) -> FileContractValidationReceipt:
    """Scan one safe PostgreSQL COPY file and issue an immutable receipt."""

    if str(getattr(artifact, "format", "")).replace("_", "-").lower() != "mssql-delimited":
        raise FileContractValidationError("file_contract_receipt.unsupported_wire")
    if bool(getattr(artifact, "compressed", False)):
        raise FileContractValidationError("file_contract_receipt.compressed_wire_unsupported")
    codec = _require_safe_codec(artifact)
    integrity = artifact.require_integrity_receipt()
    frozen_schema = _normalized_schema(schema)
    names = tuple(name for name, _dtype in frozen_schema)
    if tuple(str(name) for name in getattr(artifact, "columns", ())) != names:
        raise FileContractValidationError("file_contract_receipt.schema_alignment")
    if len({name.casefold() for name in names}) != len(names):
        raise FileContractValidationError("file_contract_receipt.schema_identity_ambiguous")
    indexes = {name: index for index, name in enumerate(names)}
    declared = contract.columns or {}
    missing = tuple(name for name in declared if name not in indexes)
    if missing:
        raise FileContractValidationError(f"file_contract_receipt.contract_column_missing:{missing[0]}")
    required = tuple(indexes[name] for name, column in declared.items() if not column.nullable)
    rows = _scan_rows(
        Path(artifact.file_path),
        schema=frozen_schema,
        contract=contract,
        required=required,
        codec=codec,
    )
    if rows != integrity.require_rows_exported():
        raise FileContractValidationError("file_contract_receipt.row_count_mismatch")
    receipt = FileContractValidationReceipt(
        artifact_sha256=integrity.sha256,
        artifact_size_bytes=integrity.size_bytes,
        contract_sha256=_contract_digest(contract),
        schema_sha256=_schema_digest(frozen_schema),
        validated_schema=frozen_schema,
        wire_contract_sha256=artifact.wire_contract().sha256,
        rows_validated=rows,
    )
    artifact.contract_validation_receipt = receipt
    return receipt


def require_file_contract_validation(
    artifact: Any,
    contract: SchemaContract,
    *,
    schema: tuple[tuple[str, str], ...] | None = None,
    verification_budget: FileVerificationBudget | None = None,
) -> FileContractValidationReceipt:
    """Verify a source-issued receipt; a configuration boolean is never proof."""

    receipt = getattr(artifact, "contract_validation_receipt", None)
    if not isinstance(receipt, FileContractValidationReceipt):
        raise FileContractValidationError("file_contract_receipt.required")
    if verification_budget is None:
        receipt.verify(artifact, contract, schema=schema)
    else:
        receipt.verify(artifact, contract, schema=schema, verification_budget=verification_budget)
    return receipt


def reissue_file_contract_validation(
    artifact: Any,
    *,
    schema: tuple[tuple[str, str], ...],
    contract: SchemaContract,
) -> FileContractValidationReceipt:
    """Rescan framework-rewritten bytes and replace their now-stale receipt."""

    artifact.contract_validation_receipt = None
    return validate_mssql_delimited_file_contract(artifact, schema=schema, contract=contract)


def _scan_rows(
    path: Path,
    *,
    schema: tuple[tuple[str, str], ...],
    contract: SchemaContract,
    required: tuple[int, ...],
    codec: BulkTextCodec,
) -> int:
    rows = 0
    try:
        with path.open("rb") as handle:
            for values in iter_wire_rows(handle, len(schema), codec):
                null_required = next((index for index in required if values[index] == b""), None)
                if null_required is not None:
                    raise FileContractValidationError(
                        f"file_contract_receipt.not_null_violation:{schema[null_required][0]}"
                    )
                logical_row = {
                    name: _decode_wire_value(value, dtype=dtype, codec=codec)
                    for (name, dtype), value in zip(schema, values, strict=True)
                }
                result = (
                    import_module("dpone.type_system.enforcement")
                    .ContractEnforcementService()
                    .enforce(
                        rows=(logical_row,),
                        contract=contract,
                        run_id="file-contract-validation",
                        load_id="file-contract-validation",
                        conflict_policy="fail",
                        row_offset=rows,
                    )
                )
                if not result.passed:
                    diagnostic = result.diagnostics[0]
                    raise FileContractValidationError(
                        f"file_contract_receipt.logical_value_violation:{diagnostic.column}:{diagnostic.reason_code}"
                    )
                rows += 1
    except BulkTextFileReadError as exc:
        raise FileContractValidationError(f"file_contract_receipt.{exc.blocker}") from exc
    except OSError as exc:
        raise FileContractValidationError("file_contract_receipt.file_unavailable") from exc
    return rows


def _decode_wire_value(raw: bytes, *, dtype: str, codec: BulkTextCodec) -> object:
    """Decode one immutable COPY cell for portable logical validation."""

    try:
        return decode_wire_value(raw, dtype=dtype, codec=codec)
    except BulkTextFileReadError as exc:
        raise FileContractValidationError(f"file_contract_receipt.{exc.blocker}") from exc


def _require_safe_codec(artifact: Any) -> BulkTextCodec:
    codec = getattr(artifact, "bulk_text_codec", None)
    if not isinstance(codec, BulkTextCodec):
        raise FileContractValidationError("file_contract_receipt.bulk_text_codec_required")
    if codec.field_terminator != "\t" or codec.row_terminator != "\n":
        raise FileContractValidationError("file_contract_receipt.bulk_text_codec_unsupported")
    marker = codec.empty_string_marker
    if not marker or marker == codec.marker_prefix or any(token in marker for token in ("\t", "\r", "\n")):
        raise FileContractValidationError("file_contract_receipt.bulk_text_codec_unsupported")
    try:
        encoded_marker = marker.encode("utf-8")
    except UnicodeError as exc:
        raise FileContractValidationError("file_contract_receipt.bulk_text_codec_unsupported") from exc
    if not encoded_marker or encoded_marker == b"":
        raise FileContractValidationError("file_contract_receipt.bulk_text_codec_unsupported")
    return codec


def _contract_digest(contract: SchemaContract) -> str:
    return _digest(contract.to_dict())


def _schema_digest(schema: tuple[tuple[str, str], ...]) -> str:
    return _digest([[str(name), str(dtype)] for name, dtype in schema])


def _normalized_schema(
    schema: tuple[tuple[str, str], ...],
) -> tuple[tuple[str, str], ...]:
    return tuple((str(name), str(dtype)) for name, dtype in schema)


def _first_duplicate_casefold(values: Sequence[str]) -> str | None:
    seen: set[str] = set()
    for value in values:
        identity = value.casefold()
        if identity in seen:
            return value
        seen.add(identity)
    return None


def _digest(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


__all__ = [
    "FileContractValidationError",
    "FileContractValidationReceipt",
    "project_key_snapshot_schema_contract",
    "OPAQUE_NATIVE_VALIDATOR_VERSION",
    "admit_mssql_native_file_contract",
    "attach_mssql_export_contract",
    "require_file_contract_validation",
    "reissue_file_contract_validation",
    "validate_mssql_delimited_file_contract",
]
