"""Single canonical validator for complete native parent journal v4 snapshots."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Any

from dpone.adapters.mssql_native_publication_journal import validate_publication_state
from dpone.contracts.mssql_tds_api import (
    NativeBulkTransportPolicy,
    NativeChunkReceipt,
    NativeStageComplete,
    WindowContractError,
)

_FIELDS = {
    "version",
    "identity",
    "phase",
    "chunks",
    "complete",
    "observations",
    "publication",
    "completion_metadata",
    "rollback_history",
    "limits",
}
_IDENTITY_FIELDS = {
    "run_id",
    "target_id",
    "source_query_id",
    "window_fingerprint",
    "schema_fingerprint",
    "wire_fingerprint",
    "transport",
}


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def decode_native_chunk_receipt(value: Any) -> NativeChunkReceipt:
    """Decode the canonical receipt rules shared by loading and retirement."""
    try:
        receipt = NativeChunkReceipt(**json.loads(json.dumps(value)))
        if any(
            type(getattr(receipt, name)) is not int or getattr(receipt, name) < 0
            for name in ("ordinal", "rows", "encoded_bytes")
        ):
            raise ValueError("invalid counters")
        if any(
            not isinstance(getattr(receipt, name), str) or not getattr(receipt, name)
            for name in ("attempt_id", "stage_id", "file_sha256", "typed_digest")
        ):
            raise ValueError("invalid identity")
        for name in ("file_sha256", "typed_digest"):
            digest = getattr(receipt, name)
            if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
                raise ValueError("invalid digest")
        if not isinstance(receipt.consumed_part_evidence, dict):
            raise ValueError("invalid evidence")
        return receipt
    except (TypeError, ValueError) as error:
        raise WindowContractError("mssql_native.invalid_receipt") from error


def validate_native_v4_snapshot(value: object, expected_identity: dict[str, object]) -> NativeStageComplete | None:
    """Validate every persisted v4 field and recompute stage/publication authority."""
    if (
        not isinstance(value, dict)
        or set(value) != _FIELDS
        or type(value.get("version")) is not int
        or value["version"] != 4
    ):
        raise ValueError("invalid v4 record")
    identity = value["identity"]
    keys = set(_IDENTITY_FIELDS)
    if isinstance(identity, dict) and "source_read_mode" in identity:
        keys.add("source_read_mode")
    if not isinstance(identity, dict) or set(identity) != keys:
        raise ValueError("invalid identity shape")
    transport = NativeBulkTransportPolicy.from_mapping(identity.get("transport"))
    if transport.to_dict() != identity["transport"]:
        raise ValueError("unresolved transport")
    if identity.get("source_read_mode", "raw_single_query") != "raw_single_query":
        raise ValueError("invalid source read mode")
    if identity != expected_identity:
        raise WindowContractError("mssql_native.journal_identity_changed")
    if value["phase"] not in ("staging", "stage_complete", "reextract_required"):
        raise ValueError("invalid phase")
    if not isinstance(value["chunks"], dict):
        raise ValueError("invalid chunks")
    receipts: list[NativeChunkReceipt] = []
    for key, chunk in value["chunks"].items():
        if str(int(key)) != key or int(key) < 0 or not isinstance(chunk, dict):
            raise ValueError("invalid ordinal")
        attempt = chunk["attempt"]
        if type(attempt) is not int or not 0 <= attempt <= 2:
            raise ValueError("invalid attempt")
        if chunk["phase"] not in ("staging", "verified"):
            raise ValueError("invalid chunk phase")
        if chunk["phase"] == "verified":
            receipt = decode_native_chunk_receipt(chunk["receipt"])
            if receipt.ordinal != int(key) or receipt.attempt_id != f"{identity['run_id']}-{int(key)}-{attempt}":
                raise ValueError("invalid attempt binding")
            if chunk.get("file") is not None:
                for name in ("ordinal", "rows", "encoded_bytes", "file_sha256", "typed_digest"):
                    if getattr(receipt, name) != chunk["file"][name]:
                        raise ValueError("changed sealed file binding")
            receipts.append(receipt)
    if not isinstance(value["completion_metadata"], dict):
        raise ValueError("invalid completion metadata")
    if not isinstance(value["observations"], list) or any(not isinstance(item, dict) for item in value["observations"]):
        raise ValueError("invalid observations")
    if not isinstance(value["rollback_history"], list) or any(
        not isinstance(item, dict) for item in value["rollback_history"]
    ):
        raise ValueError("invalid rollback history")
    if value["limits"] is not None and not isinstance(value["limits"], dict):
        raise ValueError("invalid limits")
    validate_publication_state(value)
    if value["phase"] != "stage_complete":
        return None
    chunks = value["chunks"]
    if (
        not chunks
        or set(chunks) != {str(index) for index in range(len(chunks))}
        or any(chunk["phase"] != "verified" for chunk in chunks.values())
    ):
        raise WindowContractError("mssql_native.noncontiguous_receipts")
    ordered = tuple(decode_native_chunk_receipt(chunks[str(index)]["receipt"]) for index in range(len(chunks)))
    if len({receipt.stage_id for receipt in ordered}) != len(ordered):
        raise WindowContractError("mssql_native.shared_stage_identity")
    rows = sum(receipt.rows for receipt in ordered)
    receipt_digest = _digest([asdict(receipt) for receipt in ordered])
    metadata_digest = _digest(value["completion_metadata"])
    expected = {"rows": rows, "receipt_digest": receipt_digest, "metadata_digest": metadata_digest}
    if value["complete"] != expected:
        raise WindowContractError("mssql_native.stage_complete_changed")
    return NativeStageComplete(
        receipts=ordered,
        rows=rows,
        receipt_digest=receipt_digest,
        metadata_digest=metadata_digest,
        observations=tuple(json.loads(json.dumps(value["observations"]))),
    )


__all__ = ("decode_native_chunk_receipt", "validate_native_v4_snapshot")
