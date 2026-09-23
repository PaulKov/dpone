"""Closed directory decoding bound to a caller-admitted parent and resource policy.

The directory model owns deterministic encoding and conservative future-proof
byte admission. This boundary validates stored input without trusting its stated
limits. It neither creates missing records nor reconstructs corrupt history.
"""

from dataclasses import asdict, fields, replace
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_tds_directory import (
    TdsCoordinatorCommand,
    TdsCoordinatorDirectory,
    TdsDirectoryLimits,
    TdsDirectorySlot,
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    encode_directory,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership, TdsProcessIdentity
from dpone.contracts.mssql_tds_worker_codec import decode_state
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


def _fields(cls: Any, value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != {field.name for field in fields(cls)}:
        raise ValueError("record_fields")
    return dict(value)


def _operation_id(value: Any) -> UUID:
    if type(value) is not str:
        raise ValueError("operation_id_type")
    identity = UUID(value)
    if str(identity) != value or not identity.int:
        raise ValueError("operation_id_spelling")
    return identity


def _slot(value: Any) -> TdsDirectorySlot:
    slot = _fields(TdsDirectorySlot, value)
    slot["operation_id"] = _operation_id(slot["operation_id"])
    slot["command"] = TdsCoordinatorCommand(slot["command"])
    for name, cls in (("local_containment", TdsLocalContainment), ("remote_settlement", TdsRemoteSettlement)):
        if slot[name] is not None:
            proof = _fields(cls, slot[name])
            proof["operation_id"] = _operation_id(proof["operation_id"])
            slot[name] = cls(**proof)
    return TdsDirectorySlot(**slot)


def decode_directory(
    payload: bytes, *, parent: TdsAttemptIdentity, limits: TdsDirectoryLimits
) -> TdsCoordinatorDirectory:
    """Read only the exact previously admitted directory within its fixed limits.

    Caller-provided limits bound bytes before JSON parsing and entries before
    constructing nested models. Stored limits cannot enlarge those bounds.
    The existing strict attempt codec validates retirement lifecycle evidence.
    Empty, mismatched or malformed records fail closed with a static diagnostic.
    """
    try:
        if type(parent) is not TdsAttemptIdentity or type(limits) is not TdsDirectoryLimits:
            raise ValueError("admission_types")
        if type(payload) is not bytes or len(payload) > limits.max_encoded_bytes:
            raise ValueError("record_bytes")
        value = strict_json_object(payload)
        schema = value.pop("schema", None)
        if schema not in ("dpone.tds.coordinator-directory.v1", "dpone.tds.coordinator-directory.v2"):
            raise ValueError("schema")
        if "schema_version" in value:
            raise ValueError("record_fields")
        value["schema_version"] = 1 if schema.endswith(".v1") else 2
        data = _fields(TdsCoordinatorDirectory, value)
        stored_parent = TdsAttemptIdentity(**_fields(TdsAttemptIdentity, data["parent"]))
        stored_limits = TdsDirectoryLimits(**_fields(TdsDirectoryLimits, data["limits"]))
        if stored_parent != parent or stored_limits != limits:
            raise ValueError("admission_changed")
        if type(data["slots"]) is not list or len(data["slots"]) > limits.max_entries:
            raise ValueError("entry_limit")
        data["parent"], data["limits"] = stored_parent, stored_limits
        data["slots"] = tuple(_slot(slot) for slot in data["slots"])
        if data["retirement_authority"] is not None:
            data["retirement_authority"] = decode_state(canonical_json_bytes(data["retirement_authority"]))
        return TdsCoordinatorDirectory(**data)
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
        raise ValueError("mssql_native.tds_directory_record_invalid") from None


# Ownership has at most 256 Unicode scalars (1024 UTF-8 bytes), plus fixed
# keys, canonical UUID and bounded integer. Keep this outside the domain's
# directory budget so ownership cannot consume reserved retirement capacity.
DIRECTORY_OWNERSHIP_ENVELOPE_BYTES = 2048


def encode_directory_record(state: TdsCoordinatorDirectory, ownership: TdsAttemptOwnership) -> bytes:
    """Encode durable ownership and state; the store alone supplies revision.

    This represents data, not permission to write or proof of SQL settlement.
    Snapshot validation prevents an owner from preceding any reserved fence.
    """
    TdsDirectorySnapshot(state, ownership, 1)
    return canonical_json_bytes(
        {
            "schema": "dpone.tds.coordinator-directory-storage.v1",
            "ownership": asdict(ownership),
            "directory": strict_json_object(encode_directory(state)),
        }
    )


def decode_directory_record(
    payload: bytes, *, revision: int, parent: TdsAttemptIdentity, limits: TdsDirectoryLimits
) -> TdsDirectorySnapshot:
    """Decode a bounded, closed envelope using independently admitted limits.

    Never trust limits from the stored envelope. No writer is acquired by
    reading this immutable snapshot, and malformed history is never repaired.
    """
    try:
        if type(limits) is not TdsDirectoryLimits or type(payload) is not bytes:
            raise ValueError("admission_types")
        if len(payload) > limits.max_encoded_bytes + DIRECTORY_OWNERSHIP_ENVELOPE_BYTES:
            raise ValueError("record_bytes")
        value = strict_json_object(payload)
        if set(value) != {"schema", "ownership", "directory"}:
            raise ValueError("record_fields")
        if value["schema"] != "dpone.tds.coordinator-directory-storage.v1":
            raise ValueError("schema")
        ownership = TdsAttemptOwnership(**_fields(TdsAttemptOwnership, value["ownership"]))
        state = decode_directory(canonical_json_bytes(value["directory"]), parent=parent, limits=limits)
        return TdsDirectorySnapshot(state, ownership, revision)
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
        raise ValueError("mssql_native.tds_directory_storage_record_invalid") from None


def process_identity_digest(process: TdsProcessIdentity) -> str:
    """Canonical actual-process binding; this digest establishes no containment."""
    if type(process) is not TdsProcessIdentity:
        raise ValueError("mssql_native.tds_process_identity_invalid")
    replace(process)
    return sha256(b"dpone.tds.process-identity.v1\0" + canonical_json_bytes(asdict(process))).hexdigest()
