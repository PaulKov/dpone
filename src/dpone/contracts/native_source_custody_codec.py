"""Closed canonical source-custody wire encoding without storage or SQL effects."""

from __future__ import annotations

from uuid import UUID

from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import NativeSourceCustodyError, SourceExecutorBinding

_EXECUTOR_SCHEMA = "dpone.native-source-executor-binding.v1"
_EXECUTOR_FIELDS = frozenset(
    {"schema", "generation_id", "guard_epoch", "invocation_id", "reservation", "profile", "command"}
)


def encode_source_executor_binding(value: SourceExecutorBinding) -> bytes:
    """Snapshot and revalidate the complete binding at a capability boundary."""
    if type(value) is not SourceExecutorBinding:
        raise NativeSourceCustodyError("expected a source executor binding")
    value.__post_init__()
    return encode_native_delivery_json(
        {
            "schema": _EXECUTOR_SCHEMA,
            "generation_id": str(value.generation_id),
            "guard_epoch": value.guard_epoch,
            "invocation_id": str(value.invocation_id),
            "reservation": _reference_payload(value.reservation),
            "profile": _reference_payload(value.profile),
            "command": _reference_payload(value.command),
        }
    )


def decode_source_executor_binding(payload: bytes) -> SourceExecutorBinding:
    """Decode exactly the registered writer identity, never a success claim."""
    value = decode_native_delivery_json(payload)
    if type(value) is not dict or set(value) != _EXECUTOR_FIELDS or value["schema"] != _EXECUTOR_SCHEMA:
        raise NativeSourceCustodyError("writer binding requires the exact registered schema fields")
    epoch = value["guard_epoch"]
    if type(epoch) is not int:
        raise NativeSourceCustodyError("guard epoch must be an exact integer")
    binding = SourceExecutorBinding(
        generation_id=_uuid(value["generation_id"]),
        guard_epoch=epoch,
        invocation_id=_uuid(value["invocation_id"]),
        reservation=_reference(value["reservation"]),
        profile=_reference(value["profile"]),
        command=_reference(value["command"]),
    )
    if encode_source_executor_binding(binding) != payload:
        raise NativeSourceCustodyError("writer binding bytes must be canonical")
    return binding


def _reference_payload(value: OriginalRef) -> dict[str, NativeJsonValue]:
    return {"locator": value.locator, "sha256": value.sha256}


def _reference(value: NativeJsonValue) -> OriginalRef:
    if type(value) is not dict or set(value) != {"locator", "sha256"}:
        raise NativeSourceCustodyError("writer original reference has invalid fields")
    locator, digest = value["locator"], value["sha256"]
    if type(locator) is not str or type(digest) is not str:
        raise NativeSourceCustodyError("writer original coordinates must be strings")
    return OriginalRef(locator, digest)


def _uuid(value: NativeJsonValue) -> UUID:
    if type(value) is not str:
        raise NativeSourceCustodyError("UUID wire coordinate must be a string")
    try:
        result = UUID(value)
    except ValueError as exc:
        raise NativeSourceCustodyError("invalid UUID wire coordinate") from exc
    if str(result) != value:
        raise NativeSourceCustodyError("UUID wire coordinate must be canonical")
    return result
