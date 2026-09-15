"""Canonical generation descriptors with explicit external readback envelopes."""

from __future__ import annotations

from hashlib import sha256
from uuid import UUID

from dpone.contracts.native_delivery import (
    FrozenGeneration,
    GenerationReservation,
    NativeGenerationContractError,
    SourceClosureReceipt,
)
from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef


def encode_source_closure_receipt(value: SourceClosureReceipt) -> bytes:
    """Exclude the descriptor of this payload to avoid digest self-reference."""
    if type(value) is not SourceClosureReceipt:
        raise NativeGenerationContractError("expected exact source closure receipt")
    value.__post_init__()
    return prepare_source_closure_receipt(
        generation_id=value.generation_id,
        guard_epoch=value.guard_epoch,
        revision=value.revision,
        reservation=value.reservation,
        completion=value.completion,
    )


def prepare_source_closure_receipt(
    *, generation_id: UUID, guard_epoch: int, revision: int, reservation: OriginalRef, completion: OriginalRef
) -> bytes:
    """Prepare canonical bytes before publication without inventing a receipt.

    Valid coordinates are not proof of custody. The caller must authenticate the
    completion and independently read back the subsequently published original.
    """
    GenerationReservation(generation_id, guard_epoch, revision, reservation)
    if type(completion) is not OriginalRef:
        raise NativeGenerationContractError("source closure requires exact completion original")
    completion.__post_init__()
    return encode_native_delivery_json(
        {
            "schema": "dpone.native-source-closure-receipt.v1",
            "generation_id": str(generation_id),
            "guard_epoch": guard_epoch,
            "revision": revision,
            "reservation": _ref(reservation),
            "completion": _ref(completion),
        }
    )


def decode_source_closure_receipt(payload: bytes, *, receipt: OriginalRef) -> SourceClosureReceipt:
    """Verify a canonical payload and external descriptor, not its authority."""
    value = _object(
        decode_native_delivery_json(payload),
        {"schema", "generation_id", "guard_epoch", "revision", "reservation", "completion"},
    )
    if value["schema"] != "dpone.native-source-closure-receipt.v1":
        raise NativeGenerationContractError("unsupported source closure schema")
    _verify_descriptor(payload, receipt)
    result = SourceClosureReceipt(
        _uuid(value["generation_id"]),
        _integer(value["guard_epoch"]),
        _integer(value["revision"]),
        _decode_ref(value["reservation"]),
        _decode_ref(value["completion"]),
        receipt,
    )
    if encode_source_closure_receipt(result) != payload:
        raise NativeGenerationContractError("source closure bytes must be canonical")
    return result


def encode_frozen_generation(value: FrozenGeneration) -> bytes:
    """Embed the closure's readback envelope, excluding this frozen descriptor."""
    if type(value) is not FrozenGeneration:
        raise NativeGenerationContractError("expected exact frozen generation")
    value.__post_init__()
    return prepare_frozen_generation(value.closure, revision=value.revision)


def prepare_frozen_generation(closure: SourceClosureReceipt, *, revision: int) -> bytes:
    """Prepare freeze bytes from an independently read-back published closure.

    Exact identity is derived from the closure, not duplicated caller inputs.
    Digest verification does not replace the caller's original authentication.
    """
    if type(closure) is not SourceClosureReceipt:
        raise NativeGenerationContractError("expected exact source closure receipt")
    closure_payload = encode_source_closure_receipt(closure)
    _verify_descriptor(closure_payload, closure.receipt)
    GenerationReservation(closure.generation_id, closure.guard_epoch, revision, closure.reservation)
    if revision != closure.revision + 1:
        raise NativeGenerationContractError("freeze must use the next closure revision")
    return encode_native_delivery_json(
        {
            "schema": "dpone.native-frozen-generation.v1",
            "generation_id": str(closure.generation_id),
            "guard_epoch": closure.guard_epoch,
            "revision": revision,
            "reservation": _ref(closure.reservation),
            "closure": {
                "payload": decode_native_delivery_json(closure_payload),
                "receipt": _ref(closure.receipt),
            },
        }
    )


def decode_frozen_generation(payload: bytes, *, frozen: OriginalRef) -> FrozenGeneration:
    """Decode exact nested identities; callers must authenticate both originals."""
    value = _object(
        decode_native_delivery_json(payload),
        {"schema", "generation_id", "guard_epoch", "revision", "reservation", "closure"},
    )
    if value["schema"] != "dpone.native-frozen-generation.v1":
        raise NativeGenerationContractError("unsupported frozen generation schema")
    _verify_descriptor(payload, frozen)
    envelope = _object(value["closure"], {"payload", "receipt"})
    closure = decode_source_closure_receipt(
        encode_native_delivery_json(envelope["payload"]), receipt=_decode_ref(envelope["receipt"])
    )
    result = FrozenGeneration(
        _uuid(value["generation_id"]),
        _integer(value["guard_epoch"]),
        _integer(value["revision"]),
        _decode_ref(value["reservation"]),
        closure,
        frozen,
    )
    if encode_frozen_generation(result) != payload:
        raise NativeGenerationContractError("frozen generation bytes must be canonical")
    return result


def _object(value: NativeJsonValue, fields: set[str]) -> dict[str, NativeJsonValue]:
    if type(value) is not dict or set(value) != fields:
        raise NativeGenerationContractError("generation payload requires exact fields")
    return value


def _ref(value: OriginalRef) -> dict[str, NativeJsonValue]:
    return {"locator": value.locator, "sha256": value.sha256}


def _decode_ref(value: NativeJsonValue) -> OriginalRef:
    raw = _object(value, {"locator", "sha256"})
    locator, digest = raw["locator"], raw["sha256"]
    if type(locator) is not str or type(digest) is not str:
        raise NativeGenerationContractError("original coordinates must be exact strings")
    return OriginalRef(locator, digest)


def _integer(value: NativeJsonValue) -> int:
    if type(value) is not int:
        raise NativeGenerationContractError("generation integer must not be coerced")
    return value


def _uuid(value: NativeJsonValue) -> UUID:
    if type(value) is not str:
        raise NativeGenerationContractError("generation UUID must be a string")
    try:
        result = UUID(value)
    except ValueError as error:
        raise NativeGenerationContractError("invalid generation UUID") from error
    if str(result) != value:
        raise NativeGenerationContractError("generation UUID must be canonical")
    return result


def _verify_descriptor(payload: bytes, reference: OriginalRef) -> None:
    if type(reference) is not OriginalRef:
        raise NativeGenerationContractError("exact external descriptor required")
    reference.__post_init__()
    if reference.sha256 != "sha256:" + sha256(payload).hexdigest():
        raise NativeGenerationContractError("descriptor digest differs from payload")
