"""Source custody records and canonical ledger wire values; shapes grant no authority."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Literal
from uuid import UUID

from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef

_EXECUTOR_SCHEMA = "dpone.native-source-executor-binding.v1"


_EXECUTOR_FIELDS = frozenset(
    {"schema", "generation_id", "guard_epoch", "invocation_id", "reservation", "profile", "command"}
)


class NativeSourceCustodyError(ValueError):
    """A source identity or custody transition is incomplete or inconsistent."""


@dataclass(frozen=True, slots=True)
class SourceExecutorBinding:
    """One pre-dispatch invocation bound to the complete reserved generation.

    The protected ledger must authenticate the reservation, profile and command
    originals and compare the existing physical owner/epoch before binding. An
    identical retained record is readback evidence, never permission to launch
    the invocation again after uncertainty.
    """

    generation_id: UUID
    guard_epoch: int
    invocation_id: UUID
    reservation: OriginalRef
    profile: OriginalRef
    command: OriginalRef

    def __post_init__(self) -> None:
        if type(self.generation_id) is not UUID or type(self.invocation_id) is not UUID:
            raise NativeSourceCustodyError("generation and invocation identities must be UUID values")
        if type(self.guard_epoch) is not int or not 1 <= self.guard_epoch <= 9223372036854775807:
            raise NativeSourceCustodyError("guard epoch must be an exact positive SQL bigint")
        for reference in (self.reservation, self.profile, self.command):
            if type(reference) is not OriginalRef:
                raise NativeSourceCustodyError("writer binding requires every exact original reference")
            reference.__post_init__()


@dataclass(frozen=True, slots=True)
class SourceAdmissionClosure:
    """Durable closed admission; neither normal termination nor build success.

    The receipt describes the independently read ledger payload and is excluded
    from that payload, avoiding a digest that would need to contain itself.
    """

    executor: SourceExecutorBinding
    admission_sequence: int
    revision: int
    receipt: OriginalRef

    def __post_init__(self) -> None:
        if type(self.executor) is not SourceExecutorBinding:
            raise NativeSourceCustodyError("admission closure requires the exact executor")
        self.executor.__post_init__()
        for value in (self.admission_sequence, self.revision):
            if type(value) is not int or not 1 <= value <= 9223372036854775807:
                raise NativeSourceCustodyError("closure sequence and revision must be positive SQL bigint values")
        if type(self.receipt) is not OriginalRef:
            raise NativeSourceCustodyError("admission closure requires an exact external receipt")
        self.receipt.__post_init__()


@dataclass(frozen=True, slots=True)
class SourceTrustedBuildCompletion:
    """Positive owned build originals; the caller must authenticate every one.

    These references cannot prove success merely by having valid shapes. The
    consuming recorder resolves the exact profile, command, membership, toolchain,
    artifacts and normal joined termination before any durable transition.
    """

    executor: SourceExecutorBinding
    command: OriginalRef
    toolchain: OriginalRef
    build_evidence: OriginalRef
    artifact_inventory: OriginalRef
    termination: OriginalRef

    def __post_init__(self) -> None:
        if type(self.executor) is not SourceExecutorBinding:
            raise NativeSourceCustodyError("positive completion requires the exact executor")
        self.executor.__post_init__()
        _require_originals(self.command, self.toolchain, self.build_evidence, self.artifact_inventory, self.termination)
        if self.command != self.executor.command:
            raise NativeSourceCustodyError("positive completion command differs from the admitted executor")


@dataclass(frozen=True, slots=True)
class SourceReadGrant:
    """One retained restricted read capability; its descriptor is external."""

    read_id: UUID
    executor: SourceExecutorBinding
    purpose: Literal["QUALITY", "EXPORT"]
    plan: OriginalRef
    revision: int
    receipt: OriginalRef

    def __post_init__(self) -> None:
        if type(self.read_id) is not UUID or type(self.executor) is not SourceExecutorBinding:
            raise NativeSourceCustodyError("read grant identity is invalid")
        self.executor.__post_init__()
        if type(self.purpose) is not str or self.purpose not in {"QUALITY", "EXPORT"}:
            raise NativeSourceCustodyError("read purpose is invalid")
        if type(self.revision) is not int or not 1 <= self.revision <= 9223372036854775807:
            raise NativeSourceCustodyError("read revision is invalid")
        for value in (self.plan, self.receipt):
            if type(value) is not OriginalRef:
                raise NativeSourceCustodyError("read grant requires exact original references")
            value.__post_init__()


@dataclass(frozen=True, slots=True)
class SourceCustodySnapshot:
    """Last confirmed source phase; UNKNOWN preserves reads and charged custody."""

    generation_id: UUID
    guard_epoch: int
    revision: int
    executor: SourceExecutorBinding | None
    reservation: OriginalRef
    closure: OriginalRef | None
    frozen: OriginalRef | None
    quality: OriginalRef | None
    export_plan: OriginalRef | None
    active_reads: tuple[SourceReadGrant, ...]
    state: Literal["RESERVED", "BUILDING", "FROZEN", "SEALED"]
    writer_admission: Literal["OPEN", "CLOSED"]
    outcome: Literal["ACTIVE", "FAILED", "UNKNOWN", "SUCCEEDED"]

    def __post_init__(self) -> None:
        if type(self.generation_id) is not UUID or type(self.reservation) is not OriginalRef:
            raise NativeSourceCustodyError("custody snapshot identity is invalid")
        self.reservation.__post_init__()
        for value in (self.guard_epoch, self.revision):
            if type(value) is not int or not 1 <= value <= 9223372036854775807:
                raise NativeSourceCustodyError("custody epoch/revision must be positive SQL bigint values")
        if self.state not in {"RESERVED", "BUILDING", "FROZEN", "SEALED"}:
            raise NativeSourceCustodyError("unknown source phase")
        if self.writer_admission not in {"OPEN", "CLOSED"} or self.outcome not in {
            "ACTIVE",
            "FAILED",
            "UNKNOWN",
            "SUCCEEDED",
        }:
            raise NativeSourceCustodyError("invalid source admission/outcome")
        for reference in (self.closure, self.frozen, self.quality, self.export_plan):
            if reference is not None:
                if type(reference) is not OriginalRef:
                    raise NativeSourceCustodyError("custody originals must be exact references")
                reference.__post_init__()
        if type(self.active_reads) is not tuple or len(self.active_reads) > 1:
            raise NativeSourceCustodyError("source custody permits at most one restricted read")
        if self.executor is not None:
            if type(self.executor) is not SourceExecutorBinding:
                raise NativeSourceCustodyError("invalid custody executor")
            self.executor.__post_init__()
            if (self.executor.generation_id, self.executor.guard_epoch, self.executor.reservation) != (
                self.generation_id,
                self.guard_epoch,
                self.reservation,
            ):
                raise NativeSourceCustodyError("executor differs from reserved generation")
        for grant in self.active_reads:
            if type(grant) is not SourceReadGrant:
                raise NativeSourceCustodyError("invalid active read grant")
            grant.__post_init__()
            if grant.executor != self.executor or grant.revision > self.revision:
                raise NativeSourceCustodyError("active read differs from current custody")
            if grant.purpose == "EXPORT" and (
                self.quality is None or self.export_plan is None or grant.plan != self.export_plan
            ):
                raise NativeSourceCustodyError("active export requires accepted quality and its exact retained plan")
        if self.state == "RESERVED":
            if self.executor is not None or any(
                (self.closure, self.frozen, self.quality, self.export_plan, self.active_reads)
            ):
                raise NativeSourceCustodyError("reserved generation has premature execution evidence")
            if self.outcome not in {"ACTIVE", "FAILED"}:
                raise NativeSourceCustodyError("reserved generation has invalid outcome")
        elif self.executor is None:
            raise NativeSourceCustodyError("an admitted generation requires its exact executor")
        if self.state == "BUILDING" and any((self.frozen, self.quality, self.export_plan, self.active_reads)):
            raise NativeSourceCustodyError("building generation has premature frozen evidence")
        if self.state in {"FROZEN", "SEALED"} and (
            self.closure is None or self.frozen is None or self.writer_admission != "CLOSED"
        ):
            raise NativeSourceCustodyError("frozen generation requires closed admission and positive build evidence")
        if self.closure is not None and self.writer_admission != "CLOSED":
            raise NativeSourceCustodyError("positive build completion requires closed writer admission")
        if self.export_plan is not None and self.quality is None:
            raise NativeSourceCustodyError("export requires accepted final quality")
        if self.state == "SEALED":
            if self.quality is None or self.export_plan is None or self.active_reads or self.outcome != "SUCCEEDED":
                raise NativeSourceCustodyError("sealed generation lacks complete terminal proof")
        elif self.outcome == "SUCCEEDED":
            raise NativeSourceCustodyError("only a sealed generation can succeed")


def require_trusted_build_completion(
    snapshot: SourceCustodySnapshot,
    closure: SourceAdmissionClosure,
    completion: SourceTrustedBuildCompletion,
) -> None:
    """Check positive recording identity, never authenticate or mutate originals.

    The admission descriptor keeps its historical revision while custody may
    advance. The caller verifies that descriptor and every positive original,
    then compares the current revision and physical owner in its SQL transaction.
    """
    if (
        type(snapshot) is not SourceCustodySnapshot
        or type(closure) is not SourceAdmissionClosure
        or type(completion) is not SourceTrustedBuildCompletion
    ):
        raise NativeSourceCustodyError("positive completion requires exact custody records")
    snapshot.__post_init__()
    closure.__post_init__()
    completion.__post_init__()
    if (
        snapshot.state != "BUILDING"
        or snapshot.writer_admission != "CLOSED"
        or snapshot.outcome not in {"ACTIVE", "UNKNOWN"}
        or snapshot.executor != closure.executor
        or snapshot.executor != completion.executor
        or closure.revision > snapshot.revision
    ):
        raise NativeSourceCustodyError("positive completion differs from closed current source custody")


def _require_originals(*references: OriginalRef) -> None:
    for reference in references:
        if type(reference) is not OriginalRef:
            raise NativeSourceCustodyError("trusted invocation requires exact original references")
        reference.__post_init__()


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


def encode_source_admission_closure(value: SourceAdmissionClosure) -> bytes:
    """Encode the ledger payload, never its enclosing receipt reference."""
    if type(value) is not SourceAdmissionClosure:
        raise NativeSourceCustodyError("expected a source admission closure")
    value.__post_init__()
    return encode_native_delivery_json(
        {
            "schema": "dpone.native-source-admission-closure.v1",
            "executor": decode_native_delivery_json(encode_source_executor_binding(value.executor)),
            "admission_sequence": value.admission_sequence,
            "revision": value.revision,
        }
    )


def decode_source_admission_closure(payload: bytes, *, receipt: OriginalRef) -> SourceAdmissionClosure:
    """Verify canonical payload against its independently resolved descriptor.

    The caller must authenticate the descriptor's ledger and locator; matching
    bytes alone does not grant authority or prove successful build completion.
    """
    if type(receipt) is not OriginalRef:
        raise NativeSourceCustodyError("admission closure requires an exact external receipt")
    receipt.__post_init__()
    value = _mapping(decode_native_delivery_json(payload), {"schema", "executor", "admission_sequence", "revision"})
    if value["schema"] != "dpone.native-source-admission-closure.v1":
        raise NativeSourceCustodyError("invalid admission closure schema")
    closure = SourceAdmissionClosure(
        executor=decode_source_executor_binding(encode_native_delivery_json(value["executor"])),
        admission_sequence=_integer(value["admission_sequence"]),
        revision=_integer(value["revision"]),
        receipt=OriginalRef(receipt.locator, receipt.sha256),
    )
    if encode_source_admission_closure(closure) != payload or receipt.sha256 != "sha256:" + sha256(payload).hexdigest():
        raise NativeSourceCustodyError("admission closure payload differs from canonical bytes or receipt digest")
    return closure


def encode_source_trusted_build_completion(value: SourceTrustedBuildCompletion) -> bytes:
    """Encode positive references, not a caller-controlled success flag."""
    if type(value) is not SourceTrustedBuildCompletion:
        raise NativeSourceCustodyError("expected a trusted source build completion")
    value.__post_init__()
    return encode_native_delivery_json(
        {
            "schema": "dpone.native-source-trusted-build-completion.v1",
            "executor": decode_native_delivery_json(encode_source_executor_binding(value.executor)),
            "command": _reference_payload(value.command),
            "toolchain": _reference_payload(value.toolchain),
            "build_evidence": _reference_payload(value.build_evidence),
            "artifact_inventory": _reference_payload(value.artifact_inventory),
            "termination": _reference_payload(value.termination),
        }
    )


def decode_source_trusted_build_completion(payload: bytes) -> SourceTrustedBuildCompletion:
    """Decode closed canonical shape; authenticated resolution belongs to callers."""
    value = _mapping(
        decode_native_delivery_json(payload),
        {"schema", "executor", "command", "toolchain", "build_evidence", "artifact_inventory", "termination"},
    )
    if value["schema"] != "dpone.native-source-trusted-build-completion.v1":
        raise NativeSourceCustodyError("unsupported trusted source build completion schema")
    completion = SourceTrustedBuildCompletion(
        executor=decode_source_executor_binding(encode_native_delivery_json(value["executor"])),
        command=_reference(value["command"]),
        toolchain=_reference(value["toolchain"]),
        build_evidence=_reference(value["build_evidence"]),
        artifact_inventory=_reference(value["artifact_inventory"]),
        termination=_reference(value["termination"]),
    )
    if encode_source_trusted_build_completion(completion) != payload:
        raise NativeSourceCustodyError("trusted source completion bytes must be canonical")
    return completion


def _mapping(value: NativeJsonValue, fields: set[str]) -> dict[str, NativeJsonValue]:
    if type(value) is not dict or set(value) != fields:
        raise NativeSourceCustodyError("trusted invocation record has incomplete or extra fields")
    return value


def _integer(value: NativeJsonValue) -> int:
    if type(value) is not int:
        raise NativeSourceCustodyError("trusted invocation integer must be exact")
    return value


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
