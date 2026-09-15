"""Closed canonical source-custody wire encoding without storage or SQL effects."""

from __future__ import annotations

from hashlib import sha256
from typing import Literal, cast
from uuid import UUID

from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import (
    NativeSourceCustodyError,
    SourceAdmissionClosure,
    SourceExecutorBinding,
    SourceTrustedBuildCompletion,
    TrustedDbtCommandEntry,
    TrustedDbtCommandPlan,
    TrustedDbtInvocationCompletion,
)

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


def encode_trusted_dbt_command_plan(value: TrustedDbtCommandPlan) -> bytes:
    """Encode only a complete fixed phase, preserving every literal argv byte."""
    if type(value) is not TrustedDbtCommandPlan:
        raise NativeSourceCustodyError("expected a trusted dbt command plan")
    value.__post_init__()
    return encode_native_delivery_json(
        {
            "schema": value.schema,
            "executor_invocation_id": str(value.executor_invocation_id),
            "phase": value.phase,
            "commands": [
                {
                    "position": entry.position,
                    "verb": entry.verb,
                    "argv_template": list(entry.argv_template),
                    "command_timeout_seconds": entry.command_timeout_seconds,
                    "termination_allowance_seconds": entry.termination_allowance_seconds,
                }
                for entry in value.commands
            ],
            "project_root": _reference_payload(value.project_root),
            "output_root": _reference_payload(value.output_root),
            "profile_root": _reference_payload(value.profile_root),
            "total_termination_budget_seconds": value.total_termination_budget_seconds,
        }
    )


def decode_trusted_dbt_command_plan(payload: bytes) -> TrustedDbtCommandPlan:
    """Reject extra slots/fields and noncanonical serialized command membership."""
    value = _mapping(
        decode_native_delivery_json(payload),
        {
            "schema",
            "executor_invocation_id",
            "phase",
            "commands",
            "project_root",
            "output_root",
            "profile_root",
            "total_termination_budget_seconds",
        },
    )
    if value["schema"] != "dpone.trusted-dbt-command-plan.v1" or type(value["commands"]) is not list:
        raise NativeSourceCustodyError("invalid trusted command plan wire schema")
    entries = tuple(_command_entry(entry) for entry in value["commands"])
    plan = TrustedDbtCommandPlan(
        schema="dpone.trusted-dbt-command-plan.v1",
        executor_invocation_id=_uuid(value["executor_invocation_id"]),
        phase=cast(Literal["BUILD", "QUALITY"], _string(value["phase"])),
        commands=entries,
        project_root=_reference(value["project_root"]),
        output_root=_reference(value["output_root"]),
        profile_root=_reference(value["profile_root"]),
        total_termination_budget_seconds=_integer(value["total_termination_budget_seconds"]),
    )
    if encode_trusted_dbt_command_plan(plan) != payload:
        raise NativeSourceCustodyError("trusted command plan bytes must be canonical")
    return plan


def encode_trusted_dbt_invocation_completion(value: TrustedDbtInvocationCompletion) -> bytes:
    """Encode captured positive facts without adding runtime state or timestamps."""
    if type(value) is not TrustedDbtInvocationCompletion:
        raise NativeSourceCustodyError("expected a trusted dbt invocation completion")
    value.__post_init__()
    return encode_native_delivery_json(
        {
            "schema": value.schema,
            "executor": decode_native_delivery_json(encode_source_executor_binding(value.executor)),
            "command": _reference_payload(value.command),
            "toolchain": _reference_payload(value.toolchain),
            "qualification": _reference_payload(value.qualification),
            "started_at": value.started_at,
            "finished_at": value.finished_at,
            "elapsed_microseconds": value.elapsed_microseconds,
            "exit_code": value.exit_code,
            "command_count": value.command_count,
        }
    )


def decode_trusted_dbt_invocation_completion(payload: bytes) -> TrustedDbtInvocationCompletion:
    """Decode a positive-only record; authenticity remains the reader's obligation."""
    value = _mapping(
        decode_native_delivery_json(payload),
        {
            "schema",
            "executor",
            "command",
            "toolchain",
            "qualification",
            "started_at",
            "finished_at",
            "elapsed_microseconds",
            "exit_code",
            "command_count",
        },
    )
    if value["schema"] != "dpone.trusted-dbt-invocation-completion.v1" or _integer(value["exit_code"]) != 0:
        raise NativeSourceCustodyError("invalid trusted completion wire schema/outcome")
    completion = TrustedDbtInvocationCompletion(
        schema="dpone.trusted-dbt-invocation-completion.v1",
        executor=decode_source_executor_binding(encode_native_delivery_json(value["executor"])),
        command=_reference(value["command"]),
        toolchain=_reference(value["toolchain"]),
        qualification=_reference(value["qualification"]),
        started_at=_string(value["started_at"]),
        finished_at=_string(value["finished_at"]),
        elapsed_microseconds=_integer(value["elapsed_microseconds"]),
        exit_code=0,
        command_count=_integer(value["command_count"]),
    )
    if encode_trusted_dbt_invocation_completion(completion) != payload:
        raise NativeSourceCustodyError("trusted completion bytes must be canonical")
    return completion


def _mapping(value: NativeJsonValue, fields: set[str]) -> dict[str, NativeJsonValue]:
    if type(value) is not dict or set(value) != fields:
        raise NativeSourceCustodyError("trusted invocation record has incomplete or extra fields")
    return value


def _integer(value: NativeJsonValue) -> int:
    if type(value) is not int:
        raise NativeSourceCustodyError("trusted invocation integer must be exact")
    return value


def _string(value: NativeJsonValue) -> str:
    if type(value) is not str:
        raise NativeSourceCustodyError("trusted invocation string must be exact")
    return value


def _command_entry(value: NativeJsonValue) -> TrustedDbtCommandEntry:
    fields = _mapping(
        value, {"position", "verb", "argv_template", "command_timeout_seconds", "termination_allowance_seconds"}
    )
    arguments = fields["argv_template"]
    if type(arguments) is not list:
        raise NativeSourceCustodyError("trusted command argv must be an array")
    return TrustedDbtCommandEntry(
        position=_integer(fields["position"]),
        verb=cast(Literal["parse", "ls", "build", "test"], _string(fields["verb"])),
        argv_template=tuple(_string(argument) for argument in arguments),
        command_timeout_seconds=_integer(fields["command_timeout_seconds"]),
        termination_allowance_seconds=_integer(fields["termination_allowance_seconds"]),
    )


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
