"""Qualified invocation records, canonical bytes and locked argument policy.

These decisions consume authenticated records. They do not acquire originals,
qualify a runtime, observe a process, or authorize command dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
    SourceExecutorBinding,
    _integer,
    _mapping,
    _reference,
    _reference_payload,
    _require_originals,
    _uuid,
    decode_source_executor_binding,
    encode_source_executor_binding,
)
from dpone.contracts.native_trusted_dbt_environment import TrustedDbtOwnedRoot, TrustedDbtQualification


def _string(value: NativeJsonValue) -> str:
    if type(value) is not str:
        raise NativeSourceCustodyError("trusted invocation string must be exact")
    return value


@dataclass(frozen=True, slots=True)
class TrustedDbtCommandEntry:
    """One immutable command slot; qualification authenticates its literal argv."""

    position: int
    verb: Literal["parse", "ls", "build", "test"]
    argv_template: tuple[str, ...]
    command_timeout_seconds: int
    termination_allowance_seconds: int

    def __post_init__(self) -> None:
        if type(self.position) is not int or self.position < 0:
            raise NativeSourceCustodyError("command position must be an exact nonnegative integer")
        if type(self.verb) is not str or self.verb not in {"parse", "ls", "build", "test"}:
            raise NativeSourceCustodyError("unsupported trusted dbt command verb")
        if type(self.argv_template) is not tuple or not self.argv_template:
            raise NativeSourceCustodyError("trusted command requires immutable argv")
        if any(type(argument) is not str or "\x00" in argument for argument in self.argv_template):
            raise NativeSourceCustodyError("trusted command arguments must be exact strings without NUL")
        if self.argv_template[0] != "dbt":
            raise NativeSourceCustodyError("trusted command must use the qualified dbt entry point")
        # These are the global switches emitted by the existing command builders.
        index = 1
        while index < len(self.argv_template) and self.argv_template[index] in {
            "--quiet",
            "--no-use-colors",
            "--warn-error",
        }:
            index += 1
        if index == len(self.argv_template) or self.argv_template[index] != self.verb:
            raise NativeSourceCustodyError("command verb differs from its literal argv")
        for allowance in (self.command_timeout_seconds, self.termination_allowance_seconds):
            if type(allowance) is not int or allowance <= 0:
                raise NativeSourceCustodyError("command time bounds must be exact positive integers")


@dataclass(frozen=True, slots=True)
class TrustedDbtCommandPlan:
    """Finite BUILD or QUALITY membership under authenticated owned roots.

    The recorder substitutes only predefined whole path arguments, after root
    authentication. This value neither resolves credentials nor grants dispatch.
    """

    schema: Literal["dpone.trusted-dbt-command-plan.v1"]
    executor_invocation_id: UUID
    phase: Literal["BUILD", "QUALITY"]
    commands: tuple[TrustedDbtCommandEntry, ...]
    project_root: OriginalRef
    output_root: OriginalRef
    profile_root: OriginalRef
    total_termination_budget_seconds: int

    def __post_init__(self) -> None:
        if type(self.schema) is not str or self.schema != "dpone.trusted-dbt-command-plan.v1":
            raise NativeSourceCustodyError("unsupported trusted command plan schema")
        if type(self.executor_invocation_id) is not UUID:
            raise NativeSourceCustodyError("trusted plan requires the exact executor invocation UUID")
        if type(self.phase) is not str or self.phase not in {"BUILD", "QUALITY"}:
            raise NativeSourceCustodyError("unsupported trusted invocation phase")
        expected = ("parse", "ls", "build") if self.phase == "BUILD" else ("test",)
        if type(self.commands) is not tuple or len(self.commands) != len(expected):
            raise NativeSourceCustodyError("trusted invocation has incomplete or extra command membership")
        for position, (entry, verb) in enumerate(zip(self.commands, expected, strict=True)):
            if type(entry) is not TrustedDbtCommandEntry:
                raise NativeSourceCustodyError("trusted invocation requires exact command entries")
            entry.__post_init__()
            if entry.position != position or entry.verb != verb:
                raise NativeSourceCustodyError("trusted invocation command ordering differs from its phase")
        _require_originals(self.project_root, self.output_root, self.profile_root)
        if type(self.total_termination_budget_seconds) is not int or self.total_termination_budget_seconds <= 0:
            raise NativeSourceCustodyError("total termination budget must be an exact positive integer")


@dataclass(frozen=True, slots=True)
class TrustedDbtInvocationCompletion:
    """Positive qualified normal return; wall time does not override monotonic time.

    A record is not process observation by itself. Its producer must own the
    complete admitted command sequence and independently verify publication.
    """

    schema: Literal["dpone.trusted-dbt-invocation-completion.v1"]
    executor: SourceExecutorBinding
    command: OriginalRef
    toolchain: OriginalRef
    qualification: OriginalRef
    started_at: str
    finished_at: str
    elapsed_microseconds: int
    exit_code: Literal[0]
    command_count: int

    def __post_init__(self) -> None:
        if type(self.schema) is not str or self.schema != "dpone.trusted-dbt-invocation-completion.v1":
            raise NativeSourceCustodyError("unsupported trusted completion schema")
        if type(self.executor) is not SourceExecutorBinding:
            raise NativeSourceCustodyError("completion requires its exact executor binding")
        self.executor.__post_init__()
        _require_originals(self.command, self.toolchain, self.qualification)
        for timestamp in (self.started_at, self.finished_at):
            if type(timestamp) is not str:
                raise NativeSourceCustodyError("completion timestamp must be a canonical UTC string")
            try:
                parsed = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError as exc:
                raise NativeSourceCustodyError("completion timestamp must use UTC microseconds") from exc
            if parsed.isoformat(timespec="microseconds") + "Z" != timestamp:
                raise NativeSourceCustodyError("completion timestamp must use canonical UTC microseconds")
        if type(self.elapsed_microseconds) is not int or self.elapsed_microseconds < 0:
            raise NativeSourceCustodyError("completion elapsed time must be an exact nonnegative integer")
        if type(self.exit_code) is not int or self.exit_code != 0:
            raise NativeSourceCustodyError("completion requires an exact zero exit code")
        if type(self.command_count) is not int or self.command_count not in {1, 3}:
            raise NativeSourceCustodyError("completion requires the complete BUILD or QUALITY membership")


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


@dataclass(frozen=True, slots=True)
class AuthenticatedInvocationPlan:
    """Freshly authenticated plan and its three role-specific root identities."""

    plan: TrustedDbtCommandPlan
    project: TrustedDbtOwnedRoot
    output: TrustedDbtOwnedRoot
    profile: TrustedDbtOwnedRoot


def require_admitted_command(executor: SourceExecutorBinding, command: OriginalRef) -> None:
    """Reject a substituted command before any original acquisition."""
    if executor.command != command:
        raise NativeSourceCustodyError("command differs from the admitted executor")


def require_qualified_invocation(
    qualified: TrustedDbtQualification,
    plan: TrustedDbtCommandPlan,
    *,
    command: OriginalRef,
    toolchain: OriginalRef,
    executor: SourceExecutorBinding,
) -> None:
    """Check qualification first, then invocation, before acquiring owned roots."""
    if (qualified.command_plan, qualified.toolchain, qualified.profile) != (command, toolchain, executor.profile):
        raise NativeSourceCustodyError("qualification differs from the exact admitted plan/toolchain/profile")
    if plan.executor_invocation_id != executor.invocation_id:
        raise NativeSourceCustodyError("command plan differs from the admitted invocation")


def require_owned_root_identities(roots: tuple[TrustedDbtOwnedRoot, ...], executor: SourceExecutorBinding) -> None:
    """Validate roles and distinct identities after all three roots are acquired."""
    for root, role in zip(roots, ("PROJECT", "OUTPUT", "PROFILE"), strict=True):
        if root.role != role or root.executor_invocation_id != executor.invocation_id:
            raise NativeSourceCustodyError("owned root role or invocation differs from admission")
    if len({(root.device, root.inode) for root in roots}) != 3:
        raise NativeSourceCustodyError("project, output and profile roots must be distinct directories")


def invocation_path_slots(
    authenticated: AuthenticatedInvocationPlan, *, position: int, args: tuple[str, ...]
) -> dict[str, int]:
    """Validate literal arguments and return only the four permitted path slots."""
    template = authenticated.plan.commands[position].argv_template
    if type(args) is not tuple or len(args) != len(template) or any(type(arg) is not str for arg in args):
        raise NativeSourceCustodyError("command arguments differ from the locked plan")
    substitutions = {
        "--project-dir": "PROJECT_DIR",
        "--profiles-dir": "PROFILE_DIR",
        "--target-path": "TARGET_DIR",
        "--log-path": "LOG_DIR",
    }
    slots: dict[str, int] = {}
    for flag in substitutions:
        if template.count(flag) != 1:
            raise NativeSourceCustodyError("locked command requires each explicit owned path exactly once")
        index = template.index(flag) + 1
        if index == len(template):
            raise NativeSourceCustodyError("locked command path argument is missing")
        slots[flag] = index
    for index, (expected, actual) in enumerate(zip(template, args, strict=True)):
        placeholder = next((substitutions[flag] for flag, slot in slots.items() if slot == index), None)
        if expected != actual and expected != placeholder:
            raise NativeSourceCustodyError("command arguments differ from the locked plan")
    return slots
