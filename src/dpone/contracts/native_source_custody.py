"""Trusted source invocation identities; values alone never authorize a launch."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from dpone.contracts.native_identity import OriginalRef


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


def _require_originals(*references: OriginalRef) -> None:
    for reference in references:
        if type(reference) is not OriginalRef:
            raise NativeSourceCustodyError("trusted invocation requires exact original references")
        reference.__post_init__()
