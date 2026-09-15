"""Trusted source invocation identities; values alone never authorize a launch."""

from __future__ import annotations

from dataclasses import dataclass
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
