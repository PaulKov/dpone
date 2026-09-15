"""Native generation reservation readback; identity does not authorize dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.native_identity import OriginalRef


class NativeGenerationContractError(ValueError):
    """A generation admission value is incomplete or inconsistent."""


@dataclass(frozen=True, slots=True)
class GenerationReservation:
    """A retained generation and revision under an existing physical guard."""

    generation_id: UUID
    guard_epoch: int
    revision: int
    reservation: OriginalRef

    def __post_init__(self) -> None:
        if type(self.generation_id) is not UUID or type(self.reservation) is not OriginalRef:
            raise NativeGenerationContractError("reservation requires exact generation and original identity")
        self.reservation.__post_init__()
        for value in (self.guard_epoch, self.revision):
            if type(value) is not int or not 1 <= value <= 9223372036854775807:
                raise NativeGenerationContractError("reservation epoch/revision must be positive SQL bigint values")


@dataclass(frozen=True, slots=True)
class SourceClosureReceipt:
    """Descriptor of an accepted positive completion snapshot, not admission closure.

    The independently resolved receipt is outside its own canonical payload.
    Authentication and current physical custody remain the consumer's duty.
    """

    generation_id: UUID
    guard_epoch: int
    revision: int
    reservation: OriginalRef
    completion: OriginalRef
    receipt: OriginalRef

    def __post_init__(self) -> None:
        GenerationReservation(self.generation_id, self.guard_epoch, self.revision, self.reservation)
        for reference in (self.completion, self.receipt):
            if type(reference) is not OriginalRef:
                raise NativeGenerationContractError("source closure requires exact completion and receipt originals")
            reference.__post_init__()


@dataclass(frozen=True, slots=True)
class FrozenGeneration:
    """One immutable freeze descriptor; no read, dispatch or release grant.

    The closure identifies the accepted BUILDING completion revision. Freeze is
    its next revision and preserves the exact generation, epoch and reservation.
    """

    generation_id: UUID
    guard_epoch: int
    revision: int
    reservation: OriginalRef
    closure: SourceClosureReceipt
    frozen: OriginalRef

    def __post_init__(self) -> None:
        GenerationReservation(self.generation_id, self.guard_epoch, self.revision, self.reservation)
        if type(self.closure) is not SourceClosureReceipt or type(self.frozen) is not OriginalRef:
            raise NativeGenerationContractError("frozen generation requires exact closure and descriptor")
        self.closure.__post_init__()
        self.frozen.__post_init__()
        if (self.generation_id, self.guard_epoch, self.reservation, self.revision) != (
            self.closure.generation_id,
            self.closure.guard_epoch,
            self.closure.reservation,
            self.closure.revision + 1,
        ):
            raise NativeGenerationContractError("freeze must preserve exact closure identity at the next revision")
