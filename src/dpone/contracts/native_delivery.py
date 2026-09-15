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
