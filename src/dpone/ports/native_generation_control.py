"""Generation freeze ledger capabilities; authentication remains upstream."""

from typing import Protocol
from uuid import UUID

from dpone.contracts.native_delivery import FrozenGeneration, GenerationReservation
from dpone.contracts.native_source_custody import (
    SourceAdmissionClosure,
    SourceCustodySnapshot,
    SourceTrustedBuildCompletion,
)


class SourceGenerationFreezeLedger(Protocol):
    """Keep physical custody checks inside each atomic ledger operation.

    Historical metadata reads are preparation only, never current-owner proof.
    Mutation and independent current-read consume the identical complete request.
    Neither method dispatches work, publishes originals or releases capacity.
    """

    def read_custody(self, generation_id: UUID) -> SourceCustodySnapshot: ...

    def read_admission_closure(self, generation_id: UUID) -> SourceAdmissionClosure: ...

    def inspect_freeze(
        self,
        reservation: GenerationReservation,
        admission: SourceAdmissionClosure,
        completion: SourceTrustedBuildCompletion,
        frozen: FrozenGeneration,
        *,
        expected_revision: int,
    ) -> SourceCustodySnapshot:
        """Prove exact pending or accepted metadata under current physical locks.

        A failure is never evidence that the operation has not happened. A
        pending result permits only a revision-checked metadata CAS, not a child
        launch or data movement. CAS must revalidate ownership independently.
        """
        ...

    def record_freeze(
        self,
        reservation: GenerationReservation,
        admission: SourceAdmissionClosure,
        completion: SourceTrustedBuildCompletion,
        frozen: FrozenGeneration,
        *,
        expected_revision: int,
    ) -> SourceCustodySnapshot: ...

    def read_freeze(
        self,
        reservation: GenerationReservation,
        admission: SourceAdmissionClosure,
        completion: SourceTrustedBuildCompletion,
        frozen: FrozenGeneration,
        *,
        expected_revision: int,
    ) -> SourceCustodySnapshot: ...
