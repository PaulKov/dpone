"""Narrow effect-runtime ports for the unreleased MSSQL R1 V3 provider set."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from dpone.contracts.mssql_r1_v3_candidate_proof import MssqlCandidateEffectProofV3
from dpone.contracts.mssql_r1_v3_effect import MssqlR1EffectAttemptEnvelopeV3
from dpone.contracts.mssql_r1_v3_errors import MssqlR1V3ContractError as MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_plan import R1XminMutationPlanV1 as R1XminMutationPlanV1
from dpone.contracts.mssql_r1_v3_quality import MssqlBatchQualityEvidenceV3, MssqlXminQualityEvidenceV3
from dpone.contracts.mssql_r1_v3_receipt import MssqlR1EffectReceiptV3
from dpone.contracts.mssql_r1_v3_replay import (
    CommittedEffectReplayProofV3 as CommittedEffectReplayProofV3,
)
from dpone.contracts.mssql_r1_v3_replay import (
    EffectReceiptProofRequestV3,
    EffectReplayProofV3,
)
from dpone.contracts.mssql_r1_v3_stage_consumption import MssqlConsumedSealedStageSetV3
from dpone.contracts.mssql_r1_v3_staging import R1SealedStageManifestV1, R1TypedStageScanEvidenceV1
from dpone.contracts.mssql_r1_v3_transaction import MssqlR1TransactionBindingV3
from dpone.contracts.mssql_r1_v3_transaction_authority import (
    MssqlAdmittedGenerationAuthoritySetV3,
    MssqlConsumedGenerationAuthoritySetV3,
)


class MssqlR1TransactionV3(Protocol):
    """Opaque injected same-database UoW handle shared by all atomic steps."""

    @property
    def transaction_id(self) -> UUID: ...

    @property
    def binding(self) -> MssqlR1TransactionBindingV3: ...

    def assert_active(self) -> None: ...


class MssqlR1TransactionSessionV3Port(Protocol):
    def begin(self) -> MssqlR1TransactionV3: ...

    def assert_active(self, transaction: MssqlR1TransactionV3) -> None: ...

    def dispatch_commit(self, transaction: MssqlR1TransactionV3) -> None: ...

    def rollback(self, transaction: MssqlR1TransactionV3) -> None: ...

    def close(self) -> None: ...


class MssqlR1TransactionSessionFactoryV3Port(Protocol):
    def open(self) -> MssqlR1TransactionSessionV3Port: ...


class MssqlSealedStageAttestorV3Port(Protocol):
    def attest(
        self,
        transaction: MssqlR1TransactionV3,
        manifest: R1SealedStageManifestV1,
    ) -> R1TypedStageScanEvidenceV1: ...


class MssqlGenerationAuthoritySetTransactionV3Port(Protocol):
    def resolve_and_admit(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
    ) -> MssqlAdmittedGenerationAuthoritySetV3: ...

    def consume(
        self,
        transaction: MssqlR1TransactionV3,
        admitted: MssqlAdmittedGenerationAuthoritySetV3,
        receipt: MssqlR1EffectReceiptV3,
    ) -> MssqlConsumedGenerationAuthoritySetV3: ...


class MssqlSealedStageConsumptionV3Port(Protocol):
    def consume(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
        receipt: MssqlR1EffectReceiptV3,
    ) -> MssqlConsumedSealedStageSetV3: ...


class MssqlCandidateEffectProofV3Port(Protocol):
    def prove(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
        receipt: MssqlR1EffectReceiptV3,
    ) -> MssqlCandidateEffectProofV3: ...


class MssqlBatchMutationV3Port(Protocol):
    def mutate(self, transaction: MssqlR1TransactionV3, attempt: MssqlR1EffectAttemptEnvelopeV3) -> None: ...

    def update_row_hashes(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
    ) -> None: ...


class MssqlXminMutationV3Port(Protocol):
    def apply_delta(self, transaction: MssqlR1TransactionV3, attempt: MssqlR1EffectAttemptEnvelopeV3) -> None: ...

    def update_row_hashes(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
    ) -> None: ...

    def write_checkpoint(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
    ) -> None: ...


class MssqlBatchQualityV3Port(Protocol):
    def evaluate(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
    ) -> MssqlBatchQualityEvidenceV3: ...


class MssqlXminQualityV3Port(Protocol):
    def evaluate(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
    ) -> MssqlXminQualityEvidenceV3: ...


class MssqlV3EffectReceiptStorePort(Protocol):
    def append(self, transaction: MssqlR1TransactionV3, receipt: MssqlR1EffectReceiptV3) -> None: ...

    def probe_fresh(self, request: EffectReceiptProofRequestV3) -> EffectReplayProofV3: ...


class MssqlTargetWriterFenceV3Port(Protocol):
    def admit(self, transaction: MssqlR1TransactionV3, attempt: MssqlR1EffectAttemptEnvelopeV3) -> None: ...

    def advance_head(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
        receipt: MssqlR1EffectReceiptV3,
    ) -> None: ...


__all__ = [name for name in tuple(globals()) if name.endswith(("Port", "V3"))]
