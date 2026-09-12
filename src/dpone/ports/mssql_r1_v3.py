"""Capability-oriented ports for the unreleased MSSQL R1 V3 provider set."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_authority import (
    MssqlSignedPayloadVerificationV1,
    SignedGenerationAuthoritySetCommandV1,
    SignedRevokedOverrideCommandV1,
)
from dpone.contracts.mssql_r1_v3_control_proof import (
    MssqlOpenStageRecoveryFreshProofV1,
    MssqlOpenStageRecoveryReceiptV1,
    MssqlR1ControlFreshProofV1,
    MssqlR1ControlReceiptV1,
    MssqlR1V3ContractError,
    require_canonical_text,
)
from dpone.contracts.mssql_r1_v3_effect import (
    MssqlR1EffectAttemptEnvelopeV3,
    MssqlR1EffectRequestV3,
    MssqlR1EffectSealIntentV3,
    MssqlR1SealOutcomeV3,
)
from dpone.contracts.mssql_r1_v3_plan import R1BatchMutationPlanV1, R1XminMutationPlanV1
from dpone.contracts.mssql_r1_v3_receipt import (
    MssqlR1EffectReceiptV3,
)
from dpone.contracts.mssql_r1_v3_registration import (
    MssqlTargetRegistrationVerificationV1,
    SignedTargetRegistrationCommandV1,
)
from dpone.contracts.mssql_r1_v3_rendered import (
    MssqlR1RenderedMutationBundleV1,
    MssqlR1RendererAuthorityV1,
    MssqlR1VerifiedRenderedMutationV1,
)
from dpone.contracts.mssql_r1_v3_replay import (
    CommittedEffectReplayProofV3,
    EffectReplayProofV3,
)
from dpone.contracts.mssql_r1_v3_schema_attestation import MssqlR1SchemaAttestationV3, MssqlR1SchemaContractV3
from dpone.contracts.mssql_r1_v3_staging import (
    R1BoundedStageChunkV1,
    R1OpenStagePlanV1,
    R1SealedStageManifestV1,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    EffectReceiptProofRequestV3 as EffectReceiptProofRequestV3,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlAdmittedGenerationAuthoritySetV3 as MssqlAdmittedGenerationAuthoritySetV3,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlBatchMutationV3Port as MssqlBatchMutationV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlBatchQualityEvidenceV3 as MssqlBatchQualityEvidenceV3,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlBatchQualityV3Port as MssqlBatchQualityV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlCandidateEffectProofV3 as MssqlCandidateEffectProofV3,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlCandidateEffectProofV3Port as MssqlCandidateEffectProofV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlConsumedGenerationAuthoritySetV3 as MssqlConsumedGenerationAuthoritySetV3,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlConsumedSealedStageSetV3 as MssqlConsumedSealedStageSetV3,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlGenerationAuthoritySetTransactionV3Port as MssqlGenerationAuthoritySetTransactionV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlR1TransactionBindingV3 as MssqlR1TransactionBindingV3,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlR1TransactionSessionFactoryV3Port as MssqlR1TransactionSessionFactoryV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlR1TransactionSessionV3Port as MssqlR1TransactionSessionV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlR1TransactionV3 as MssqlR1TransactionV3,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlSealedStageAttestorV3Port as MssqlSealedStageAttestorV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlSealedStageConsumptionV3Port as MssqlSealedStageConsumptionV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlTargetWriterFenceV3Port as MssqlTargetWriterFenceV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlV3EffectReceiptStorePort as MssqlV3EffectReceiptStorePort,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlXminMutationV3Port as MssqlXminMutationV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlXminQualityEvidenceV3 as MssqlXminQualityEvidenceV3,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    MssqlXminQualityV3Port as MssqlXminQualityV3Port,
)
from dpone.ports.mssql_r1_v3_effect_runtime import (
    R1TypedStageScanEvidenceV1 as R1TypedStageScanEvidenceV1,
)


class MssqlR1PreSourceStatusV3(StrEnum):
    SOURCE_REQUIRED = "source_required"
    SEALED_RESUME = "sealed_resume"
    REPLAY_COMMITTED = "replay_committed"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class MssqlR1PreSourceOutcomeV3:
    status: MssqlR1PreSourceStatusV3
    sealed_request: MssqlR1EffectRequestV3 | None = None
    attempt: MssqlR1EffectAttemptEnvelopeV3 | None = None
    replay: EffectReplayProofV3 | None = None
    blocker: str | None = None

    def __post_init__(self) -> None:
        populated = (
            self.sealed_request is not None,
            self.attempt is not None,
            self.replay is not None,
            self.blocker is not None,
        )
        expected = {
            MssqlR1PreSourceStatusV3.SOURCE_REQUIRED: (False, False, False, False),
            MssqlR1PreSourceStatusV3.SEALED_RESUME: (True, True, False, False),
            MssqlR1PreSourceStatusV3.REPLAY_COMMITTED: (False, False, True, False),
            MssqlR1PreSourceStatusV3.BLOCKED: (False, False, False, True),
        }
        if populated != expected.get(self.status):
            raise MssqlR1V3ContractError("pre-source outcome discriminator/payload mismatch")
        if self.status is MssqlR1PreSourceStatusV3.SEALED_RESUME and self.attempt.request != self.sealed_request:  # type: ignore[union-attr]
            raise MssqlR1V3ContractError("sealed resume attempt differs from retained request")
        if self.status is MssqlR1PreSourceStatusV3.REPLAY_COMMITTED and not isinstance(
            self.replay, CommittedEffectReplayProofV3
        ):
            raise MssqlR1V3ContractError("replay-committed outcome requires exact committed proof")
        if self.blocker is not None:
            require_canonical_text(self.blocker, "blocker", maximum_bytes=512)


@dataclass(frozen=True, slots=True)
class MssqlR1SealedRunResultV3:
    receipt: MssqlR1EffectReceiptV3
    replayed: bool


class MssqlTargetRegistrationSignatureVerifierV1Port(Protocol):
    def verify_registration(
        self,
        command: SignedTargetRegistrationCommandV1,
    ) -> MssqlTargetRegistrationVerificationV1: ...


class MssqlGenerationAuthoritySetVerifierV3Port(Protocol):
    def verify_authority_set(
        self, command: SignedGenerationAuthoritySetCommandV1
    ) -> MssqlSignedPayloadVerificationV1: ...


class MssqlGenerationAuthoritySetSignatureVerifierV1Port(MssqlGenerationAuthoritySetVerifierV3Port, Protocol):
    """Compatibility name for the unreleased narrow authority-set verifier."""


class BlobSignatureVerifierV1Port(
    MssqlTargetRegistrationSignatureVerifierV1Port,
    MssqlGenerationAuthoritySetSignatureVerifierV1Port,
    Protocol,
):
    """Internal prototype compatibility; never bind this broad port into active R1."""

    def verify_revoked_override(self, command: SignedRevokedOverrideCommandV1) -> MssqlSignedPayloadVerificationV1: ...


class MssqlTargetRegistrationProvisionerPort(Protocol):
    def provision(self, command: SignedTargetRegistrationCommandV1) -> MssqlR1ControlReceiptV1: ...

    def probe_fresh(self, registration_id: UUID) -> MssqlR1ControlFreshProofV1 | None: ...


class MssqlGenerationAuthoritySetImporterV3Port(Protocol):
    def import_authority_set(self, command: SignedGenerationAuthoritySetCommandV1) -> MssqlR1ControlReceiptV1: ...

    def probe_authority_import_fresh(self, control_effect_key: bytes) -> MssqlR1ControlFreshProofV1 | None: ...


class MssqlGenerationAuthorityImporterPort(MssqlGenerationAuthoritySetImporterV3Port, Protocol):
    """Compatibility name for the unreleased narrow authority-set importer."""


class MssqlR1StageWriterPort(Protocol):
    def register_open(self, plan: R1OpenStagePlanV1) -> None: ...

    def write(self, plan: R1OpenStagePlanV1, chunk: R1BoundedStageChunkV1) -> None: ...

    def renew(self, artifact_id: UUID, owner_epoch: int) -> bool: ...

    def seal(self, plan: R1OpenStagePlanV1) -> R1SealedStageManifestV1: ...

    def recover_expired_open(
        self,
        operation_key: bytes,
        effect_key: bytes,
        old_artifact_set_digest: bytes,
    ) -> MssqlOpenStageRecoveryReceiptV1: ...

    def probe_recovery_fresh(self, recovery_effect_key: bytes) -> MssqlOpenStageRecoveryFreshProofV1 | None: ...


class MssqlTargetAuthorityPreparationV3Port(Protocol):
    def pre_source(self, effect_key: bytes) -> MssqlR1PreSourceOutcomeV3: ...

    def seal(self, intent: MssqlR1EffectSealIntentV3) -> MssqlR1SealOutcomeV3: ...

    def load_sealed(self, effect_key: bytes) -> MssqlR1EffectRequestV3 | None: ...

    def take_over_sealed(self, effect_key: bytes, owner_id_digest: bytes) -> MssqlR1EffectAttemptEnvelopeV3: ...


class MssqlR1MutationPlanRendererPort(Protocol):
    def render(self, plan: R1BatchMutationPlanV1 | R1XminMutationPlanV1) -> MssqlR1RenderedMutationBundleV1: ...


class MssqlR1RendererAuthorityResolverPort(Protocol):
    """Resolve trust independently from bundle fields using the signed environment profile."""

    def resolve(self, resolved_profile_digest: bytes) -> MssqlR1RendererAuthorityV1: ...


class MssqlR1RenderedMutationVerifierPort(Protocol):
    """Attest exact rendered bytes with an independently injected verifier."""

    def verify(
        self,
        plan: R1BatchMutationPlanV1 | R1XminMutationPlanV1,
        bundle: MssqlR1RenderedMutationBundleV1,
        authority: MssqlR1RendererAuthorityV1,
    ) -> MssqlR1VerifiedRenderedMutationV1: ...


class MssqlR1SchemaAuthorityV3Port(Protocol):
    def install_and_attest(
        self,
        transaction: MssqlR1TransactionV3,
        expected: MssqlR1SchemaContractV3,
    ) -> MssqlR1SchemaAttestationV3: ...

    def attest_fresh(self, registration_id: UUID) -> MssqlR1SchemaAttestationV3: ...


class PostgresMssqlR1SealedEffectRunnerPort(Protocol):
    def run_sealed(self, attempt: MssqlR1EffectAttemptEnvelopeV3) -> MssqlR1SealedRunResultV3: ...


__all__ = [name for name in tuple(globals()) if name.endswith(("Port", "V3"))]
