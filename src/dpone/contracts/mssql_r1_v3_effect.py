"""Sealed semantic effect and server-built retry envelope for MSSQL R1 V3."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1EffectContractV3,
    MssqlR1EffectIdentityV3,
    MssqlR1V3ContractError,
    canonical_bytes,
    canonical_utc_text,
    decode_canonical_bytes,
    expect_bytes,
    expect_tuple,
    expect_uuid,
    parse_canonical_utc_text,
    require_digest,
    require_positive,
    require_uuid,
)
from dpone.contracts.mssql_r1_v3_mutation import (
    MssqlBatchGenerationTransitionV3,
    MssqlXminGenerationTransitionV3,
)
from dpone.contracts.mssql_r1_v3_plan import MutationPlanV1, R1BatchMutationPlanV1, R1XminMutationPlanV1
from dpone.contracts.mssql_r1_v3_registration import MssqlTargetRegistrationPayloadV1
from dpone.contracts.mssql_r1_v3_rendered import MssqlR1RenderedMutationBundleV1
from dpone.contracts.mssql_r1_v3_staging import (
    R1SealedStageManifestV1,
    R1StageArtifactKindV1,
    sealed_stage_set_digest,
)
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_authority import MssqlRevokedRegistrationCompletionOverridePayloadV1
    from dpone.contracts.mssql_r1_v3_mutation import MssqlGenerationAuthoritySetV2


_EFFECT_REQUEST_DOMAIN = b"dpone.mssql-effect-request.v3\0"
_ATTEMPT_ENVELOPE_DOMAIN = b"dpone.mssql-effect-attempt.v3\0"


@dataclass(frozen=True, slots=True)
class MssqlR1EffectRequestV3:
    """Immutable semantic effect bytes; retry ownership and epoch are deliberately absent."""

    identity: MssqlR1EffectIdentityV3
    registration_id: UUID
    registration_payload_digest: bytes
    registration_payload_bytes: bytes
    verification_policy_digest: bytes
    admitted_at_server_time: datetime
    source_snapshot_digest: bytes
    source_schema_digest: bytes
    artifacts: tuple[R1SealedStageManifestV1, ...]
    mutation_plan: MutationPlanV1
    rendered_bundle: MssqlR1RenderedMutationBundleV1
    generation: MssqlBatchGenerationTransitionV3 | MssqlXminGenerationTransitionV3
    contract: MssqlR1EffectContractV3 = MssqlR1EffectContractV3()

    def __post_init__(self) -> None:
        if not isinstance(self.contract, MssqlR1EffectContractV3):
            raise MssqlR1V3ContractError("effect request requires the exact V3 contract")
        if not isinstance(self.identity, MssqlR1EffectIdentityV3):
            raise MssqlR1V3ContractError("effect identity must use V3")
        require_uuid(self.registration_id, "registration_id")
        for name in (
            "registration_payload_digest",
            "verification_policy_digest",
            "source_snapshot_digest",
            "source_schema_digest",
        ):
            require_digest(getattr(self, name), name)
        registration = MssqlTargetRegistrationPayloadV1.from_canonical_bytes(self.registration_payload_bytes)
        if (
            registration.registration_id != self.registration_id
            or registration.payload_digest != self.registration_payload_digest
            or registration.target_binding_uuid != self.identity.target_binding_uuid
        ):
            raise MssqlR1V3ContractError("effect request registration payload differs from sealed identity")
        canonical_utc_text(self.admitted_at_server_time, "admitted_at_server_time")
        if not isinstance(self.artifacts, tuple) or not self.artifacts:
            raise MssqlR1V3ContractError("effect request requires sealed artifacts")
        if not all(isinstance(item, R1SealedStageManifestV1) for item in self.artifacts):
            raise MssqlR1V3ContractError("effect request requires exact sealed V3 artifacts")
        expected_kinds = (
            (R1StageArtifactKindV1.BATCH_PAYLOAD,)
            if self.identity.source_mode is SourceMode.BATCH_FULL_REFRESH
            else (R1StageArtifactKindV1.XMIN_DELTA, R1StageArtifactKindV1.XMIN_COMPLETE_KEYS)
        )
        if tuple(item.artifact_kind for item in self.artifacts) != expected_kinds:
            raise MssqlR1V3ContractError("effect artifacts do not follow the fixed mode order")
        if any(
            item.effect_key != self.identity.effect_key or item.target_binding_uuid != self.identity.target_binding_uuid
            for item in self.artifacts
        ):
            raise MssqlR1V3ContractError("effect artifact differs from request identity")
        expected_plan_type = (
            R1BatchMutationPlanV1
            if self.identity.source_mode is SourceMode.BATCH_FULL_REFRESH
            else R1XminMutationPlanV1
        )
        if not isinstance(self.mutation_plan, expected_plan_type):
            raise MssqlR1V3ContractError("mutation plan differs from request source mode")
        if self.mutation_plan.effect_key != self.identity.effect_key:
            raise MssqlR1V3ContractError("mutation plan differs from request identity")
        if self.mutation_plan.target_identity != registration.physical_identity:
            raise MssqlR1V3ContractError("mutation plan target differs from signed registration")
        plan_manifests = (
            (self.mutation_plan.stage_manifest,)
            if isinstance(self.mutation_plan, R1BatchMutationPlanV1)
            else (self.mutation_plan.delta_manifest, self.mutation_plan.complete_keys_manifest)
        )
        if plan_manifests != self.artifacts:
            raise MssqlR1V3ContractError("mutation plan differs from sealed artifact manifests")
        self._validate_plan_and_artifacts()
        if not isinstance(self.rendered_bundle, MssqlR1RenderedMutationBundleV1):
            raise MssqlR1V3ContractError("effect request requires a rendered mutation bundle")
        self.rendered_bundle.validate_retained_for_plan(self.mutation_plan)
        expected_generation_type = (
            MssqlBatchGenerationTransitionV3
            if self.identity.source_mode is SourceMode.BATCH_FULL_REFRESH
            else MssqlXminGenerationTransitionV3
        )
        if not isinstance(self.generation, expected_generation_type):
            raise MssqlR1V3ContractError("effect generation contract differs from source mode")
        plan_coordinates = (
            self.mutation_plan.expected_writer_generation,
            self.mutation_plan.candidate_writer_generation,
            self.mutation_plan.expected_head_revision,
            self.mutation_plan.candidate_head_revision,
        )
        generation_coordinates = (
            self.generation.expected_writer_generation,
            self.generation.candidate_writer_generation,
            self.generation.expected_head_revision,
            self.generation.candidate_head_revision,
        )
        if plan_coordinates != generation_coordinates:
            raise MssqlR1V3ContractError("effect generation coordinates differ from mutation plan")
        if (
            self.generation.effect_key != self.identity.effect_key
            or self.generation.source_snapshot_digest != self.source_snapshot_digest
            or self.generation.artifact_set_digest != self.artifact_set_digest
            or self.generation.mutation_plan_digest != self.mutation_plan.digest
        ):
            raise MssqlR1V3ContractError("effect generation authority differs from sealed request")

    def _validate_plan_and_artifacts(self) -> None:
        plan = self.mutation_plan
        if any(
            item.schema_digest != self.source_schema_digest or item.type_policy_digest != plan.type_policy_digest
            for item in self.artifacts
        ):
            raise MssqlR1V3ContractError("artifact schema/type policy differs from sealed request")
        business = self.artifacts[0].ordered_business_columns
        if plan.ordered_target_columns != tuple(item.target_column for item in business):
            raise MssqlR1V3ContractError("mutation plan target columns differ from stage mapping")
        if plan.target_identity.target_contract_revision < 1:
            raise MssqlR1V3ContractError("mutation plan target identity is invalid")
        if isinstance(plan, R1XminMutationPlanV1) and isinstance(self.generation, MssqlXminGenerationTransitionV3):
            keys = tuple(item for item in business if item.is_business_key)
            if plan.complete_keys_manifest.ordered_business_columns != keys:
                raise MssqlR1V3ContractError("XMin complete-key mapping differs from delta key")
            if plan.delta_manifest.type_policy_digest != plan.complete_keys_manifest.type_policy_digest:
                raise MssqlR1V3ContractError("XMin stage manifests use different type policy")
            if self.generation.complete_key_count != plan.complete_keys_manifest.observed_row_count:
                raise MssqlR1V3ContractError("XMin complete-key count differs from sealed manifest")
            baseline = self.generation.expected_writer_generation is None or (
                self.generation.candidate_writer_generation != self.generation.expected_writer_generation
            )
            if baseline != (plan.previous_checkpoint_revision is None):
                raise MssqlR1V3ContractError("XMin checkpoint predecessor differs from generation transition")
        elif (
            isinstance(plan, R1BatchMutationPlanV1)
            and isinstance(self.generation, MssqlBatchGenerationTransitionV3)
            and self.generation.payload_row_count != plan.stage_manifest.observed_row_count
        ):
            raise MssqlR1V3ContractError("Batch payload count differs from sealed manifest")

    @property
    def artifact_set_digest(self) -> bytes:
        return sealed_stage_set_digest(self.artifacts)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _EFFECT_REQUEST_DOMAIN,
            (
                self.contract.canonical_bytes,
                self.identity.canonical_bytes,
                self.registration_id,
                self.registration_payload_digest,
                self.registration_payload_bytes,
                self.verification_policy_digest,
                self.admitted_at_server_time,
                self.source_snapshot_digest,
                self.source_schema_digest,
                tuple(item.canonical_bytes for item in self.artifacts),
                self.mutation_plan.canonical_bytes,
                self.rendered_bundle.canonical_bytes,
                self.generation.canonical_bytes,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @property
    def authority_set(self) -> MssqlGenerationAuthoritySetV2:
        return self.generation.authority_set

    @property
    def source_authority_sha256(self) -> bytes:
        return MssqlTargetRegistrationPayloadV1.from_canonical_bytes(
            self.registration_payload_bytes
        ).route_source_authority_sha256

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1EffectRequestV3:
        """Reconstruct a sealed request solely from retained canonical bytes."""
        values = decode_canonical_bytes(payload, _EFFECT_REQUEST_DOMAIN, field_count=13)
        contract = MssqlR1EffectContractV3.from_canonical_bytes(expect_bytes(values[0], "contract"))
        identity = MssqlR1EffectIdentityV3.from_canonical_bytes(expect_bytes(values[1], "identity"))
        artifact_bytes = expect_tuple(values[9], "artifacts")
        artifacts = tuple(
            R1SealedStageManifestV1.from_canonical_bytes(expect_bytes(item, "artifact")) for item in artifact_bytes
        )
        plan = (
            R1BatchMutationPlanV1.from_canonical_bytes(expect_bytes(values[10], "mutation_plan"))
            if identity.source_mode is SourceMode.BATCH_FULL_REFRESH
            else R1XminMutationPlanV1.from_canonical_bytes(expect_bytes(values[10], "mutation_plan"))
        )
        generation = (
            MssqlBatchGenerationTransitionV3.from_canonical_bytes(expect_bytes(values[12], "generation"))
            if identity.source_mode is SourceMode.BATCH_FULL_REFRESH
            else MssqlXminGenerationTransitionV3.from_canonical_bytes(expect_bytes(values[12], "generation"))
        )
        return MssqlR1EffectRequestV3(
            identity,
            expect_uuid(values[2], "registration_id"),
            expect_bytes(values[3], "registration_payload_digest"),
            expect_bytes(values[4], "registration_payload_bytes"),
            expect_bytes(values[5], "verification_policy_digest"),
            parse_canonical_utc_text(values[6], "admitted_at_server_time"),
            expect_bytes(values[7], "source_snapshot_digest"),
            expect_bytes(values[8], "source_schema_digest"),
            artifacts,
            plan,
            MssqlR1RenderedMutationBundleV1.from_canonical_bytes(expect_bytes(values[11], "rendered_bundle")),
            generation,
            contract,
        )


@dataclass(frozen=True, slots=True)
class MssqlR1EffectAttemptEnvelopeV3:
    """Server-authorized retry ownership around unchanged sealed semantic bytes."""

    request: MssqlR1EffectRequestV3
    operation_epoch: int
    operation_projection_revision: int
    owner_id_digest: bytes
    server_lease_expires_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.request, MssqlR1EffectRequestV3):
            raise MssqlR1V3ContractError("attempt envelope requires an exact sealed V3 request")
        require_positive(self.operation_epoch, "operation_epoch")
        require_positive(self.operation_projection_revision, "operation_projection_revision")
        require_digest(self.owner_id_digest, "owner_id_digest")
        canonical_utc_text(self.server_lease_expires_at, "server_lease_expires_at")
        if self.server_lease_expires_at <= self.request.admitted_at_server_time:
            raise MssqlR1V3ContractError("attempt lease must expire after target admission")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _ATTEMPT_ENVELOPE_DOMAIN,
            (
                self.request.canonical_bytes,
                self.operation_epoch,
                self.operation_projection_revision,
                self.owner_id_digest,
                self.server_lease_expires_at,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1EffectAttemptEnvelopeV3:
        from dpone.contracts.mssql_r1_v3_identity import parse_canonical_utc_text

        values = decode_canonical_bytes(payload, _ATTEMPT_ENVELOPE_DOMAIN, field_count=5)
        return cls(
            MssqlR1EffectRequestV3.from_canonical_bytes(expect_bytes(values[0], "request")),
            require_positive(values[1], "operation_epoch"),
            require_positive(values[2], "operation_projection_revision"),
            require_digest(values[3], "owner_id_digest"),
            parse_canonical_utc_text(values[4], "server_lease_expires_at"),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1EffectSealIntentV3:
    identity: MssqlR1EffectIdentityV3
    registration_id: UUID
    registration_payload_digest: bytes
    registration_payload_bytes: bytes
    verification_policy_digest: bytes
    admitted_at_server_time: datetime
    source_snapshot_digest: bytes
    source_schema_digest: bytes
    artifacts: tuple[R1SealedStageManifestV1, ...]
    mutation_plan: MutationPlanV1
    generation: MssqlBatchGenerationTransitionV3 | MssqlXminGenerationTransitionV3
    contract: MssqlR1EffectContractV3 = MssqlR1EffectContractV3()


@dataclass(frozen=True, slots=True)
class MssqlR1SealOutcomeV3:
    request: MssqlR1EffectRequestV3
    initial_attempt: MssqlR1EffectAttemptEnvelopeV3

    def __post_init__(self) -> None:
        if not isinstance(self.request, MssqlR1EffectRequestV3) or self.initial_attempt.request != self.request:
            raise MssqlR1V3ContractError("seal outcome must bind one retained request and server-built attempt")


def validate_revoked_override_for_attempt(
    request: MssqlR1EffectRequestV3,
    attempt: MssqlR1EffectAttemptEnvelopeV3,
    override: MssqlRevokedRegistrationCompletionOverridePayloadV1,
) -> None:
    expected = (
        request.identity.target_binding_uuid,
        request.identity.effect_key,
        request.registration_id,
        request.registration_payload_digest,
        request.digest,
        request.artifact_set_digest,
        request.mutation_plan.digest,
        attempt.operation_epoch,
        attempt.operation_projection_revision,
        request.verification_policy_digest,
    )
    observed = (
        override.target_binding_uuid,
        override.effect_key,
        override.registration_id,
        override.registration_payload_digest,
        override.sealed_intent_digest,
        override.artifact_set_digest,
        override.mutation_plan_digest,
        override.expected_operation_epoch,
        override.expected_operation_projection_revision,
        override.expected_verification_policy_digest,
    )
    if observed != expected:
        raise MssqlR1V3ContractError("revoked override differs from sealed effect attempt")


__all__ = [
    "MssqlR1EffectAttemptEnvelopeV3",
    "MssqlR1EffectSealIntentV3",
    "MssqlR1SealOutcomeV3",
    "MssqlR1EffectRequestV3",
    "MutationPlanV1",
    "R1BatchMutationPlanV1",
    "R1XminMutationPlanV1",
    "validate_revoked_override_for_attempt",
]
