"""Ephemeral same-transaction candidate proof for MSSQL R1 V3 effects."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_effect import MssqlR1EffectAttemptEnvelopeV3
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    canonical_utc_text,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    parse_canonical_utc_text,
    require_digest,
    require_positive,
    require_uuid,
)
from dpone.contracts.mssql_r1_v3_plan import R1XminMutationPlanV1
from dpone.contracts.mssql_r1_v3_receipt import (
    MssqlR1EffectReceiptV3,
    MssqlXminEffectReceiptBodyV3,
)
from dpone.contracts.mssql_r1_v3_receipt_projection import receipt_stage_manifests, validate_receipt_for_request_v3
from dpone.contracts.mssql_r1_v3_stage_consumption import (
    CheckpointReplayObservationV3,
    MssqlConsumedSealedStageSetV3,
)
from dpone.contracts.mssql_r1_v3_transaction import MssqlR1TransactionBindingV3
from dpone.contracts.mssql_r1_v3_transaction_authority import MssqlConsumedGenerationAuthoritySetV3
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode

_OPERATION_DOMAIN = b"dpone-r1-candidate-operation-observation-v3\0"
_HEAD_DOMAIN = b"dpone-r1-candidate-writer-head-observation-v3\0"
_PROOF_DOMAIN = b"dpone-r1-candidate-effect-proof-v3\0"


class MssqlCandidateOperationStateV3(StrEnum):
    COMMITTED = "committed"


@dataclass(frozen=True, slots=True)
class MssqlCandidateOperationObservationV3:
    operation_key: bytes
    effect_key: bytes
    operation_epoch: int
    operation_projection_revision: int
    owner_id_digest: bytes
    server_lease_expires_at: datetime
    sealed_request_digest: bytes
    committed_receipt_id: UUID
    committed_receipt_digest: bytes
    state: MssqlCandidateOperationStateV3 = MssqlCandidateOperationStateV3.COMMITTED

    def __post_init__(self) -> None:
        for name in (
            "operation_key",
            "effect_key",
            "owner_id_digest",
            "sealed_request_digest",
            "committed_receipt_digest",
        ):
            require_digest(getattr(self, name), name)
        require_positive(self.operation_epoch, "operation_epoch")
        require_positive(self.operation_projection_revision, "operation_projection_revision")
        canonical_utc_text(self.server_lease_expires_at, "server_lease_expires_at")
        require_uuid(self.committed_receipt_id, "committed_receipt_id")
        if self.state is not MssqlCandidateOperationStateV3.COMMITTED:
            raise MssqlR1V3ContractError("candidate operation must be the exact COMMITTED projection")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_OPERATION_DOMAIN, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlCandidateOperationObservationV3:
        values = list(decode_canonical_bytes(payload, _OPERATION_DOMAIN, field_count=10))
        values[5] = parse_canonical_utc_text(values[5], "server_lease_expires_at")
        values[9] = expect_enum(MssqlCandidateOperationStateV3, values[9], "candidate operation state")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlCandidateWriterHeadObservationV3:
    target_generation: int
    row_hash_generation: int
    head_revision: int
    last_receipt_id: UUID
    last_receipt_digest: bytes
    active_effect_key: bytes
    active_operation_epoch: int
    active_operation_projection_revision: int

    def __post_init__(self) -> None:
        require_positive(self.target_generation, "target_generation")
        require_positive(self.row_hash_generation, "row_hash_generation")
        require_positive(self.head_revision, "head_revision")
        require_uuid(self.last_receipt_id, "last_receipt_id")
        require_digest(self.last_receipt_digest, "last_receipt_digest")
        require_digest(self.active_effect_key, "active_effect_key")
        require_positive(self.active_operation_epoch, "active_operation_epoch")
        require_positive(self.active_operation_projection_revision, "active_operation_projection_revision")
        if self.target_generation != self.row_hash_generation:
            raise MssqlR1V3ContractError("candidate target and row-hash generations differ")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_HEAD_DOMAIN, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlCandidateWriterHeadObservationV3:
        return cls(*decode_canonical_bytes(payload, _HEAD_DOMAIN, field_count=8))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlCandidateEffectProofV3:
    transaction_binding: MssqlR1TransactionBindingV3
    sealed_request_digest: bytes
    attempt_payload: bytes
    attempt_digest: bytes
    receipt_payload: bytes
    receipt_digest: bytes
    operation_observation: MssqlCandidateOperationObservationV3
    writer_head_observation: MssqlCandidateWriterHeadObservationV3
    target_generation: int
    row_hash_generation: int
    checkpoint_observation: CheckpointReplayObservationV3 | None
    consumed_artifact_set: MssqlConsumedSealedStageSetV3
    consumed_authority_set: MssqlConsumedGenerationAuthoritySetV3
    override_observation: None
    registration_id: UUID
    registration_payload_digest: bytes
    registered_physical_authority_digest: bytes
    schema_attestation_digest: bytes
    schema_projection_revision: int

    def __post_init__(self) -> None:
        self.transaction_binding.assert_active()
        require_digest(self.sealed_request_digest, "sealed_request_digest")
        attempt = MssqlR1EffectAttemptEnvelopeV3.from_canonical_bytes(self.attempt_payload)
        if attempt.digest != require_digest(self.attempt_digest, "attempt_digest") or (
            attempt.request.digest != self.sealed_request_digest
        ):
            raise MssqlR1V3ContractError("candidate proof attempt differs from exact sealed request")
        receipt = MssqlR1EffectReceiptV3.from_canonical_bytes(self.receipt_payload)
        if receipt.digest != require_digest(self.receipt_digest, "receipt_digest"):
            raise MssqlR1V3ContractError("candidate proof receipt digest differs from exact payload")
        require_positive(self.target_generation, "target_generation")
        require_positive(self.row_hash_generation, "row_hash_generation")
        require_uuid(self.registration_id, "registration_id")
        for name in (
            "registration_payload_digest",
            "registered_physical_authority_digest",
            "schema_attestation_digest",
        ):
            require_digest(getattr(self, name), name)
        require_positive(self.schema_projection_revision, "schema_projection_revision")
        if self.override_observation is not None:
            raise MssqlR1V3ContractError("revoked-registration override is absent in R1")
        header = receipt.header
        validate_receipt_for_request_v3(attempt.request, receipt)
        if (attempt.operation_epoch, attempt.operation_projection_revision) != (
            header.operation_epoch,
            header.operation_projection_revision,
        ):
            raise MssqlR1V3ContractError("candidate proof attempt differs from exact receipt epoch")
        operation = self.operation_observation
        head = self.writer_head_observation
        exact = (
            header.sealed_request_digest,
            header.operation_key,
            header.effect_key,
            header.operation_epoch,
            header.operation_projection_revision,
            header.receipt_id,
            receipt.digest,
            header.registration_id,
            header.registration_payload_digest,
            header.registered_physical_authority_digest,
        )
        observed = (
            self.sealed_request_digest,
            operation.operation_key,
            operation.effect_key,
            operation.operation_epoch,
            operation.operation_projection_revision,
            operation.committed_receipt_id,
            operation.committed_receipt_digest,
            self.registration_id,
            self.registration_payload_digest,
            self.registered_physical_authority_digest,
        )
        if observed != exact:
            raise MssqlR1V3ContractError("candidate proof differs from exact receipt/operation authority")
        if (operation.owner_id_digest, operation.server_lease_expires_at) != (
            attempt.owner_id_digest,
            attempt.server_lease_expires_at,
        ):
            raise MssqlR1V3ContractError("candidate operation owner/lease differs from exact attempt")
        if (
            head.target_generation,
            head.row_hash_generation,
            head.head_revision,
            head.last_receipt_id,
            head.last_receipt_digest,
            head.active_effect_key,
            head.active_operation_epoch,
            head.active_operation_projection_revision,
            self.row_hash_generation,
        ) != (
            self.target_generation,
            self.target_generation,
            header.candidate_head_revision,
            header.receipt_id,
            receipt.digest,
            header.effect_key,
            header.operation_epoch,
            header.operation_projection_revision,
            self.target_generation,
        ):
            raise MssqlR1V3ContractError("candidate proof writer-head/generation observation is inconsistent")
        if self.target_generation != header.candidate_writer_generation:
            raise MssqlR1V3ContractError("candidate proof generation differs from exact receipt")
        self._validate_consumption(receipt)
        if (header.receipt_kind is SourceMode.XMIN_CURRENT_STATE) != (self.checkpoint_observation is not None):
            raise MssqlR1V3ContractError("candidate proof checkpoint discriminator differs from receipt kind")
        if self.checkpoint_observation is not None and (
            self.checkpoint_observation.writer_generation,
            self.checkpoint_observation.checkpoint_revision,
            self.checkpoint_observation.checkpoint_payload,
            self.checkpoint_observation.consuming_receipt_id,
        ) != (
            self.target_generation,
            *_xmin_checkpoint(attempt, receipt),
            header.receipt_id,
        ):
            raise MssqlR1V3ContractError("candidate proof checkpoint differs from exact receipt/generation")

    def assert_for(
        self,
        transaction_binding: MssqlR1TransactionBindingV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
        receipt: MssqlR1EffectReceiptV3,
    ) -> None:
        transaction_binding.assert_active()
        if transaction_binding.canonical_bytes != self.transaction_binding.canonical_bytes:
            raise MssqlR1V3ContractError("candidate proof differs from trusted transaction binding")
        if (attempt.canonical_bytes, attempt.digest) != (self.attempt_payload, self.attempt_digest):
            raise MssqlR1V3ContractError("candidate proof differs from trusted attempt")
        if (receipt.canonical_bytes, receipt.digest) != (self.receipt_payload, self.receipt_digest):
            raise MssqlR1V3ContractError("candidate proof differs from trusted receipt")

    def _validate_consumption(self, receipt: MssqlR1EffectReceiptV3) -> None:
        binding = self.transaction_binding
        stages = self.consumed_artifact_set
        authorities = self.consumed_authority_set
        common = (binding.transaction_id, binding.session_identity_digest, receipt.header.receipt_id, receipt.digest)
        if (
            stages.transaction_id,
            stages.session_identity_digest,
            stages.consuming_receipt_id,
            stages.consuming_receipt_digest,
        ) != common or (
            authorities.values.transaction_id,
            authorities.values.session_identity_digest,
            authorities.consuming_receipt_id,
            authorities.consuming_receipt_digest,
        ) != common:
            raise MssqlR1V3ContractError("candidate proof consumption is not bound to this transaction/receipt")
        if (
            stages.effect_key,
            stages.sealed_request_digest,
            authorities.values.effect_key,
            authorities.values.sealed_request_digest,
        ) != (receipt.header.effect_key, self.sealed_request_digest) * 2:
            raise MssqlR1V3ContractError("candidate proof consumed resources differ from sealed effect")
        if authorities.authority_set.digest != receipt.header.authority_set_digest:
            raise MssqlR1V3ContractError("candidate proof consumed authority set differs from receipt")
        expected_manifests = receipt_stage_manifests(receipt)
        if (stages.manifest_payloads, stages.manifest_digests) != expected_manifests:
            raise MssqlR1V3ContractError("candidate proof consumed stage differs from receipt body")
        issuance = authorities.values
        if (
            receipt.header.generation_authority_issuance_id,
            receipt.header.generation_authority_issuance_payload_digest,
            receipt.header.generation_authority_verification_receipt_digest,
        ) != (
            issuance.issuance_id,
            issuance.issuance_payload_digest,
            issuance.verification_receipt_digest,
        ):
            raise MssqlR1V3ContractError("candidate proof receipt differs from consumed issuance")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _PROOF_DOMAIN,
            (
                self.transaction_binding.canonical_bytes,
                self.sealed_request_digest,
                self.attempt_payload,
                self.attempt_digest,
                self.receipt_payload,
                self.receipt_digest,
                self.operation_observation.canonical_bytes,
                self.writer_head_observation.canonical_bytes,
                self.target_generation,
                self.row_hash_generation,
                None if self.checkpoint_observation is None else self.checkpoint_observation.canonical_bytes,
                self.consumed_artifact_set.canonical_bytes,
                self.consumed_authority_set.canonical_bytes,
                None,
                self.registration_id,
                self.registration_payload_digest,
                self.registered_physical_authority_digest,
                self.schema_attestation_digest,
                self.schema_projection_revision,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlCandidateEffectProofV3:
        values = list(decode_canonical_bytes(payload, _PROOF_DOMAIN, field_count=19))
        values[0] = MssqlR1TransactionBindingV3.from_canonical_bytes(expect_bytes(values[0], "transaction_binding"))
        values[2] = expect_bytes(values[2], "attempt_payload")
        values[6] = MssqlCandidateOperationObservationV3.from_canonical_bytes(
            expect_bytes(values[6], "operation_observation")
        )
        values[7] = MssqlCandidateWriterHeadObservationV3.from_canonical_bytes(
            expect_bytes(values[7], "writer_head_observation")
        )
        values[10] = (
            None
            if values[10] is None
            else CheckpointReplayObservationV3.from_canonical_bytes(expect_bytes(values[10], "checkpoint"))
        )
        values[11] = MssqlConsumedSealedStageSetV3.from_canonical_bytes(
            expect_bytes(values[11], "consumed_artifact_set")
        )
        values[12] = MssqlConsumedGenerationAuthoritySetV3.from_canonical_bytes(
            expect_bytes(values[12], "consumed_authority_set")
        )
        return cls(*values)  # type: ignore[arg-type]


def _xmin_checkpoint(attempt: MssqlR1EffectAttemptEnvelopeV3, receipt: MssqlR1EffectReceiptV3) -> tuple[int, bytes]:
    if not isinstance(receipt.body, MssqlXminEffectReceiptBodyV3) or not isinstance(
        attempt.request.mutation_plan, R1XminMutationPlanV1
    ):
        raise MssqlR1V3ContractError("Batch candidate cannot contain a checkpoint")
    return attempt.request.mutation_plan.candidate_checkpoint_revision, receipt.body.candidate_checkpoint_payload


__all__ = [
    "MssqlCandidateEffectProofV3",
    "MssqlCandidateOperationObservationV3",
    "MssqlCandidateOperationStateV3",
    "MssqlCandidateWriterHeadObservationV3",
]
