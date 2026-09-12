"""Operation, recovery-proof, and receipt-draft models for MSSQL authority V2."""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_target_authority_v2_identity import (
    MssqlArtifactAuthorityV2,
    MssqlCheckpointPointerV2,
    MssqlGenerationAuthorityClaimV1,
    MssqlMutationResultV2,
    MssqlQualityEvidenceV2,
    MssqlReceiptContractError,
    MssqlSealedIntentV2,
    MssqlTargetHeadV2,
    MssqlTargetIdentityV2,
    MssqlXminCheckpointTransitionV2,
    ReceiptKind,
    WriterMode,
    require_digest,
    require_positive,
)
from dpone.contracts.mssql_target_authority_v2_receipts import (
    MssqlBatchEffectReceiptV2,
    MssqlEffectReceiptHeaderV2,
    MssqlEffectReceiptV2,
    MssqlXminEffectDraftV2,
    build_receipt,
    checkpoint_pointer_from_receipt,
)


class ReceiptProofOutcome(StrEnum):
    COMMITTED = "committed"
    KNOWN_NOT_COMMITTED = "known_not_committed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class MssqlTargetAdmissionV2:
    head: MssqlTargetHeadV2 | None
    operation_epoch: int
    sealed_intent: MssqlSealedIntentV2
    replay_receipt: MssqlEffectReceiptV2 | None = None

    def __post_init__(self) -> None:
        require_positive(self.operation_epoch, "operation_epoch")
        if self.replay_receipt is not None and self.replay_receipt.header.effect_key != self.sealed_intent.effect_key:
            raise MssqlReceiptContractError("replay receipt differs from sealed effect")


@dataclass(frozen=True, slots=True)
class MssqlReceiptProofRequestV2:
    effect_key: bytes
    target_binding_uuid: UUID
    operation_key: bytes
    contract_version: str
    receipt_header: MssqlEffectReceiptHeaderV2
    expected_target_identity: MssqlTargetIdentityV2
    target_identity: MssqlTargetIdentityV2
    expected_head: MssqlTargetHeadV2 | None
    expected_checkpoint: MssqlCheckpointPointerV2 | None
    candidate_checkpoint: MssqlCheckpointPointerV2 | None
    sealed_intent: MssqlSealedIntentV2
    artifacts: tuple[MssqlArtifactAuthorityV2, ...]
    expected_operation_epoch: int
    max_descendant_proof_receipts: int
    generation_authority: MssqlGenerationAuthorityClaimV1 | None = None

    def __post_init__(self) -> None:
        require_digest(self.effect_key, "effect_key")
        require_digest(self.operation_key, "operation_key")
        require_positive(self.expected_operation_epoch, "expected_operation_epoch")
        if (
            self.receipt_header.effect_key != self.effect_key
            or self.receipt_header.operation_key != self.operation_key
            or self.receipt_header.contract_version != self.contract_version
            or self.receipt_header.operation_epoch != self.expected_operation_epoch
        ):
            raise MssqlReceiptContractError("proof receipt header conflicts with operation authority")
        if not isinstance(self.target_binding_uuid, UUID):
            raise MssqlReceiptContractError("target_binding_uuid must be a UUID")
        if self.target_binding_uuid != self.target_identity.target_binding_uuid:
            raise MssqlReceiptContractError("proof target identity conflicts with binding")
        if self.target_identity != self.receipt_header.target_identity:
            raise MssqlReceiptContractError("proof candidate target differs from receipt header")
        if self.target_binding_uuid != self.expected_target_identity.target_binding_uuid:
            raise MssqlReceiptContractError("proof expected target identity conflicts with binding")
        if self.effect_key != self.sealed_intent.effect_key:
            raise MssqlReceiptContractError("proof sealed intent conflicts with effect")
        sealed_fields = (
            self.sealed_intent.source_snapshot_authority,
            self.sealed_intent.source_snapshot_digest,
            self.sealed_intent.source_schema_digest,
            self.sealed_intent.artifact_set_manifest,
            self.sealed_intent.artifact_manifest_digest,
            self.sealed_intent.mutation_plan_digest,
        )
        if any(value is None for value in sealed_fields):
            raise MssqlReceiptContractError("proof requires the complete durable sealed intent")
        if self.artifacts != self.sealed_intent.artifacts or not self.artifacts:
            raise MssqlReceiptContractError("proof requires the exact sealed artifact set")
        if self.expected_head is not None:
            if self.expected_head.target_binding_uuid != self.target_binding_uuid:
                raise MssqlReceiptContractError("proof expected head conflicts with binding")
            if self.expected_head.checkpoint != self.expected_checkpoint:
                raise MssqlReceiptContractError("proof expected checkpoint conflicts with head")
            if (
                self.expected_head.recovery_domain_uuid != self.expected_target_identity.recovery_domain_uuid
                or self.expected_head.recovery_domain_epoch != self.expected_target_identity.recovery_domain_epoch
            ):
                raise MssqlReceiptContractError("proof expected recovery domain conflicts with target")
        if self.generation_authority is not None and self.generation_authority.effect_key != self.effect_key:
            raise MssqlReceiptContractError("proof generation authority conflicts with effect")
        if not 1 <= self.max_descendant_proof_receipts <= 100_000:
            raise MssqlReceiptContractError("descendant proof bound is invalid")

    @property
    def sealed_intent_digest(self) -> bytes:
        return self.sealed_intent.intent_digest

    def matches_receipt(self, receipt: MssqlEffectReceiptV2) -> bool:
        """Reject a receipt that does not bind the requested immutable authority."""

        rebuilt = build_receipt(receipt.header, receipt.body)
        header = receipt.header
        return (
            receipt == rebuilt
            and header == self.receipt_header
            and header.intent_digest == self.sealed_intent.intent_digest
            and header.target_binding_uuid == self.target_binding_uuid
            and checkpoint_pointer_from_receipt(receipt) == self.candidate_checkpoint
            and getattr(receipt.body, "generation_authority_id", None)
            == (None if self.generation_authority is None else self.generation_authority.authority_id)
        )


@dataclass(frozen=True, slots=True)
class MssqlReceiptProofV2:
    outcome: ReceiptProofOutcome
    receipt: MssqlEffectReceiptV2 | None = None
    retry_operation_epoch: int | None = None
    recovery_code: str | None = None

    def __post_init__(self) -> None:
        if self.outcome is ReceiptProofOutcome.COMMITTED:
            if self.receipt is None or self.retry_operation_epoch is not None or self.recovery_code is not None:
                raise MssqlReceiptContractError("committed proof requires an exact receipt only")
        if self.outcome is ReceiptProofOutcome.KNOWN_NOT_COMMITTED:
            if self.receipt is not None or self.retry_operation_epoch is None or self.recovery_code is not None:
                raise MssqlReceiptContractError("known-not-committed proof requires a retry epoch only")
            require_positive(self.retry_operation_epoch, "retry_operation_epoch")
        if self.outcome is ReceiptProofOutcome.UNKNOWN:
            if self.receipt is not None or self.retry_operation_epoch is not None or not self.recovery_code:
                raise MssqlReceiptContractError("unknown proof requires a recovery code only")


@dataclass(frozen=True, slots=True)
class MssqlBatchEffectDraftV2:
    source_snapshot_digest: bytes
    source_schema_digest: bytes
    source_payload_digest: bytes
    staging_intent_digest: bytes
    mutation_plan_digest: bytes
    effect_type: str = "full_refresh"
    generation_authority_id: UUID | None = None
    previous_checkpoint_state_key_digest: bytes | None = None
    previous_checkpoint_writer_generation: int | None = None
    previous_checkpoint_revision: int | None = None
    candidate_checkpoint_state_key_digest: bytes | None = None
    candidate_checkpoint_writer_generation: int | None = None
    candidate_checkpoint_revision: int | None = None

    def build_body(
        self,
        mutation: MssqlMutationResultV2,
        quality: MssqlQualityEvidenceV2,
    ) -> MssqlBatchEffectReceiptV2:
        return MssqlBatchEffectReceiptV2(
            **{item.name: getattr(self, item.name) for item in fields(self)},
            before_row_count=mutation.before_row_count,
            after_row_count=mutation.after_row_count,
            payload_bytes=mutation.payload_bytes,
            quality_evidence=quality.payload,
        )


ReceiptDraftV2 = MssqlBatchEffectDraftV2 | MssqlXminEffectDraftV2


@dataclass(frozen=True, slots=True)
class MssqlEffectRequestV2:
    header: MssqlEffectReceiptHeaderV2
    sealed_intent: MssqlSealedIntentV2
    receipt_draft: ReceiptDraftV2
    checkpoint_transition: MssqlXminCheckpointTransitionV2 | None = None
    generation_authority: MssqlGenerationAuthorityClaimV1 | None = None
    max_descendant_proof_receipts: int = 100_000
    expected_target_identity: MssqlTargetIdentityV2 | None = None

    def __post_init__(self) -> None:
        if self.header.effect_key != self.sealed_intent.effect_key:
            raise MssqlReceiptContractError("sealed intent effect key conflicts with receipt header")
        if self.header.intent_digest != self.sealed_intent.intent_digest:
            raise MssqlReceiptContractError("sealed intent digest conflicts with receipt header")
        if isinstance(self.receipt_draft, MssqlBatchEffectDraftV2):
            if self.receipt_draft.staging_intent_digest != self.sealed_intent.intent_digest:
                raise MssqlReceiptContractError("Batch draft differs from sealed intent")
        if self.generation_authority is not None and self.generation_authority.effect_key != self.header.effect_key:
            raise MssqlReceiptContractError("generation authority differs from sealed effect")
        if not 1 <= self.max_descendant_proof_receipts <= 100_000:
            raise MssqlReceiptContractError("descendant proof bound is invalid")
        expected_kind = (
            ReceiptKind.BATCH if isinstance(self.receipt_draft, MssqlBatchEffectDraftV2) else ReceiptKind.XMIN
        )
        if self.header.receipt_kind is not expected_kind:
            raise MssqlReceiptContractError("receipt draft kind differs from header")
        if (self.checkpoint_transition is None) != (expected_kind is ReceiptKind.BATCH):
            raise MssqlReceiptContractError("only XMin effects require a checkpoint transition")
        _validate_batch_generation_transition(self)
        _validate_generation_authority(self)
        _validate_sealed_authority(self)
        recovery_changes = self.header.expected_recovery_domain_uuid is not None and (
            self.header.expected_recovery_domain_uuid != self.header.candidate_recovery_domain_uuid
            or self.header.expected_recovery_domain_epoch != self.header.candidate_recovery_domain_epoch
        )
        if recovery_changes and self.expected_target_identity is None:
            raise MssqlReceiptContractError("recovery rollover requires the complete expected target identity")

    def build_receipt(
        self,
        mutation: MssqlMutationResultV2,
        quality: MssqlQualityEvidenceV2,
    ) -> MssqlEffectReceiptV2:
        return build_receipt(self.header, self.receipt_draft.build_body(mutation, quality))

    def receipt_proof_request(self, *, expected_head: MssqlTargetHeadV2 | None) -> MssqlReceiptProofRequestV2:
        if expected_head is None and self.header.expected_writer_generation is not None:
            if self.header.previous_receipt_id is None or self.header.expected_recovery_domain_uuid is None:
                raise MssqlReceiptContractError("non-initial proof requires a complete expected head")
            expected_head = MssqlTargetHeadV2(
                target_binding_uuid=self.header.target_binding_uuid,
                writer_mode=WriterMode(self.header.writer_mode.value),
                writer_generation=self.header.expected_writer_generation,
                head_revision=self.header.previous_head_revision or 0,
                last_receipt_id=self.header.previous_receipt_id,
                recovery_domain_epoch=self.header.expected_recovery_domain_epoch or 0,
                recovery_domain_uuid=self.header.expected_recovery_domain_uuid,
                checkpoint=_previous_checkpoint(self.receipt_draft),
            )
        return MssqlReceiptProofRequestV2(
            effect_key=self.header.effect_key,
            target_binding_uuid=self.header.target_binding_uuid,
            operation_key=self.header.operation_key,
            contract_version=self.header.contract_version,
            receipt_header=self.header,
            expected_target_identity=self.expected_target_identity or self.header.target_identity,
            target_identity=self.header.target_identity,
            expected_head=expected_head,
            expected_checkpoint=_previous_checkpoint(self.receipt_draft),
            candidate_checkpoint=_candidate_checkpoint(self.receipt_draft),
            sealed_intent=self.sealed_intent,
            artifacts=self.sealed_intent.artifacts,
            expected_operation_epoch=self.header.operation_epoch,
            max_descendant_proof_receipts=self.max_descendant_proof_receipts,
            generation_authority=self.generation_authority,
        )

    def matches_receipt(self, receipt: MssqlEffectReceiptV2) -> bool:
        """Validate a transaction-local replay without trusting effect_key alone."""

        if receipt != build_receipt(receipt.header, receipt.body):
            return False
        if any(
            getattr(receipt.header, item.name) != getattr(self.header, item.name)
            for item in fields(self.header)
            if item.name != "operation_epoch"
        ):
            return False
        return all(
            getattr(receipt.body, item.name, object()) == getattr(self.receipt_draft, item.name)
            for item in fields(self.receipt_draft)
        )

    def for_retry(self, operation_epoch: int) -> MssqlEffectRequestV2:
        require_positive(operation_epoch, "operation_epoch")
        return replace(self, header=replace(self.header, operation_epoch=operation_epoch))


def _validate_sealed_authority(request: MssqlEffectRequestV2) -> None:
    intent = request.sealed_intent
    draft = request.receipt_draft
    expected = (
        draft.source_snapshot_digest,
        draft.source_schema_digest,
        getattr(draft, "staging_intent_digest", intent.artifact_set_digest),
        draft.mutation_plan_digest,
    )
    actual = (
        intent.source_snapshot_digest,
        intent.source_schema_digest,
        intent.artifact_manifest_digest,
        intent.mutation_plan_digest,
    )
    if any(value is not None for value in actual) and actual != expected:
        raise MssqlReceiptContractError("sealed intent differs from receipt draft authority")


def _validate_generation_authority(request: MssqlEffectRequestV2) -> None:
    claim = request.generation_authority
    authority_id = getattr(request.receipt_draft, "generation_authority_id", None)
    if (claim is None) != (authority_id is None):
        raise MssqlReceiptContractError("generation authority identity is incomplete")
    if claim is not None and authority_id != claim.authority_id:
        raise MssqlReceiptContractError("generation authority identity differs from receipt draft")
    generation_changes = request.header.expected_writer_generation != request.header.candidate_writer_generation
    required_kind: str | None = None
    if isinstance(request.receipt_draft, MssqlBatchEffectDraftV2) and request.sealed_intent.row_count == 0:
        required_kind = "empty_refresh"
    elif request.header.expected_writer_generation is None:
        required_kind = "initial_cutover"
    elif generation_changes:
        required_kind = "rebaseline"
    if required_kind is not None and (claim is None or claim.authority_kind != required_kind):
        raise MssqlReceiptContractError(f"generation authority must be {required_kind}")


def _validate_batch_generation_transition(request: MssqlEffectRequestV2) -> None:
    """Require every newly prepared Batch snapshot to own a new content generation."""

    if not isinstance(request.receipt_draft, MssqlBatchEffectDraftV2):
        return
    expected = request.header.expected_writer_generation
    candidate = request.header.candidate_writer_generation
    if expected is not None and candidate != expected + 1:
        raise MssqlReceiptContractError("Batch full refresh must advance an adjacent writer generation")


def _previous_checkpoint(draft: ReceiptDraftV2) -> MssqlCheckpointPointerV2 | None:
    values = (
        draft.previous_checkpoint_state_key_digest,
        draft.previous_checkpoint_writer_generation,
        draft.previous_checkpoint_revision,
    )
    if all(value is not None for value in values):
        return MssqlCheckpointPointerV2(values[0], values[1], values[2])  # type: ignore[arg-type]
    return None


def _candidate_checkpoint(draft: ReceiptDraftV2) -> MssqlCheckpointPointerV2 | None:
    if isinstance(draft, MssqlXminEffectDraftV2):
        return MssqlCheckpointPointerV2(
            draft.candidate_checkpoint_state_key_digest,
            draft.candidate_checkpoint_writer_generation,
            draft.committed_checkpoint_revision,
        )
    values = (
        draft.candidate_checkpoint_state_key_digest,
        draft.candidate_checkpoint_writer_generation,
        draft.candidate_checkpoint_revision,
    )
    if all(value is not None for value in values):
        return MssqlCheckpointPointerV2(values[0], values[1], values[2])  # type: ignore[arg-type]
    return None


__all__ = [name for name in globals() if name.startswith("Mssql") or name in {"ReceiptDraftV2", "ReceiptProofOutcome"}]
