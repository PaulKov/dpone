"""Exact XMin mutation metrics retained by MSSQL authority V2 receipts."""

from __future__ import annotations

from dataclasses import dataclass, fields
from uuid import UUID

from dpone.contracts.mssql_target_authority_v2_identity import (
    MssqlMutationResultV2,
    MssqlQualityEvidenceV2,
    MssqlReceiptContractError,
    ReceiptKind,
    canonical_hash,
    require_count,
    require_digest,
    require_uuid,
)


@dataclass(frozen=True, slots=True)
class MssqlXminMutationResultV2(MssqlMutationResultV2):
    """Exact XMin target mutation counters retained for durable replay."""

    inserted_row_count: int
    updated_row_count: int
    hard_deleted_row_count: int
    unchanged_row_count: int
    delta_row_count: int

    def __post_init__(self) -> None:
        MssqlMutationResultV2.__post_init__(self)
        validate_xmin_mutation_counts(
            before_row_count=self.before_row_count,
            inserted_row_count=self.inserted_row_count,
            updated_row_count=self.updated_row_count,
            hard_deleted_row_count=self.hard_deleted_row_count,
            unchanged_row_count=self.unchanged_row_count,
            after_row_count=self.after_row_count,
            delta_row_count=self.delta_row_count,
        )


@dataclass(frozen=True, slots=True)
class MssqlXminEffectReceiptV2:
    """Canonical XMin authority body including exact durable result metrics."""

    effect_type: str
    state_key_digest: bytes
    previous_checkpoint_state_key_digest: bytes | None
    previous_checkpoint_writer_generation: int | None
    previous_checkpoint_revision: int | None
    source_snapshot_digest: bytes
    source_schema_digest: bytes
    scope_digest: bytes
    previous_xmin: int | None
    candidate_xmin: int
    visible_horizon_xmax: int
    xid_epoch: int
    candidate_checkpoint_state_key_digest: bytes
    candidate_checkpoint_writer_generation: int
    committed_checkpoint_revision: int
    delta_manifest_digest: bytes
    complete_key_manifest_digest: bytes
    frozen_state_payload: bytes
    frozen_state_digest: bytes
    mutation_plan_digest: bytes
    before_row_count: int
    inserted_row_count: int
    updated_row_count: int
    hard_deleted_row_count: int
    unchanged_row_count: int
    after_row_count: int
    delta_row_count: int
    payload_bytes: int
    quality_evidence: bytes
    generation_authority_id: UUID | None = None

    def __post_init__(self) -> None:
        if self.effect_type not in {"baseline", "incremental", "no_effect"}:
            raise MssqlReceiptContractError("XMin effect type is unsupported")
        _validate_receipt_authority(self)
        validate_xmin_mutation_counts(
            before_row_count=self.before_row_count,
            inserted_row_count=self.inserted_row_count,
            updated_row_count=self.updated_row_count,
            hard_deleted_row_count=self.hard_deleted_row_count,
            unchanged_row_count=self.unchanged_row_count,
            after_row_count=self.after_row_count,
            delta_row_count=self.delta_row_count,
            effect_type=self.effect_type,
        )

    @property
    def receipt_kind(self) -> ReceiptKind:
        return ReceiptKind.XMIN

    @property
    def digest(self) -> bytes:
        return canonical_hash(
            b"dpone-r1-xmin-receipt-body-v1",
            tuple(getattr(self, item.name) for item in fields(self)) + (self.receipt_kind,),
        )

    @property
    def quality_evidence_digest(self) -> bytes:
        return MssqlQualityEvidenceV2(self.quality_evidence).digest


@dataclass(frozen=True, slots=True)
class MssqlXminEffectDraftV2:
    """Immutable source/checkpoint authority completed by target mutation metrics."""

    effect_type: str
    state_key_digest: bytes
    previous_checkpoint_state_key_digest: bytes | None
    previous_checkpoint_writer_generation: int | None
    previous_checkpoint_revision: int | None
    source_snapshot_digest: bytes
    source_schema_digest: bytes
    scope_digest: bytes
    previous_xmin: int | None
    candidate_xmin: int
    visible_horizon_xmax: int
    xid_epoch: int
    candidate_checkpoint_state_key_digest: bytes
    candidate_checkpoint_writer_generation: int
    committed_checkpoint_revision: int
    delta_manifest_digest: bytes
    complete_key_manifest_digest: bytes
    frozen_state_payload: bytes
    frozen_state_digest: bytes
    mutation_plan_digest: bytes
    generation_authority_id: UUID | None = None

    def build_mutation_result(
        self,
        *,
        before_row_count: int,
        after_row_count: int,
        payload_bytes: int,
        inserted_row_count: int,
        updated_row_count: int,
        hard_deleted_row_count: int,
        unchanged_row_count: int,
        delta_row_count: int,
    ) -> MssqlXminMutationResultV2:
        """Create the only mutation result accepted by this typed draft."""

        return MssqlXminMutationResultV2(
            before_row_count=before_row_count,
            after_row_count=after_row_count,
            payload_bytes=payload_bytes,
            inserted_row_count=inserted_row_count,
            updated_row_count=updated_row_count,
            hard_deleted_row_count=hard_deleted_row_count,
            unchanged_row_count=unchanged_row_count,
            delta_row_count=delta_row_count,
        )

    def build_body(
        self,
        mutation: MssqlMutationResultV2,
        quality: MssqlQualityEvidenceV2,
    ) -> MssqlXminEffectReceiptV2:
        """Complete the durable body, rejecting generic process-local counters."""

        if not isinstance(mutation, MssqlXminMutationResultV2):
            raise MssqlReceiptContractError("XMin receipt requires exact mutation counters")
        return MssqlXminEffectReceiptV2(
            **{item.name: getattr(self, item.name) for item in fields(self)},
            before_row_count=mutation.before_row_count,
            inserted_row_count=mutation.inserted_row_count,
            updated_row_count=mutation.updated_row_count,
            hard_deleted_row_count=mutation.hard_deleted_row_count,
            unchanged_row_count=mutation.unchanged_row_count,
            after_row_count=mutation.after_row_count,
            delta_row_count=mutation.delta_row_count,
            payload_bytes=mutation.payload_bytes,
            quality_evidence=quality.payload,
        )


def validate_xmin_mutation_counts(
    *,
    before_row_count: int,
    inserted_row_count: int,
    updated_row_count: int,
    hard_deleted_row_count: int,
    unchanged_row_count: int,
    after_row_count: int,
    delta_row_count: int,
    effect_type: str | None = None,
) -> None:
    """Enforce SQL-bigint bounds, target algebra, and optional effect identity."""

    counts = {
        "before_row_count": before_row_count,
        "inserted_row_count": inserted_row_count,
        "updated_row_count": updated_row_count,
        "hard_deleted_row_count": hard_deleted_row_count,
        "unchanged_row_count": unchanged_row_count,
        "after_row_count": after_row_count,
        "delta_row_count": delta_row_count,
    }
    for name, value in counts.items():
        require_count(value, name)
    valid = (
        before_row_count == hard_deleted_row_count + updated_row_count + unchanged_row_count
        and after_row_count == inserted_row_count + updated_row_count + unchanged_row_count
        and after_row_count == before_row_count - hard_deleted_row_count + inserted_row_count
        and inserted_row_count + updated_row_count <= delta_row_count
    )
    no_mutations = inserted_row_count == updated_row_count == hard_deleted_row_count == 0
    if effect_type is not None:
        valid = valid and (effect_type == "no_effect") == no_mutations
    if not valid:
        raise MssqlReceiptContractError("XMin mutation counter invariant or no_effect classification failed")


def _validate_receipt_authority(body: MssqlXminEffectReceiptV2) -> None:
    for item in fields(body):
        value = getattr(body, item.name)
        if item.name.endswith("digest") and value is not None:
            require_digest(value, item.name)
    for name in (
        "candidate_xmin",
        "visible_horizon_xmax",
        "xid_epoch",
        "candidate_checkpoint_writer_generation",
        "committed_checkpoint_revision",
        "payload_bytes",
    ):
        require_count(getattr(body, name), name)
    if body.previous_xmin is not None:
        require_count(body.previous_xmin, "previous_xmin")
    if body.generation_authority_id is not None:
        require_uuid(body.generation_authority_id, "generation_authority_id")
    previous = (
        body.previous_checkpoint_state_key_digest,
        body.previous_checkpoint_writer_generation,
        body.previous_checkpoint_revision,
    )
    if any(value is not None for value in previous) and not all(value is not None for value in previous):
        raise MssqlReceiptContractError("previous checkpoint pointer must be complete")


__all__ = [
    "MssqlXminEffectDraftV2",
    "MssqlXminEffectReceiptV2",
    "MssqlXminMutationResultV2",
    "validate_xmin_mutation_counts",
]
