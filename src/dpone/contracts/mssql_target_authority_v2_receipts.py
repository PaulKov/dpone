"""Typed immutable Batch/XMin receipts and chain proof for MSSQL authority V2."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Protocol
from uuid import UUID

from dpone.contracts.mssql_target_authority_v2_identity import (
    CONTRACT_VERSION,
    MAX_SQL_BIGINT,
    MssqlCheckpointPointerV2,
    MssqlQualityEvidenceV2,
    MssqlReceiptContractError,
    MssqlTargetHeadV2,
    MssqlTargetIdentityV2,
    ReceiptKind,
    SourceMode,
    WriterMode,
    build_effect_key,
    canonical_hash,
    deterministic_receipt_id,
    require_count,
    require_digest,
    require_positive,
    require_uuid,
)
from dpone.contracts.mssql_target_authority_v2_xmin_metrics import (
    MssqlXminEffectDraftV2 as MssqlXminEffectDraftV2,
)
from dpone.contracts.mssql_target_authority_v2_xmin_metrics import (
    MssqlXminEffectReceiptV2,
    MssqlXminMutationResultV2,
)


@dataclass(frozen=True, slots=True)
class MssqlEffectReceiptHeaderV2:
    receipt_kind: ReceiptKind
    operation_key: bytes
    effect_key: bytes
    target_identity: MssqlTargetIdentityV2
    writer_mode: WriterMode
    expected_writer_generation: int | None
    candidate_writer_generation: int
    previous_head_revision: int | None
    committed_head_revision: int
    operation_epoch: int
    previous_receipt_id: UUID | None
    previous_receipt_digest: bytes | None
    previous_recovery_identity_digest: bytes | None
    expected_recovery_domain_uuid: UUID | None
    expected_recovery_domain_epoch: int | None
    candidate_recovery_identity_digest: bytes
    candidate_recovery_domain_uuid: UUID
    candidate_recovery_domain_epoch: int
    route_identity_sha256: bytes
    source_authority_sha256: bytes
    intent_digest: bytes
    type_policy_digest: bytes
    hash_policy_digest: bytes
    quality_policy_digest: bytes
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        digest_names = (
            "operation_key",
            "effect_key",
            "candidate_recovery_identity_digest",
            "route_identity_sha256",
            "source_authority_sha256",
            "intent_digest",
            "type_policy_digest",
            "hash_policy_digest",
            "quality_policy_digest",
        )
        for name in digest_names:
            require_digest(getattr(self, name), name)
        require_positive(self.candidate_writer_generation, "candidate_writer_generation")
        require_positive(self.committed_head_revision, "committed_head_revision")
        require_positive(self.operation_epoch, "operation_epoch")
        require_positive(self.candidate_recovery_domain_epoch, "candidate_recovery_domain_epoch")
        require_uuid(self.candidate_recovery_domain_uuid, "candidate_recovery_domain_uuid")
        _validate_optional_recovery(self)
        _validate_transition(self)
        if (
            self.candidate_recovery_domain_uuid != self.target_identity.recovery_domain_uuid
            or self.candidate_recovery_domain_epoch != self.target_identity.recovery_domain_epoch
        ):
            raise MssqlReceiptContractError("candidate recovery domain differs from target identity")
        expected = build_effect_key(
            operation_key=self.operation_key,
            source_mode=SourceMode(self.writer_mode.value),
            target_binding_uuid=self.target_binding_uuid,
            contract_version=self.contract_version,
        )
        if self.effect_key != expected:
            raise MssqlReceiptContractError("effect key differs from stable operation identity")
        if (self.receipt_kind is ReceiptKind.BATCH) != (self.writer_mode is WriterMode.BATCH_FULL_REFRESH):
            raise MssqlReceiptContractError("receipt kind and writer mode disagree")

    @property
    def target_binding_uuid(self) -> UUID:
        return self.target_identity.target_binding_uuid


def _validate_optional_recovery(header: MssqlEffectReceiptHeaderV2) -> None:
    if (header.previous_receipt_id is None) != (header.previous_receipt_digest is None):
        raise MssqlReceiptContractError("receipt predecessor identity must be complete")
    if header.previous_recovery_identity_digest is not None:
        require_digest(header.previous_recovery_identity_digest, "previous_recovery_identity_digest")
    expected_absent = header.expected_recovery_domain_uuid is None and header.expected_recovery_domain_epoch is None
    if not expected_absent:
        require_uuid(header.expected_recovery_domain_uuid, "expected_recovery_domain_uuid")
        require_positive(header.expected_recovery_domain_epoch, "expected_recovery_domain_epoch")
    if header.expected_writer_generation is None:
        if not expected_absent or header.previous_recovery_identity_digest is not None:
            raise MssqlReceiptContractError("initial receipt cannot name previous recovery authority")
    elif expected_absent or header.previous_recovery_identity_digest is None:
        raise MssqlReceiptContractError("non-initial receipt requires previous recovery authority")


def _validate_transition(header: MssqlEffectReceiptHeaderV2) -> None:
    expected, previous = header.expected_writer_generation, header.previous_head_revision
    predecessor_absent = header.previous_receipt_id is None and header.previous_receipt_digest is None
    if expected is None:
        valid = (
            previous is None
            and predecessor_absent
            and (
                header.candidate_writer_generation,
                header.committed_head_revision,
            )
            == (1, 1)
        )
    else:
        require_positive(expected, "expected_writer_generation")
        if previous is None:
            valid = False
        else:
            require_positive(previous, "previous_head_revision")
            same = header.candidate_writer_generation == expected and header.committed_head_revision == previous + 1
            advanced = (
                expected < MAX_SQL_BIGINT
                and header.candidate_writer_generation == expected + 1
                and header.committed_head_revision == 1
            )
            valid = not predecessor_absent and same != advanced
    if not valid:
        raise MssqlReceiptContractError("receipt transition must be adjacent")


class _ReceiptBody(Protocol):
    receipt_kind: ReceiptKind

    @property
    def digest(self) -> bytes: ...


def _validate_digest_fields(value: Any) -> None:
    for field_info in fields(value):
        field_value = getattr(value, field_info.name)
        if field_info.name.endswith("digest") and field_value is not None:
            require_digest(field_value, field_info.name)


@dataclass(frozen=True, slots=True)
class MssqlBatchEffectReceiptV2:
    source_snapshot_digest: bytes
    source_schema_digest: bytes
    source_payload_digest: bytes
    staging_intent_digest: bytes
    mutation_plan_digest: bytes
    before_row_count: int
    after_row_count: int
    payload_bytes: int
    quality_evidence: bytes
    effect_type: str = "full_refresh"
    generation_authority_id: UUID | None = None
    previous_checkpoint_state_key_digest: bytes | None = None
    previous_checkpoint_writer_generation: int | None = None
    previous_checkpoint_revision: int | None = None
    candidate_checkpoint_state_key_digest: bytes | None = None
    candidate_checkpoint_writer_generation: int | None = None
    candidate_checkpoint_revision: int | None = None
    receipt_kind: ReceiptKind = ReceiptKind.BATCH

    def __post_init__(self) -> None:
        if self.effect_type != "full_refresh":
            raise MssqlReceiptContractError("Batch effect type must be full_refresh")
        _validate_digest_fields(self)
        for name in ("before_row_count", "after_row_count", "payload_bytes"):
            require_count(getattr(self, name), name)
        _validate_optional_checkpoint(self)
        if self.generation_authority_id is not None:
            require_uuid(self.generation_authority_id, "generation_authority_id")

    @property
    def digest(self) -> bytes:
        return canonical_hash(
            b"dpone-r1-batch-receipt-body-v1",
            tuple(getattr(self, item.name) for item in fields(self)),
        )

    @property
    def quality_evidence_digest(self) -> bytes:
        return MssqlQualityEvidenceV2(self.quality_evidence).digest


def _validate_optional_checkpoint(body: MssqlBatchEffectReceiptV2) -> None:
    previous = (
        body.previous_checkpoint_state_key_digest,
        body.previous_checkpoint_writer_generation,
        body.previous_checkpoint_revision,
    )
    candidate = (
        body.candidate_checkpoint_state_key_digest,
        body.candidate_checkpoint_writer_generation,
        body.candidate_checkpoint_revision,
    )
    for label, group in (("previous", previous), ("candidate", candidate)):
        if any(value is not None for value in group) and not all(value is not None for value in group):
            raise MssqlReceiptContractError(f"{label} checkpoint pointer must be complete")
        if all(value is not None for value in group):
            require_digest(group[0], f"{label}_checkpoint_state_key_digest")  # type: ignore[arg-type]
            require_positive(group[1], f"{label}_checkpoint_writer_generation")
            require_positive(group[2], f"{label}_checkpoint_revision")


ReceiptBodyV2 = MssqlBatchEffectReceiptV2 | MssqlXminEffectReceiptV2


@dataclass(frozen=True, slots=True)
class MssqlEffectReceiptV2:
    receipt_id: UUID
    header: MssqlEffectReceiptHeaderV2
    body: ReceiptBodyV2
    body_digest: bytes
    receipt_digest: bytes


def build_receipt(header: MssqlEffectReceiptHeaderV2, body: ReceiptBodyV2) -> MssqlEffectReceiptV2:
    if header.receipt_kind is not body.receipt_kind:
        raise MssqlReceiptContractError("receipt body kind differs from header")
    body_digest = body.digest
    receipt_digest = canonical_hash(
        b"dpone-r1-effect-receipt-v2",
        tuple(getattr(header, item.name) for item in fields(header)) + (body_digest,),
    )
    return MssqlEffectReceiptV2(
        deterministic_receipt_id(header.effect_key),
        header,
        body,
        body_digest,
        receipt_digest,
    )


def prove_receipt_descendant(
    historical: MssqlEffectReceiptV2,
    descendants: tuple[MssqlEffectReceiptV2, ...],
    *,
    max_receipts: int,
) -> bool:
    if not isinstance(descendants, tuple):
        raise MssqlReceiptContractError("descendant proof chain must be an immutable tuple")
    if not 1 <= max_receipts <= 100_000 or len(descendants) > max_receipts:
        raise MssqlReceiptContractError("descendant proof exceeds configured bound")
    if historical != build_receipt(historical.header, historical.body):
        raise MssqlReceiptContractError("historical receipt digest is invalid")
    previous = historical
    seen = {historical.receipt_id}
    for candidate in descendants:
        if candidate != build_receipt(candidate.header, candidate.body):
            raise MssqlReceiptContractError("receipt chain contains an invalid digest")
        if (
            candidate.header.previous_receipt_id != previous.receipt_id
            or candidate.header.previous_receipt_digest != previous.receipt_digest
        ):
            raise MssqlReceiptContractError("receipt chain predecessor is broken")
        if candidate.receipt_id in seen:
            raise MssqlReceiptContractError("receipt chain contains a cycle")
        seen.add(candidate.receipt_id)
        previous = candidate
    return True


def checkpoint_pointer_from_receipt(receipt: MssqlEffectReceiptV2) -> MssqlCheckpointPointerV2 | None:
    """Project the exact nullable checkpoint pointer persisted by a receipt."""

    body = receipt.body
    state_key = getattr(body, "candidate_checkpoint_state_key_digest", None)
    generation = getattr(body, "candidate_checkpoint_writer_generation", None)
    revision = getattr(body, "committed_checkpoint_revision", None)
    if revision is None:
        revision = getattr(body, "candidate_checkpoint_revision", None)
    if state_key is None and generation is None and revision is None:
        return None
    if state_key is None or generation is None or revision is None:
        raise MssqlReceiptContractError("checkpoint projection is incomplete")
    return MssqlCheckpointPointerV2(state_key, generation, revision)


def head_matches_receipt(head: MssqlTargetHeadV2, receipt: MssqlEffectReceiptV2) -> bool:
    """Compare complete mutable head authority with one immutable receipt."""

    header = receipt.header
    return (
        head.target_binding_uuid == header.target_binding_uuid
        and head.writer_mode is header.writer_mode
        and head.writer_generation == header.candidate_writer_generation
        and head.head_revision == header.committed_head_revision
        and head.last_receipt_id == receipt.receipt_id
        and head.recovery_domain_uuid == header.candidate_recovery_domain_uuid
        and head.recovery_domain_epoch == header.candidate_recovery_domain_epoch
        and head.checkpoint == checkpoint_pointer_from_receipt(receipt)
    )


def xmin_mutation_result_from_receipt(receipt: MssqlEffectReceiptV2) -> MssqlXminMutationResultV2:
    """Project exact XMin metrics using only independently verified receipt bytes."""

    if receipt != build_receipt(receipt.header, receipt.body) or not isinstance(receipt.body, MssqlXminEffectReceiptV2):
        raise MssqlReceiptContractError("XMin replay requires an exact canonical receipt")
    body = receipt.body
    return MssqlXminMutationResultV2(
        before_row_count=body.before_row_count,
        after_row_count=body.after_row_count,
        payload_bytes=body.payload_bytes,
        inserted_row_count=body.inserted_row_count,
        updated_row_count=body.updated_row_count,
        hard_deleted_row_count=body.hard_deleted_row_count,
        unchanged_row_count=body.unchanged_row_count,
        delta_row_count=body.delta_row_count,
    )


__all__ = [
    name
    for name in globals()
    if name.startswith("Mssql")
    or name
    in {
        "ReceiptBodyV2",
        "ReceiptKind",
        "build_receipt",
        "checkpoint_pointer_from_receipt",
        "head_matches_receipt",
        "prove_receipt_descendant",
        "xmin_mutation_result_from_receipt",
    }
]
