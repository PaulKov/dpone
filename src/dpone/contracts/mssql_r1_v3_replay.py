"""Typed fresh-session replay observations for MSSQL R1 V3 effects."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.mssql_r1_v3_effect import MssqlR1EffectRequestV3
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bool,
    expect_bytes,
    expect_enum,
    expect_text,
    expect_tuple,
    require_digest,
)
from dpone.contracts.mssql_r1_v3_plan import R1XminMutationPlanV1
from dpone.contracts.mssql_r1_v3_receipt import MssqlR1EffectReceiptV3
from dpone.contracts.mssql_r1_v3_receipt_projection import validate_receipt_for_request_v3
from dpone.contracts.mssql_r1_v3_replay_observation import (
    ArtifactSetReplayObservationV3,
    AuthoritySetReplayObservationV3,
    CheckpointReplayObservationV3,
    EffectReceiptProofRequestV3,
    OperationReplayObservationV3,
    R1ReplayOperationStateV3,
    R1ReplayResourceStateV3,
    R1UnknownReplayReasonV3,
    WriterHeadReplayObservationV3,
)

_PROOF_DOMAIN = b"dpone-r1-effect-replay-proof-v3\0"


@dataclass(frozen=True, slots=True)
class CommittedEffectReplayProofV3:
    proof_request: EffectReceiptProofRequestV3
    sealed_request: MssqlR1EffectRequestV3
    receipt: MssqlR1EffectReceiptV3
    operation: OperationReplayObservationV3
    head: WriterHeadReplayObservationV3
    artifacts: ArtifactSetReplayObservationV3
    authorities: AuthoritySetReplayObservationV3
    checkpoint: CheckpointReplayObservationV3 | None = None
    override: None = None

    def __post_init__(self) -> None:
        _validate_common(self.proof_request, self.sealed_request, self.operation)
        header = self.receipt.header
        validate_receipt_for_request_v3(self.sealed_request, self.receipt)
        if (
            self.operation.state is not R1ReplayOperationStateV3.COMMITTED
            or header.operation_epoch != self.proof_request.operation_epoch
            or header.operation_projection_revision != self.proof_request.operation_projection_revision
            or self.operation.committed_receipt_id != header.receipt_id
            or self.operation.committed_receipt_digest != self.receipt.digest
            or not self.head.proves_descendant_of(self.receipt)
            or self.head.target_generation < header.candidate_writer_generation
            or self.head.head_revision < header.candidate_head_revision
        ):
            raise MssqlR1V3ContractError("committed replay proof does not prove the exact effect")
        _validate_resources(self.sealed_request, self.artifacts, self.authorities, self.receipt, committed=True)
        _validate_checkpoint(self.sealed_request, self.checkpoint, self.receipt.header.receipt_id, committed=True)
        if (header.revoked_override_id, header.revoked_override_digest) != (
            self.proof_request.expected_override_id,
            self.proof_request.expected_override_digest,
        ):
            raise MssqlR1V3ContractError("committed receipt override differs from proof request")
        if self.override is not None:
            raise MssqlR1V3ContractError("revoked-registration override is absent in R1")

    @property
    def canonical_bytes(self) -> bytes:
        return _proof_bytes("committed", self)

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> CommittedEffectReplayProofV3:
        kind, raw = decode_canonical_bytes(payload, _PROOF_DOMAIN, field_count=2)
        if expect_text(kind, "proof_kind") != "committed":
            raise MssqlR1V3ContractError("replay proof kind is not committed")
        values = expect_tuple(raw, "committed_proof", size=9)
        _reject_override(values[8])
        return cls(
            EffectReceiptProofRequestV3.from_canonical_bytes(expect_bytes(values[0], "proof_request")),
            MssqlR1EffectRequestV3.from_canonical_bytes(expect_bytes(values[1], "sealed_request")),
            MssqlR1EffectReceiptV3.from_canonical_bytes(expect_bytes(values[2], "receipt")),
            OperationReplayObservationV3.from_canonical_bytes(expect_bytes(values[3], "operation")),
            WriterHeadReplayObservationV3.from_canonical_bytes(expect_bytes(values[4], "head")),
            ArtifactSetReplayObservationV3.from_canonical_bytes(expect_bytes(values[5], "artifacts")),
            AuthoritySetReplayObservationV3.from_canonical_bytes(expect_bytes(values[6], "authorities")),
            _optional_checkpoint(values[7]),
            None,
        )


@dataclass(frozen=True, slots=True)
class KnownNotCommittedEffectReplayProofV3:
    proof_request: EffectReceiptProofRequestV3
    sealed_request: MssqlR1EffectRequestV3
    receipt_absent: bool
    operation: OperationReplayObservationV3
    predecessor_head: WriterHeadReplayObservationV3 | None
    artifacts: ArtifactSetReplayObservationV3
    authorities: AuthoritySetReplayObservationV3
    checkpoint: CheckpointReplayObservationV3 | None = None
    override: None = None

    def __post_init__(self) -> None:
        _validate_common(self.proof_request, self.sealed_request, self.operation)
        if self.receipt_absent is not True or self.operation.state is not R1ReplayOperationStateV3.SEALED:
            raise MssqlR1V3ContractError("retry requires positive receipt absence and unchanged SEALED state")
        plan = self.sealed_request.mutation_plan
        if plan.expected_writer_generation is None:
            if self.predecessor_head is not None:
                raise MssqlR1V3ContractError("initial effect cannot observe a predecessor head")
        elif self.predecessor_head is None or (
            self.predecessor_head.target_generation,
            self.predecessor_head.head_revision,
            self.predecessor_head.last_receipt_id,
            self.predecessor_head.last_receipt_digest,
            self.predecessor_head.descendants,
        ) != (
            plan.expected_writer_generation,
            plan.expected_head_revision,
            plan.predecessor_receipt_id,
            plan.predecessor_receipt_digest,
            (),
        ):
            raise MssqlR1V3ContractError("known noncommit predecessor head differs from sealed plan")
        _validate_resources(self.sealed_request, self.artifacts, self.authorities, None, committed=False)
        _validate_checkpoint(self.sealed_request, self.checkpoint, None, committed=False)
        if self.override is not None:
            raise MssqlR1V3ContractError("revoked-registration override is absent in R1")

    @property
    def canonical_bytes(self) -> bytes:
        return _proof_bytes("known-not-committed", self)

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> KnownNotCommittedEffectReplayProofV3:
        kind, raw = decode_canonical_bytes(payload, _PROOF_DOMAIN, field_count=2)
        if expect_text(kind, "proof_kind") != "known-not-committed":
            raise MssqlR1V3ContractError("replay proof kind is not known-not-committed")
        values = expect_tuple(raw, "known_noncommit_proof", size=9)
        _reject_override(values[8])
        return cls(
            EffectReceiptProofRequestV3.from_canonical_bytes(expect_bytes(values[0], "proof_request")),
            MssqlR1EffectRequestV3.from_canonical_bytes(expect_bytes(values[1], "sealed_request")),
            expect_bool(values[2], "receipt_absent"),
            OperationReplayObservationV3.from_canonical_bytes(expect_bytes(values[3], "operation")),
            None
            if values[4] is None
            else WriterHeadReplayObservationV3.from_canonical_bytes(expect_bytes(values[4], "predecessor_head")),
            ArtifactSetReplayObservationV3.from_canonical_bytes(expect_bytes(values[5], "artifacts")),
            AuthoritySetReplayObservationV3.from_canonical_bytes(expect_bytes(values[6], "authorities")),
            _optional_checkpoint(values[7]),
            None,
        )


@dataclass(frozen=True, slots=True)
class UnknownEffectReplayProofV3:
    proof_request: EffectReceiptProofRequestV3
    reason_code: R1UnknownReplayReasonV3
    observed_digest: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.reason_code, R1UnknownReplayReasonV3):
            raise MssqlR1V3ContractError("unknown replay reason is unsupported")
        require_digest(self.observed_digest, "observed_digest")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _PROOF_DOMAIN,
            ("unknown", self.proof_request.canonical_bytes, self.reason_code, self.observed_digest),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> UnknownEffectReplayProofV3:
        kind, proof, reason, observed = decode_canonical_bytes(payload, _PROOF_DOMAIN, field_count=4)
        if expect_text(kind, "proof_kind") != "unknown":
            raise MssqlR1V3ContractError("replay proof kind is not unknown")
        return cls(
            EffectReceiptProofRequestV3.from_canonical_bytes(expect_bytes(proof, "proof_request")),
            expect_enum(R1UnknownReplayReasonV3, reason, "reason_code"),
            expect_bytes(observed, "observed_digest"),
        )


EffectReplayProofV3 = CommittedEffectReplayProofV3 | KnownNotCommittedEffectReplayProofV3 | UnknownEffectReplayProofV3


def _validate_common(
    proof: EffectReceiptProofRequestV3,
    request: MssqlR1EffectRequestV3,
    operation: OperationReplayObservationV3,
) -> None:
    expected = (request.digest, request.identity.effect_key, proof.operation_epoch, proof.operation_projection_revision)
    observed = (
        proof.sealed_request_digest,
        proof.effect_key,
        operation.operation_epoch,
        operation.operation_projection_revision,
    )
    if expected != observed or (
        operation.operation_key,
        operation.effect_key,
        operation.sealed_request_digest,
    ) != (request.identity.operation_key, request.identity.effect_key, request.digest):
        raise MssqlR1V3ContractError("replay proof request/operation differs from retained sealed request")


def _validate_resources(
    request: MssqlR1EffectRequestV3,
    artifacts: ArtifactSetReplayObservationV3,
    authorities: AuthoritySetReplayObservationV3,
    receipt: MssqlR1EffectReceiptV3 | None,
    *,
    committed: bool,
) -> None:
    expected_receipt = None if receipt is None else receipt.header.receipt_id
    state = R1ReplayResourceStateV3.CONSUMED if committed else R1ReplayResourceStateV3.SEALED
    if (
        artifacts.artifact_ids != tuple(item.artifact_id for item in request.artifacts)
        or artifacts.manifest_digests != tuple(item.manifest_digest for item in request.artifacts)
        or artifacts.states != (state,) * len(request.artifacts)
        or artifacts.consuming_receipt_ids != (expected_receipt,) * len(request.artifacts)
        or artifacts.consuming_receipt_digests
        != ((None if receipt is None else receipt.digest),) * len(request.artifacts)
    ):
        raise MssqlR1V3ContractError("artifact replay observation differs from sealed request")
    authority_state = R1ReplayResourceStateV3.CONSUMED if committed else R1ReplayResourceStateV3.ISSUED
    authorities.validate_for_request(
        request,
        authority_state,
        expected_receipt,
        None if receipt is None else receipt.digest,
    )
    if receipt is not None and (
        authorities.issuance_id,
        authorities.issuance_payload_digest,
        authorities.verification_receipt_digest,
    ) != (
        receipt.header.generation_authority_issuance_id,
        receipt.header.generation_authority_issuance_payload_digest,
        receipt.header.generation_authority_verification_receipt_digest,
    ):
        raise MssqlR1V3ContractError("authority replay observation differs from receipt issuance")


def _validate_checkpoint(
    request: MssqlR1EffectRequestV3,
    checkpoint: CheckpointReplayObservationV3 | None,
    receipt_id: UUID | None,
    *,
    committed: bool,
) -> None:
    plan = request.mutation_plan
    if not isinstance(plan, R1XminMutationPlanV1):
        if checkpoint is not None:
            raise MssqlR1V3ContractError("Batch replay forbids checkpoint observation")
        return
    expected_payload = plan.checkpoint_candidate_payload if committed else plan.previous_checkpoint_payload
    expected_revision = plan.candidate_checkpoint_revision if committed else plan.previous_checkpoint_revision
    expected_generation = plan.checkpoint_writer_generation if committed else plan.expected_writer_generation
    if expected_payload is None:
        if checkpoint is not None:
            raise MssqlR1V3ContractError("initial XMin noncommit has no predecessor checkpoint")
        return
    if checkpoint is None or (
        checkpoint.writer_generation,
        checkpoint.checkpoint_revision,
        checkpoint.checkpoint_payload,
        checkpoint.consuming_receipt_id,
    ) != (expected_generation, expected_revision, expected_payload, receipt_id):
        raise MssqlR1V3ContractError("checkpoint replay observation differs from sealed XMin plan")


def _proof_bytes(kind: str, proof: CommittedEffectReplayProofV3 | KnownNotCommittedEffectReplayProofV3) -> bytes:
    values = tuple(
        value.canonical_bytes if hasattr(value, "canonical_bytes") else value
        for value in (getattr(proof, name) for name in proof.__dataclass_fields__)
    )
    return canonical_bytes(_PROOF_DOMAIN, (kind, values))


def _optional_checkpoint(value: object) -> CheckpointReplayObservationV3 | None:
    return (
        None if value is None else CheckpointReplayObservationV3.from_canonical_bytes(expect_bytes(value, "checkpoint"))
    )


def _reject_override(value: object) -> None:
    if value is not None:
        raise MssqlR1V3ContractError("revoked-registration override is absent in R1")
    return None


__all__ = [name for name in tuple(globals()) if name.endswith(("V1", "V3")) or name == "EffectReplayProofV3"]
