"""Build effect receipts and verify their agreement with a retained request."""

from __future__ import annotations

from dataclasses import fields
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
)
from dpone.contracts.mssql_r1_v3_plan import R1BatchMutationPlanV1, R1XminMutationPlanV1
from dpone.contracts.mssql_r1_v3_quality import (
    MssqlBatchQualityEvidenceV3,
    MssqlQualityEvidenceV3,
    MssqlXminQualityEvidenceV3,
    decode_quality_evidence,
)
from dpone.contracts.mssql_r1_v3_receipt import (
    _HEADER_DOMAIN,
    EffectReceiptBodyV3,
    MssqlBatchEffectReceiptBodyV3,
    MssqlR1EffectReceiptHeaderV3,
    MssqlR1EffectReceiptV3,
    MssqlR1ReceiptObservationV3,
    MssqlXminEffectReceiptBodyV3,
    _receipt_uuid,
)

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_effect import (
        MssqlR1EffectAttemptEnvelopeV3,
        MssqlR1EffectRequestV3,
    )
    from dpone.contracts.mssql_r1_v3_transaction_authority import MssqlAdmittedGenerationAuthoritySetV3


def build_receipt_v3(
    attempt: MssqlR1EffectAttemptEnvelopeV3,
    body: EffectReceiptBodyV3,
    *,
    committed_at: datetime,
    admitted_authorities: MssqlAdmittedGenerationAuthoritySetV3,
) -> MssqlR1EffectReceiptV3:
    request = attempt.request
    plan = request.mutation_plan
    _cross_check_body(request, body)
    admitted_authorities.validate_for_attempt(attempt)
    issuance = admitted_authorities.values
    values = dict(
        receipt_id=UUID(int=0),
        receipt_kind=request.identity.source_mode,
        contract_version=request.contract.effect_contract_version,
        contract_digest=request.contract.digest,
        operation_key=request.identity.operation_key,
        effect_key=request.identity.effect_key,
        physical_coordinate_digest=plan.target_identity.physical_object_coordinate_digest,
        registered_physical_authority_digest=plan.target_identity.registered_physical_authority_digest,
        target_binding_uuid=request.identity.target_binding_uuid,
        target_object_uuid=plan.target_identity.target_object_uuid,
        recovery_identity_digest=request.generation.recovery_identity_digest,
        writer_mode=request.identity.source_mode,
        expected_writer_generation=plan.expected_writer_generation,
        candidate_writer_generation=plan.candidate_writer_generation,
        expected_head_revision=plan.expected_head_revision,
        candidate_head_revision=plan.candidate_head_revision,
        operation_epoch=attempt.operation_epoch,
        operation_projection_revision=attempt.operation_projection_revision,
        predecessor_receipt_id=plan.predecessor_receipt_id,
        predecessor_receipt_digest=plan.predecessor_receipt_digest,
        route_identity_sha256=request.identity.route_identity_sha256,
        source_authority_sha256=request.source_authority_sha256,
        registration_id=request.registration_id,
        registration_payload_digest=request.registration_payload_digest,
        verification_policy_digest=request.verification_policy_digest,
        registration_admitted_at=request.admitted_at_server_time,
        sealed_request_digest=request.digest,
        artifact_set_digest=request.artifact_set_digest,
        mutation_plan_digest=plan.digest,
        generation_transition_digest=request.generation.digest,
        generation_authority_issuance_id=issuance.issuance_id,
        generation_authority_issuance_payload_digest=issuance.issuance_payload_digest,
        generation_authority_verification_receipt_digest=issuance.verification_receipt_digest,
        authority_set_digest=request.authority_set.digest,
        body_digest=body.digest,
        revoked_override_id=None,
        revoked_override_digest=None,
        committed_at=committed_at,
    )
    identity_bytes = canonical_bytes(
        _HEADER_DOMAIN,
        tuple(values[item.name] for item in fields(MssqlR1EffectReceiptHeaderV3) if item.name != "receipt_id"),
    )
    values["receipt_id"] = _receipt_uuid(identity_bytes)
    return MssqlR1EffectReceiptV3(MssqlR1EffectReceiptHeaderV3(**values), body)  # type: ignore[arg-type]


def _cross_check_body(request: MssqlR1EffectRequestV3, body: EffectReceiptBodyV3) -> None:
    if (body.source_snapshot_digest, body.source_schema_digest) != (
        request.source_snapshot_digest,
        request.source_schema_digest,
    ):
        raise MssqlR1V3ContractError("receipt body differs from sealed source authority")
    quality = decode_quality_evidence(body.quality_bytes)
    if quality.probe_contract_digest != request.mutation_plan.probe_contract_digest:
        raise MssqlR1V3ContractError("receipt quality differs from sealed probe contract")
    if isinstance(body, MssqlBatchEffectReceiptBodyV3) and isinstance(request.mutation_plan, R1BatchMutationPlanV1):
        manifest = request.mutation_plan.stage_manifest
        if (body.manifest_bytes, body.manifest_digest, body.payload_row_count) != (
            manifest.canonical_bytes,
            manifest.manifest_digest,
            manifest.observed_row_count,
        ):
            raise MssqlR1V3ContractError("Batch receipt body differs from sealed manifest")
    elif isinstance(body, MssqlXminEffectReceiptBodyV3) and isinstance(request.mutation_plan, R1XminMutationPlanV1):
        plan = request.mutation_plan
        if (
            (body.delta_manifest_bytes, body.delta_manifest_digest)
            != (
                plan.delta_manifest.canonical_bytes,
                plan.delta_manifest.manifest_digest,
            )
            or (body.complete_keys_manifest_bytes, body.complete_keys_manifest_digest)
            != (
                plan.complete_keys_manifest.canonical_bytes,
                plan.complete_keys_manifest.manifest_digest,
            )
            or body.candidate_checkpoint_payload != plan.checkpoint_candidate_payload
            or body.previous_checkpoint_payload != plan.previous_checkpoint_payload
            or body.candidate_checkpoint_value != plan.checkpoint_candidate_value
            or body.previous_checkpoint_value != plan.previous_checkpoint_value
        ):
            raise MssqlR1V3ContractError("XMin receipt body differs from sealed plan")
    else:
        raise MssqlR1V3ContractError("receipt body differs from sealed source mode")


def validate_receipt_for_request_v3(request: MssqlR1EffectRequestV3, receipt: MssqlR1EffectReceiptV3) -> None:
    """Require an exact immutable receipt projection of one retained request."""

    header = receipt.header
    plan = request.mutation_plan
    observed = (
        header.contract_version,
        header.contract_digest,
        header.operation_key,
        header.effect_key,
        header.physical_coordinate_digest,
        header.registered_physical_authority_digest,
        header.target_binding_uuid,
        header.target_object_uuid,
        header.recovery_identity_digest,
        header.writer_mode,
        header.expected_writer_generation,
        header.candidate_writer_generation,
        header.expected_head_revision,
        header.candidate_head_revision,
        header.route_identity_sha256,
        header.source_authority_sha256,
        header.registration_id,
        header.registration_payload_digest,
        header.verification_policy_digest,
        header.registration_admitted_at,
        header.sealed_request_digest,
        header.artifact_set_digest,
        header.mutation_plan_digest,
        header.generation_transition_digest,
        header.authority_set_digest,
    )
    expected = (
        request.contract.effect_contract_version,
        request.contract.digest,
        request.identity.operation_key,
        request.identity.effect_key,
        plan.target_identity.physical_object_coordinate_digest,
        plan.target_identity.registered_physical_authority_digest,
        request.identity.target_binding_uuid,
        plan.target_identity.target_object_uuid,
        request.generation.recovery_identity_digest,
        request.identity.source_mode,
        plan.expected_writer_generation,
        plan.candidate_writer_generation,
        plan.expected_head_revision,
        plan.candidate_head_revision,
        request.identity.route_identity_sha256,
        request.source_authority_sha256,
        request.registration_id,
        request.registration_payload_digest,
        request.verification_policy_digest,
        request.admitted_at_server_time,
        request.digest,
        request.artifact_set_digest,
        plan.digest,
        request.generation.digest,
        request.authority_set.digest,
    )
    if observed != expected:
        raise MssqlR1V3ContractError("receipt differs from retained sealed request")


def receipt_stage_manifests(receipt: MssqlR1EffectReceiptV3) -> tuple[tuple[bytes, ...], tuple[bytes, ...]]:
    body = receipt.body
    if isinstance(body, MssqlBatchEffectReceiptBodyV3):
        return (body.manifest_bytes,), (body.manifest_digest,)
    return (
        (body.delta_manifest_bytes, body.complete_keys_manifest_bytes),
        (body.delta_manifest_digest, body.complete_keys_manifest_digest),
    )


__all__ = [
    "build_receipt_v3",
    "validate_receipt_for_request_v3",
    "receipt_stage_manifests",
    "receipt_body_for_observation",
]


def receipt_body_for_observation(
    attempt: MssqlR1EffectAttemptEnvelopeV3,
    quality: MssqlQualityEvidenceV3,
    observation: MssqlR1ReceiptObservationV3,
) -> EffectReceiptBodyV3:
    plan = attempt.request.mutation_plan
    if isinstance(plan, R1BatchMutationPlanV1) and isinstance(quality, MssqlBatchQualityEvidenceV3):
        before = observation.batch_target_row_count_before
        if before is None:
            raise MssqlR1V3ContractError("Batch receipt requires a target-observed predecessor row count")
        return MssqlBatchEffectReceiptBodyV3(
            attempt.request.source_snapshot_digest,
            attempt.request.source_schema_digest,
            plan.stage_manifest.canonical_bytes,
            plan.stage_manifest.manifest_digest,
            plan.stage_manifest.observed_row_count,
            before,
            quality.candidate_target_rows,
            quality.canonical_bytes,
            quality.digest,
        )
    if isinstance(plan, R1XminMutationPlanV1) and isinstance(quality, MssqlXminQualityEvidenceV3):
        if observation.batch_target_row_count_before is not None:
            raise MssqlR1V3ContractError("XMin receipt forbids a Batch predecessor observation")
        return MssqlXminEffectReceiptBodyV3(
            attempt.request.source_snapshot_digest,
            attempt.request.source_schema_digest,
            plan.delta_manifest.canonical_bytes,
            plan.delta_manifest.manifest_digest,
            plan.complete_keys_manifest.canonical_bytes,
            plan.complete_keys_manifest.manifest_digest,
            quality.affected_key_count,
            quality.inserted_count,
            quality.updated_count,
            quality.deleted_count,
            quality.delta_no_effect_count,
            plan.previous_checkpoint_payload,
            plan.previous_checkpoint_value,
            plan.checkpoint_candidate_payload,
            plan.checkpoint_candidate_value,
            quality.canonical_bytes,
            quality.digest,
        )
    raise MssqlR1V3ContractError("quality kind differs from the sealed mutation plan")
