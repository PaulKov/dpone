from __future__ import annotations

import hashlib
import inspect
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from dpone.contracts.mssql_r1_v3_authority import (
    MssqlGenerationAuthorityPurposeV2,
    MssqlGenerationAuthorityRefV2,
    MssqlGenerationAuthoritySetIssuancePayloadV1,
    MssqlGenerationAuthoritySetV2,
    MssqlRevokedRegistrationCompletionOverridePayloadV1,
    MssqlSignedPayloadKindV1,
    MssqlSignedPayloadVerificationV1,
    SignedGenerationAuthoritySetCommandV1,
)
from dpone.contracts.mssql_r1_v3_candidate_proof import (
    MssqlCandidateEffectProofV3,
    MssqlCandidateOperationObservationV3,
    MssqlCandidateWriterHeadObservationV3,
)
from dpone.contracts.mssql_r1_v3_codec import decode_canonical_bytes
from dpone.contracts.mssql_r1_v3_control import (
    MssqlOpenStageRecoveryReceiptV1,
    MssqlR1ControlOperationV1,
    MssqlR1ControlReceiptV1,
    build_control_effect_key,
    build_open_stage_recovery_effect_key,
)
from dpone.contracts.mssql_r1_v3_control_proof import (
    MssqlOpenStageRecoveryFreshProofV1,
    MssqlR1ControlFreshProofV1,
    MssqlR1WriterHeadControlObservationV1,
)
from dpone.contracts.mssql_r1_v3_effect import (
    MssqlR1EffectAttemptEnvelopeV3,
    MssqlR1EffectRequestV3,
    MssqlR1EffectSealIntentV3,
    R1BatchMutationPlanV1,
    R1XminMutationPlanV1,
)
from dpone.contracts.mssql_r1_v3_identity import (
    EFFECT_CONTRACT_VERSION,
    MssqlR1EffectContractV3,
    MssqlR1EffectIdentityV3,
    MssqlR1V3ContractError,
    canonical_identifier_digest,
)
from dpone.contracts.mssql_r1_v3_mutation import (
    MssqlBatchGenerationTransitionV3,
    MssqlXminGenerationTransitionV3,
    R1MutationResourceLimitsV1,
)
from dpone.contracts.mssql_r1_v3_plan import R1MutationTemplateSetV1, writer_head_predecessor_digest
from dpone.contracts.mssql_r1_v3_quality import MssqlBatchQualityEvidenceV3, MssqlXminQualityEvidenceV3
from dpone.contracts.mssql_r1_v3_receipt import (
    MssqlBatchEffectReceiptBodyV3,
    MssqlR1EffectReceiptV3,
    MssqlXminEffectReceiptBodyV3,
)
from dpone.contracts.mssql_r1_v3_receipt_projection import build_receipt_v3
from dpone.contracts.mssql_r1_v3_registration import (
    MssqlTargetRegistrationPayloadV1,
    MssqlTargetRegistrationVerificationV1,
    RegistrationActionV1,
    SignedTargetRegistrationCommandV1,
)
from dpone.contracts.mssql_r1_v3_rendered import (
    QUOTING_POLICY_VERSION,
    RENDERER_ID,
    RENDERER_VERSION,
    MssqlR1RenderedMutationBundleV1,
    MssqlR1RenderedStatementV1,
    MssqlR1RendererAdmissionEvidenceV1,
    MssqlR1RendererAuthorityV1,
    MssqlR1VerifiedRenderedMutationV1,
)
from dpone.contracts.mssql_r1_v3_replay import (
    ArtifactSetReplayObservationV3,
    AuthoritySetReplayObservationV3,
    CheckpointReplayObservationV3,
    CommittedEffectReplayProofV3,
    EffectReceiptProofRequestV3,
    KnownNotCommittedEffectReplayProofV3,
    OperationReplayObservationV3,
    R1ReplayOperationStateV3,
    R1ReplayResourceStateV3,
    R1UnknownReplayReasonV3,
    UnknownEffectReplayProofV3,
    WriterHeadReplayObservationV3,
)
from dpone.contracts.mssql_r1_v3_replay_observation import ReceiptDescendantLinkV3
from dpone.contracts.mssql_r1_v3_stage_consumption import (
    MssqlConsumedSealedStageSetV3,
    MssqlStageConsumptionStateV3,
)
from dpone.contracts.mssql_r1_v3_staging import (
    R1OpenStagePlanV1,
    R1SealedStageManifestV1,
    R1StageArtifactKindV1,
    R1StageBusinessColumnV1,
    R1StageCellStateV1,
    R1TypedStageCellV1,
    R1TypedStageRowV1,
    R1TypedStageScanEvidenceV1,
    artifact_digest_for_rows,
    canonical_artifact_digest,
    sealed_stage_set_digest,
)
from dpone.contracts.mssql_r1_v3_transaction import (
    MssqlR1SqlServerSessionIdentityV3,
    MssqlR1TransactionBindingV3,
    MssqlR1TransactionStateV3,
)
from dpone.contracts.mssql_r1_v3_transaction_authority import (
    MssqlAdmittedGenerationAuthoritySetV3,
    MssqlConsumedGenerationAuthoritySetV3,
    authority_observation_values,
)
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode

D1 = bytes.fromhex("11" * 32)
D2 = bytes.fromhex("22" * 32)
D3 = bytes.fromhex("33" * 32)
BINDING = UUID("10000000-0000-4000-8000-000000000001")
OBJECT = UUID("10000000-0000-4000-8000-000000000002")
REGISTRATION = UUID("10000000-0000-4000-8000-000000000003")
NOW = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


def _registration(*, action: RegistrationActionV1 = RegistrationActionV1.INITIAL):
    rotating = action is RegistrationActionV1.ROTATE
    return MssqlTargetRegistrationPayloadV1(
        registration_id=REGISTRATION,
        registration_action=action,
        predecessor_registration_id=UUID("10000000-0000-4000-8000-000000000004") if rotating else None,
        expected_active_registration_revision=4 if rotating else None,
        issued_at=NOW,
        expires_at=NOW + timedelta(days=30),
        nonce=b"n" * 16,
        profile_id="postgres16-mssql2022-r1",
        capability_tuple_digest=D1,
        resolved_profile_digest=D2,
        route_source_authority_sha256=D3,
        target_object_profile="ordinary_disk_rowstore_v1",
        catalog_projection_version="dpone-mssql-target-catalog-v1",
        revocation_revision=0,
        target_binding_uuid=BINDING,
        target_object_uuid=OBJECT,
        recovery_domain_uuid=UUID("10000000-0000-4000-8000-000000000005"),
        recovery_domain_epoch=1,
        server_instance_identity_sha256=D1,
        database_guid=UUID("10000000-0000-4000-8000-000000000006"),
        database_family_guid=UUID("10000000-0000-4000-8000-000000000007"),
        recovery_fork_guid=UUID("10000000-0000-4000-8000-000000000008"),
        database_name_digest=canonical_identifier_digest("warehouse"),
        schema_name_digest=canonical_identifier_digest("dbo"),
        object_name_digest=canonical_identifier_digest("orders"),
        database_name="warehouse",
        schema_name="dbo",
        object_name="orders",
        object_id=37,
        physical_generation_uuid=UUID("10000000-0000-4000-8000-000000000009"),
        catalog_contract_digest=D2,
        target_contract_revision=1,
    )


def _column() -> R1StageBusinessColumnV1:
    return R1StageBusinessColumnV1(1, "order_id", 1, "order_id", "pg.int8-mssql.bigint.v1", False, True)


def _stage_row(kind: R1StageArtifactKindV1, key: bytes, row_payload: bytes = b"r1") -> R1TypedStageRowV1:
    complete = kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS
    return R1TypedStageRowV1(
        kind,
        (R1TypedStageCellV1(1, "pg.int8-mssql.bigint.v1", R1StageCellStateV1.VALUE, b"int8:" + key),),
        key,
        None if complete else row_payload,
        None if complete else hashlib.sha256(row_payload).digest(),
    )


def _rendered_admission(
    plan: R1BatchMutationPlanV1 | R1XminMutationPlanV1,
) -> tuple[MssqlR1RenderedMutationBundleV1, MssqlR1RendererAuthorityV1, MssqlR1RendererAdmissionEvidenceV1]:
    authority = MssqlR1RendererAuthorityV1(
        D2,
        RENDERER_ID,
        RENDERER_VERSION,
        D3,
        plan.template_set.renderer_contract_digest,
        QUOTING_POLICY_VERSION,
        D1,
    )
    statements = tuple(
        MssqlR1RenderedStatementV1(
            step,
            statement := f"/* {step.value} */ SELECT 1".encode(),
            hashlib.sha256(statement).digest(),
            D1,
            D2,
        )
        for step in plan.template_set.ordered_steps
    )
    candidate = MssqlR1RenderedMutationBundleV1(
        plan.digest,
        RENDERER_ID,
        RENDERER_VERSION,
        D3,
        plan.template_set.renderer_contract_digest,
        QUOTING_POLICY_VERSION,
        statements,
        authority.digest,
        D1,
    )
    evidence = MssqlR1RendererAdmissionEvidenceV1(
        D2,
        authority.digest,
        plan.digest,
        candidate.execution_payload_digest,
        authority.verification_policy_digest,
        "dpone-mssql-r1-renderer-verifier",
        "1",
        D2,
    )
    return replace(candidate, admission_evidence_digest=evidence.digest), authority, evidence


def _rendered_bundle(plan: R1BatchMutationPlanV1 | R1XminMutationPlanV1) -> MssqlR1RenderedMutationBundleV1:
    bundle, authority, evidence = _rendered_admission(plan)
    assert MssqlR1VerifiedRenderedMutationV1(plan, bundle, authority, evidence, D2).bundle == bundle
    return bundle


def _open_plan(
    kind: R1StageArtifactKindV1,
    *,
    effect_key: bytes = D1,
    artifact_id: UUID = UUID("20000000-0000-4000-8000-000000000001"),
) -> R1OpenStagePlanV1:
    complete = kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS
    return R1OpenStagePlanV1(
        artifact_id=artifact_id,
        artifact_kind=kind,
        target_binding_uuid=BINDING,
        effect_key=effect_key,
        owner_epoch=1,
        server_lease_seconds=30,
        exact_stage_ddl_digest=D2,
        schema_digest=D2,
        catalog_contract_digest=D3,
        permission_contract_digest=D1,
        type_policy_digest=D2,
        ordered_business_columns=(_column(),),
        canonical_key_payload_column="__dpone_key",
        canonical_row_payload_column=None if complete else "__dpone_row",
        canonical_row_hash_column=None if complete else "__dpone_hash",
        maximum_key_bytes=64,
        maximum_row_bytes=0 if complete else 4096,
    )


def _sealed_manifest(
    kind: R1StageArtifactKindV1,
    *,
    effect_key: bytes = D1,
    artifact_id: UUID = UUID("20000000-0000-4000-8000-000000000001"),
    row_keys: tuple[bytes, ...] = (b"k1", b"k2"),
) -> R1SealedStageManifestV1:
    plan = _open_plan(kind, effect_key=effect_key, artifact_id=artifact_id)
    complete = kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS
    rows = tuple(_stage_row(kind, key) for key in row_keys)
    artifact_digest, payload_bytes = artifact_digest_for_rows(kind, D2, rows)
    evidence = R1TypedStageScanEvidenceV1(
        kind,
        artifact_digest,
        len(rows),
        payload_bytes,
        len(rows),
        len(rows),
        0 if complete else len(rows),
        len(rows),
        D3,
    )
    return R1SealedStageManifestV1(
        artifact_id=plan.artifact_id,
        artifact_kind=kind,
        target_binding_uuid=BINDING,
        effect_key=effect_key,
        schema_digest=D2,
        catalog_contract_digest=D3,
        permission_contract_digest=D1,
        type_policy_digest=D2,
        object_uuid=UUID("20000000-0000-4000-8000-000000000002"),
        object_id=71,
        physical_token=UUID("20000000-0000-4000-8000-000000000003"),
        target_local_schema="dpone_stage",
        target_local_object="orders_01",
        ordered_business_columns=plan.ordered_business_columns,
        canonical_key_payload_column=plan.canonical_key_payload_column,
        canonical_row_payload_column=plan.canonical_row_payload_column,
        canonical_row_hash_column=plan.canonical_row_hash_column,
        maximum_key_bytes=plan.maximum_key_bytes,
        maximum_row_bytes=plan.maximum_row_bytes,
        observed_row_count=len(rows),
        observed_payload_bytes=payload_bytes,
        artifact_digest=artifact_digest,
        typed_scan_evidence=evidence,
        row_hash_rule=None if complete else "sha256_canonical_row_v1",
        open_stage_plan_digest=plan.digest,
        exact_stage_ddl_digest=plan.exact_stage_ddl_digest,
    )


def _authority(
    purpose: MssqlGenerationAuthorityPurposeV2,
    *,
    expected_generation: int | None,
    candidate_generation: int,
    expected_head_revision: int | None = None,
    candidate_head_revision: int = 1,
) -> MssqlGenerationAuthorityRefV2:
    return MssqlGenerationAuthorityRefV2(
        authority_id=UUID(int=purpose_order(purpose)),
        purpose=purpose,
        effect_key=D1,
        source_snapshot_digest=D2,
        artifact_set_digest=D3,
        mutation_plan_digest=D1,
        expected_writer_generation=expected_generation,
        candidate_writer_generation=candidate_generation,
        expected_head_revision=(None if expected_generation is None else 7)
        if expected_head_revision is None
        else expected_head_revision,
        candidate_head_revision=candidate_head_revision,
        recovery_identity_digest=D2,
    )


def purpose_order(purpose: MssqlGenerationAuthorityPurposeV2) -> int:
    return {
        MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER: 1,
        MssqlGenerationAuthorityPurposeV2.REBASELINE: 2,
        MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH: 3,
    }[purpose]


def _batch_request() -> tuple[MssqlR1EffectRequestV3, MssqlR1EffectAttemptEnvelopeV3]:
    identity = MssqlR1EffectIdentityV3(D1, "receipt-run", SourceMode.BATCH_FULL_REFRESH, BINDING)
    artifact = _sealed_manifest(R1StageArtifactKindV1.BATCH_PAYLOAD, effect_key=identity.effect_key)
    plan = R1BatchMutationPlanV1(
        identity.effect_key,
        artifact,
        _registration().physical_identity,
        ("order_id",),
        D2,
        D1,
        D3,
        R1MutationTemplateSetV1.batch_v1(),
        R1MutationResourceLimitsV1(100, 1_000_000, 2_000_000, 30),
        None,
        1,
        None,
        1,
    )
    artifact_set = sealed_stage_set_digest((artifact,))
    authority = MssqlGenerationAuthorityRefV2(
        UUID(int=1),
        MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER,
        identity.effect_key,
        D2,
        artifact_set,
        plan.digest,
        None,
        1,
        None,
        1,
        D3,
    )
    transition = MssqlBatchGenerationTransitionV3(
        identity.effect_key,
        None,
        None,
        1,
        None,
        1,
        artifact.observed_row_count,
        D2,
        artifact_set,
        plan.digest,
        D3,
        MssqlGenerationAuthoritySetV2((authority,)),
    )
    request = MssqlR1EffectRequestV3(
        identity,
        REGISTRATION,
        _registration().payload_digest,
        _registration().canonical_bytes,
        D1,
        NOW,
        D2,
        D2,
        (artifact,),
        plan,
        _rendered_bundle(plan),
        transition,
    )
    return request, MssqlR1EffectAttemptEnvelopeV3(request, 1, 1, D3, NOW + timedelta(minutes=1))


def _proof_request(attempt: MssqlR1EffectAttemptEnvelopeV3) -> EffectReceiptProofRequestV3:
    return EffectReceiptProofRequestV3(
        attempt.request.digest,
        attempt.request.identity.effect_key,
        attempt.operation_epoch,
        attempt.operation_projection_revision,
    )


def _batch_descendant_receipt(predecessor: MssqlR1EffectReceiptV3) -> MssqlR1EffectReceiptV3:
    identity = MssqlR1EffectIdentityV3(D1, "descendant-run", SourceMode.BATCH_FULL_REFRESH, BINDING)
    artifact = _sealed_manifest(R1StageArtifactKindV1.BATCH_PAYLOAD, effect_key=identity.effect_key)
    plan = R1BatchMutationPlanV1(
        identity.effect_key,
        artifact,
        _registration().physical_identity,
        ("order_id",),
        D2,
        D1,
        D3,
        R1MutationTemplateSetV1.batch_v1(),
        R1MutationResourceLimitsV1(100, 1_000_000, 2_000_000, 30),
        1,
        2,
        1,
        1,
        predecessor.header.receipt_id,
        predecessor.digest,
        writer_head_predecessor_digest(
            _registration().physical_identity,
            1,
            1,
            predecessor.header.receipt_id,
            predecessor.digest,
        ),
    )
    artifact_set = sealed_stage_set_digest((artifact,))
    transition = MssqlBatchGenerationTransitionV3(
        identity.effect_key,
        SourceMode.BATCH_FULL_REFRESH,
        1,
        2,
        1,
        1,
        artifact.observed_row_count,
        D2,
        artifact_set,
        plan.digest,
        D3,
        MssqlGenerationAuthoritySetV2(),
    )
    request = MssqlR1EffectRequestV3(
        identity,
        REGISTRATION,
        _registration().payload_digest,
        _registration().canonical_bytes,
        D1,
        NOW,
        D2,
        D2,
        (artifact,),
        plan,
        _rendered_bundle(plan),
        transition,
    )
    attempt = MssqlR1EffectAttemptEnvelopeV3(request, 1, 1, D3, NOW + timedelta(minutes=1))
    quality = MssqlBatchQualityEvidenceV3(D3, 2, 2, 2, 2, 2, 2, 2, 2)
    body = MssqlBatchEffectReceiptBodyV3(
        D2,
        D2,
        artifact.canonical_bytes,
        artifact.manifest_digest,
        2,
        2,
        2,
        quality.canonical_bytes,
        quality.digest,
    )
    return build_receipt_v3(
        attempt,
        body,
        committed_at=NOW + timedelta(minutes=3),
        admitted_authorities=_admitted(request),
    )


def _operation(
    attempt: MssqlR1EffectAttemptEnvelopeV3,
    state: R1ReplayOperationStateV3,
    receipt: MssqlR1EffectReceiptV3 | None = None,
):
    return OperationReplayObservationV3(
        attempt.request.identity.operation_key,
        attempt.request.identity.effect_key,
        state,
        attempt.operation_epoch,
        attempt.operation_projection_revision,
        attempt.request.digest,
        None if receipt is None else receipt.header.receipt_id,
        None if receipt is None else receipt.digest,
    )


def _admitted(
    request: MssqlR1EffectRequestV3,
    binding: MssqlR1TransactionBindingV3 | None = None,
) -> MssqlAdmittedGenerationAuthoritySetV3:
    authority_set = request.authority_set
    issuance = None
    verification = None
    issuance_revision = None
    ref_revisions: tuple[int, ...] = ()
    if authority_set.refs:
        issuance = MssqlGenerationAuthoritySetIssuancePayloadV1(
            UUID(int=80),
            BINDING,
            request.registration_id,
            request.registration_payload_digest,
            0,
            NOW,
            NOW + timedelta(days=1),
            b"i" * 16,
            authority_set.canonical_bytes,
            authority_set.digest,
        )
        verification = MssqlSignedPayloadVerificationV1(
            MssqlSignedPayloadKindV1.GENERATION_AUTHORITY_SET,
            issuance.payload_digest,
            D1,
            D2,
            D3,
            D1,
            "cosign-v2.4",
            NOW,
            issuance.canonical_bytes,
        )
        issuance_revision = 1
        ref_revisions = tuple(range(1, len(authority_set.refs) + 1))
    values = authority_observation_values(
        transaction_id=UUID(int=81) if binding is None else binding.transaction_id,
        session_identity_digest=D2 if binding is None else binding.session_identity_digest,
        effect_key=request.identity.effect_key,
        sealed_request_digest=request.digest,
        authority_set=authority_set,
        issuance=issuance,
        verification=verification,
        issuance_projection_revision=issuance_revision,
        ref_projection_revisions=ref_revisions,
    )
    return MssqlAdmittedGenerationAuthoritySetV3(values)


def _transaction_binding() -> MssqlR1TransactionBindingV3:
    transaction_id = UUID(int=81)
    session = MssqlR1SqlServerSessionIdentityV3(
        D1,
        7,
        UUID(int=82),
        UUID(int=83),
        UUID(int=84),
        51,
        transaction_id,
    )
    return MssqlR1TransactionBindingV3(
        transaction_id,
        b"b" * 16,
        session.canonical_bytes,
        session.digest,
        MssqlR1TransactionStateV3.ACTIVE,
    )


def _resources(
    request,
    state: R1ReplayResourceStateV3,
    receipt: MssqlR1EffectReceiptV3 | None = None,
):
    receipt_id = None if receipt is None else receipt.header.receipt_id
    receipt_digest = None if receipt is None else receipt.digest
    artifacts = ArtifactSetReplayObservationV3(
        tuple(item.artifact_id for item in request.artifacts),
        tuple(item.manifest_digest for item in request.artifacts),
        (state,) * len(request.artifacts),
        (receipt_id,) * len(request.artifacts),
        (receipt_digest,) * len(request.artifacts),
    )
    refs = request.authority_set.refs
    admitted = _admitted(request)
    issuance = admitted.values
    authority_state = R1ReplayResourceStateV3.ISSUED if state is R1ReplayResourceStateV3.SEALED else state
    authorities = AuthoritySetReplayObservationV3(
        issuance.issuance_id,
        issuance.issuance_payload,
        issuance.issuance_payload_digest,
        issuance.verification_payload,
        issuance.verification_receipt_digest,
        None if not refs else authority_state,
        None if not refs else receipt_id,
        None if not refs else receipt_digest,
        tuple(item.authority_id for item in refs),
        tuple(item.digest for item in refs),
        (authority_state,) * len(refs),
        (receipt_id,) * len(refs),
        (receipt_digest,) * len(refs),
    )
    return artifacts, authorities


def test_v3_effect_identity_is_domain_separated_and_legacy_version_is_rejected() -> None:
    contract = MssqlR1EffectContractV3()
    identity = MssqlR1EffectIdentityV3(D1, "scheduled-run-17", SourceMode.BATCH_FULL_REFRESH, BINDING)

    assert contract.effect_contract_version == EFFECT_CONTRACT_VERSION
    assert contract.digest.hex() == "5fa9041f67cec1632baf128c558fe88f7da829b0181065484c080400219cd39c"
    assert identity.operation_key.hex() == "1e50fe9b210f538e89f2c5c582520f06e735cc642368a5714fca8a46043129ee"
    assert identity.effect_key.hex() == "9bd2052b4bc1cf9842fd51dc63e9f00b111d902e92d5fda600ebe51c6f896f2f"
    with pytest.raises(MssqlR1V3ContractError, match="exact V3"):
        MssqlR1EffectContractV3(effect_contract_version="mssql_effect_receipt_v2")


def test_registration_payload_and_verification_are_non_self_referential() -> None:
    payload = _registration()
    verification = MssqlTargetRegistrationVerificationV1(
        registration_payload_digest=payload.payload_digest,
        signature_bundle_digest=D1,
        signer_identity_digest=D2,
        trusted_root_digest=D3,
        cosign_policy_digest=D1,
        verifier_version="cosign-v2.4",
        verified_at=NOW,
        certificate_identity_digest=D2,
        certificate_issuer_digest=D3,
        payload_bytes=payload.canonical_bytes,
    )

    assert payload.canonical_bytes.startswith(b"dpone-mssql-target-registration-v1\0")
    assert payload.payload_digest.hex() == "6bc4690968a0fd3485b98306dbd93e8d41ec476f248045c6ca19fd77c6cce1c1"
    assert verification.payload_bytes == payload.canonical_bytes
    assert verification.registration_payload_digest == payload.payload_digest
    assert (
        verification.verification_policy_digest.hex()
        == "21b10c7028a37a183e82c7f8197bca2a45b4cb7a409edcbb02e170593a478e2e"
    )
    assert verification.receipt_digest.hex() == "0e72dc75839e6ea5151f8fb07a3c7631647b098f0e7bb01ea03633bd8444602c"
    assert MssqlTargetRegistrationVerificationV1.from_canonical_bytes(verification.canonical_bytes) == verification
    assert verification.verification_policy_digest != verification.signature_bundle_digest
    command = SignedTargetRegistrationCommandV1(payload.canonical_bytes, b"bundle-a")
    bound = replace(verification, signature_bundle_digest=hashlib.sha256(command.sigstore_bundle).digest())
    bound.validate_for_command(command)
    with pytest.raises(MssqlR1V3ContractError, match="command"):
        bound.validate_for_command(SignedTargetRegistrationCommandV1(payload.canonical_bytes, b"bundle-b"))
    other_payload = replace(payload, nonce=b"o" * 16)
    with pytest.raises(MssqlR1V3ContractError, match="command"):
        bound.validate_for_command(SignedTargetRegistrationCommandV1(other_payload.canonical_bytes, b"bundle-a"))
    assert "signature" not in payload.__dataclass_fields__
    with pytest.raises(MssqlR1V3ContractError, match="initial registration"):
        replace(payload, predecessor_registration_id=UUID(int=99))


@pytest.mark.parametrize("kind", tuple(R1StageArtifactKindV1))
def test_stage_kinds_bind_exact_shapes_and_unsigned_row_order(kind: R1StageArtifactKindV1) -> None:
    plan = _open_plan(kind)
    manifest = _sealed_manifest(kind)
    rows = (_stage_row(kind, b"k1"), _stage_row(kind, b"k2"))

    assert manifest.matches_open_plan(plan)
    assert (
        plan.digest.hex()
        == {
            R1StageArtifactKindV1.BATCH_PAYLOAD: "f3b826b2611db939ac89e6daef920d057e00d47178672d8558fc56e4957f2369",
            R1StageArtifactKindV1.XMIN_DELTA: "cdbf72453c66c5bb28b744d9f1c05c1fcccc0f5110641c61c49ca9621025bbf5",
            R1StageArtifactKindV1.XMIN_COMPLETE_KEYS: "b1653183c8e8ef0dc14737b77fb27cf8b44d4ddbabfc11d9fff21a621f6702a6",
        }[kind]
    )
    assert (
        manifest.manifest_digest.hex()
        == {
            R1StageArtifactKindV1.BATCH_PAYLOAD: "0abfe84dd257f8d01ee286751199b2a5d6f5658289a31b66cee7616a315fb8a6",
            R1StageArtifactKindV1.XMIN_DELTA: "59086170ec397ba1a42db69530098bbb40d3bae9dbd107efe742e7b86828956d",
            R1StageArtifactKindV1.XMIN_COMPLETE_KEYS: "c5135ea7affb89429dc8d6b4bfdfe8e7c1fc47b0ba6f65319b7a5c3ff0645872",
        }[kind]
    )
    assert (
        canonical_artifact_digest(manifest, rows).hex()
        == {
            R1StageArtifactKindV1.BATCH_PAYLOAD: "f22dd44010623641ada8eba815028bf678c3e57cfbc67173ce52d09e58ff1fed",
            R1StageArtifactKindV1.XMIN_DELTA: "964e0ddc7a736dadf739e10bba269e96a40f1605d96b417e127352a60b627a5e",
            R1StageArtifactKindV1.XMIN_COMPLETE_KEYS: "c8357e4859c31ca5ca1bf438278f1ad9911982eb74085dfd3593c80e8985a2c7",
        }[kind]
    )
    with pytest.raises(MssqlR1V3ContractError, match="strict unsigned key order"):
        canonical_artifact_digest(manifest, tuple(reversed(rows)))
    if kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS:
        with pytest.raises(MssqlR1V3ContractError, match="complete-keys"):
            replace(rows[0], canonical_row_payload=b"not-empty")


@pytest.mark.parametrize(
    ("previous_mode", "expected", "rows", "purposes"),
    (
        (None, None, 1, (MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER,)),
        (
            None,
            None,
            0,
            (MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER, MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH),
        ),
        (SourceMode.BATCH_FULL_REFRESH, 4, 1, ()),
        (SourceMode.BATCH_FULL_REFRESH, 4, 0, (MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH,)),
        (SourceMode.XMIN_CURRENT_STATE, 4, 1, (MssqlGenerationAuthorityPurposeV2.REBASELINE,)),
        (
            SourceMode.XMIN_CURRENT_STATE,
            4,
            0,
            (MssqlGenerationAuthorityPurposeV2.REBASELINE, MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH),
        ),
    ),
)
def test_batch_generation_authority_matrix_is_closed(previous_mode, expected, rows, purposes) -> None:
    candidate = 1 if expected is None else expected + 1
    authority_set = MssqlGenerationAuthoritySetV2(
        tuple(_authority(item, expected_generation=expected, candidate_generation=candidate) for item in purposes)
    )

    transition = MssqlBatchGenerationTransitionV3(
        effect_key=D1,
        previous_writer_mode=previous_mode,
        expected_writer_generation=expected,
        candidate_writer_generation=candidate,
        expected_head_revision=None if expected is None else 7,
        candidate_head_revision=1,
        payload_row_count=rows,
        source_snapshot_digest=D2,
        artifact_set_digest=D3,
        mutation_plan_digest=D1,
        recovery_identity_digest=D2,
        authority_set=authority_set,
    )

    assert transition.required_purposes == purposes
    with pytest.raises(MssqlR1V3ContractError, match="generation"):
        replace(transition, candidate_writer_generation=candidate + 1)


def test_explicit_rebaseline_is_only_valid_for_same_mode_with_exact_authority() -> None:
    rebaseline = _authority(
        MssqlGenerationAuthorityPurposeV2.REBASELINE,
        expected_generation=4,
        candidate_generation=5,
        expected_head_revision=7,
    )
    transition = MssqlBatchGenerationTransitionV3(
        D1,
        SourceMode.BATCH_FULL_REFRESH,
        4,
        5,
        7,
        1,
        1,
        D2,
        D3,
        D1,
        D2,
        MssqlGenerationAuthoritySetV2((rebaseline,)),
        True,
    )
    assert transition.required_purposes == (MssqlGenerationAuthorityPurposeV2.REBASELINE,)
    with pytest.raises(MssqlR1V3ContractError, match="initial Batch"):
        replace(transition, previous_writer_mode=None, expected_writer_generation=None, expected_head_revision=None)
    with pytest.raises(MssqlR1V3ContractError, match="one non-explicit encoding"):
        replace(transition, previous_writer_mode=SourceMode.XMIN_CURRENT_STATE)


def test_authority_set_rejects_wrong_order_duplicate_and_partial_binding() -> None:
    initial = _authority(
        MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER, expected_generation=None, candidate_generation=1
    )
    empty = _authority(
        MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH, expected_generation=None, candidate_generation=1
    )

    authority_set = MssqlGenerationAuthoritySetV2((initial, empty))
    assert authority_set.purposes == (
        MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER,
        MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH,
    )
    assert initial.digest.hex() == "f0b86a7ac17a3c032259b2a17aacf40e45bee257a6a722c51b763df89affe542"
    assert authority_set.digest.hex() == "9342d514fcc0c4ccab632c7254dc5841d362ae7576d4cb0e225c91ee5a546946"
    with pytest.raises(MssqlR1V3ContractError, match="canonical order"):
        MssqlGenerationAuthoritySetV2((empty, initial))
    with pytest.raises(MssqlR1V3ContractError, match="duplicate"):
        MssqlGenerationAuthoritySetV2((initial, initial))
    with pytest.raises(MssqlR1V3ContractError, match="same immutable effect"):
        MssqlGenerationAuthoritySetV2((initial, replace(empty, effect_key=D2)))


def test_xmin_ordinary_empty_complete_keys_requires_same_generation_empty_authority() -> None:
    empty = _authority(
        MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH,
        expected_generation=4,
        candidate_generation=4,
        expected_head_revision=7,
        candidate_head_revision=8,
    )
    authority_set = MssqlGenerationAuthoritySetV2((empty,))

    transition = MssqlXminGenerationTransitionV3(
        effect_key=D1,
        previous_writer_mode=SourceMode.XMIN_CURRENT_STATE,
        expected_writer_generation=4,
        candidate_writer_generation=4,
        expected_head_revision=7,
        candidate_head_revision=8,
        complete_key_count=0,
        source_snapshot_digest=D2,
        artifact_set_digest=D3,
        mutation_plan_digest=D1,
        recovery_identity_digest=D2,
        authority_set=authority_set,
    )

    assert transition.required_purposes == (MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH,)
    assert transition.digest.hex() == "b398ccc4ed5b886eedcd11cc744ca623a6662860e502138e16b2fbba1d8b7733"
    with pytest.raises(MssqlR1V3ContractError, match="closed matrix"):
        replace(transition, authority_set=MssqlGenerationAuthoritySetV2())


@pytest.mark.parametrize(
    ("previous", "expected", "explicit", "keys", "candidate", "head", "purposes"),
    (
        (None, None, False, 1, 1, 1, (MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER,)),
        (
            None,
            None,
            False,
            0,
            1,
            1,
            (MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER, MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH),
        ),
        (SourceMode.BATCH_FULL_REFRESH, 4, False, 1, 5, 1, (MssqlGenerationAuthorityPurposeV2.REBASELINE,)),
        (
            SourceMode.BATCH_FULL_REFRESH,
            4,
            False,
            0,
            5,
            1,
            (MssqlGenerationAuthorityPurposeV2.REBASELINE, MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH),
        ),
        (SourceMode.XMIN_CURRENT_STATE, 4, True, 1, 5, 1, (MssqlGenerationAuthorityPurposeV2.REBASELINE,)),
        (
            SourceMode.XMIN_CURRENT_STATE,
            4,
            True,
            0,
            5,
            1,
            (MssqlGenerationAuthorityPurposeV2.REBASELINE, MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH),
        ),
        (SourceMode.XMIN_CURRENT_STATE, 4, False, 1, 4, 8, ()),
    ),
)
def test_xmin_generation_matrix(previous, expected, explicit, keys, candidate, head, purposes) -> None:
    authority_set = MssqlGenerationAuthoritySetV2(
        tuple(
            _authority(
                purpose,
                expected_generation=expected,
                candidate_generation=candidate,
                expected_head_revision=None if expected is None else 7,
                candidate_head_revision=head,
            )
            for purpose in purposes
        )
    )
    transition = MssqlXminGenerationTransitionV3(
        D1,
        previous,
        expected,
        candidate,
        None if expected is None else 7,
        head,
        keys,
        D2,
        D3,
        D1,
        D2,
        authority_set,
        explicit,
    )
    assert transition.required_purposes == purposes


def test_control_and_open_recovery_receipts_keep_distinct_identities() -> None:
    control_key = build_control_effect_key(MssqlR1ControlOperationV1.PROVISION, D1, D2, None)
    control = MssqlR1ControlReceiptV1(
        control_receipt_id=UUID("30000000-0000-4000-8000-000000000001"),
        control_effect_key=control_key,
        operation=MssqlR1ControlOperationV1.PROVISION,
        physical_object_coordinate_digest=D1,
        registered_physical_authority_digest=D2,
        target_binding_uuid=BINDING,
        registration_id=REGISTRATION,
        input_payload_digest=D2,
        verification_receipt_digest=D3,
        expected_active_registration_revision=None,
        committed_active_registration_revision=1,
        expected_writer_head_revision=None,
        observed_writer_head_revision=1,
        expected_writer_head_digest=None,
        observed_writer_head_digest=D3,
        schema_contract_digest=D1,
        permission_contract_digest=D2,
        affected_authority_set_digest=None,
        affected_override_payload_digest=None,
        committed_at=NOW,
    )
    recovery_key = build_open_stage_recovery_effect_key(D1, D2, D3, 4, 11)
    recovery = MssqlOpenStageRecoveryReceiptV1(
        recovery_receipt_id=UUID("30000000-0000-4000-8000-000000000002"),
        effect_key=D2,
        recovery_effect_key=recovery_key,
        operation_key=D1,
        old_artifact_set_digest=D3,
        abandoned_artifact_ids=(UUID(int=1),),
        expected_operation_epoch=4,
        committed_operation_epoch=5,
        expected_operation_projection_revision=11,
        committed_operation_projection_revision=12,
        expected_writer_head_digest=None,
        observed_writer_head_digest=None,
        new_artifact_ids=(UUID(int=2),),
        new_open_plan_set_digest=D1,
        committed_at=NOW,
    )

    assert control.control_effect_key != recovery.recovery_effect_key
    assert control.receipt_digest != recovery.receipt_digest
    assert control_key.hex() == "1a76f13965110b80a4770db013ce3f7cccb0f030bbcb8b17fb3f0376c08faa3e"
    assert control.receipt_digest.hex() == "f09679d9a0aa0d55de9a59041e0f3bcc0b5e68b1fdd61a5d0cd9ab4b9824eb88"
    assert recovery_key.hex() == "72f2175050b5d0edb6e770b75dcf048e1daf7eebd6242a78efebeb25e802f116"
    assert recovery.receipt_digest.hex() == "1a00a7f370fe7e266751e3d9abea8031f4e57164d713627eaf88e54f54640d90"
    assert MssqlR1ControlReceiptV1.from_canonical_bytes(control.canonical_bytes) == control
    assert MssqlOpenStageRecoveryReceiptV1.from_canonical_bytes(recovery.canonical_bytes) == recovery
    overlapping_artifact_bytes = recovery.canonical_bytes.replace(UUID(int=2).bytes, UUID(int=1).bytes)
    assert overlapping_artifact_bytes != recovery.canonical_bytes
    with pytest.raises(MssqlR1V3ContractError, match="old and new artifact IDs must be disjoint"):
        MssqlOpenStageRecoveryReceiptV1.from_canonical_bytes(overlapping_artifact_bytes)
    control_proof = MssqlR1ControlFreshProofV1(
        control,
        REGISTRATION,
        1,
        D2,
        D1,
        D2,
        MssqlR1WriterHeadControlObservationV1(1, D3),
        None,
        None,
    )
    recovery_proof = MssqlOpenStageRecoveryFreshProofV1(
        recovery,
        5,
        12,
        "ADMITTED",
        (UUID(int=1),),
        (UUID(int=2),),
        D1,
        None,
    )
    assert MssqlR1ControlFreshProofV1.from_canonical_bytes(control_proof.canonical_bytes) == control_proof
    assert MssqlOpenStageRecoveryFreshProofV1.from_canonical_bytes(recovery_proof.canonical_bytes) == recovery_proof
    assert control_proof.digest.hex() == "b813bde8956995ad62f76cf0af597022cd88336c204cb11e94ade4812ece2157"
    assert recovery_proof.digest.hex() == "528021ff318b0c8952210e51ccb72485ef7e79bf378158ac8516d0a73cfb9eec"
    with pytest.raises(MssqlR1V3ContractError, match="committed receipt/state"):
        replace(control_proof, observed_writer_head=MssqlR1WriterHeadControlObservationV1(1, D2))
    with pytest.raises(MssqlR1V3ContractError, match="revision 1"):
        replace(control, observed_writer_head_revision=2)
    with pytest.raises(MssqlR1V3ContractError, match="operation epoch"):
        replace(recovery, committed_operation_epoch=7)


def test_effect_request_seals_plan_artifact_and_generation_while_attempt_epoch_is_replaceable() -> None:
    identity = MssqlR1EffectIdentityV3(D1, "scheduled-run-17", SourceMode.BATCH_FULL_REFRESH, BINDING)
    artifact = _sealed_manifest(R1StageArtifactKindV1.BATCH_PAYLOAD, effect_key=identity.effect_key)
    limits = R1MutationResourceLimitsV1(100, 1_000_000, 2_000_000, 30)
    plan = R1BatchMutationPlanV1(
        effect_key=identity.effect_key,
        stage_manifest=artifact,
        target_identity=_registration().physical_identity,
        ordered_target_columns=("order_id",),
        type_policy_digest=D2,
        hash_policy_digest=D1,
        probe_contract_digest=D3,
        template_set=R1MutationTemplateSetV1.batch_v1(),
        resource_limits=limits,
        expected_writer_generation=None,
        candidate_writer_generation=1,
        expected_head_revision=None,
        candidate_head_revision=1,
    )
    artifact_set_digest = sealed_stage_set_digest((artifact,))
    authority = MssqlGenerationAuthorityRefV2(
        authority_id=UUID(int=1),
        purpose=MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER,
        effect_key=identity.effect_key,
        source_snapshot_digest=D2,
        artifact_set_digest=artifact_set_digest,
        mutation_plan_digest=plan.digest,
        expected_writer_generation=None,
        candidate_writer_generation=1,
        expected_head_revision=None,
        candidate_head_revision=1,
        recovery_identity_digest=D3,
    )
    transition = MssqlBatchGenerationTransitionV3(
        effect_key=identity.effect_key,
        previous_writer_mode=None,
        expected_writer_generation=None,
        candidate_writer_generation=1,
        expected_head_revision=None,
        candidate_head_revision=1,
        payload_row_count=artifact.observed_row_count,
        source_snapshot_digest=D2,
        artifact_set_digest=artifact_set_digest,
        mutation_plan_digest=plan.digest,
        recovery_identity_digest=D3,
        authority_set=MssqlGenerationAuthoritySetV2((authority,)),
    )
    request = MssqlR1EffectRequestV3(
        identity=identity,
        registration_id=REGISTRATION,
        registration_payload_digest=_registration().payload_digest,
        registration_payload_bytes=_registration().canonical_bytes,
        verification_policy_digest=D1,
        admitted_at_server_time=NOW,
        source_snapshot_digest=D2,
        source_schema_digest=D2,
        artifacts=(artifact,),
        mutation_plan=plan,
        rendered_bundle=_rendered_bundle(plan),
        generation=transition,
    )
    attempt = MssqlR1EffectAttemptEnvelopeV3(request, 1, 1, D3, NOW + timedelta(minutes=1))

    assert artifact_set_digest.hex() == "edc70a2c67d6ea65a35d5090ca57a96c53b298cca48091fdd6ea34592d29f847"
    assert plan.digest.hex() == "77a542304787ccc51de1247fa4e2d9ef46ddc2ec28e8f2276a8790aa39693cdb"
    assert transition.digest.hex() == "a476107328684041c68fb3f0519856335a634642617c97c2e431be06914e294b"
    assert request.digest.hex() == "520691325ea1614e2b6cfd61d09ba29031f73f07fe79fd9a12c2d4e24050cb86"
    assert attempt.digest.hex() == "c91c872420e072117d01e81c91df109777e0ae9686499172967a37bfdcc99009"
    assert MssqlR1EffectRequestV3.from_canonical_bytes(request.canonical_bytes) == request
    assert MssqlR1EffectAttemptEnvelopeV3.from_canonical_bytes(attempt.canonical_bytes) == attempt
    assert replace(attempt, operation_epoch=2).request.digest == request.digest
    with pytest.raises(MssqlR1V3ContractError, match="artifact manifests"):
        replace(request, mutation_plan=replace(plan, stage_manifest=replace(artifact, artifact_id=UUID(int=99))))


def test_xmin_plan_and_revoked_registration_override_have_distinct_golden_domains() -> None:
    delta = _sealed_manifest(R1StageArtifactKindV1.XMIN_DELTA)
    keys = _sealed_manifest(
        R1StageArtifactKindV1.XMIN_COMPLETE_KEYS,
        artifact_id=UUID("20000000-0000-4000-8000-000000000004"),
    )
    plan = R1XminMutationPlanV1(
        effect_key=D1,
        delta_manifest=delta,
        complete_keys_manifest=keys,
        target_identity=_registration().physical_identity,
        ordered_target_columns=("order_id",),
        type_policy_digest=D2,
        hash_policy_digest=D1,
        probe_contract_digest=D3,
        checkpoint_state_key_digest=D2,
        checkpoint_writer_generation=4,
        previous_checkpoint_revision=7,
        previous_checkpoint_payload=b"xmin=41",
        previous_checkpoint_value=41,
        candidate_checkpoint_revision=8,
        checkpoint_candidate_payload=b"xmin=42",
        checkpoint_candidate_value=42,
        template_set=R1MutationTemplateSetV1.xmin_v1(),
        resource_limits=R1MutationResourceLimitsV1(100, 1_000_000, 2_000_000, 30),
        expected_writer_generation=4,
        candidate_writer_generation=4,
        expected_head_revision=7,
        candidate_head_revision=8,
        predecessor_receipt_id=UUID(int=9),
        predecessor_receipt_digest=D1,
        expected_writer_head_digest=writer_head_predecessor_digest(
            _registration().physical_identity, 4, 7, UUID(int=9), D1
        ),
    )
    override = MssqlRevokedRegistrationCompletionOverridePayloadV1(
        override_id=UUID("30000000-0000-4000-8000-000000000003"),
        target_binding_uuid=BINDING,
        effect_key=D1,
        registration_id=REGISTRATION,
        registration_payload_digest=D2,
        revocation_receipt_digest=D3,
        sealed_intent_digest=D1,
        artifact_set_digest=D2,
        mutation_plan_digest=plan.digest,
        expected_operation_epoch=4,
        expected_operation_projection_revision=7,
        expected_verification_policy_digest=D3,
        expires_at=NOW + timedelta(hours=1),
    )

    assert plan.digest.hex() == "60346cb15921cef15306cb2e4bef56a0dff76a0205e46e1f05fbeed49a715fe4"
    assert override.payload_digest.hex() == "3c6da9489a205e5093562806382d3ce5515255383afa903754bb7df6aa90f0c9"


def test_canonical_decoders_reject_legacy_truncated_trailing_and_tampered_bytes() -> None:
    registration = _registration()
    assert MssqlTargetRegistrationPayloadV1.from_canonical_bytes(registration.canonical_bytes) == registration
    domain = b"dpone-mssql-target-registration-v1\0"
    first_size = int.from_bytes(registration.canonical_bytes[len(domain) : len(domain) + 4], "big")
    first_value = registration.canonical_bytes[len(domain) + 4 : len(domain) + 4 + first_size]
    assert first_value == str(REGISTRATION).encode()
    for payload in (registration.canonical_bytes[:-1], registration.canonical_bytes + b"x"):
        with pytest.raises(MssqlR1V3ContractError):
            MssqlTargetRegistrationPayloadV1.from_canonical_bytes(payload)
    invalid_utf8 = bytearray(registration.canonical_bytes)
    invalid_utf8[len(domain) + 4] = 0xFF
    with pytest.raises(MssqlR1V3ContractError, match="valid UTF-8"):
        MssqlTargetRegistrationPayloadV1.from_canonical_bytes(bytes(invalid_utf8))
    request, attempt = _batch_request()
    for payload in (request.canonical_bytes[:-1], request.canonical_bytes + b"x"):
        with pytest.raises(MssqlR1V3ContractError):
            MssqlR1EffectRequestV3.from_canonical_bytes(payload)
    with pytest.raises(MssqlR1V3ContractError, match="unknown or legacy domain"):
        decode_canonical_bytes(b"dpone.mssql-effect-request.v2\0", b"dpone.mssql-effect-request.v3\0", field_count=0)
    changed = bytearray(attempt.canonical_bytes)
    changed[-1] ^= 1
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1EffectAttemptEnvelopeV3.from_canonical_bytes(bytes(changed))
    unknown_nested = request.canonical_bytes.replace(
        b"dpone-r1-batch-mutation-plan-v1\0", b"dpone-r1-batch-mutation-plan-v2\0", 1
    )
    with pytest.raises(MssqlR1V3ContractError, match="unknown or legacy domain"):
        MssqlR1EffectRequestV3.from_canonical_bytes(unknown_nested)
    quality = MssqlBatchQualityEvidenceV3(D3, 1, 1, 1, 1, 1, 1, 1, 1)
    unknown_mode = quality.canonical_bytes.replace(b"batch_full_refresh", b"unknown_mode_value", 1)
    with pytest.raises(MssqlR1V3ContractError, match="source_mode is unsupported"):
        MssqlBatchQualityEvidenceV3.from_canonical_bytes(unknown_mode)


def test_stage_artifact_uses_unsigned_order_and_rejects_duplicates() -> None:
    rows = tuple(_stage_row(R1StageArtifactKindV1.BATCH_PAYLOAD, key, b"row") for key in (b"\x7f", b"\x80", b"\xff"))
    digest, size = artifact_digest_for_rows(R1StageArtifactKindV1.BATCH_PAYLOAD, D1, rows)
    assert len(digest) == 32 and size > 0
    for invalid in ((rows[1], rows[0]), (rows[0], rows[0])):
        with pytest.raises(MssqlR1V3ContractError, match="strict unsigned key order"):
            artifact_digest_for_rows(R1StageArtifactKindV1.BATCH_PAYLOAD, D1, invalid)
    with pytest.raises(MssqlR1V3ContractError, match="uint32"):
        replace(_open_plan(R1StageArtifactKindV1.BATCH_PAYLOAD), maximum_key_bytes=2**32)


def test_signed_authority_is_one_exact_payload_and_commands_do_not_confer_trust() -> None:
    authority_set = MssqlGenerationAuthoritySetV2(
        (
            _authority(
                MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER, expected_generation=None, candidate_generation=1
            ),
        )
    )
    payload = MssqlGenerationAuthoritySetIssuancePayloadV1(
        UUID(int=20),
        BINDING,
        REGISTRATION,
        _registration().payload_digest,
        0,
        NOW,
        NOW + timedelta(hours=1),
        b"a" * 16,
        authority_set.canonical_bytes,
        authority_set.digest,
    )
    assert MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(payload.canonical_bytes) == payload
    verification = MssqlSignedPayloadVerificationV1(
        MssqlSignedPayloadKindV1.GENERATION_AUTHORITY_SET,
        payload.payload_digest,
        D1,
        D2,
        D3,
        D1,
        "cosign-v2.4",
        NOW,
        payload.canonical_bytes,
    )
    assert MssqlSignedPayloadVerificationV1.from_canonical_bytes(verification.canonical_bytes) == verification
    assert payload.payload_digest.hex() == "71da04bc4e4af148944b5ed350a64bcad068398a47397f86b13d8e85a080459b"
    assert verification.receipt_digest.hex() == "2f4f584ef1343faebbcd422fdfddce298d5405d89acc7b2be870820105ade213"
    assert SignedGenerationAuthoritySetCommandV1(payload.canonical_bytes, b"detached-bundle").payload_bytes == (
        payload.canonical_bytes
    )
    command = SignedGenerationAuthoritySetCommandV1(payload.canonical_bytes, b"detached-bundle")
    bound = replace(verification, signature_bundle_digest=hashlib.sha256(command.sigstore_bundle).digest())
    bound.validate_for_command(command)
    with pytest.raises(MssqlR1V3ContractError, match="command"):
        bound.validate_for_command(SignedGenerationAuthoritySetCommandV1(payload.canonical_bytes, b"other-bundle"))
    other_payload = replace(payload, nonce=b"b" * 16)
    with pytest.raises(MssqlR1V3ContractError, match="command"):
        bound.validate_for_command(
            SignedGenerationAuthoritySetCommandV1(other_payload.canonical_bytes, command.sigstore_bundle)
        )
    with pytest.raises(MssqlR1V3ContractError, match="exact non-empty set"):
        replace(payload, authority_set_digest=D1)
    with pytest.raises(MssqlR1V3ContractError, match="wrong payload domain"):
        SignedTargetRegistrationCommandV1(payload.canonical_bytes, b"bundle")
    with pytest.raises(MssqlR1V3ContractError, match="canonical identifier"):
        replace(_registration(), database_name="other")
    physical = _registration().physical_identity
    renamed = replace(
        physical,
        database_name="other",
        database_name_digest=canonical_identifier_digest("other"),
    )
    assert renamed.physical_object_coordinate_digest == physical.physical_object_coordinate_digest
    assert renamed.registered_physical_authority_digest != physical.registered_physical_authority_digest


def test_sealed_request_rejects_empty_authority_count_bypass() -> None:
    request, _ = _batch_request()
    artifact = request.artifacts[0]
    digest, payload_bytes = artifact_digest_for_rows(R1StageArtifactKindV1.BATCH_PAYLOAD, D2, ())
    evidence = R1TypedStageScanEvidenceV1(R1StageArtifactKindV1.BATCH_PAYLOAD, digest, 0, payload_bytes, 0, 0, 0, 0, D3)
    empty = replace(
        artifact,
        observed_row_count=0,
        observed_payload_bytes=payload_bytes,
        artifact_digest=digest,
        typed_scan_evidence=evidence,
    )
    plan = replace(request.mutation_plan, stage_manifest=empty)
    with pytest.raises(MssqlR1V3ContractError, match="payload count differs"):
        replace(request, artifacts=(empty,), mutation_plan=plan)


def test_xmin_request_rejects_empty_complete_keys_count_bypass() -> None:
    identity = MssqlR1EffectIdentityV3(D1, "xmin-bypass", SourceMode.XMIN_CURRENT_STATE, BINDING)
    delta = _sealed_manifest(R1StageArtifactKindV1.XMIN_DELTA, effect_key=identity.effect_key)
    keys = _sealed_manifest(
        R1StageArtifactKindV1.XMIN_COMPLETE_KEYS,
        effect_key=identity.effect_key,
        artifact_id=UUID(int=40),
    )
    digest, payload_bytes = artifact_digest_for_rows(R1StageArtifactKindV1.XMIN_COMPLETE_KEYS, D2, ())
    empty_keys = replace(
        keys,
        observed_row_count=0,
        observed_payload_bytes=payload_bytes,
        artifact_digest=digest,
        typed_scan_evidence=R1TypedStageScanEvidenceV1(
            R1StageArtifactKindV1.XMIN_COMPLETE_KEYS, digest, 0, payload_bytes, 0, 0, 0, 0, D3
        ),
    )
    plan = R1XminMutationPlanV1(
        identity.effect_key,
        delta,
        empty_keys,
        _registration().physical_identity,
        ("order_id",),
        D2,
        D1,
        D3,
        D2,
        4,
        7,
        b"xmin=41",
        41,
        8,
        b"xmin=42",
        42,
        R1MutationTemplateSetV1.xmin_v1(),
        R1MutationResourceLimitsV1(100, 1_000_000, 2_000_000, 30),
        4,
        4,
        7,
        8,
        UUID(int=9),
        D1,
        writer_head_predecessor_digest(_registration().physical_identity, 4, 7, UUID(int=9), D1),
    )
    artifact_set = sealed_stage_set_digest((delta, empty_keys))
    transition = MssqlXminGenerationTransitionV3(
        identity.effect_key,
        SourceMode.XMIN_CURRENT_STATE,
        4,
        4,
        7,
        8,
        1,
        D2,
        artifact_set,
        plan.digest,
        D3,
        MssqlGenerationAuthoritySetV2(),
    )
    with pytest.raises(MssqlR1V3ContractError, match="complete-key count differs"):
        MssqlR1EffectRequestV3(
            identity,
            REGISTRATION,
            _registration().payload_digest,
            _registration().canonical_bytes,
            D1,
            NOW,
            D2,
            D2,
            (delta, empty_keys),
            plan,
            _rendered_bundle(plan),
            transition,
        )


def test_v3_receipt_and_replay_bind_request_quality_artifacts_and_authorities() -> None:
    request, attempt = _batch_request()
    quality = MssqlBatchQualityEvidenceV3(D3, 2, 2, 2, 2, 2, 2, 2, 2)
    body = MssqlBatchEffectReceiptBodyV3(
        D2,
        D2,
        request.artifacts[0].canonical_bytes,
        request.artifacts[0].manifest_digest,
        2,
        0,
        2,
        quality.canonical_bytes,
        quality.digest,
    )
    receipt = build_receipt_v3(
        attempt,
        body,
        committed_at=NOW + timedelta(minutes=2),
        admitted_authorities=_admitted(request),
    )
    artifacts, authorities = _resources(request, R1ReplayResourceStateV3.CONSUMED, receipt)
    proof = CommittedEffectReplayProofV3(
        _proof_request(attempt),
        request,
        receipt,
        _operation(attempt, R1ReplayOperationStateV3.COMMITTED, receipt),
        WriterHeadReplayObservationV3(1, 1, 1, receipt.header.receipt_id, receipt.digest),
        artifacts,
        authorities,
    )
    assert receipt.header.effect_key == request.identity.effect_key
    assert receipt.header.source_authority_sha256 == _registration().route_source_authority_sha256
    assert receipt.header.body_digest == body.digest
    assert str(receipt.header.receipt_id) == "e585f6c1-5a48-08dd-e2c2-081f6b781f67"
    assert receipt.header.digest.hex() == "e9795e570d4bb66b00823946e748cb41f3715c9b772256708223b48788128701"
    assert body.digest.hex() == "1bacbd88af11eb1658c09201b52826b6d5a561a93e651b37d018bbd4c4b6eed6"
    assert receipt.digest.hex() == "0a79707643bf95dbab3906555e780554060998e59135c51924cebc6ffcd84608"
    assert MssqlR1EffectReceiptV3.from_canonical_bytes(receipt.canonical_bytes).header == receipt.header
    assert type(receipt.header).from_canonical_bytes(receipt.header.canonical_bytes) == receipt.header
    assert type(body).from_canonical_bytes(body.canonical_bytes) == body
    assert MssqlR1EffectReceiptV3.from_canonical_bytes(receipt.canonical_bytes) == receipt
    assert proof.receipt == receipt
    assert CommittedEffectReplayProofV3.from_canonical_bytes(proof.canonical_bytes) == proof
    assert proof.digest.hex() == "f83e4258cb66c59a198c2cb333b3678d4916b480b0bc888384628510b6bfa467"
    descendant = _batch_descendant_receipt(receipt)
    descendant_proof = replace(
        proof,
        head=WriterHeadReplayObservationV3(
            2,
            2,
            1,
            descendant.header.receipt_id,
            descendant.digest,
            (ReceiptDescendantLinkV3(descendant),),
        ),
    )
    assert CommittedEffectReplayProofV3.from_canonical_bytes(descendant_proof.canonical_bytes) == descendant_proof
    assert descendant_proof.canonical_bytes.startswith(b"dpone-r1-effect-replay-proof-v3\0")
    assert descendant_proof.digest.hex() == "c47a3c99aa1def1d2b6c377d82dcfbd8fa0bb62c99e674240911c065a280931e"
    with pytest.raises(MssqlR1V3ContractError, match="exact effect"):
        replace(proof, head=replace(proof.head, target_generation=9, row_hash_generation=9, head_revision=999))
    assert _proof_request(attempt).canonical_bytes
    with pytest.raises(MssqlR1V3ContractError, match="exact typed body"):
        replace(receipt, body=replace(body, target_row_count_before=1))
    with pytest.raises(MssqlR1V3ContractError, match="manifest"):
        build_receipt_v3(
            attempt,
            replace(body, manifest_digest=D1),
            committed_at=NOW + timedelta(minutes=2),
            admitted_authorities=_admitted(request),
        )
    staged, _ = _resources(request, R1ReplayResourceStateV3.SEALED)
    refs = request.authority_set.refs
    issued = AuthoritySetReplayObservationV3(
        UUID(int=80),
        _admitted(request).values.issuance_payload,
        _admitted(request).values.issuance_payload_digest,
        _admitted(request).values.verification_payload,
        _admitted(request).values.verification_receipt_digest,
        R1ReplayResourceStateV3.ISSUED,
        None,
        None,
        tuple(item.authority_id for item in refs),
        tuple(item.digest for item in refs),
        (R1ReplayResourceStateV3.ISSUED,) * len(refs),
        (None,) * len(refs),
        (None,) * len(refs),
    )
    known_absent = KnownNotCommittedEffectReplayProofV3(
        _proof_request(attempt),
        request,
        True,
        _operation(attempt, R1ReplayOperationStateV3.SEALED),
        None,
        staged,
        issued,
    )
    unknown = UnknownEffectReplayProofV3(_proof_request(attempt), R1UnknownReplayReasonV3.AMBIGUOUS_RECEIPT, D3)
    assert known_absent.receipt_absent and unknown.reason_code is R1UnknownReplayReasonV3.AMBIGUOUS_RECEIPT
    assert KnownNotCommittedEffectReplayProofV3.from_canonical_bytes(known_absent.canonical_bytes) == known_absent
    assert UnknownEffectReplayProofV3.from_canonical_bytes(unknown.canonical_bytes) == unknown
    assert known_absent.digest.hex() == "509ce703e308fe4d6d6bec12ca8f3f5b82ebe5d0cdf24a76f64ca582b862103b"
    assert unknown.digest.hex() == "45a7f8c6fc0472ada0b64420b65fbf44095072cccde2fc18b19f59c079fccdfe"
    with pytest.raises(MssqlR1V3ContractError, match="artifact replay observation"):
        replace(
            proof,
            artifacts=replace(
                proof.artifacts,
                consuming_receipt_digests=(D1,) * len(proof.artifacts.artifact_ids),
            ),
        )
    issuance = MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(
        issued.issuance_payload  # type: ignore[arg-type]
    )
    verification = MssqlSignedPayloadVerificationV1.from_canonical_bytes(
        issued.verification_payload  # type: ignore[arg-type]
    )
    foreign_issuance = replace(issuance, registration_id=UUID(int=999))
    foreign_verification = replace(
        verification,
        payload_digest=foreign_issuance.payload_digest,
        payload_bytes=foreign_issuance.canonical_bytes,
    )
    with pytest.raises(MssqlR1V3ContractError, match="not exact authority"):
        replace(
            known_absent,
            authorities=replace(
                issued,
                issuance_id=foreign_issuance.issuance_id,
                issuance_payload=foreign_issuance.canonical_bytes,
                issuance_payload_digest=foreign_issuance.payload_digest,
                verification_payload=foreign_verification.canonical_bytes,
                verification_receipt_digest=foreign_verification.receipt_digest,
            ),
        )


def test_transaction_authority_stage_consumption_and_candidate_proof_are_exact() -> None:
    request, attempt = _batch_request()
    binding = _transaction_binding()
    admitted = _admitted(request, binding)
    quality = MssqlBatchQualityEvidenceV3(D3, 2, 2, 2, 2, 2, 2, 2, 2)
    manifest = request.artifacts[0]
    body = MssqlBatchEffectReceiptBodyV3(
        D2,
        D2,
        manifest.canonical_bytes,
        manifest.manifest_digest,
        2,
        0,
        2,
        quality.canonical_bytes,
        quality.digest,
    )
    receipt = build_receipt_v3(
        attempt,
        body,
        committed_at=NOW + timedelta(minutes=2),
        admitted_authorities=admitted,
    )
    consumed_authority = MssqlConsumedGenerationAuthoritySetV3.from_admitted(
        admitted,
        receipt.header.receipt_id,
        receipt.digest,
    )
    consumed_stage = MssqlConsumedSealedStageSetV3(
        binding.transaction_id,
        binding.session_identity_digest,
        request.identity.effect_key,
        request.digest,
        receipt.header.receipt_id,
        receipt.digest,
        (manifest.artifact_id,),
        (manifest.artifact_kind,),
        (manifest.canonical_bytes,),
        (manifest.manifest_digest,),
        (NOW,),
        (NOW + timedelta(days=1),),
        (NOW + timedelta(minutes=2),),
        (2,),
        (MssqlStageConsumptionStateV3.CONSUMED,),
    )
    operation = MssqlCandidateOperationObservationV3(
        request.identity.operation_key,
        request.identity.effect_key,
        attempt.operation_epoch,
        attempt.operation_projection_revision,
        attempt.owner_id_digest,
        attempt.server_lease_expires_at,
        request.digest,
        receipt.header.receipt_id,
        receipt.digest,
    )
    head = MssqlCandidateWriterHeadObservationV3(
        1,
        1,
        1,
        receipt.header.receipt_id,
        receipt.digest,
        request.identity.effect_key,
        attempt.operation_epoch,
        attempt.operation_projection_revision,
    )
    proof = MssqlCandidateEffectProofV3(
        binding,
        request.digest,
        attempt.canonical_bytes,
        attempt.digest,
        receipt.canonical_bytes,
        receipt.digest,
        operation,
        head,
        1,
        1,
        None,
        consumed_stage,
        consumed_authority,
        None,
        request.registration_id,
        request.registration_payload_digest,
        request.mutation_plan.target_identity.registered_physical_authority_digest,
        D3,
        1,
    )

    assert MssqlAdmittedGenerationAuthoritySetV3.from_canonical_bytes(admitted.canonical_bytes) == admitted
    assert (
        MssqlConsumedGenerationAuthoritySetV3.from_canonical_bytes(consumed_authority.canonical_bytes)
        == consumed_authority
    )
    assert MssqlConsumedSealedStageSetV3.from_canonical_bytes(consumed_stage.canonical_bytes) == consumed_stage
    assert MssqlCandidateEffectProofV3.from_canonical_bytes(proof.canonical_bytes) == proof
    proof.assert_for(binding, attempt, receipt)
    assert admitted.digest.hex() == "52984483e2192b8a40b3380840564c8d91835e0335e89ca065e4f82cbea5af89"
    assert consumed_authority.digest.hex() == "0de79f5dd6551d836511d0f4379ce51e0cbc721ad48630dbdb3e2023f50a631b"
    assert consumed_stage.digest.hex() == "56df351f18040c6f7bba3d73bc3c84a8bd3bc221f1445f7f7027268800159d76"
    assert proof.digest.hex() == "5cc98e16aa4b4dfad84af853334be5e2ad2caff8ecd8d853169391cb21259e7e"
    issuance = MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(
        admitted.values.issuance_payload  # type: ignore[arg-type]
    )
    verification = MssqlSignedPayloadVerificationV1.from_canonical_bytes(
        admitted.values.verification_payload  # type: ignore[arg-type]
    )
    expired_issuance = replace(
        issuance,
        issued_at=NOW - timedelta(days=2),
        expires_at=NOW - timedelta(days=1),
    )
    expired_verification = replace(
        verification,
        payload_digest=expired_issuance.payload_digest,
        payload_bytes=expired_issuance.canonical_bytes,
    )
    expired_admitted = replace(
        admitted,
        values=replace(
            admitted.values,
            issuance_payload=expired_issuance.canonical_bytes,
            issuance_payload_digest=expired_issuance.payload_digest,
            verification_payload=expired_verification.canonical_bytes,
            verification_receipt_digest=expired_verification.receipt_digest,
        ),
    )
    with pytest.raises(MssqlR1V3ContractError, match="not valid at target admission"):
        expired_admitted.validate_for_attempt(attempt)
    with pytest.raises(MssqlR1V3ContractError, match="ACTIVE"):
        replace(proof, transaction_binding=binding.transitioned(MssqlR1TransactionStateV3.ROLLED_BACK))
    with pytest.raises(MssqlR1V3ContractError, match="writer-head"):
        replace(proof, writer_head_observation=replace(head, head_revision=2))
    with pytest.raises(MssqlR1V3ContractError, match="attempt differs"):
        replace(proof, attempt_payload=replace(attempt, owner_id_digest=D1).canonical_bytes)
    replaced_owner_attempt = replace(attempt, owner_id_digest=D1)
    replaced_owner_proof = replace(
        proof,
        attempt_payload=replaced_owner_attempt.canonical_bytes,
        attempt_digest=replaced_owner_attempt.digest,
        operation_observation=replace(operation, owner_id_digest=D1),
    )
    with pytest.raises(MssqlR1V3ContractError, match="trusted attempt"):
        replaced_owner_proof.assert_for(binding, attempt, receipt)
    replaced_lease_attempt = replace(attempt, server_lease_expires_at=NOW + timedelta(minutes=5))
    replaced_lease_proof = replace(
        proof,
        attempt_payload=replaced_lease_attempt.canonical_bytes,
        attempt_digest=replaced_lease_attempt.digest,
        operation_observation=replace(
            operation,
            server_lease_expires_at=replaced_lease_attempt.server_lease_expires_at,
        ),
    )
    with pytest.raises(MssqlR1V3ContractError, match="trusted attempt"):
        replaced_lease_proof.assert_for(binding, attempt, receipt)
    with pytest.raises(MssqlR1V3ContractError, match="owner/lease"):
        replace(proof, operation_observation=replace(operation, owner_id_digest=D1))
    with pytest.raises(MssqlR1V3ContractError, match="owner/lease"):
        replace(
            proof,
            operation_observation=replace(
                operation,
                server_lease_expires_at=NOW + timedelta(minutes=5),
            ),
        )
    with pytest.raises(MssqlR1V3ContractError, match="trusted transaction"):
        proof.assert_for(replace(binding, factory_binding_token=b"x" * 16), attempt, receipt)
    other_receipt = build_receipt_v3(
        attempt,
        body,
        committed_at=NOW + timedelta(minutes=3),
        admitted_authorities=admitted,
    )
    with pytest.raises(MssqlR1V3ContractError, match="trusted receipt"):
        proof.assert_for(binding, attempt, other_receipt)
    with pytest.raises(MssqlR1V3ContractError, match="generation differs"):
        replace(
            proof,
            target_generation=2,
            row_hash_generation=2,
            writer_head_observation=replace(head, target_generation=2, row_hash_generation=2),
        )
    other_manifest = _sealed_manifest(
        R1StageArtifactKindV1.BATCH_PAYLOAD,
        effect_key=request.identity.effect_key,
        artifact_id=manifest.artifact_id,
        row_keys=(b"other-1", b"other-2"),
    )
    wrong_stage = replace(
        consumed_stage,
        manifest_payloads=(other_manifest.canonical_bytes,),
        manifest_digests=(other_manifest.manifest_digest,),
    )
    with pytest.raises(MssqlR1V3ContractError, match="receipt body"):
        replace(proof, consumed_artifact_set=wrong_stage)
    with pytest.raises(MssqlR1V3ContractError, match="partial"):
        replace(consumed_stage, projection_revisions=())
    with pytest.raises(MssqlR1V3ContractError, match="timestamps"):
        replace(consumed_stage, consumed_at=(NOW + timedelta(days=2),))
    with pytest.raises(MssqlR1V3ContractError, match="projection revisions"):
        replace(admitted, values=replace(admitted.values, ref_projection_revisions=()))
    with pytest.raises(MssqlR1V3ContractError, match="issuance identity"):
        replace(
            receipt.header,
            generation_authority_issuance_payload_digest=None,
        )
    with pytest.raises(MssqlR1V3ContractError, match="issuance discriminator"):
        replace(
            receipt.header,
            generation_authority_issuance_id=None,
            generation_authority_issuance_payload_digest=None,
            generation_authority_verification_receipt_digest=None,
        )
    with pytest.raises(MssqlR1V3ContractError, match="issuance discriminator"):
        replace(receipt.header, authority_set_digest=MssqlGenerationAuthoritySetV2().digest)


def test_xmin_v3_receipt_binds_exact_metrics_checkpoint_and_replay_proof() -> None:
    identity = MssqlR1EffectIdentityV3(D1, "xmin-receipt", SourceMode.XMIN_CURRENT_STATE, BINDING)
    delta = _sealed_manifest(R1StageArtifactKindV1.XMIN_DELTA, effect_key=identity.effect_key)
    keys = _sealed_manifest(
        R1StageArtifactKindV1.XMIN_COMPLETE_KEYS,
        effect_key=identity.effect_key,
        artifact_id=UUID(int=60),
    )
    plan = R1XminMutationPlanV1(
        identity.effect_key,
        delta,
        keys,
        _registration().physical_identity,
        ("order_id",),
        D2,
        D1,
        D3,
        D2,
        1,
        None,
        None,
        None,
        1,
        b"xmin=42",
        42,
        R1MutationTemplateSetV1.xmin_v1(),
        R1MutationResourceLimitsV1(100, 1_000_000, 2_000_000, 30),
        None,
        1,
        None,
        1,
    )
    artifact_set = sealed_stage_set_digest((delta, keys))
    authority = MssqlGenerationAuthorityRefV2(
        UUID(int=1),
        MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER,
        identity.effect_key,
        D2,
        artifact_set,
        plan.digest,
        None,
        1,
        None,
        1,
        D3,
    )
    transition = MssqlXminGenerationTransitionV3(
        identity.effect_key,
        None,
        None,
        1,
        None,
        1,
        2,
        D2,
        artifact_set,
        plan.digest,
        D3,
        MssqlGenerationAuthoritySetV2((authority,)),
    )
    request = MssqlR1EffectRequestV3(
        identity,
        REGISTRATION,
        _registration().payload_digest,
        _registration().canonical_bytes,
        D1,
        NOW,
        D2,
        D2,
        (delta, keys),
        plan,
        _rendered_bundle(plan),
        transition,
    )
    attempt = MssqlR1EffectAttemptEnvelopeV3(request, 1, 1, D3, NOW + timedelta(minutes=1))
    quality = MssqlXminQualityEvidenceV3(D3, 2, 2, 2, 2, 0, 0, 0, 2, 2, 2, 0, 2, 0, 0, 0, 0, 0, 0, True)
    body = MssqlXminEffectReceiptBodyV3(
        D2,
        D2,
        delta.canonical_bytes,
        delta.manifest_digest,
        keys.canonical_bytes,
        keys.manifest_digest,
        2,
        2,
        0,
        0,
        0,
        None,
        None,
        plan.checkpoint_candidate_payload,
        plan.checkpoint_candidate_value,
        quality.canonical_bytes,
        quality.digest,
    )
    receipt = build_receipt_v3(
        attempt,
        body,
        committed_at=NOW + timedelta(minutes=2),
        admitted_authorities=_admitted(request),
    )
    assert type(body).from_canonical_bytes(body.canonical_bytes) == body
    assert MssqlR1EffectReceiptV3.from_canonical_bytes(receipt.canonical_bytes) == receipt
    artifacts, authorities = _resources(request, R1ReplayResourceStateV3.CONSUMED, receipt)
    proof = CommittedEffectReplayProofV3(
        _proof_request(attempt),
        request,
        receipt,
        _operation(attempt, R1ReplayOperationStateV3.COMMITTED, receipt),
        WriterHeadReplayObservationV3(1, 1, 1, receipt.header.receipt_id, receipt.digest),
        artifacts,
        authorities,
        CheckpointReplayObservationV3(
            1,
            plan.candidate_checkpoint_revision,
            plan.checkpoint_candidate_payload,
            receipt.header.receipt_id,
        ),
    )
    assert proof.receipt == receipt
    assert CommittedEffectReplayProofV3.from_canonical_bytes(proof.canonical_bytes) == proof
    assert proof.checkpoint is not None
    assert proof.checkpoint.canonical_bytes.startswith(b"dpone-r1-effect-replay-observation-v3\0checkpoint\0")
    assert proof.digest.hex() == "5de37e1d987ed247519bb3fc59031330118c48c32adf53b2a12acaac3338e880"
    with pytest.raises(MssqlR1V3ContractError, match="metrics"):
        replace(body, updated_count=1)
    with pytest.raises(MssqlR1V3ContractError, match="checkpoint"):
        replace(proof, checkpoint=replace(proof.checkpoint, checkpoint_payload=b"xmin=41"))

    binding = _transaction_binding()
    admitted = _admitted(request, binding)
    consumed_authority = MssqlConsumedGenerationAuthoritySetV3.from_admitted(
        admitted,
        receipt.header.receipt_id,
        receipt.digest,
    )
    consumed_stage = MssqlConsumedSealedStageSetV3(
        binding.transaction_id,
        binding.session_identity_digest,
        request.identity.effect_key,
        request.digest,
        receipt.header.receipt_id,
        receipt.digest,
        tuple(item.artifact_id for item in request.artifacts),
        tuple(item.artifact_kind for item in request.artifacts),
        tuple(item.canonical_bytes for item in request.artifacts),
        tuple(item.manifest_digest for item in request.artifacts),
        (NOW,) * len(request.artifacts),
        (NOW + timedelta(days=1),) * len(request.artifacts),
        (NOW + timedelta(minutes=2),) * len(request.artifacts),
        tuple(range(1, len(request.artifacts) + 1)),
        (MssqlStageConsumptionStateV3.CONSUMED,) * len(request.artifacts),
    )
    operation = MssqlCandidateOperationObservationV3(
        request.identity.operation_key,
        request.identity.effect_key,
        attempt.operation_epoch,
        attempt.operation_projection_revision,
        attempt.owner_id_digest,
        attempt.server_lease_expires_at,
        request.digest,
        receipt.header.receipt_id,
        receipt.digest,
    )
    head = MssqlCandidateWriterHeadObservationV3(
        1,
        1,
        1,
        receipt.header.receipt_id,
        receipt.digest,
        request.identity.effect_key,
        attempt.operation_epoch,
        attempt.operation_projection_revision,
    )
    candidate = MssqlCandidateEffectProofV3(
        binding,
        request.digest,
        attempt.canonical_bytes,
        attempt.digest,
        receipt.canonical_bytes,
        receipt.digest,
        operation,
        head,
        1,
        1,
        CheckpointReplayObservationV3(
            1,
            plan.candidate_checkpoint_revision,
            plan.checkpoint_candidate_payload,
            receipt.header.receipt_id,
        ),
        consumed_stage,
        consumed_authority,
        None,
        request.registration_id,
        request.registration_payload_digest,
        request.mutation_plan.target_identity.registered_physical_authority_digest,
        D3,
        1,
    )
    assert MssqlCandidateEffectProofV3.from_canonical_bytes(candidate.canonical_bytes) == candidate
    with pytest.raises(MssqlR1V3ContractError, match="checkpoint differs"):
        replace(
            candidate,
            checkpoint_observation=replace(
                candidate.checkpoint_observation,
                checkpoint_revision=plan.candidate_checkpoint_revision + 1,
            ),
        )


def test_xmin_v3_receipt_accepts_deleted_keys_outside_delta_manifest() -> None:
    identity = MssqlR1EffectIdentityV3(D1, "xmin-delete-receipt", SourceMode.XMIN_CURRENT_STATE, BINDING)
    delta = _sealed_manifest(
        R1StageArtifactKindV1.XMIN_DELTA,
        effect_key=identity.effect_key,
        row_keys=tuple(f"d{i:02}".encode() for i in range(8)),
    )
    keys = _sealed_manifest(
        R1StageArtifactKindV1.XMIN_COMPLETE_KEYS,
        effect_key=identity.effect_key,
        artifact_id=UUID(int=61),
        row_keys=tuple(f"k{i:02}".encode() for i in range(13)),
    )
    plan = R1XminMutationPlanV1(
        identity.effect_key,
        delta,
        keys,
        _registration().physical_identity,
        ("order_id",),
        D2,
        D1,
        D3,
        D2,
        1,
        None,
        None,
        None,
        1,
        b"xmin=43",
        43,
        R1MutationTemplateSetV1.xmin_v1(),
        R1MutationResourceLimitsV1(100, 1_000_000, 2_000_000, 30),
        None,
        1,
        None,
        1,
    )
    artifact_set = sealed_stage_set_digest((delta, keys))
    authority = MssqlGenerationAuthorityRefV2(
        UUID(int=1),
        MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER,
        identity.effect_key,
        D2,
        artifact_set,
        plan.digest,
        None,
        1,
        None,
        1,
        D3,
    )
    transition = MssqlXminGenerationTransitionV3(
        identity.effect_key,
        None,
        None,
        1,
        None,
        1,
        13,
        D2,
        artifact_set,
        plan.digest,
        D3,
        MssqlGenerationAuthoritySetV2((authority,)),
    )
    request = MssqlR1EffectRequestV3(
        identity,
        REGISTRATION,
        _registration().payload_digest,
        _registration().canonical_bytes,
        D1,
        NOW,
        D2,
        D2,
        (delta, keys),
        plan,
        _rendered_bundle(plan),
        transition,
    )
    quality = MssqlXminQualityEvidenceV3(D3, 10, 8, 8, 8, 2, 2, 2, 8, 13, 13, 10, 5, 2, 1, 6, 0, 0, 0, True)
    body = MssqlXminEffectReceiptBodyV3(
        D2,
        D2,
        delta.canonical_bytes,
        delta.manifest_digest,
        keys.canonical_bytes,
        keys.manifest_digest,
        10,
        5,
        2,
        2,
        1,
        None,
        None,
        b"xmin=43",
        43,
        quality.canonical_bytes,
        quality.digest,
    )
    attempt = MssqlR1EffectAttemptEnvelopeV3(request, 1, 1, D3, NOW + timedelta(minutes=1))
    receipt = build_receipt_v3(
        attempt,
        body,
        committed_at=NOW + timedelta(minutes=2),
        admitted_authorities=_admitted(request),
    )

    assert body.deleted_count == quality.expected_absent_keys == 2
    assert delta.observed_row_count == quality.expected_present_keys == 8
    assert quality.affected_key_count == delta.observed_row_count + body.deleted_count == 10
    assert MssqlR1EffectReceiptV3.from_canonical_bytes(receipt.canonical_bytes) == receipt
    assert quality.digest.hex() == "c85edce706e0f48a319d761b0b7caa00e02ce12d3985f3db590dda975285f119"
    assert body.digest.hex() == "c3bf544bd67cc64497acf95533a7803b3d51283ccd2ed6bfc2c3c01f58a70d2c"
    assert receipt.digest.hex() == "1765abd787740e245d3c86a9b70a28e17595eae309cdb7f020a908b3200f69ac"
    with pytest.raises(MssqlR1V3ContractError, match="metrics"):
        replace(body, deleted_count=1)
    with pytest.raises(MssqlR1V3ContractError, match="metrics"):
        replace(body, affected_count=9)
    wrong_delta = _sealed_manifest(
        R1StageArtifactKindV1.XMIN_DELTA,
        effect_key=identity.effect_key,
        row_keys=tuple(f"d{i:02}".encode() for i in range(10)),
    )
    with pytest.raises(MssqlR1V3ContractError, match="metrics"):
        replace(
            body,
            delta_manifest_bytes=wrong_delta.canonical_bytes,
            delta_manifest_digest=wrong_delta.manifest_digest,
        )


def test_open_recovery_preserves_uninitialized_or_exact_business_head() -> None:
    key = build_open_stage_recovery_effect_key(D1, D2, D3, 1, 1)
    receipt = MssqlOpenStageRecoveryReceiptV1(
        UUID(int=50),
        D2,
        key,
        D1,
        D3,
        (UUID(int=1),),
        1,
        2,
        1,
        2,
        None,
        None,
        (UUID(int=2),),
        D1,
        NOW,
    )
    assert receipt.committed_operation_projection_revision == 2
    with pytest.raises(MssqlR1V3ContractError, match="positive"):
        build_open_stage_recovery_effect_key(D1, D2, D3, 1, None)  # type: ignore[arg-type]
    with pytest.raises(MssqlR1V3ContractError, match="positive"):
        replace(receipt, expected_operation_projection_revision=None)  # type: ignore[arg-type]
    with pytest.raises(MssqlR1V3ContractError, match="cannot change"):
        replace(receipt, expected_writer_head_digest=D1, observed_writer_head_digest=D2)


def test_r1_v3_ports_expose_typed_atomic_provider_closure() -> None:
    from dpone.ports import mssql_r1_v3 as ports

    required = {
        ports.MssqlR1StageWriterPort: {"register_open", "write", "renew", "seal", "recover_expired_open"},
        ports.MssqlTargetAuthorityPreparationV3Port: {"pre_source", "seal", "load_sealed", "take_over_sealed"},
        ports.MssqlXminMutationV3Port: {"apply_delta", "update_row_hashes", "write_checkpoint"},
        ports.MssqlV3EffectReceiptStorePort: {"append", "probe_fresh"},
        ports.MssqlTargetWriterFenceV3Port: {"admit", "advance_head"},
    }
    for protocol, members in required.items():
        assert members <= set(protocol.__dict__)
    assert "object" not in str(ports.PostgresMssqlR1SealedEffectRunnerPort.run_sealed.__annotations__)


def test_stage_rows_bind_typed_cells_canonical_payload_and_stored_hash() -> None:
    cell = R1TypedStageCellV1(1, "pg.int8-mssql.bigint.v1", R1StageCellStateV1.VALUE, b"int8:1")
    row_payload = b"row:1"
    row = R1TypedStageRowV1(
        R1StageArtifactKindV1.BATCH_PAYLOAD,
        (cell,),
        b"key:1",
        row_payload,
        hashlib.sha256(row_payload).digest(),
    )
    assert row.canonical_row_hash == hashlib.sha256(row_payload).digest()
    with pytest.raises(MssqlR1V3ContractError, match="stored row hash"):
        replace(row, canonical_row_hash=D1)
    with pytest.raises(MssqlR1V3ContractError, match="complete-keys"):
        replace(row, artifact_kind=R1StageArtifactKindV1.XMIN_COMPLETE_KEYS)


def test_stage_cell_codec_distinguishes_null_empty_text_and_empty_bytea() -> None:
    cells = (
        R1TypedStageCellV1(1, "pg.text-mssql.nvarchar.v1", R1StageCellStateV1.NULL, b""),
        R1TypedStageCellV1(1, "pg.text-mssql.nvarchar.v1", R1StageCellStateV1.VALUE, b""),
        R1TypedStageCellV1(1, "pg.bytea-mssql.varbinary.v1", R1StageCellStateV1.VALUE, b""),
    )
    encoded = tuple(cell.canonical_bytes for cell in cells)
    assert len(set(encoded)) == 3
    assert tuple(R1TypedStageCellV1.from_canonical_bytes(value) for value in encoded) == cells
    assert tuple(hashlib.sha256(value).hexdigest() for value in encoded) == (
        "5a0b3198b1dbd2ba7ff478dddada5f869f44fc580f73bff2302832f660e1b905",
        "0eb6f2e23058c683e6c73d5bd8c615d2446a6436a9afc14359ef7c3966b97e97",
        "ba6bf4a03ee2cb879ae8fdf48fce197a6f1734f7b4a553cd9230abd8f3d2c2be",
    )
    with pytest.raises(MssqlR1V3ContractError, match="NULL"):
        replace(cells[0], canonical_scalar_bytes=b"unexpected")


def test_renderer_bundle_is_retained_exactly_and_cannot_be_submitted_as_semantic_intent() -> None:
    request, _ = _batch_request()
    bundle = request.rendered_bundle
    _, authority, evidence = _rendered_admission(request.mutation_plan)
    decoded = MssqlR1RenderedMutationBundleV1.from_canonical_bytes(bundle.canonical_bytes)
    assert decoded == bundle
    assert MssqlR1RendererAuthorityV1.from_canonical_bytes(authority.canonical_bytes) == authority
    assert MssqlR1RendererAdmissionEvidenceV1.from_canonical_bytes(evidence.canonical_bytes) == evidence
    assert bundle.renderer_authority_digest == authority.digest
    assert bundle.admission_evidence_digest == evidence.digest
    assert not hasattr(bundle, "renderer_authority")
    assert authority.digest.hex() == "355740edeadbf4629b6c8b5139ee0f23bc42f4b6823e9a82c586b3805c556289"
    assert evidence.digest.hex() == "cc9d94bd4f23fb16ce3d3c264d276b495e8b52269bf69a66b1c6b34e59f593de"
    assert bundle.digest.hex() == "7262576d99282c450cb5b0e172ec053c5bc782f92e9c744da895a165b7129089"
    with pytest.raises(MssqlR1V3ContractError, match="digest differs"):
        replace(bundle.statements[0], statement_utf8_bytes=b"SELECT 2")
    forged_sql = b"DROP TABLE dbo.orders"
    forged_statement = replace(
        bundle.statements[0],
        statement_utf8_bytes=forged_sql,
        statement_digest=hashlib.sha256(forged_sql).digest(),
    )
    forged = replace(bundle, statements=(forged_statement, *bundle.statements[1:]))
    persisted: list[MssqlR1RenderedMutationBundleV1] = []
    with pytest.raises(MssqlR1V3ContractError, match="independent admission"):
        admitted = MssqlR1VerifiedRenderedMutationV1(
            request.mutation_plan,
            forged,
            authority,
            evidence,
            _registration().resolved_profile_digest,
        )
        persisted.append(admitted.bundle)
    assert persisted == []
    proposed_authority = replace(authority, renderer_build_digest=D1)
    proposed_evidence = replace(
        evidence,
        renderer_authority_digest=proposed_authority.digest,
        rendered_execution_digest=replace(forged, renderer_build_digest=D1).execution_payload_digest,
    )
    with pytest.raises(MssqlR1V3ContractError, match="independently resolved"):
        MssqlR1VerifiedRenderedMutationV1(
            request.mutation_plan,
            replace(
                forged,
                renderer_build_digest=D1,
                renderer_authority_digest=proposed_authority.digest,
                admission_evidence_digest=proposed_evidence.digest,
            ),
            authority,
            evidence,
            _registration().resolved_profile_digest,
        )
    intent = MssqlR1EffectSealIntentV3(
        request.identity,
        request.registration_id,
        request.registration_payload_digest,
        request.registration_payload_bytes,
        request.verification_policy_digest,
        request.admitted_at_server_time,
        request.source_snapshot_digest,
        request.source_schema_digest,
        request.artifacts,
        request.mutation_plan,
        request.generation,
    )
    assert "rendered_bundle" not in intent.__dataclass_fields__
    assert not hasattr(intent, "seal_with")


def test_signature_verifier_boundary_accepts_only_typed_commands() -> None:
    from dpone.ports.mssql_r1_v3 import (
        BlobSignatureVerifierV1Port,
        MssqlR1MutationPlanRendererPort,
        MssqlR1RenderedMutationVerifierPort,
        MssqlR1RendererAuthorityResolverPort,
    )

    parameters = inspect.signature(BlobSignatureVerifierV1Port.verify_registration).parameters
    assert tuple(parameters) == ("self", "command")
    payload = _registration()
    command = SignedTargetRegistrationCommandV1(payload.canonical_bytes, b"bundle-a")
    assert command.payload == payload
    with pytest.raises(MssqlR1V3ContractError):
        SignedTargetRegistrationCommandV1(payload.canonical_bytes[:-1], b"bundle-a")
    assert tuple(inspect.signature(MssqlR1MutationPlanRendererPort.render).parameters) == ("self", "plan")
    assert tuple(inspect.signature(MssqlR1RendererAuthorityResolverPort.resolve).parameters) == (
        "self",
        "resolved_profile_digest",
    )
    assert tuple(inspect.signature(MssqlR1RenderedMutationVerifierPort.verify).parameters) == (
        "self",
        "plan",
        "bundle",
        "authority",
    )


def test_pre_source_outcome_is_a_strict_discriminated_union() -> None:
    from dpone.ports.mssql_r1_v3 import MssqlR1PreSourceOutcomeV3, MssqlR1PreSourceStatusV3

    request, attempt = _batch_request()
    assert MssqlR1PreSourceOutcomeV3(MssqlR1PreSourceStatusV3.SOURCE_REQUIRED).status is (
        MssqlR1PreSourceStatusV3.SOURCE_REQUIRED
    )
    assert MssqlR1PreSourceOutcomeV3(MssqlR1PreSourceStatusV3.SEALED_RESUME, request, attempt).attempt == attempt
    with pytest.raises(MssqlR1V3ContractError, match="discriminator"):
        MssqlR1PreSourceOutcomeV3(MssqlR1PreSourceStatusV3.REPLAY_COMMITTED)
    unknown = UnknownEffectReplayProofV3(_proof_request(attempt), R1UnknownReplayReasonV3.AMBIGUOUS_RECEIPT, D1)
    with pytest.raises(MssqlR1V3ContractError, match="exact committed"):
        MssqlR1PreSourceOutcomeV3(MssqlR1PreSourceStatusV3.REPLAY_COMMITTED, replay=unknown)
