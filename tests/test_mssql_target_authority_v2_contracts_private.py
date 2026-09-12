"""Pure canonical V2 obligations: receipts, pre-source admission and sealing."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from dpone.contracts.mssql_target_authority_v2 import (
    MAX_SQL_BIGINT,
    MssqlArtifactAuthorityV2,
    MssqlBatchEffectDraftV2,
    MssqlBatchEffectReceiptV2,
    MssqlBusinessKeyV2,
    MssqlEffectReceiptHeaderV2,
    MssqlEffectRequestV2,
    MssqlGenerationAuthorityClaimV1,
    MssqlReceiptContractError,
    MssqlReceiptProofV2,
    MssqlSealedIntentV2,
    MssqlTargetHeadV2,
    MssqlTargetIdentityV2,
    MssqlXminCheckpointTransitionV2,
    MssqlXminEffectDraftV2,
    MssqlXminEffectReceiptV2,
    ReceiptKind,
    ReceiptProofOutcome,
    SourceMode,
    WriterMode,
    build_effect_key,
    build_operation_key,
    build_receipt,
    deterministic_receipt_id,
    prove_receipt_descendant,
    xmin_mutation_result_from_receipt,
)
from dpone.contracts.mssql_target_authority_v2_pre_source import (
    MssqlPreSourceAdmissionOutcomeV2,
    MssqlPreSourceAdmissionV2,
    MssqlPreSourceAuthorityRequestV2,
)
from dpone.contracts.mssql_target_authority_v2_preparation import (
    MssqlArtifactSealRequestV2,
    MssqlIntentSealRequestV2,
    MssqlOpenStagingArtifactV2,
    MssqlPreparedEffectRequestV2,
)
from dpone.contracts.mssql_target_authority_v2_xmin_metrics import MssqlXminMutationResultV2

DIGEST_A = bytes.fromhex("11" * 32)
DIGEST_B = bytes.fromhex("22" * 32)
TARGET = UUID("11111111-1111-4111-8111-111111111111")
RECOVERY = UUID("33333333-3333-4333-8333-333333333333")


def _target_identity() -> MssqlTargetIdentityV2:
    return MssqlTargetIdentityV2(
        target_binding_uuid=TARGET,
        target_object_uuid=UUID("44444444-4444-4444-8444-444444444444"),
        server_instance_identity_sha256=DIGEST_A,
        database_guid=UUID("55555555-5555-4555-8555-555555555555"),
        database_family_guid=UUID("66666666-6666-4666-8666-666666666666"),
        recovery_fork_guid=UUID("77777777-7777-4777-8777-777777777777"),
        recovery_domain_uuid=RECOVERY,
        recovery_domain_epoch=1,
        database_name_digest=DIGEST_A,
        schema_name_digest=DIGEST_B,
        object_name_digest=DIGEST_A,
        object_id=17,
        physical_generation_uuid=UUID("88888888-8888-4888-8888-888888888888"),
        catalog_contract_digest=DIGEST_B,
        target_contract_revision=1,
        business_key=MssqlBusinessKeyV2(
            column_name="order_id", source_type="int8", target_type="bigint", maximum_ordinary_indexes=4
        ),
    )


def _header(**overrides: object) -> MssqlEffectReceiptHeaderV2:
    operation_key = build_operation_key(
        route_identity_sha256=DIGEST_A,
        invocation_identity="scheduled-run-17",
        source_mode=SourceMode.BATCH_FULL_REFRESH,
        target_binding_uuid=TARGET,
    )
    effect_key = build_effect_key(
        operation_key=operation_key, source_mode=SourceMode.BATCH_FULL_REFRESH, target_binding_uuid=TARGET
    )
    values: dict[str, object] = {
        "receipt_kind": ReceiptKind.BATCH,
        "operation_key": operation_key,
        "effect_key": effect_key,
        "target_identity": _target_identity(),
        "writer_mode": WriterMode.BATCH_FULL_REFRESH,
        "expected_writer_generation": 2,
        "candidate_writer_generation": 3,
        "previous_head_revision": 9,
        "committed_head_revision": 1,
        "operation_epoch": 1,
        "previous_receipt_id": UUID("22222222-2222-4222-8222-222222222222"),
        "previous_receipt_digest": DIGEST_B,
        "previous_recovery_identity_digest": DIGEST_A,
        "expected_recovery_domain_uuid": RECOVERY,
        "expected_recovery_domain_epoch": 1,
        "candidate_recovery_identity_digest": DIGEST_A,
        "candidate_recovery_domain_uuid": RECOVERY,
        "candidate_recovery_domain_epoch": 1,
        "route_identity_sha256": DIGEST_A,
        "source_authority_sha256": DIGEST_B,
        "intent_digest": DIGEST_A,
        "type_policy_digest": DIGEST_B,
        "hash_policy_digest": DIGEST_A,
        "quality_policy_digest": DIGEST_B,
    }
    values.update(overrides)
    return MssqlEffectReceiptHeaderV2(**values)


def _batch_body() -> MssqlBatchEffectReceiptV2:
    return MssqlBatchEffectReceiptV2(
        source_snapshot_digest=DIGEST_A,
        source_schema_digest=DIGEST_B,
        source_payload_digest=DIGEST_A,
        staging_intent_digest=DIGEST_B,
        mutation_plan_digest=DIGEST_A,
        before_row_count=10,
        after_row_count=12,
        payload_bytes=4096,
        quality_evidence=b"quality-v1",
    )


def test_operation_and_effect_identity_exclude_runtime_counters() -> None:
    operation = build_operation_key(
        route_identity_sha256=DIGEST_A,
        invocation_identity="scheduled-run-17",
        source_mode=SourceMode.BATCH_FULL_REFRESH,
        target_binding_uuid=TARGET,
    )
    effect = build_effect_key(
        operation_key=operation, source_mode=SourceMode.BATCH_FULL_REFRESH, target_binding_uuid=TARGET
    )
    assert operation.hex() == "b2618a77f7f1c9a75c1883cf101d3cf5d912407325f026289bff534442f7c531"
    assert effect.hex() == "42593188307385c87c54c3bb0029babb6edbe5ce5743e7f87270c803f5c2c702"


def test_receipt_is_typed_deterministic_and_immutable() -> None:
    receipt = build_receipt(_header(), _batch_body())
    assert receipt.receipt_id == deterministic_receipt_id(receipt.header.effect_key)
    assert receipt.body_digest == receipt.body.digest
    assert receipt.receipt_digest.hex() == "9dac3e787bf0cff7b995a1f5afcbde4ad6952d22451ce02d18cf58daf36212f1"
    with pytest.raises(FrozenInstanceError):
        receipt.header.operation_epoch = 2


def test_receipt_rejects_body_kind_or_identity_conflict() -> None:
    with pytest.raises(MssqlReceiptContractError, match="body kind"):
        build_receipt(
            _header(),
            MssqlXminEffectReceiptV2(
                effect_type="incremental",
                state_key_digest=DIGEST_A,
                previous_checkpoint_state_key_digest=DIGEST_B,
                previous_checkpoint_writer_generation=2,
                previous_checkpoint_revision=9,
                source_snapshot_digest=DIGEST_B,
                source_schema_digest=DIGEST_A,
                scope_digest=DIGEST_B,
                previous_xmin=None,
                candidate_xmin=1,
                visible_horizon_xmax=2,
                xid_epoch=0,
                candidate_checkpoint_state_key_digest=DIGEST_A,
                candidate_checkpoint_writer_generation=2,
                committed_checkpoint_revision=10,
                delta_manifest_digest=DIGEST_B,
                complete_key_manifest_digest=DIGEST_A,
                frozen_state_payload=b"frozen-v1",
                frozen_state_digest=DIGEST_B,
                mutation_plan_digest=DIGEST_B,
                before_row_count=1,
                inserted_row_count=0,
                updated_row_count=1,
                hard_deleted_row_count=0,
                unchanged_row_count=0,
                after_row_count=1,
                delta_row_count=1,
                payload_bytes=1,
                quality_evidence=b"quality-v1",
            ),
        )
    with pytest.raises(MssqlReceiptContractError, match="effect key"):
        build_receipt(replace(_header(), effect_key=DIGEST_A), _batch_body())


def test_receipt_transition_requires_adjacent_generation_or_revision() -> None:
    with pytest.raises(MssqlReceiptContractError, match="adjacent"):
        _header(candidate_writer_generation=2, committed_head_revision=11)
    with pytest.raises(MssqlReceiptContractError, match="adjacent"):
        _header(candidate_writer_generation=4)


def test_typed_body_rejects_partial_checkpoint_pointer() -> None:
    with pytest.raises(MssqlReceiptContractError, match="checkpoint pointer"):
        replace(_batch_body(), previous_checkpoint_state_key_digest=DIGEST_A)


def test_fresh_proof_outcomes_have_disjoint_authority() -> None:
    receipt = build_receipt(_header(), _batch_body())
    assert MssqlReceiptProofV2(ReceiptProofOutcome.COMMITTED, receipt=receipt).receipt == receipt
    assert (
        MssqlReceiptProofV2(ReceiptProofOutcome.KNOWN_NOT_COMMITTED, retry_operation_epoch=2).retry_operation_epoch == 2
    )
    with pytest.raises(MssqlReceiptContractError, match="recovery code only"):
        MssqlReceiptProofV2(ReceiptProofOutcome.UNKNOWN, receipt=receipt, recovery_code="ambiguous")


def test_receipt_counter_overflow_fails_closed() -> None:
    with pytest.raises(MssqlReceiptContractError, match="SQL bigint"):
        _header(operation_epoch=MAX_SQL_BIGINT + 1)


def test_xmin_receipt_persists_and_projects_exact_mutation_counters() -> None:
    body = MssqlXminEffectReceiptV2(
        effect_type="incremental",
        state_key_digest=DIGEST_A,
        previous_checkpoint_state_key_digest=DIGEST_A,
        previous_checkpoint_writer_generation=2,
        previous_checkpoint_revision=9,
        source_snapshot_digest=DIGEST_A,
        source_schema_digest=DIGEST_B,
        scope_digest=DIGEST_A,
        previous_xmin=100,
        candidate_xmin=110,
        visible_horizon_xmax=120,
        xid_epoch=1,
        candidate_checkpoint_state_key_digest=DIGEST_A,
        candidate_checkpoint_writer_generation=2,
        committed_checkpoint_revision=10,
        delta_manifest_digest=DIGEST_A,
        complete_key_manifest_digest=DIGEST_B,
        frozen_state_payload=b"frozen",
        frozen_state_digest=DIGEST_A,
        mutation_plan_digest=DIGEST_B,
        before_row_count=10,
        inserted_row_count=2,
        updated_row_count=3,
        hard_deleted_row_count=1,
        unchanged_row_count=6,
        after_row_count=11,
        delta_row_count=5,
        payload_bytes=4096,
        quality_evidence=b"quality",
    )
    operation_key = build_operation_key(
        route_identity_sha256=DIGEST_A,
        invocation_identity="scheduled-run-17",
        source_mode=SourceMode.XMIN_CURRENT_STATE,
        target_binding_uuid=TARGET,
    )
    effect_key = build_effect_key(
        operation_key=operation_key, source_mode=SourceMode.XMIN_CURRENT_STATE, target_binding_uuid=TARGET
    )
    xmin_header = _header(
        receipt_kind=ReceiptKind.XMIN,
        writer_mode=WriterMode.XMIN_CURRENT_STATE,
        operation_key=operation_key,
        effect_key=effect_key,
        expected_writer_generation=2,
        candidate_writer_generation=2,
        previous_head_revision=9,
        committed_head_revision=10,
    )
    receipt = build_receipt(xmin_header, body)
    assert xmin_mutation_result_from_receipt(receipt) == MssqlXminMutationResultV2(
        before_row_count=10,
        after_row_count=11,
        payload_bytes=4096,
        inserted_row_count=2,
        updated_row_count=3,
        hard_deleted_row_count=1,
        unchanged_row_count=6,
        delta_row_count=5,
    )
    assert replace(body, inserted_row_count=3, after_row_count=12, delta_row_count=6).digest != body.digest


def test_xmin_receipt_rejects_invalid_counter_algebra_and_no_effect_mismatch() -> None:
    baseline = dict(
        effect_type="no_effect",
        state_key_digest=DIGEST_A,
        previous_checkpoint_state_key_digest=DIGEST_A,
        previous_checkpoint_writer_generation=2,
        previous_checkpoint_revision=9,
        source_snapshot_digest=DIGEST_A,
        source_schema_digest=DIGEST_B,
        scope_digest=DIGEST_A,
        previous_xmin=100,
        candidate_xmin=110,
        visible_horizon_xmax=120,
        xid_epoch=1,
        candidate_checkpoint_state_key_digest=DIGEST_A,
        candidate_checkpoint_writer_generation=2,
        committed_checkpoint_revision=10,
        delta_manifest_digest=DIGEST_A,
        complete_key_manifest_digest=DIGEST_B,
        frozen_state_payload=b"frozen",
        frozen_state_digest=DIGEST_A,
        mutation_plan_digest=DIGEST_B,
        before_row_count=10,
        inserted_row_count=0,
        updated_row_count=0,
        hard_deleted_row_count=0,
        unchanged_row_count=10,
        after_row_count=10,
        delta_row_count=0,
        payload_bytes=4096,
        quality_evidence=b"quality",
    )
    MssqlXminEffectReceiptV2(**baseline)
    with pytest.raises(MssqlReceiptContractError, match="no_effect"):
        MssqlXminEffectReceiptV2(**{**baseline, "inserted_row_count": 1, "after_row_count": 11})
    with pytest.raises(MssqlReceiptContractError, match="counter invariant"):
        MssqlXminEffectReceiptV2(**{**baseline, "unchanged_row_count": 9})


def test_bounded_descendant_proof_accepts_exact_chain_and_rejects_breaks() -> None:
    historical = build_receipt(_header(), _batch_body())
    next_operation = build_operation_key(
        route_identity_sha256=DIGEST_A,
        invocation_identity="scheduled-run-18",
        source_mode=SourceMode.BATCH_FULL_REFRESH,
        target_binding_uuid=TARGET,
    )
    next_effect = build_effect_key(
        operation_key=next_operation, source_mode=SourceMode.BATCH_FULL_REFRESH, target_binding_uuid=TARGET
    )
    current = build_receipt(
        _header(
            operation_key=next_operation,
            effect_key=next_effect,
            expected_writer_generation=3,
            candidate_writer_generation=3,
            previous_head_revision=1,
            committed_head_revision=2,
            previous_receipt_id=historical.receipt_id,
            previous_receipt_digest=historical.receipt_digest,
        ),
        _batch_body(),
    )
    assert prove_receipt_descendant(historical, (current,), max_receipts=2) is True
    with pytest.raises(MssqlReceiptContractError, match="chain"):
        prove_receipt_descendant(historical, (replace(current, receipt_digest=DIGEST_A),), max_receipts=2)
    with pytest.raises(MssqlReceiptContractError, match="bound"):
        prove_receipt_descendant(historical, (current,), max_receipts=0)


def test_proof_request_freezes_complete_target_recovery_checkpoint_and_artifact_authority() -> None:
    header = _header(intent_digest=_batch_body().staging_intent_digest)
    artifact = MssqlArtifactAuthorityV2(
        artifact_id=UUID("90000000-0000-4000-8000-000000000001"),
        artifact_kind="batch_payload",
        object_uuid=UUID("90000000-0000-4000-8000-000000000002"),
        object_id=91,
        physical_token=UUID("90000000-0000-4000-8000-000000000003"),
        catalog_digest=DIGEST_A,
        row_count=12,
        payload_bytes=4096,
        ordered_logical_digest=DIGEST_B,
        permission_contract_digest=DIGEST_A,
    )
    intent = MssqlSealedIntentV2(
        header.effect_key,
        header.intent_digest,
        _batch_body().staging_intent_digest,
        12,
        4096,
        artifacts=(artifact,),
        source_snapshot_digest=_batch_body().source_snapshot_digest,
        source_schema_digest=_batch_body().source_schema_digest,
        artifact_manifest_digest=_batch_body().staging_intent_digest,
        mutation_plan_digest=_batch_body().mutation_plan_digest,
        source_snapshot_authority=b"snapshot-authority-v1",
        artifact_set_manifest=b"artifact-manifest-v1",
    )
    draft = MssqlBatchEffectDraftV2(
        source_snapshot_digest=_batch_body().source_snapshot_digest,
        source_schema_digest=_batch_body().source_schema_digest,
        source_payload_digest=_batch_body().source_payload_digest,
        staging_intent_digest=_batch_body().staging_intent_digest,
        mutation_plan_digest=_batch_body().mutation_plan_digest,
        generation_authority_id=UUID("90000000-0000-4000-8000-000000000004"),
    )
    claim = MssqlGenerationAuthorityClaimV1(
        UUID("90000000-0000-4000-8000-000000000004"), "rebaseline", header.effect_key, DIGEST_A
    )
    proof = MssqlEffectRequestV2(header, intent, draft, generation_authority=claim).receipt_proof_request(
        expected_head=None
    )
    assert proof.target_identity == header.target_identity
    assert proof.operation_key == header.operation_key
    assert proof.sealed_intent == intent
    assert proof.artifacts == (artifact,)
    with pytest.raises(MssqlReceiptContractError, match="complete durable sealed intent"):
        replace(proof, sealed_intent=replace(intent, source_snapshot_authority=None))


def test_generation_authority_matches_every_sealed_destructive_fact() -> None:
    header = _header(intent_digest=_batch_body().staging_intent_digest)
    body = _batch_body()
    artifact = MssqlArtifactAuthorityV2(
        artifact_id=UUID("90000000-0000-4000-8000-000000000011"),
        artifact_kind="batch_payload",
        object_uuid=UUID("90000000-0000-4000-8000-000000000012"),
        object_id=92,
        physical_token=UUID("90000000-0000-4000-8000-000000000013"),
        catalog_digest=DIGEST_A,
        row_count=0,
        payload_bytes=0,
        ordered_logical_digest=DIGEST_B,
        permission_contract_digest=DIGEST_A,
    )
    intent = MssqlSealedIntentV2(
        header.effect_key,
        header.intent_digest,
        body.staging_intent_digest,
        0,
        0,
        artifacts=(artifact,),
        source_snapshot_digest=body.source_snapshot_digest,
        source_schema_digest=body.source_schema_digest,
        artifact_manifest_digest=body.staging_intent_digest,
        mutation_plan_digest=body.mutation_plan_digest,
        source_snapshot_authority=b"snapshot-authority-v1",
        artifact_set_manifest=b"artifact-manifest-v1",
    )
    claim = MssqlGenerationAuthorityClaimV1(
        authority_id=UUID("90000000-0000-4000-8000-000000000014"),
        authority_kind="empty_refresh",
        effect_key=header.effect_key,
        payload_digest=DIGEST_A,
    )
    draft = MssqlBatchEffectDraftV2(
        source_snapshot_digest=body.source_snapshot_digest,
        source_schema_digest=body.source_schema_digest,
        source_payload_digest=body.source_payload_digest,
        staging_intent_digest=body.staging_intent_digest,
        mutation_plan_digest=body.mutation_plan_digest,
        generation_authority_id=claim.authority_id,
    )
    request = MssqlEffectRequestV2(header, intent, draft, generation_authority=claim)
    proof = request.receipt_proof_request(expected_head=None)
    assert proof.target_identity == header.target_identity
    assert proof.operation_key == header.operation_key
    assert proof.expected_checkpoint is None
    assert proof.artifacts == (artifact,)
    assert proof.sealed_intent == intent
    with pytest.raises(MssqlReceiptContractError, match="generation authority"):
        MssqlEffectRequestV2(header, intent, replace(draft, generation_authority_id=None), generation_authority=claim)


def test_batch_effect_request_requires_a_new_adjacent_content_generation() -> None:
    header = _header(intent_digest=_batch_body().staging_intent_digest)
    artifact = MssqlArtifactAuthorityV2(
        artifact_id=UUID("91000000-0000-4000-8000-000000000001"),
        artifact_kind="batch_payload",
        object_uuid=UUID("91000000-0000-4000-8000-000000000002"),
        object_id=93,
        physical_token=UUID("91000000-0000-4000-8000-000000000003"),
        catalog_digest=DIGEST_A,
        row_count=12,
        payload_bytes=4096,
        ordered_logical_digest=DIGEST_B,
        permission_contract_digest=DIGEST_A,
    )
    intent = MssqlSealedIntentV2(
        header.effect_key,
        header.intent_digest,
        _batch_body().staging_intent_digest,
        12,
        4096,
        artifacts=(artifact,),
        source_snapshot_digest=_batch_body().source_snapshot_digest,
        source_schema_digest=_batch_body().source_schema_digest,
        artifact_manifest_digest=_batch_body().staging_intent_digest,
        mutation_plan_digest=_batch_body().mutation_plan_digest,
        source_snapshot_authority=b"snapshot-authority-v1",
        artifact_set_manifest=b"artifact-manifest-v1",
    )
    authority_id = UUID("91000000-0000-4000-8000-000000000004")
    draft = MssqlBatchEffectDraftV2(
        source_snapshot_digest=_batch_body().source_snapshot_digest,
        source_schema_digest=_batch_body().source_schema_digest,
        source_payload_digest=_batch_body().source_payload_digest,
        staging_intent_digest=_batch_body().staging_intent_digest,
        mutation_plan_digest=_batch_body().mutation_plan_digest,
        generation_authority_id=authority_id,
    )
    claim = MssqlGenerationAuthorityClaimV1(authority_id, "rebaseline", header.effect_key, DIGEST_A)
    assert MssqlEffectRequestV2(header, intent, draft, generation_authority=claim)
    with pytest.raises(MssqlReceiptContractError, match="Batch full refresh"):
        MssqlEffectRequestV2(
            replace(
                header,
                candidate_writer_generation=header.expected_writer_generation,
                committed_head_revision=(header.previous_head_revision or 0) + 1,
            ),
            intent,
            draft,
            generation_authority=claim,
        )


def _pre_source_request(**overrides: object) -> MssqlPreSourceAuthorityRequestV2:
    header = _header()
    values: dict[str, object] = {
        "route_identity_sha256": DIGEST_A,
        "invocation_identity": "scheduled-run-17",
        "source_mode": SourceMode.BATCH_FULL_REFRESH,
        "target_identity": _target_identity(),
        "expected_head": None,
        "candidate_writer_generation": 1,
        "candidate_head_revision": 1,
        "source_authority_sha256": DIGEST_B,
        "type_policy_digest": header.type_policy_digest,
        "hash_policy_digest": header.hash_policy_digest,
        "quality_policy_digest": header.quality_policy_digest,
        "owner_id_digest": DIGEST_A,
        "lease_seconds": 30,
    }
    values.update(overrides)
    return MssqlPreSourceAuthorityRequestV2(**values)


def test_request_derives_stable_keys_without_runtime_or_source_objects() -> None:
    request = _pre_source_request()
    assert request.operation_key.hex() == "b2618a77f7f1c9a75c1883cf101d3cf5d912407325f026289bff534442f7c531"
    assert request.effect_key.hex() == "42593188307385c87c54c3bb0029babb6edbe5ce5743e7f87270c803f5c2c702"
    assert "connector" not in request.__dataclass_fields__
    assert replace(request, owner_id_digest=DIGEST_B, lease_seconds=60).operation_key == request.operation_key
    with pytest.raises(FrozenInstanceError):
        request.lease_seconds = 60


def test_request_rejects_non_adjacent_claim_and_unbounded_lease() -> None:
    with pytest.raises(ValueError, match="initial target transition"):
        _pre_source_request(candidate_writer_generation=2)
    with pytest.raises(ValueError, match="lease_seconds"):
        _pre_source_request(lease_seconds=3601)
    with pytest.raises(ValueError, match="lease_seconds"):
        _pre_source_request(lease_seconds=True)


def test_replay_match_binds_stable_route_source_target_and_policy_authority() -> None:
    historical = build_receipt(_header(), _batch_body())
    request = _pre_source_request()
    assert request.matches_replay_receipt(historical)
    assert not replace(request, source_authority_sha256=DIGEST_A).matches_replay_receipt(historical)


def test_later_head_does_not_hide_retry_stable_historical_receipt() -> None:
    historical = build_receipt(_header(), _batch_body())
    later_head = MssqlTargetHeadV2(
        historical.header.target_binding_uuid,
        historical.header.writer_mode,
        4,
        7,
        UUID("99999999-9999-4999-8999-999999999999"),
        historical.header.candidate_recovery_domain_epoch,
        historical.header.candidate_recovery_domain_uuid,
    )
    request = _pre_source_request(expected_head=later_head, candidate_writer_generation=5, candidate_head_revision=1)
    assert request.matches_replay_receipt(historical)


def test_admission_outcome_has_closed_receipt_and_epoch_shapes() -> None:
    historical = build_receipt(_header(), _batch_body())
    replay = MssqlPreSourceAdmissionV2(
        outcome=MssqlPreSourceAdmissionOutcomeV2.REPLAY_SUPPRESSED,
        operation_key=historical.header.operation_key,
        effect_key=historical.header.effect_key,
        operation_epoch=historical.header.operation_epoch,
        replay_receipt=historical,
    )
    assert replay.source_io_performed is False
    with pytest.raises(ValueError, match="replay receipt"):
        MssqlPreSourceAdmissionV2(
            outcome=MssqlPreSourceAdmissionOutcomeV2.REPLAY_SUPPRESSED,
            operation_key=historical.header.operation_key,
            effect_key=historical.header.effect_key,
            operation_epoch=1,
        )


def test_request_rejects_source_mode_head_mismatch() -> None:
    header = _header()
    head = MssqlTargetHeadV2(
        header.target_binding_uuid,
        header.writer_mode,
        header.expected_writer_generation or 1,
        header.previous_head_revision or 1,
        header.previous_receipt_id,
        header.expected_recovery_domain_epoch or 1,
        header.expected_recovery_domain_uuid,
    )
    with pytest.raises(ValueError, match="mode transition"):
        _pre_source_request(source_mode=SourceMode.XMIN_CURRENT_STATE, expected_head=head)


def _prepared_request() -> MssqlEffectRequestV2:
    header = _header()
    artifact = MssqlArtifactAuthorityV2(
        artifact_id=UUID("90000000-0000-4000-8000-000000000001"),
        artifact_kind="batch_payload",
        object_uuid=UUID("90000000-0000-4000-8000-000000000002"),
        object_id=71,
        physical_token=UUID("90000000-0000-4000-8000-000000000003"),
        catalog_digest=b"c" * 32,
        row_count=12,
        payload_bytes=4096,
        ordered_logical_digest=b"o" * 32,
        permission_contract_digest=b"p" * 32,
    )
    intent = MssqlSealedIntentV2(
        effect_key=header.effect_key,
        intent_digest=DIGEST_A,
        artifact_set_digest=DIGEST_B,
        row_count=12,
        payload_bytes=4096,
        artifacts=(artifact,),
        source_snapshot_digest=DIGEST_A,
        source_schema_digest=DIGEST_B,
        artifact_manifest_digest=DIGEST_A,
        mutation_plan_digest=DIGEST_B,
        source_snapshot_authority=b"snapshot-v1",
        artifact_set_manifest=b"artifact-set-v1",
    )
    authority_id = UUID("91000000-0000-4000-8000-000000000004")
    claim = MssqlGenerationAuthorityClaimV1(authority_id, "rebaseline", header.effect_key, DIGEST_A)
    draft = MssqlBatchEffectDraftV2(
        source_snapshot_digest=DIGEST_A,
        source_schema_digest=DIGEST_B,
        source_payload_digest=DIGEST_A,
        staging_intent_digest=DIGEST_A,
        mutation_plan_digest=DIGEST_B,
        generation_authority_id=authority_id,
    )
    return MssqlEffectRequestV2(header, intent, draft, generation_authority=claim)


def _open(request: MssqlEffectRequestV2) -> MssqlOpenStagingArtifactV2:
    artifact = request.sealed_intent.artifacts[0]
    return MssqlOpenStagingArtifactV2(
        target_binding_uuid=request.header.target_binding_uuid,
        effect_key=request.header.effect_key,
        operation_epoch=request.header.operation_epoch,
        owner_id_digest=b"w" * 32,
        artifact_id=artifact.artifact_id,
        artifact_kind=artifact.artifact_kind,
        object_uuid=artifact.object_uuid,
        object_id=artifact.object_id,
        physical_token=artifact.physical_token,
        catalog_digest=artifact.catalog_digest,
        retention_until=datetime.now(UTC) + timedelta(days=1),
    )


def test_prepared_effect_request_round_trips_exact_authority() -> None:
    request = _prepared_request()
    prepared = MssqlPreparedEffectRequestV2.from_request(request)
    assert prepared.codec_version == "dpone-mssql-effect-request-v2-json-v1"
    assert prepared.rehydrate() == request
    assert prepared.request_payload.startswith(b"dpone-r1-effect-request-v2\x00")


def test_prepared_effect_request_round_trips_xmin_checkpoint_variant() -> None:
    batch = _prepared_request()
    operation_key = build_operation_key(
        route_identity_sha256=DIGEST_A,
        invocation_identity="scheduled-run-17",
        source_mode=SourceMode.XMIN_CURRENT_STATE,
        target_binding_uuid=batch.header.target_binding_uuid,
    )
    effect_key = build_effect_key(
        operation_key=operation_key,
        source_mode=SourceMode.XMIN_CURRENT_STATE,
        target_binding_uuid=batch.header.target_binding_uuid,
    )
    header = replace(
        batch.header,
        receipt_kind=ReceiptKind.XMIN,
        candidate_writer_generation=2,
        committed_head_revision=10,
        writer_mode=WriterMode.XMIN_CURRENT_STATE,
        operation_key=operation_key,
        effect_key=effect_key,
        intent_digest=DIGEST_A,
    )
    intent = replace(batch.sealed_intent, effect_key=effect_key, artifact_manifest_digest=DIGEST_B)
    draft = MssqlXminEffectDraftV2(
        effect_type="incremental",
        state_key_digest=b"k" * 32,
        previous_checkpoint_state_key_digest=b"k" * 32,
        previous_checkpoint_writer_generation=2,
        previous_checkpoint_revision=8,
        source_snapshot_digest=DIGEST_A,
        source_schema_digest=DIGEST_B,
        scope_digest=b"q" * 32,
        previous_xmin=10,
        candidate_xmin=11,
        visible_horizon_xmax=12,
        xid_epoch=1,
        candidate_checkpoint_state_key_digest=b"k" * 32,
        candidate_checkpoint_writer_generation=2,
        committed_checkpoint_revision=9,
        delta_manifest_digest=b"d" * 32,
        complete_key_manifest_digest=b"c" * 32,
        frozen_state_payload=b"frozen",
        frozen_state_digest=b"f" * 32,
        mutation_plan_digest=DIGEST_B,
    )
    request = MssqlEffectRequestV2(header, intent, draft, MssqlXminCheckpointTransitionV2(b"k" * 32, 2, 8, 9))
    assert MssqlPreparedEffectRequestV2.from_request(request).rehydrate() == request


def test_prepared_effect_request_rejects_tamper_and_noncanonical_payload() -> None:
    prepared = MssqlPreparedEffectRequestV2.from_request(_prepared_request())
    with pytest.raises(MssqlReceiptContractError, match="sealed effect request digest"):
        replace(prepared, request_payload=prepared.request_payload + b" ")


def test_seal_contract_binds_open_artifact_and_exact_effect_request() -> None:
    request = _prepared_request()
    opened = _open(request)
    artifact = request.sealed_intent.artifacts[0]
    artifact_seal = MssqlArtifactSealRequestV2(
        opened=opened,
        row_count=artifact.row_count,
        payload_bytes=artifact.payload_bytes,
        ordered_logical_digest=artifact.ordered_logical_digest,
    )
    intent_seal = MssqlIntentSealRequestV2(
        sealed_intent_id=UUID("90000000-0000-4000-8000-000000000004"),
        operation_epoch=request.header.operation_epoch,
        owner_id_digest=opened.owner_id_digest,
        retention_until=opened.retention_until,
        request=request,
    )
    assert artifact_seal.expected_authority.permission_contract_digest is None
    assert intent_seal.prepared_request.rehydrate() == request
    with pytest.raises(MssqlReceiptContractError, match="artifact set"):
        replace(intent_seal, request=replace(request, sealed_intent=replace(request.sealed_intent, artifacts=())))
