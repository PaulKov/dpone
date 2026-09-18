from __future__ import annotations

import json
from dataclasses import replace

import pytest

from dpone.contracts.clickhouse_cluster_publication import digest_payload as v1_digest_payload
from dpone.contracts.clickhouse_external_replication import (
    EXTERNAL_AUTHORITY_SCHEMA_VERSION,
    INTERNAL_AUTHORITY_SCHEMA_VERSION,
    ArtifactIdentity,
    ExternalArtifactReceipt,
    ExternalAuthorityPhase,
    ExternalAuthorityRecord,
    ExternalContractError,
    ExternalMember,
    ExternalMemberRecord,
    ExternalMemberStageState,
    ExternalPublicationError,
    ExternalPublicationRequest,
    ExternalTopology,
    MemberGenerationObservation,
    MemberPublicationState,
    PhysicalGeneration,
    ReplicationMode,
    classify_member_publication,
    derive_generation_id,
    derive_member_id,
    derive_operation_id,
    derive_target_key,
)


def _digest(label: str) -> str:
    return v1_digest_payload({"label": label})


def _member(replica_num: int, *, internal: bool = False) -> ExternalMember:
    return ExternalMember.create(
        shard_num=1,
        replica_num=replica_num,
        internal_replication=internal,
    )


def _generation(label: str, *, content: str | None = None) -> PhysicalGeneration:
    return PhysicalGeneration(
        uuid=f"uuid-{label}",
        engine_full="MergeTree() ORDER BY tuple()",
        schema_digest=_digest("schema"),
        content_digest=_digest(f"content-{content or label}"),
        row_count=2,
    )


def _authority() -> ExternalAuthorityRecord:
    members = tuple(
        sorted(
            (
                ExternalMemberRecord(
                    member_id=_member(replica).member_id,
                    stage_state=ExternalMemberStageState.READY,
                    predecessor=_generation(f"old-{replica}", content="old"),
                    candidate=_generation(f"new-{replica}", content="new"),
                )
                for replica in (1, 2)
            ),
            key=lambda item: item.member_id,
        )
    )
    artifact = ArtifactIdentity(
        sha256=_digest("artifact"),
        byte_size=128,
        row_count=2,
        schema_digest=_digest("schema"),
        wire_digest=_digest("wire"),
    )
    target_key = derive_target_key("analytics_cluster", "analytics", "target_table")
    operation_id = derive_operation_id(
        scheduler_invocation="scheduled-run",
        target_key=target_key,
        normalized_plan_digest=_digest("plan"),
    )
    return ExternalAuthorityRecord(
        target_key=target_key,
        operation_id=operation_id,
        fence_token="opaque-fence",
        phase=ExternalAuthorityPhase.STAGED,
        dispatch_epoch=1,
        inventory_digest=_digest("inventory"),
        plan_digest=_digest("plan"),
        database="analytics",
        target="target_table",
        candidate="target_table__dpone_ext_1234",
        artifact=artifact,
        generation_id=derive_generation_id(
            operation_id=operation_id,
            artifact_sha256=artifact.sha256,
            schema_digest=artifact.schema_digest,
            row_count=artifact.row_count,
        ),
        members=members,
    )


def test_external_topology_requires_uniform_false_mode_and_stable_order() -> None:
    first, second = _member(1), _member(2)
    topology = ExternalTopology(
        cluster="analytics_cluster",
        replication_mode=ReplicationMode.EXTERNAL,
        members=(second, first),
    )

    topology.validate()

    assert topology.ordered_members == tuple(sorted((first, second), key=lambda item: item.member_id))
    assert topology.digest == replace(topology, members=(first, second)).digest
    with pytest.raises(ExternalContractError, match="MODE_MISMATCH"):
        replace(topology, replication_mode=ReplicationMode.INTERNAL).validate()
    with pytest.raises(ExternalContractError, match="MODE_MISMATCH"):
        replace(topology, members=(first, _member(2, internal=True))).validate()
    with pytest.raises(ExternalContractError, match="INVENTORY_INVALID"):
        replace(topology, members=(first, first)).validate()


def test_operation_generation_and_member_identity_are_deterministic() -> None:
    target_key = derive_target_key("analytics_cluster", "analytics", "target_table")
    plan_digest = _digest("plan")
    operation = derive_operation_id(
        scheduler_invocation="scheduled-run",
        target_key=target_key,
        normalized_plan_digest=plan_digest,
    )

    assert target_key == v1_digest_payload(
        {"cluster": "analytics_cluster", "database": "analytics", "target": "target_table"}
    )
    assert operation == derive_operation_id(
        scheduler_invocation="scheduled-run",
        target_key=target_key,
        normalized_plan_digest=plan_digest,
    )
    assert operation != derive_operation_id(
        scheduler_invocation="another-run",
        target_key=target_key,
        normalized_plan_digest=plan_digest,
    )
    assert derive_member_id(1, 2) == derive_member_id(1, 2)
    assert derive_member_id(1, 2) != derive_member_id(1, 1)
    assert derive_generation_id(
        operation_id=operation,
        artifact_sha256=_digest("artifact"),
        schema_digest=_digest("schema"),
        row_count=2,
    ) == derive_generation_id(
        operation_id=operation,
        artifact_sha256=_digest("artifact"),
        schema_digest=_digest("schema"),
        row_count=2,
    )


def test_orchestration_request_derives_identity_and_public_error_is_redacted() -> None:
    artifact = ExternalArtifactReceipt(
        artifact_id="artifact-v1",
        sha256=_digest("artifact"),
        byte_size=128,
        row_count=2,
        schema_sha256=_digest("schema"),
        content_sha256=_digest("content"),
        replayable=True,
    )
    request = ExternalPublicationRequest(
        cluster="analytics_cluster",
        database="analytics",
        target="target_table",
        scheduler_invocation="scheduled-run",
        plan_sha256=_digest("plan"),
        artifact=artifact,
    )

    request.validate()

    assert request.operation_id == request.operation_id
    assert request.generation_id == derive_generation_id(
        operation_id=request.operation_id,
        artifact_sha256=artifact.sha256,
        schema_digest=artifact.schema_sha256,
        row_count=artifact.row_count,
    )
    error = ExternalPublicationError(
        "DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE",
        evidence={"member_ids": [derive_member_id(1, 1)]},
    )
    assert str(error) == error.code
    assert "artifact-v1" not in json.dumps(error.evidence)


@pytest.mark.parametrize(
    ("target", "candidate", "expected"),
    [
        ("old", "new", MemberPublicationState.PENDING),
        ("new", "old", MemberPublicationState.COMMITTED),
        ("new", None, MemberPublicationState.CLEANUP_PENDING),
        ("old", None, MemberPublicationState.UNKNOWN),
    ],
)
def test_member_generation_classification_is_exact(target, candidate, expected) -> None:
    old, new = _generation("old"), _generation("new")
    values = {"old": old, "new": new, None: None}
    observed = MemberGenerationObservation(
        member_id=_member(1).member_id,
        target=values[target],
        candidate=values[candidate],
    )

    assert classify_member_publication(observed, desired=new, predecessor=old) is expected

    divergent = replace(new, content_digest=_digest("divergent"))
    assert (
        classify_member_publication(
            replace(observed, target=divergent),
            desired=new,
            predecessor=old,
        )
        is MemberPublicationState.UNKNOWN
    )


def test_absent_target_member_generation_classification_is_exact() -> None:
    new = _generation("new")
    member_id = _member(1).member_id

    assert (
        classify_member_publication(
            MemberGenerationObservation(member_id, None, new),
            desired=new,
            predecessor=None,
        )
        is MemberPublicationState.PENDING
    )
    assert (
        classify_member_publication(
            MemberGenerationObservation(member_id, new, None),
            desired=new,
            predecessor=None,
        )
        is MemberPublicationState.COMMITTED
    )


def test_external_authority_round_trip_is_canonical_and_evidence_is_redacted() -> None:
    record = _authority()

    decoded = ExternalAuthorityRecord.from_payload(record.payload)
    evidence = decoded.to_evidence()
    rendered = json.dumps(evidence, sort_keys=True)

    assert decoded == record
    assert decoded.payload == record.payload
    assert decoded.payload_sha256 == _digest_payload_text(record.payload)
    assert evidence["schema_version"] == EXTERNAL_AUTHORITY_SCHEMA_VERSION
    assert evidence["replication_mode"] == "external"
    assert [item["member_id"] for item in evidence["members"]] == sorted(member.member_id for member in record.members)
    for forbidden in (
        "analytics_cluster",
        "analytics",
        "target_table",
        "opaque-fence",
        "host",
        "address",
        "password",
        "/",
    ):
        assert forbidden not in rendered


def test_external_codec_rejects_v1_and_unknown_authority_without_mutating_v1_contract() -> None:
    v1_payload = json.dumps({"schema_version": INTERNAL_AUTHORITY_SCHEMA_VERSION})

    with pytest.raises(ExternalContractError, match="SCHEMA_UNSUPPORTED"):
        ExternalAuthorityRecord.from_payload(v1_payload)

    assert EXTERNAL_AUTHORITY_SCHEMA_VERSION != INTERNAL_AUTHORITY_SCHEMA_VERSION
    assert derive_target_key("analytics_cluster", "analytics", "target_table") == v1_digest_payload(
        {"cluster": "analytics_cluster", "database": "analytics", "target": "target_table"}
    )


def _digest_payload_text(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode()).hexdigest()
