"""Retirement admission policy: observations are supplied by trusted adapters.

These synthetic facts test policy, not deployment writer exclusion or a live
history certificate. Removing any guard below must admit the corresponding
unsafe fixture and fail its test.
"""

from dataclasses import replace
from importlib import import_module

import pytest

from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityPhase,
    ClusterInventory,
    ClusterReplica,
    ReplicaGeneration,
    VersionedAuthorityRecord,
    digest_payload,
)
from tests.test_mssql_publication_authority import BINDING_DIGEST, record


def api():
    return import_module("dpone.contracts.publication_retirement")


def observation():
    model = api()
    inventory = ClusterInventory(
        "example", tuple(ClusterReplica(host, host, 9000, 1, index, True) for index, host in enumerate(("a", "b"), 1))
    )
    legacy = replace(
        record(),
        inventory_digest=inventory.digest,
        target_key=digest_payload({"cluster": "example", "database": "analytics", "target": "example"}),
    )
    replicas = tuple(
        model.RetirementReplicaObservation(
            host=host,
            authority=VersionedAuthorityRecord(legacy, 1),
            original_payload=legacy.payload.encode(),
            generation=ReplicaGeneration(host, legacy.predecessor, legacy.desired, row_count=2),
            history_from=100,
            history_through=200,
            history_receipt_digest="1" * 64,
        )
        for host in inventory.hosts
    )
    freeze = model.PublicationFreezeObservation(
        issuer="deployment-controller",
        receipt_digest="2" * 64,
        target_key=legacy.target_key,
        binding_digest=BINDING_DIGEST,
        inventory_digest=inventory.digest,
        expected_writers=("scheduler", "worker"),
        excluded_writers=("scheduler", "worker"),
        established_at=150,
        drained_at=200,
        expires_at=300,
    )
    return model.PublicationRetirementObservation(
        binding_digest=BINDING_DIGEST,
        source_location_digest="3" * 64,
        prepared_at=100,
        inventory=inventory,
        replicas=replicas,
        freeze=freeze,
    )


def plan(value=None, now=220):
    return api().plan_retirement(value or observation(), now=now)


def test_plan_preserves_original_bytes_without_quality_or_dispatch_capability():
    value = observation()
    result = plan(value)
    assert result.observation is value
    assert result.original.record.quality_evidence is None
    assert result.original.record.phase is AuthorityPhase.PREPARED
    assert result.original_payload == value.replicas[0].original_payload
    assert result.phase == "RETIRED_UNPUBLISHED"
    assert result.origin == "legacy_retired"
    assert not hasattr(result, "permit")
    assert plan(value).digest == result.digest


@pytest.mark.parametrize(
    "field",
    [
        "ddl_correlation_token",
        "ddl_entry",
        "ddl_query_digest",
        "cleanup_correlation_token",
        "cleanup_entry",
        "cleanup_query_digest",
        "quality_reader",
        "authority_write_id",
    ],
)
def test_prior_dispatch_or_new_backend_identity_cannot_be_retired(field):
    value = observation()
    legacy = replace(value.replicas[0].authority.record, **{field: "present"})
    replicas = tuple(
        replace(item, authority=VersionedAuthorityRecord(legacy, 1), original_payload=legacy.payload.encode())
        for item in value.replicas
    )
    with pytest.raises(ValueError):
        plan(replace(value, replicas=replicas))


@pytest.mark.parametrize(
    "phase",
    [
        AuthorityPhase.DISPATCHING,
        AuthorityPhase.COMMITTED,
        AuthorityPhase.COMPLETED,
        AuthorityPhase.CLEANUP_DISPATCHING,
    ],
)
def test_later_phase_is_not_unpublished_even_when_generations_look_original(phase):
    value = observation()
    legacy = replace(value.replicas[0].authority.record, phase=phase)
    with pytest.raises(ValueError):
        plan(
            replace(
                value,
                replicas=tuple(
                    replace(
                        item, authority=VersionedAuthorityRecord(legacy, 1), original_payload=legacy.payload.encode()
                    )
                    for item in value.replicas
                ),
            )
        )


@pytest.mark.parametrize(
    "change",
    [
        {"history_from": 101},
        {"history_through": 199},
        {"history_receipt_digest": ""},
        {"history_gaps": ((120, 121),)},
        {"matching_ddl_entries": ("query-example",)},
        {"pending_requests": ("request",)},
        {"active_mutations": ("mutation",)},
        {"active_writers": ("worker",)},
        {"original_payload": b"{}"},
    ],
)
def test_missing_history_or_pending_work_blocks_retirement(change):
    value = observation()
    with pytest.raises(ValueError):
        plan(replace(value, replicas=(replace(value.replicas[0], **change), value.replicas[1])))


@pytest.mark.parametrize("field", ["target", "candidate", "target_healthy", "candidate_healthy", "host"])
def test_generation_or_replica_drift_cannot_be_retired(field):
    value = observation()
    item = value.replicas[0]
    changed = False if field.endswith("healthy") else "unexpected" if field == "host" else None
    with pytest.raises(ValueError):
        plan(
            replace(
                value,
                replicas=(replace(item, generation=replace(item.generation, **{field: changed})), value.replicas[1]),
            )
        )


@pytest.mark.parametrize("replicas", [(), (0,), (0, 0), (1, 0, 1)])
def test_missing_duplicate_or_extra_replica_is_blocked(replicas):
    value = observation()
    with pytest.raises(ValueError):
        plan(replace(value, replicas=tuple(value.replicas[index] for index in replicas)))


def test_different_legacy_revision_on_one_replica_is_blocked():
    value = observation()
    item = value.replicas[0]
    with pytest.raises(ValueError):
        plan(replace(value, replicas=(replace(item, authority=replace(item.authority, version=2)), value.replicas[1])))


@pytest.mark.parametrize(
    "change",
    [
        {"issuer": ""},
        {"receipt_digest": "bad"},
        {"target_key": "4" * 64},
        {"binding_digest": "4" * 64},
        {"inventory_digest": "4" * 64},
        {"excluded_writers": ("scheduler",)},
        {"expected_writers": ()},
        {"expected_writers": ("worker", "worker")},
        {"expires_at": 220},
        {"established_at": 201},
        {"drained_at": 221},
    ],
)
def test_incomplete_expired_or_misbound_freeze_is_blocked(change):
    value = observation()
    with pytest.raises(ValueError):
        plan(replace(value, freeze=replace(value.freeze, **change)))


def test_user_boolean_is_not_a_deployment_freeze_observation():
    with pytest.raises(ValueError):
        plan(replace(observation(), freeze=True))


def test_first_publication_requires_target_absent_on_every_replica():
    value = observation()
    legacy = replace(value.replicas[0].authority.record, predecessor=None)
    replicas = tuple(
        replace(
            item,
            authority=VersionedAuthorityRecord(legacy, 1),
            original_payload=legacy.payload.encode(),
            generation=replace(item.generation, target=None),
        )
        for item in value.replicas
    )
    assert plan(replace(value, replicas=replicas)).original.record.predecessor is None


def test_plan_digest_binds_storage_source_and_original_replica_versions():
    value = observation()
    different_source = replace(value, source_location_digest="4" * 64)
    different_binding = replace(value, binding_digest="5" * 64, freeze=replace(value.freeze, binding_digest="5" * 64))
    different_revision = replace(
        value, replicas=tuple(replace(item, authority=replace(item.authority, version=2)) for item in value.replicas)
    )
    assert len({plan(item).digest for item in (value, different_source, different_binding, different_revision)}) == 4


@pytest.mark.parametrize("now", [True, -1, 1.5, 199, 300])
def test_invalid_or_out_of_freeze_clock_is_rejected(now):
    with pytest.raises(ValueError):
        plan(now=now)


@pytest.mark.parametrize(
    "change",
    [
        {"target_key": "0" * 64},
        {"operation_id": ""},
        {"staged_rows": True},
        {"staged_rows": -1},
        {"dispatch_epoch": False},
        {"plan_digest": "invalid"},
    ],
)
def test_malformed_legacy_identity_cannot_enter_retirement_plan(change):
    value = observation()
    legacy = replace(value.replicas[0].authority.record, **change)
    value = replace(
        value,
        freeze=replace(value.freeze, target_key=legacy.target_key),
        replicas=tuple(
            replace(item, authority=VersionedAuthorityRecord(legacy, 1), original_payload=legacy.payload.encode())
            for item in value.replicas
        ),
    )
    with pytest.raises(ValueError):
        plan(value)


@pytest.mark.parametrize(
    "schema,evidence",
    [("dpone.clickhouse.cluster-full-refresh.v2", None), ("dpone.clickhouse.cluster-full-refresh.v1", "opaque")],
)
def test_legacy_envelope_version_must_agree_with_quality_field(schema, evidence):
    value = observation()
    legacy = replace(value.replicas[0].authority.record, schema_version=schema, quality_evidence=evidence)
    replicas = tuple(
        replace(item, authority=VersionedAuthorityRecord(legacy, 1), original_payload=legacy.payload.encode())
        for item in value.replicas
    )
    with pytest.raises(ValueError):
        plan(replace(value, replicas=replicas))


def test_same_uuid_with_different_metadata_is_not_an_isolated_candidate():
    value = observation()
    legacy = value.replicas[0].authority.record
    legacy = replace(legacy, desired=replace(legacy.desired, uuid=legacy.predecessor.uuid))
    replicas = tuple(
        replace(
            item,
            authority=VersionedAuthorityRecord(legacy, 1),
            original_payload=legacy.payload.encode(),
            generation=replace(item.generation, candidate=legacy.desired),
        )
        for item in value.replicas
    )
    with pytest.raises(ValueError):
        plan(replace(value, replicas=replicas))
