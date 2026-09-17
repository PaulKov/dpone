from __future__ import annotations

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.runtime.full_refresh_attempt import (
    AttemptIdentity,
    AttemptState,
    AuthenticatedResourceReadback,
    EffectKind,
    EffectState,
    InvocationKey,
    PlannedResource,
    ResourceKind,
    ResourceState,
    assert_attempt_transition,
    assert_effect_transition,
    assert_journal_safe,
    assert_resource_transition,
    require_opaque_reference,
)


def invocation(*, try_number: int = 1, map_index: int = -1) -> InvocationKey:
    return InvocationKey(
        release_id="release-1",
        deployment_id="deployment-1",
        workflow_id="workflow-1",
        workload_id="workload-1",
        dag_id="dag-1",
        airflow_dag_run_id="manual__1",
        task_id="load",
        task_instance_try_number=try_number,
        map_index=map_index,
    )


def test_scheduler_invocation_digest_is_canonical_and_complete() -> None:
    key = invocation()

    assert key.digest == "b4f34f6871f82f313837477e65915814d39fcc614b858c0705f406abe8ab7082"
    assert key.canonical_fields() == {
        "airflow_dag_run_id": "manual__1",
        "dag_id": "dag-1",
        "deployment_id": "deployment-1",
        "map_index": -1,
        "release_id": "release-1",
        "task_id": "load",
        "task_instance_try_number": 1,
        "workflow_id": "workflow-1",
        "workload_id": "workload-1",
    }
    assert invocation(try_number=2).digest != key.digest
    assert invocation(map_index=0).digest != key.digest


def test_invocation_rejects_missing_or_invalid_stable_fields() -> None:
    with pytest.raises(ValueError, match="release_id"):
        replace(invocation(), release_id=" ")
    with pytest.raises(ValueError, match="task_instance_try_number"):
        invocation(try_number=0)
    with pytest.raises(ValueError, match="map_index"):
        invocation(map_index=-2)


def test_attempt_identity_requires_database_allocated_uuid_values() -> None:
    identity = AttemptIdentity(
        invocation=invocation(),
        transfer_run_id=UUID("11111111-1111-4111-8111-111111111111"),
        attempt_nonce=UUID("22222222-2222-4222-8222-222222222222"),
    )

    assert identity.attempt_id == identity.transfer_run_id

    with pytest.raises(ValueError, match="distinct"):
        replace(identity, attempt_nonce=identity.transfer_run_id)


@pytest.mark.parametrize(
    ("previous", "next_state"),
    [
        (AttemptState.PLANNED, AttemptState.FROZEN_SOURCE),
        (AttemptState.PUBLICATION_PREPARED, AttemptState.COMMAND_SENT),
        (AttemptState.COMMAND_SENT, AttemptState.COMMIT_UNKNOWN),
        (AttemptState.BACKUP_CLEANED, AttemptState.COMPLETE),
    ],
)
def test_attempt_state_machine_accepts_only_declared_edges(
    previous: AttemptState,
    next_state: AttemptState,
) -> None:
    assert_attempt_transition(previous, next_state)


def test_attempt_state_machine_rejects_skips_and_terminal_replay() -> None:
    with pytest.raises(ValueError, match="attempt transition"):
        assert_attempt_transition(AttemptState.PLANNED, AttemptState.COMPLETE)
    with pytest.raises(ValueError, match="attempt transition"):
        assert_attempt_transition(AttemptState.COMPLETE, AttemptState.PLANNED)


def test_resource_and_effect_state_machines_require_readback_before_binding() -> None:
    assert_resource_transition(ResourceState.CREATE_PLANNED, ResourceState.CREATE_GRANTED)
    assert_resource_transition(ResourceState.CREATE_GRANTED, ResourceState.CREATE_DISPATCHED)
    assert_resource_transition(ResourceState.CREATE_GRANTED, ResourceState.CREATE_NOT_APPLIED)
    assert_resource_transition(ResourceState.CREATE_DISPATCHED, ResourceState.CREATE_DISPATCHED)
    assert_resource_transition(ResourceState.CREATE_DISPATCHED, ResourceState.CREATE_OBSERVED)
    assert_resource_transition(ResourceState.CREATE_OBSERVED, ResourceState.BOUND)
    assert_resource_transition(ResourceState.CREATE_NOT_APPLIED, ResourceState.CREATE_PLANNED)
    assert_resource_transition(ResourceState.RENAME_NOT_APPLIED, ResourceState.RENAME_PLANNED)
    assert_effect_transition(EffectState.PLANNED, EffectState.GRANTED)
    assert_effect_transition(EffectState.GRANTED, EffectState.DISPATCHED)
    assert_effect_transition(EffectState.DISPATCHED, EffectState.TERMINATED)

    with pytest.raises(ValueError, match="resource transition"):
        assert_resource_transition(ResourceState.CREATE_DISPATCHED, ResourceState.BOUND)
    with pytest.raises(ValueError, match="effect transition"):
        assert_effect_transition(EffectState.PLANNED, EffectState.DISPATCHED)

    assert ResourceKind.MSSQL_WORK_TABLE.value == "mssql_work_table"
    assert EffectKind.CREATE.value == "create"


def test_authenticated_readback_matches_every_planned_create_coordinate() -> None:
    effect_id = UUID("33333333-3333-4333-8333-333333333333")
    resource = PlannedResource(
        resource_id=UUID("44444444-4444-4444-8444-444444444444"),
        authority="clickhouse-target",
        planned_name="slice_1",
        resource_kind=ResourceKind.CLICKHOUSE_SLICE_TABLE,
        schema_hash="a" * 64,
    )
    readback = AuthenticatedResourceReadback(
        effect_id=effect_id,
        credential_version_ref="credential-v7",
        authority=resource.authority,
        planned_name=resource.planned_name,
        schema_hash=resource.schema_hash,
        exact_identity={"uuid": "55555555-5555-4555-8555-555555555555"},
        server_receipt_ref="receipt-v1",
    )

    assert readback.proves(resource, effect_id=effect_id, credential_version_ref="credential-v7")
    assert not readback.proves(resource, effect_id=effect_id, credential_version_ref="credential-v8")
    assert not replace(readback, exact_identity={"uuid": "not-a-uuid"}).proves(
        resource, effect_id=effect_id, credential_version_ref="credential-v7"
    )
    assert not replace(readback, exact_identity={"uuid": str(effect_id), "name": "slice_1"}).proves(
        resource, effect_id=effect_id, credential_version_ref="credential-v7"
    )


def test_credentials_cannot_enter_opaque_refs_or_nested_journal_facts() -> None:
    require_opaque_reference("credential-version/7", field_name="credential_version_ref")
    with pytest.raises(ValueError, match="opaque logical reference"):
        require_opaque_reference("invalid=reference", field_name="credential_version_ref")
    for unsafe in (
        {"probe": [{"token": "plaintext"}]},
        {"nested": {"client-secret": "plaintext"}},
        {"nested": [{"api_credentials": "plaintext"}]},
        {"nested": [[{"accessToken": "plaintext"}]]},
    ):
        with pytest.raises(ValueError, match="never credentials"):
            assert_journal_safe(unsafe)

    class SecretBearingObject:
        def __str__(self) -> str:
            return "must-not-be-serialized"

    for unsafe_json in (
        {"custom": SecretBearingObject()},
        {1: "non-string-key"},
        {"tuple": ("not", "json")},
        {"number": float("nan")},
    ):
        with pytest.raises(ValueError, match="JSON-native|keys|finite"):
            assert_journal_safe(unsafe_json)


def test_mssql_readback_requires_only_a_positive_object_id() -> None:
    resource = PlannedResource(
        resource_id=UUID("44444444-4444-4444-8444-444444444444"),
        authority="mssql-source",
        planned_name="work_1",
        resource_kind=ResourceKind.MSSQL_WORK_TABLE,
        schema_hash="a" * 64,
    )
    readback = AuthenticatedResourceReadback(
        effect_id=UUID("33333333-3333-4333-8333-333333333333"),
        credential_version_ref="credential-v7",
        authority=resource.authority,
        planned_name=resource.planned_name,
        schema_hash=resource.schema_hash,
        exact_identity={"object_id": 42},
        server_receipt_ref="receipt-v1",
    )

    assert readback.proves(resource, effect_id=readback.effect_id, credential_version_ref="credential-v7")
    assert not replace(readback, exact_identity={"object_id": 0}).proves(
        resource, effect_id=readback.effect_id, credential_version_ref="credential-v7"
    )
    assert not replace(readback, exact_identity={"object_id": True}).proves(
        resource, effect_id=readback.effect_id, credential_version_ref="credential-v7"
    )
    with pytest.raises(ValueError, match="opaque logical reference"):
        replace(readback, server_receipt_ref="receipt with secret material")
