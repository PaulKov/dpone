"""Pure progression never replaces a durable claim with a later publication."""

from dataclasses import replace

import pytest

from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest
from dpone.contracts.dbt_workspace_channel import WorkspaceHandoverError
from dpone.contracts.dbt_workspace_lifecycle import DbtWorkspaceLifecycleIdentity, DbtWorkspaceLifecycleReadback
from dpone.ports.dbt_workspace_handover import WorkspaceChannelReadback, WorkspaceStoredOccurrence
from dpone.services.dbt_workspace_handover import WorkspaceHandoverAction, plan_workspace_handover
from tests.test_airflow_desired_state import _desired_payload
from tests.test_dbt_workspace_handover_contracts import _channel, _claim
from tests.test_dbt_workspace_lifecycle import _readback
from tests.test_dbt_workspace_registration_contracts import _baseline


def _adopted(state="ACTIVE"):
    baseline = _baseline()
    return WorkspaceStoredOccurrence(baseline, baseline.request, replace(_readback(), state=state))


def _pending(state="ACTIVE"):
    current = _adopted(state)
    channel = replace(_channel(), environment="prod")
    desired = AirflowDesiredDeployment.from_json(_claim().desired_state_json)
    desired = replace(desired, environment="prod")
    claim = replace(
        _claim(),
        channel=channel,
        desired_state_json=desired.to_json_bytes().decode(),
        desired_state_sha256=desired.sha256,
        predecessor_activation_id=current.request.activation_id,
        predecessor_deployment_id=current.request.deployment_id,
        predecessor_request_sha256=current.request.request_sha256,
    )
    return WorkspaceChannelReadback(channel, 1, current, claim, None)


def test_registered_empty_requires_latest_before_claim():
    readback = WorkspaceChannelReadback(_channel(), 0, None, None, None)
    assert plan_workspace_handover(readback, latest=None, local_matches=False) is WorkspaceHandoverAction.OBSERVE_LATEST
    latest = AirflowDesiredDeployment.from_json(_claim().desired_state_json)
    assert plan_workspace_handover(readback, latest=latest, local_matches=False) is WorkspaceHandoverAction.CLAIM_LATEST


@pytest.mark.parametrize(
    ("state", "action"),
    [
        ("ACTIVE", WorkspaceHandoverAction.BEGIN_RETIREMENT),
        ("RETIRING", WorkspaceHandoverAction.FINALIZE_RETIREMENT),
        ("RETIRED", WorkspaceHandoverAction.PREPARE_SUCCESSOR),
    ],
)
def test_supersession_does_not_replace_claimed_saga(state, action):
    latest = AirflowDesiredDeployment.from_mapping(
        _desired_payload(occurrence_id="323e4567-e89b-42d3-a456-426614174000")
    )
    assert plan_workspace_handover(_pending(state), latest=latest, local_matches=False) is action
    assert plan_workspace_handover(_pending(state), latest=None, local_matches=False) is action


def test_idle_same_current_is_noop_or_cold_replication_without_new_claim():
    current = _adopted()
    readback = WorkspaceChannelReadback(replace(_channel(), environment="prod"), 0, current, None, None)
    latest = AirflowDesiredDeployment.from_json(current.snapshot.desired_state_json)
    assert plan_workspace_handover(readback, latest=latest, local_matches=True) is WorkspaceHandoverAction.CONVERGED
    assert (
        plan_workspace_handover(readback, latest=latest, local_matches=False)
        is WorkspaceHandoverAction.REPLICATE_CURRENT
    )
    assert readback.revision == 0 and readback.current.lifecycle.guards == _readback().guards
    assert plan_workspace_handover(readback, latest=None, local_matches=True) is WorkspaceHandoverAction.OBSERVE_LATEST


def test_retiring_or_retired_cannot_be_idle_current():
    for state in ("RETIRING", "RETIRED"):
        with pytest.raises(WorkspaceHandoverError):
            WorkspaceChannelReadback(replace(_channel(), environment="prod"), 0, _adopted(state), None, None)


def test_saved_claim_revision_and_exact_predecessor_are_required():
    readback = _pending()
    with pytest.raises(WorkspaceHandoverError):
        replace(readback, revision=2)
    with pytest.raises(WorkspaceHandoverError):
        replace(readback, current=None)


def test_same_current_uuid_with_changed_desired_bytes_is_not_converged():
    current = _adopted()
    readback = WorkspaceChannelReadback(replace(_channel(), environment="prod"), 0, current, None, None)
    latest = replace(
        AirflowDesiredDeployment.from_json(current.snapshot.desired_state_json), promoted_at="2026-07-29T09:10:11Z"
    )
    with pytest.raises(WorkspaceHandoverError):
        plan_workspace_handover(readback, latest=latest, local_matches=True)


def _prepared():
    readback = _pending("RETIRED")
    claim = readback.pending
    request = DbtWorkspaceActivationRequest.build(
        activation_id=claim.successor_activation_id,
        environment=claim.channel.environment,
        release_id=claim.successor_release_id,
        deployment_id=claim.successor_deployment_id,
        previous_deployment_id=claim.predecessor_deployment_id,
        source_inventory_sha256=claim.source_inventory_sha256,
        runtime_context_sha256=claim.runtime_context_sha256,
        write_subjects=readback.current.request.write_subjects,
        resources=readback.current.request.resources,
    )
    lifecycle = DbtWorkspaceLifecycleReadback(
        DbtWorkspaceLifecycleIdentity.from_request(request),
        request.request_sha256,
        "PREPARED",
        (replace(_readback().guards[0], fencing_epoch=2),),
    )
    return replace(readback, pending_occurrence=WorkspaceStoredOccurrence(claim, request, lifecycle))


def test_prepared_needs_pointer_then_active_confirmation():
    readback = _prepared()
    assert (
        plan_workspace_handover(readback, latest=None, local_matches=False)
        is WorkspaceHandoverAction.REPLICATE_SUCCESSOR
    )
    assert plan_workspace_handover(readback, latest=None, local_matches=True) is WorkspaceHandoverAction.COMPLETE
    assert readback.pending_occurrence.lifecycle.state == "PREPARED"


def test_completion_is_atomic_and_exact_same_current_does_not_claim_again():
    prepared = _prepared()
    successor = replace(
        prepared.pending_occurrence, lifecycle=replace(prepared.pending_occurrence.lifecycle, state="ACTIVE")
    )
    with pytest.raises(WorkspaceHandoverError):
        replace(prepared, pending_occurrence=successor)
    complete = WorkspaceChannelReadback(prepared.channel, 2, successor, None, None)
    desired = AirflowDesiredDeployment.from_json(successor.snapshot.desired_state_json)
    assert (
        plan_workspace_handover(complete, latest=desired, local_matches=False)
        is WorkspaceHandoverAction.REPLICATE_CURRENT
    )
    assert plan_workspace_handover(complete, latest=desired, local_matches=True) is WorkspaceHandoverAction.CONVERGED
    assert complete.revision == 2 and complete.current.lifecycle.guards[0].fencing_epoch == 2


def test_successor_prepared_cannot_hide_unretired_predecessor():
    prepared = _prepared()
    with pytest.raises(WorkspaceHandoverError):
        replace(prepared, current=_adopted("ACTIVE"))
