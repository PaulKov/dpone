"""Stateful executor checks; not SQL isolation or artifact-signature certification."""

from dataclasses import replace

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest
from dpone.contracts.dbt_workspace_channel import WorkspaceHandoverError
from dpone.contracts.dbt_workspace_lifecycle import DbtWorkspaceLifecycleIdentity, DbtWorkspaceLifecycleReadback
from dpone.ports.dbt_workspace_handover import WorkspaceChannelReadback, WorkspaceStoredOccurrence
from dpone.ports.dbt_workspace_handover_execution import VerifiedWorkspaceDesired
from dpone.services.dbt_workspace_handover_execution import WorkspaceHandoverExecutor
from tests.test_dbt_workspace_handover_state_machine import _adopted, _pending, _prepared


def _idle():
    pending = _pending()
    return WorkspaceChannelReadback(pending.channel, 0, pending.current, None, None)


def _latest(claim=None, occurrence_id=None):
    claim = claim or _pending().pending
    desired = AirflowDesiredDeployment.from_json(claim.desired_state_json)
    if occurrence_id is not None:
        desired = replace(desired, source=replace(desired.source, occurrence_id=occurrence_id))
    return VerifiedWorkspaceDesired(desired.to_json_bytes().decode(), "revision-1")


def _request(claim, resources=None):
    return DbtWorkspaceActivationRequest.build(
        activation_id=claim.successor_activation_id,
        environment=claim.channel.environment,
        release_id=claim.successor_release_id,
        deployment_id=claim.successor_deployment_id,
        previous_deployment_id=claim.predecessor_deployment_id,
        source_inventory_sha256=claim.source_inventory_sha256,
        runtime_context_sha256=claim.runtime_context_sha256,
        write_subjects=_adopted().request.write_subjects,
        resources=resources or _adopted().request.resources,
    )


class Store:
    """Protected-state model: each mutation changes one coherent readback."""

    def __init__(self, state, events):
        self.state = state
        self.events = events
        self.claim_race = None
        self.prepare_winner = None
        self.fail = None
        self.stuck = False

    def read_channel(self, channel):
        self.events.append("read")
        return self.state

    def claim(self, claim, *, expected_revision):
        self.events.append("claim")
        assert self.events[-2] == "remote-recheck"
        assert expected_revision == self.state.revision
        if self.claim_race:
            self.state = self.claim_race
        elif not self.stuck:
            self.state = replace(self.state, revision=claim.claim_revision, pending=claim)
        return self.state

    def begin_retirement(self, claim):
        self.events.append("retire")
        assert self.state.pending == claim
        self.state = replace(
            self.state,
            current=replace(self.state.current, lifecycle=replace(self.state.current.lifecycle, state="RETIRING")),
        )
        return self.state

    def finalize_retirement(self, claim):
        self.events.append("finalize")
        assert self.state.pending == claim
        if self.fail:
            raise self.fail
        self.state = replace(
            self.state,
            current=replace(self.state.current, lifecycle=replace(self.state.current.lifecycle, state="RETIRED")),
        )
        return self.state

    def prepare_successor(self, claim, request):
        self.events.append("prepare")
        assert self.state.pending == claim
        assert self.state.current is None or self.state.current.lifecycle.state == "RETIRED"
        request = self.prepare_winner or request
        lifecycle = DbtWorkspaceLifecycleReadback(
            DbtWorkspaceLifecycleIdentity.from_request(request),
            request.request_sha256,
            "PREPARED",
            (
                replace(
                    _adopted().lifecycle.guards[0],
                    fencing_epoch=self.state.revision + 1,
                    resource_sha256=canonical_fingerprint(request.resources[0].to_dict()),
                ),
            ),
        )
        self.state = replace(self.state, pending_occurrence=WorkspaceStoredOccurrence(claim, request, lifecycle))
        return self.state

    def complete(self, claim):
        self.events.append("complete")
        assert self.state.pending == claim
        occurrence = self.state.pending_occurrence
        current = replace(occurrence, lifecycle=replace(occurrence.lifecycle, state="ACTIVE"))
        self.state = WorkspaceChannelReadback(self.state.channel, self.state.revision + 1, current, None, None)
        return self.state


class Driver:
    """No real credentials/signatures: deliberate test adapter with event tracing."""

    def __init__(self, store, events):
        self.store = store
        self.events = events
        self.latest = _latest()
        self.remote_error = None
        self.verify_error = None
        self.remote_matches = True
        self.local = None
        self.replicated = []
        self.queue = []
        self.claim_edit = lambda claim: claim
        self.on_replicate = None

    def verified_latest(self, channel):
        self.events.append("latest")
        if self.remote_error:
            raise self.remote_error
        if self.queue:
            self.latest = self.queue.pop(0)
        return self.latest

    def remote_revision_matches(self, latest):
        self.events.append("remote-recheck")
        return self.remote_matches

    def build_claim(self, readback, latest):
        self.events.append("build-claim")
        previous = readback.current
        return self.claim_edit(
            replace(
                _pending().pending,
                expected_channel_revision=readback.revision,
                claim_revision=readback.revision + 1,
                predecessor_activation_id=None if previous is None else previous.request.activation_id,
                predecessor_deployment_id=None if previous is None else previous.request.deployment_id,
                predecessor_request_sha256=None if previous is None else previous.request.request_sha256,
                successor_activation_id=latest.desired.source.occurrence_id,
                successor_release_id=latest.desired.promotion.release_id,
                successor_deployment_id=latest.desired.promotion.deployment_id,
                desired_state_json=latest.desired_json,
                desired_state_sha256=latest.desired.sha256,
                observed_remote_revision=latest.revision,
            )
        )

    def verify_snapshot(self, channel, snapshot):
        self.events.append("verify")
        if self.verify_error:
            raise self.verify_error

    def observe_successor(self, claim):
        self.events.append("observe")
        assert self.store.state.current is None or self.store.state.current.lifecycle.state == "RETIRED"
        return _request(claim)

    def local_matches(self, occurrence):
        self.events.append("local")
        return self.local == occurrence.request

    def replicate(self, occurrence):
        self.events.append("replicate")
        self.local = occurrence.request
        self.replicated.append(occurrence)
        if self.on_replicate:
            self.on_replicate()


def _system(state=None):
    events = []
    store = Store(state or _idle(), events)
    driver = Driver(store, events)
    executor = WorkspaceHandoverExecutor(channel=store.state.channel, store=store, driver=driver)
    return executor, store, driver, events


def test_full_transition_orders_retirement_observation_pointer_then_complete():
    executor, store, driver, events = _system()
    driver.local = store.state.current.request
    result = executor.run_cycle()
    assert result.converged and result.channel_revision == 2
    phases = [
        name
        for name in events
        if name in {"claim", "retire", "finalize", "observe", "prepare", "replicate", "complete"}
    ]
    assert phases == ["claim", "retire", "finalize", "observe", "prepare", "replicate", "complete"]
    assert events.index("verify") < events.index("retire")
    assert "latest" in events[events.index("complete") + 1 :]
    assert driver.local == store.state.current.request


@pytest.mark.parametrize("state", [_pending(), _pending("RETIRING"), _pending("RETIRED"), _prepared()])
def test_pending_survives_cold_cache_without_new_claim_or_remote_dependency(state):
    executor, store, driver, events = _system(state)
    driver.remote_error = RuntimeError("remote unavailable")
    with pytest.raises(RuntimeError, match="remote unavailable"):
        executor.run_cycle()
    assert events.index("complete") < events.index("latest")
    assert "claim" not in events
    assert store.state.current.request.activation_id == state.pending.successor_activation_id
    if state.pending_occurrence is not None:
        assert "observe" not in events
        assert driver.replicated[0].request is state.pending_occurrence.request


@pytest.mark.parametrize("completed", [False, True])
def test_cold_current_replicates_without_reservation_or_retirement(completed):
    state = _idle()
    if completed:
        prepared = _prepared()
        occurrence = prepared.pending_occurrence
        state = WorkspaceChannelReadback(
            prepared.channel,
            2,
            replace(occurrence, lifecycle=replace(occurrence.lifecycle, state="ACTIVE")),
            None,
            None,
        )
    executor, store, driver, events = _system(state)
    driver.latest = VerifiedWorkspaceDesired(state.current.snapshot.desired_state_json, "same")
    assert executor.run_cycle().converged
    assert store.state == state
    assert driver.replicated[0].request is state.current.request
    assert not {"claim", "retire", "prepare", "complete"}.intersection(events)


@pytest.mark.parametrize("reason", ["COMMIT_UNKNOWN", "WAITING_ATTEMPTS", "missing_receipt"])
def test_gateway_refusal_is_preserved_without_in_cycle_retry(reason):
    executor, store, _, events = _system(_pending("RETIRING"))
    error = RuntimeError(reason)
    store.fail = error
    with pytest.raises(RuntimeError) as caught:
        executor.run_cycle()
    assert caught.value is error
    assert events.count("finalize") == 1 and "observe" not in events


def test_current_capability_revocation_blocks_before_retirement():
    executor, _, driver, events = _system(_pending())
    driver.verify_error = RuntimeError("revoked")
    with pytest.raises(RuntimeError, match="revoked"):
        executor.run_cycle()
    assert not {"retire", "finalize", "observe", "replicate"}.intersection(events)


def test_changed_remote_revision_never_claims_and_cycle_is_bounded():
    executor, store, driver, events = _system()
    driver.local = store.state.current.request
    driver.remote_matches = False
    assert not executor.run_cycle().converged
    assert events.count("remote-recheck") == 24 and "claim" not in events


def test_unchanged_mutation_readback_returns_continuation():
    executor, store, _, events = _system()
    store.stuck = True
    assert not executor.run_cycle().converged
    assert events.count("claim") == 1


def test_lost_claim_cas_replans_winning_claim_not_stale_proposal():
    executor, store, driver, events = _system()
    driver.local = store.state.current.request
    observed = _pending("RETIRED")
    winner_latest = _latest(occurrence_id="323e4567-e89b-42d3-a456-426614174000")
    winner_claim = replace(
        observed.pending,
        successor_activation_id=winner_latest.desired.source.occurrence_id,
        desired_state_json=winner_latest.desired_json,
        desired_state_sha256=winner_latest.desired.sha256,
    )
    winning = replace(observed, pending=winner_claim)
    driver.queue = [driver.latest, winner_latest]
    store.claim_race = winning
    result = executor.run_cycle()
    assert result.converged and "retire" not in events
    assert driver.replicated[0].snapshot == winning.pending
    assert driver.replicated[0].request.activation_id != observed.pending.successor_activation_id


def test_two_transition_budget_includes_resumed_pending():
    executor, _, driver, events = _system(_pending())
    driver.queue = [
        _latest(occurrence_id="323e4567-e89b-42d3-a456-426614174000"),
        _latest(occurrence_id="423e4567-e89b-42d3-a456-426614174000"),
    ]
    assert not executor.run_cycle().converged
    assert events.count("complete") == 2 and events.count("claim") == 1


def test_store_response_for_another_channel_is_rejected():
    executor, store, _, events = _system()
    store.claim_race = replace(_idle(), channel=replace(store.state.channel, desired_state_uri="s3://bucket/another"))
    with pytest.raises(WorkspaceHandoverError, match="store_channel_mismatch"):
        executor.run_cycle()
    assert "retire" not in events


@pytest.mark.parametrize("field", ["bytes", "predecessor", "revision"])
def test_claim_cannot_replace_exact_remote_bytes_or_applied_predecessor(field):
    executor, _, driver, events = _system()

    def edit(claim):
        if field == "bytes":
            different = replace(
                AirflowDesiredDeployment.from_json(claim.desired_state_json), promoted_at="2026-07-29T09:10:11Z"
            )
            return replace(
                claim, desired_state_json=different.to_json_bytes().decode(), desired_state_sha256=different.sha256
            )
        if field == "predecessor":
            return replace(claim, predecessor_request_sha256="sha256:" + "f" * 64)
        return replace(claim, observed_remote_revision="other")

    driver.claim_edit = edit
    with pytest.raises(WorkspaceHandoverError, match="claim_proposal"):
        executor.run_cycle()
    assert "claim" not in events


def test_same_occurrence_changed_body_does_not_converge():
    executor, _, driver, events = _system()
    desired = AirflowDesiredDeployment.from_json(_idle().current.snapshot.desired_state_json)
    desired = replace(desired, promoted_at="2026-07-29T09:10:11Z")
    driver.latest = VerifiedWorkspaceDesired(desired.to_json_bytes().decode(), "changed")
    with pytest.raises(WorkspaceHandoverError, match="current_desired_changed"):
        executor.run_cycle()
    assert "claim" not in events


def test_pointer_commit_is_followed_by_fresh_store_read_before_success():
    executor, store, driver, events = _system(_prepared())
    driver.on_replicate = lambda: store.complete(store.state.pending)
    assert executor.run_cycle().converged
    assert events[events.index("replicate") + 2] == "read"
    assert events.count("complete") == 1
    assert events[-2:] == ["read", "local"]


def test_competing_prepare_winner_request_is_used_not_local_proposal():
    executor, store, driver, _ = _system(_pending("RETIRED"))
    different_observation = replace(_adopted().request.resources[0], observation_sha256="sha256:" + "f" * 64)
    winner = _request(store.state.pending, (different_observation,))
    store.prepare_winner = winner
    assert executor.run_cycle().converged
    assert driver.replicated[0].request is winner
    assert store.state.current.request is winner


def test_unsupported_new_artifact_fails_before_any_mutation():
    executor, store, driver, events = _system()
    driver.local = store.state.current.request
    driver.remote_error = RuntimeError("unsupported composition")
    with pytest.raises(RuntimeError, match="unsupported composition"):
        executor.run_cycle()
    assert not {"claim", "retire", "prepare", "replicate"}.intersection(events)


def test_empty_registered_channel_does_not_invent_predecessor():
    idle = _idle()
    executor, _, _, events = _system(replace(idle, current=None))
    assert executor.run_cycle().converged
    assert "retire" not in events and "finalize" not in events


def test_cold_applied_current_can_rebuild_but_never_claim_success_when_remote_unavailable():
    executor, store, driver, events = _system()
    driver.remote_error = RuntimeError("remote unavailable")
    with pytest.raises(RuntimeError, match="remote unavailable"):
        executor.run_cycle()
    assert driver.local is store.state.current.request
    assert events.index("replicate") < events.index("latest")
    assert not {"claim", "retire", "prepare", "complete"}.intersection(events)


@pytest.mark.parametrize("phase", ["claim", "prepare_successor", "complete"])
def test_lost_mutation_acknowledgement_recovers_exact_same_occurrence_next_cycle(phase, monkeypatch):
    executor, store, driver, events = _system()
    driver.local = store.state.current.request
    original = getattr(store, phase)
    error = RuntimeError("COMMIT_UNKNOWN")

    def commit_then_disconnect(*args, **kwargs):
        original(*args, **kwargs)
        raise error

    monkeypatch.setattr(store, phase, commit_then_disconnect)
    with pytest.raises(RuntimeError) as caught:
        executor.run_cycle()
    assert caught.value is error
    saved = store.state.pending_occurrence or store.state.current
    monkeypatch.setattr(store, phase, original)
    result = executor.run_cycle()
    assert result.converged and result.activation_id == _pending().pending.successor_activation_id
    assert events.count("claim") == 1 and events.count("prepare") == 1 and events.count("complete") == 1
    if phase != "claim":
        assert store.state.current.request is saved.request
