"""Explicit v2 composition shares CREATE, evidence barriers and containment."""

import inspect
from dataclasses import replace

import pytest

from dpone.app.mssql_sqlclient_create_departure_composition import run_sqlclient_create_departure_v2
from dpone.app.mssql_sqlclient_departure_request import decode_departure_credentials_v2
from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureUnknown
from dpone.contracts.mssql_sqlclient_departure_ipc_v2 import SqlClientDepartureRequestV2, SqlClientDepartureResultV2
from dpone.contracts.mssql_sqlclient_departure_ipc_v2_codec import encode_departure_result_v2, make_departure_result_v2
from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest
from tests.mssql_sqlclient_departure_v2_fixtures import alias, corrupt, leaves, sample
from tests.test_mssql_sqlclient_create_departure_composition import Harness as LegacyHarness

# Reuse the same contract fault matrix through the v2 fixture and real actor.
from tests.test_mssql_sqlclient_create_departure_composition import (
    test_original_create_scalars_rejected_before_encoding_hashing_or_effects as test_original_create_scalars_rejected_before_encoding_hashing_or_effects,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_cleanup_attempts_all_gateways_after_containment_failure as test_cleanup_attempts_all_gateways_after_containment_failure,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_close_refuses_other_thread_before_gateway_io as test_close_refuses_other_thread_before_gateway_io,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_create_failure_uses_original_retention_and_does_not_repeat_journal_failure as test_create_failure_uses_original_retention_and_does_not_repeat_journal_failure,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_failed_budget_capture_never_gets_new_deadline as test_failed_budget_capture_never_gets_new_deadline,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_final_context_assertion_failure_prevents_return as test_final_context_assertion_failure_prevents_return,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_final_pool_failure_prevents_return as test_final_pool_failure_prevents_return,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_historical_create_root_is_compared_to_launcher as test_historical_create_root_is_compared_to_launcher,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_original_inputs_fail_before_process_effects as test_original_inputs_fail_before_process_effects,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_outcome_construction_expiry_is_checked_inside_attempt_context as test_outcome_construction_expiry_is_checked_inside_attempt_context,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_partial_actor_allocation_retained as test_partial_actor_allocation_retained,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_retention_repr_does_not_expose_result_or_material as test_retention_repr_does_not_expose_result_or_material,
)
from tests.test_mssql_sqlclient_departure_supervision import (
    test_unknown_launch_capability_is_retained_and_close_never_repeated as test_unknown_launch_capability_is_retained_and_close_never_repeated,
)
from tests.test_mssql_sqlclient_departure_supervisor import (
    test_caught_parent_reentry_fault_prevents_continuation as test_caught_parent_reentry_fault_prevents_continuation,
)
from tests.test_mssql_sqlclient_departure_supervisor import (
    test_each_ack_failure_stops_next_effect as test_each_ack_failure_stops_next_effect,
)
from tests.test_mssql_sqlclient_departure_supervisor import (
    test_final_helper_actor_close_failure_prevents_return as test_final_helper_actor_close_failure_prevents_return,
)
from tests.test_mssql_sqlclient_departure_supervisor import (
    test_observer_expiry_never_delivers as test_observer_expiry_never_delivers,
)
from tests.test_mssql_sqlclient_departure_supervisor import (
    test_observer_runs_only_after_three_acks as test_observer_runs_only_after_three_acks,
)
from tests.test_mssql_sqlclient_departure_supervisor import (
    test_post_result_failures_never_exclude as test_post_result_failures_never_exclude,
)
from tests.test_mssql_sqlclient_departure_supervisor import (
    test_startup_alias_rejected_before_serialization_or_registration as test_startup_alias_rejected_before_serialization_or_registration,
)
from tests.test_mssql_sqlclient_departure_supervisor import (
    test_startup_original_binding as test_startup_original_binding,
)


class Harness(LegacyHarness):
    entrypoint = staticmethod(run_sqlclient_create_departure_v2)

    def prepare_factory(self, factory):
        from dpone.app.mssql_sqlclient_stage_locator_composition import admit_sqlclient_state_domain

        return admit_sqlclient_state_domain(
            store_factory=factory, lease=self.lease, pool=self.pool, deadline=self.now + 100.0
        )

    def __init__(self, root):
        super().__init__(root)
        self.departure = sample()
        a = self.departure.admission
        self.observer_admission = replace(a, login=replace(a.login, name="observer", original_name="observer"))
        own = replace(
            self.departure.observer,
            authority=replace(self.departure.observer.authority, login=self.observer_admission.login),
        )
        digest = observer_incarnation_digest(own)
        self.departure = replace(
            self.departure,
            observer=own,
            samples=tuple(replace(s, before_sha256=digest, after_sha256=digest) for s in self.departure.samples),
        )
        self.material = replace(self.material, username="observer")

    def run(self, **changes):
        changes.setdefault("observer_admission", self.observer_admission)
        return super().run(**changes)

    def send_request(self, body, *, deadline):
        self.hit("helper.credentials", deadline)
        self.helper_request = decode_departure_credentials_v2(
            body,
            startup=self.helper.declared_startup,
            admission_sha256=self.helper_launcher.admission_sha256,
            max_address_space_bytes=self.helper_launcher.max_address_space_bytes,
            **self.helper_deadlines,
        ).request

    def helper_result(self, *, deadline):
        result = make_departure_result_v2(self.helper_request, replace(self.departure, original=self.authority.session))
        self.helper.received_result = encode_departure_result_v2(result, request=self.helper_request)
        self.hit("helper.result", deadline)
        return self.helper.received_result


@pytest.fixture
def harness(tmp_path):
    h = Harness(tmp_path)
    yield h
    h.cleanup()


def test_v2_requires_explicit_observer_admission():
    assert (
        inspect.signature(run_sqlclient_create_departure_v2).parameters["observer_admission"].default
        is inspect.Parameter.empty
    )


def test_actual_create_then_six_v2_helper_records(harness):
    h = harness
    outcome = h.run().helper_outcome
    assert type(outcome.request) is SqlClientDepartureRequestV2
    assert type(outcome.result) is SqlClientDepartureResultV2
    assert outcome.result.departure.observer.authority.login.name == "observer"
    assert outcome.result.departure.admission.login.name == "writer"
    assert len(outcome.receipts) == 6
    assert len(list(h.evidence_root.rglob("*.json"))) == 12
    assert h.events.index("child.close") < h.events.index("helper.spawn")
    assert h.events.count("supplier") == h.events.count("observer") == 1
    assert h.events.count("helper.close") == 1
    assert not hasattr(outcome, "prepared")


@pytest.mark.parametrize("path,value", list(leaves(sample().admission)))
def test_configured_observer_aliases_reject_before_effects(harness, monkeypatch, path, value):
    from dpone.app import mssql_sqlclient_create_departure_composition as module

    calls = []
    monkeypatch.setattr(module, "_admission", lambda *args: calls.append("admission"))
    monkeypatch.setattr(module, "create_command_digest", lambda *args: calls.append("hash"))
    with pytest.raises(SqlClientCreateDepartureUnknown):
        harness.run(observer_admission=corrupt(harness.observer_admission, path, alias(value)))
    assert calls == harness.events == []


@pytest.mark.parametrize("bad", [None, sample().observer.authority])
def test_missing_or_authority_observer_rejected_before_effects(harness, bad):
    with pytest.raises(SqlClientCreateDepartureUnknown):
        harness.run(observer_admission=bad)
    assert harness.events == []


def test_configured_observer_snapshot_is_independent_before_create(harness):
    original = harness.observer_admission

    def mutate(label):
        if label == "supplier":
            object.__setattr__(original.login, "name", "mutated")
            object.__setattr__(original.login, "original_name", "mutated")

    harness.hook = mutate
    # Fixture's actual result gets an independent original authority too.
    harness.departure = replace(
        harness.departure,
        observer=replace(
            harness.departure.observer,
            authority=replace(harness.departure.observer.authority, login=replace(original.login)),
        ),
    )
    result = harness.run().helper_outcome
    assert original.login.name == "mutated"
    assert result.request.plan.observer_admission.login.name == "observer"
    assert result.request.plan.observer_admission is not original
    assert result.request.plan.observer_admission.login is not original.login


@pytest.mark.parametrize("kind", ["launch_intent", "registration", "credential_intent", "result", "local_exit"])
def test_v2_reconstruction_rejects_drifted_receipt(harness, monkeypatch, kind):
    from dpone.app import mssql_sqlclient_create_departure_composition as module
    from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind

    original = module._run_departure

    def run(state, *args):
        persist = state.persist

        def write(selected, payload):
            receipt = persist(selected, payload)
            if selected is Kind.LOCAL_EXIT:
                target = Kind(kind)
                state.receipts[target] = replace(state.receipts[target], payload_sha256="0" * 64)
            return receipt

        state.persist = write
        return original(state, *args)

    monkeypatch.setattr(module, "_run_departure", run)
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert Kind.EXCLUSION not in caught.value.retained.receipts


def test_v2_plan_cannot_mutate_parent_configured_snapshot(harness, monkeypatch):
    from dpone.app import mssql_sqlclient_create_departure_composition as module

    original = module._run_departure

    def run(state, *args):
        assert state.plan.observer_admission is not state.observer_admission
        assert state.plan.observer_admission.login is not state.observer_admission.login
        object.__setattr__(state.plan.observer_admission.login, "name", "changed")
        object.__setattr__(state.plan.observer_admission.login, "original_name", "changed")
        return original(state, *args)

    monkeypatch.setattr(module, "_run_departure", run)
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert "observer" not in harness.events
    assert caught.value.retained.observer_admission.login.name == "observer"


@pytest.mark.parametrize("mixed", ["v1", "other_request", "other_observer"])
def test_v2_rejects_result_from_another_version_request_or_observer(harness, mixed):
    from dataclasses import fields
    from uuid import UUID

    from dpone.contracts.mssql_sqlclient_departure_ipc import SqlClientDeparturePlan, SqlClientDepartureRequest
    from dpone.contracts.mssql_sqlclient_departure_ipc_codec import encode_departure_result, make_departure_result
    from tests.test_mssql_sqlclient_create_departure_codec import sample as old_sample

    def receive(*, deadline):
        r = harness.helper_request
        if mixed == "v1":
            old_plan = SqlClientDeparturePlan(
                **{f.name: getattr(r.plan, f.name) for f in fields(SqlClientDeparturePlan) if f.name != "schema"}
            )
            old = SqlClientDepartureRequest(plan=old_plan, startup=r.startup)
            raw = encode_departure_result(make_departure_result(old, old_sample()), request=old)
        else:
            if mixed == "other_request":
                r = replace(r, plan=replace(r.plan, helper_id=UUID(int=997)))
                d = harness.departure
            else:
                r = replace(r, plan=replace(r.plan, observer_admission=r.plan.creator_admission))
                d = sample()
            raw = encode_departure_result_v2(make_departure_result_v2(r, d), request=r)
        harness.helper.received_result = raw
        return raw

    harness.helper.receive_result = receive
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert caught.value.retained.raw_result == harness.helper.received_result
    assert caught.value.retained.result is None
    assert len(caught.value.retained.receipts) == 3


def test_outcome_cannot_mix_v1_and_v2_members(harness):
    from tests.test_mssql_sqlclient_departure_ipc import request as old_request

    outcome = harness.run().helper_outcome
    with pytest.raises(ValueError):
        replace(outcome, request=old_request())
