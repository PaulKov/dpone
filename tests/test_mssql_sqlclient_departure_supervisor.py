"""Helper barrier faults through the actual composed CREATE and real evidence actor."""

from dataclasses import replace

import pytest

from dpone.adapters.mssql_sqlclient_departure_evidence_actor import SqlClientDepartureEvidenceActor
from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureUnknown
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceObservation
from tests.test_mssql_sqlclient_create_departure_composition import harness as harness
from tests.test_mssql_sqlclient_observe_departure_composition import prepared as prepared
from tests.test_mssql_sqlclient_preparation_composition import composed_preparation as composed_preparation


@pytest.mark.parametrize("kind", list(Kind))
@pytest.mark.parametrize("failure", ["lost", "wrong", "observation"])
def test_each_ack_failure_stops_next_effect(harness, monkeypatch, kind, failure):
    original = SqlClientDepartureEvidenceActor.write
    observed = []

    def write(self, record, *, deadline):
        observed.append(record.kind)
        receipt = original(self, record, deadline=deadline)
        if record.kind is kind:
            if failure == "lost":
                raise OSError("synthetic_ack_lost")
            if failure == "wrong":
                return replace(receipt, byte_count=receipt.byte_count + 1)
            self._snapshot = SqlClientDepartureEvidenceObservation(record.helper_id, record.attempt_sha256)
        return receipt

    monkeypatch.setattr(SqlClientDepartureEvidenceActor, "write", write)
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert observed == list(Kind)[: list(Kind).index(kind) + 1]
    assert caught.value.retained.evidence_poisoned
    if kind is Kind.LAUNCH_INTENT:
        assert "helper.spawn" not in harness.events
    if kind in (Kind.LAUNCH_INTENT, Kind.REGISTRATION, Kind.CREDENTIAL_INTENT):
        assert "observer" not in harness.events
    assert harness.pool.live_count == 0


def test_observer_runs_only_after_three_acks(harness, monkeypatch):
    original = SqlClientDepartureEvidenceActor.write
    acknowledged = []

    def write(self, record, *, deadline):
        receipt = original(self, record, deadline=deadline)
        acknowledged.append(record.kind)
        return receipt

    monkeypatch.setattr(SqlClientDepartureEvidenceActor, "write", write)
    harness.hook = lambda label: assert_three(acknowledged) if label == "observer" else None
    outcome = harness.run()
    assert [r.kind for r in outcome.helper_outcome.receipts] == list(Kind)


def test_lost_create_registration_ack_never_constructs_or_retains_request(harness, monkeypatch):
    original = SqlClientDepartureEvidenceActor.write

    def write(self, record, *, deadline):
        receipt = original(self, record, deadline=deadline)
        if record.kind is Kind.REGISTRATION:
            raise OSError("synthetic_registration_ack_lost")
        return receipt

    monkeypatch.setattr(SqlClientDepartureEvidenceActor, "write", write)
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()

    assert caught.value.retained.request is None
    assert list(caught.value.retained.receipts) == [Kind.LAUNCH_INTENT]


def test_invalid_v1_request_binding_retains_exact_registration_ack(harness, monkeypatch):
    from dpone.app import mssql_sqlclient_departure_lifecycle as strategy

    monkeypatch.setattr(
        strategy.ipc,
        "validate_departure_request_binding",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("synthetic_invalid_binding")),
    )
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()

    retained = caught.value.retained
    assert retained.request is None
    assert list(retained.receipts) == [Kind.LAUNCH_INTENT, Kind.REGISTRATION]
    assert retained.receipts[Kind.REGISTRATION] == retained.expected[Kind.REGISTRATION]


def test_invalid_v2_request_binding_retains_exact_registration_ack(tmp_path, monkeypatch):
    from dpone.app import mssql_sqlclient_departure_lifecycle as strategy
    from tests.test_mssql_sqlclient_create_departure_composition_v2 import Harness

    h = Harness(tmp_path)
    monkeypatch.setattr(
        strategy.ipc_v2,
        "validate_departure_request_binding_v2",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("synthetic_invalid_binding")),
    )
    try:
        with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
            h.run()
        retained = caught.value.retained
        assert retained.request is None
        assert list(retained.receipts) == [Kind.LAUNCH_INTENT, Kind.REGISTRATION]
        assert retained.receipts[Kind.REGISTRATION] == retained.expected[Kind.REGISTRATION]
    finally:
        h.cleanup()


def assert_three(acknowledged):
    assert acknowledged == [Kind.LAUNCH_INTENT, Kind.REGISTRATION, Kind.CREDENTIAL_INTENT]


@pytest.mark.parametrize(
    "field,value", [("launch_nonce", b"z" * 32), ("implementation_sha256", "a" * 64), ("package_root", "/different")]
)
def test_startup_original_binding(harness, field, value):
    harness.helper_startup = replace(harness.helper_startup, **{field: value})
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert set(caught.value.retained.receipts) == {Kind.LAUNCH_INTENT}
    assert "observer" not in harness.events


@pytest.mark.parametrize("failure", ["nonzero", "close", "result_transport", "wait", "credentials"])
def test_post_result_failures_never_exclude(harness, failure):
    if failure == "nonzero":
        harness.helper_exit_code = 7
    else:
        harness.fail_at = {
            "close": "helper.close",
            "result_transport": "helper.result",
            "wait": "helper.wait",
            "credentials": "helper.credentials",
        }[failure]
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    retained = caught.value.retained
    assert Kind.EXCLUSION not in retained.receipts
    if failure in ("close", "nonzero", "wait"):
        assert Kind.RESULT in retained.receipts
    if failure == "result_transport":
        assert retained.raw_result == harness.helper.received_result
        assert retained.result is None and Kind.RESULT not in retained.receipts
    if failure == "close":
        assert retained.local_exit.exit_code == 0
        assert harness.events.count("helper.close") == 1
        with pytest.raises(SqlClientCreateDepartureUnknown):
            caught.value.close(deadline=harness.now + 99.0)
        assert harness.events.count("helper.close") == 1


@pytest.mark.parametrize("label", ["observer", "helper.startup", "helper.result", "helper.wait", "helper.close"])
def test_caught_parent_reentry_fault_prevents_continuation(harness, label):
    def hook(event):
        if event == label:
            try:
                _ = harness.attempt.directory
            except BaseException:
                pass

    harness.hook = hook
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert caught.value.retained.faulted
    assert Kind.EXCLUSION not in caught.value.retained.receipts
    if label == "observer":
        assert "helper.credentials" not in harness.events


def test_observer_expiry_never_delivers(harness):
    deadline = harness.now + 100.0
    harness.hook = lambda label: setattr(harness, "now", deadline) if label == "observer" else None
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run(operation_deadline=deadline)
    assert "helper.credentials" not in harness.events
    assert caught.value.retained.containment_deadline == deadline + 5.0


def test_final_helper_actor_close_failure_prevents_return(harness, monkeypatch):
    original = SqlClientDepartureEvidenceActor.close

    def close(self, *, deadline):
        original(self, deadline=deadline)
        raise OSError("synthetic_close_lost")

    monkeypatch.setattr(SqlClientDepartureEvidenceActor, "close", close)
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert set(caught.value.retained.receipts) == set(Kind)
    assert caught.value.retained.child_closed


@pytest.mark.parametrize("source", ["incoming", "declared", "receipt"])
@pytest.mark.parametrize("alias", ["float", "enum"])
def test_startup_alias_rejected_before_serialization_or_registration(harness, monkeypatch, source, alias):
    from enum import IntEnum

    import dpone.app.mssql_sqlclient_departure_supervisor as module

    class Alias(IntEnum):
        PID = 999

    changed = replace(harness.helper_startup, process=replace(harness.helper_startup.process))
    object.__setattr__(changed.process, "pid", 999.0 if alias == "float" else Alias.PID)
    if source == "incoming":
        harness.helper_startup = changed
    else:
        setattr(harness.helper, "declared_startup" if source == "declared" else "startup_receipt", changed)
    calls = []
    original = module.encode_startup

    def encode(value):
        calls.append(value)
        return original(value)

    monkeypatch.setattr(module, "encode_startup", encode)
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert not calls
    assert list(caught.value.retained.receipts) == [Kind.LAUNCH_INTENT]


def test_unknown_retention_rejected_before_reading_plan(tmp_path):
    from dpone.app.mssql_sqlclient_departure_supervisor import _run_departure

    class Unknown:
        @property
        def plan(self):
            raise AssertionError("unadmitted retention read")

    with pytest.raises(ValueError, match="version_invalid"):
        _run_departure(Unknown(), None, tmp_path, None, None)


def test_observe_actual_producer_hooks_precede_next_effect(prepared, monkeypatch):
    from dpone.app.mssql_sqlclient_observe_departure_composition import settle_prepared_observe
    from dpone.app.mssql_sqlclient_observe_departure_supervision import _ObserveDepartureRetention
    from tests.test_mssql_sqlclient_observe_departure_composition import scripted_helper

    h = prepared
    f = scripted_helper(h, monkeypatch)
    events = f.events
    from dpone.app import mssql_sqlclient_observe_departure_composition as composition

    original_driver = composition._run_departure

    def driver(*args):
        result = original_driver(*args)
        assert result is None
        assert args[0].owner.remote_ack is None
        assert args[0].helper.helper_evidence_closed is False
        events.append("driver return")
        return result

    monkeypatch.setattr(composition, "_run_departure", driver)
    hooks = (
        "bind_helper_evidence",
        "capture_helper_child",
        "capture_helper_process",
        "capture_helper_startup",
        "capture_helper_request",
        "capture_result",
        "capture_helper_result",
        "capture_helper_exit",
        "persist",
    )
    for name in hooks:
        original = getattr(_ObserveDepartureRetention, name)

        def traced(self, *args, _name=name, _original=original):
            events.append(args[0].value if _name == "persist" else _name)
            return _original(self, *args)

        monkeypatch.setattr(_ObserveDepartureRetention, name, traced)
    try:
        settle_prepared_observe(
            h.attempt,
            admitted_factory=h.factory,
            pool=h.pool,
            evidence_root=h.evidence_root,
            departure_launcher=f.launcher,
            observer_admission=f.admission,
            management_credentials=lambda: f.material,
            deadline=h.attempt._prepared_origin.deadline,
            helper_startup_timeout=2.0,
            termination_timeout=2.0,
        )
    except Exception as error:
        raise AssertionError((events, repr(error.__context__))) from error.__context__
    assert events == [
        "bind_helper_evidence",
        "launch_intent",
        "spawn",
        "capture_helper_child",
        "capture_helper_process",
        "startup",
        "capture_helper_startup",
        "capture_helper_request",
        "registration",
        "credential_intent",
        "send",
        "natural EOF",
        "capture_result",
        "capture_helper_result",
        "result",
        "wait",
        "capture_helper_exit",
        "close",
        "local_exit",
        "exclusion",
        "driver return",
    ]
    assert h.attempt._observe_settlement.complete is True


@pytest.mark.parametrize("kind", list(Kind))
@pytest.mark.parametrize("failure", ["lost", "wrong", "observation"])
def test_observe_each_actual_ack_failure_stops_forward_effects(prepared, monkeypatch, kind, failure):
    from dpone.app.mssql_sqlclient_observe_departure_composition import settle_prepared_observe
    from dpone.app.mssql_sqlclient_observe_departure_supervision import SqlClientObserveDepartureUnknown
    from tests.test_mssql_sqlclient_observe_departure_composition import scripted_helper

    h = prepared
    f = scripted_helper(h, monkeypatch)
    original = SqlClientDepartureEvidenceActor.write
    written = []

    def write(self, record, *, deadline):
        written.append(record.kind)
        receipt = original(self, record, deadline=deadline)
        if record.kind is kind:
            if failure == "lost":
                raise OSError("synthetic_ack_lost")
            if failure == "wrong":
                return replace(receipt, byte_count=receipt.byte_count + 1)
            self._snapshot = SqlClientDepartureEvidenceObservation(record.helper_id, record.attempt_sha256)
        return receipt

    monkeypatch.setattr(SqlClientDepartureEvidenceActor, "write", write)
    with pytest.raises(SqlClientObserveDepartureUnknown) as caught:
        settle_prepared_observe(
            h.attempt,
            admitted_factory=h.factory,
            pool=h.pool,
            evidence_root=h.evidence_root,
            departure_launcher=f.launcher,
            observer_admission=f.admission,
            management_credentials=lambda: f.material,
            deadline=h.attempt._prepared_origin.deadline,
            helper_startup_timeout=2.0,
            termination_timeout=2.0,
        )
    assert written == list(Kind)[: list(Kind).index(kind) + 1]
    assert caught.value.retained.owner.remote_ack is None
    assert h.attempt._poisoned and caught.value.retained.owner.complete is False
    if kind is Kind.LAUNCH_INTENT:
        assert "spawn" not in f.events
    if kind in (Kind.LAUNCH_INTENT, Kind.REGISTRATION, Kind.CREDENTIAL_INTENT):
        assert "send" not in f.events
    if kind is Kind.RESULT:
        assert "wait" not in f.events
