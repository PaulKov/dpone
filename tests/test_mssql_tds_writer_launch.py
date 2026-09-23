import math
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceObservation
from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest
from dpone.contracts.mssql_sqlclient_launch import SqlClientDescriptors, SqlClientLaunch, SqlClientReady, launch_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsAttemptSnapshot, TdsChildExit, TdsProcessIdentity, advance_state
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown
from dpone.services import mssql_tds_writer_admission as writer_admission_module
from dpone.services.mssql_tds_writer_authority import _ADMITTED_TOKEN, _create_sqlclient_writer_admitted
from dpone.services.mssql_tds_writer_authority_proof import assert_writer_authority, mint_writer_authority_proof
from dpone.services.mssql_tds_writer_launch import (
    SqlClientEvidenceOpenUnknown,
    SqlClientWriterLaunchUnknown,
    launch_and_register_sqlclient_writer,
)
from tests.test_mssql_sqlclient_writer_admission import _admit
from tests.test_mssql_sqlclient_writer_admission import admission as admission_fixture


class Lifecycle:
    def __init__(self):
        self._snapshot = None
        self.calls = []
        self.closed = []

    @property
    def snapshot(self):
        return self._snapshot

    @snapshot.setter
    def snapshot(self, value):
        self._snapshot = value

    def advance(self, event, *, expected_phase, deadline):
        self.calls.append((type(event).__name__, deadline))
        previous = self.snapshot
        state = advance_state(previous.state, event, expected_phase=expected_phase)
        self.snapshot = TdsAttemptSnapshot(state, previous.revision + 1)
        return self.snapshot

    def close(self, *, deadline):
        self.closed.append(deadline)


def _assert_authority(plan):
    return assert_writer_authority(
        plan.authority_proof,
        terminal=plan.terminal,
        owner=plan.p9_owner,
        transition=plan.transition,
        fence=plan.authority_fence,
    )


class Evidence:
    def __init__(self, attempt_sha256, calls):
        self.attempt_sha256 = attempt_sha256
        self.calls = calls
        self._observation = SqlClientEvidenceObservation(attempt_sha256)
        self.closed = []

    @property
    def observation(self):
        return self._observation

    def write(self, record, *, deadline):
        self.calls.append(("evidence", deadline))
        receipt = record.receipt
        self._observation = SqlClientEvidenceObservation(self.attempt_sha256, receipt)
        return receipt

    def close(self, *, deadline):
        self.closed.append(deadline)


class DecodingEvidence(Evidence):
    """Model an actor boundary that reconstructs the durable receipt value."""

    def write(self, record, *, deadline):
        self.calls.append(("evidence", deadline))
        returned = record.receipt
        observed = replace(returned)
        assert observed == returned and observed is not returned
        self._observation = SqlClientEvidenceObservation(self.attempt_sha256, observed)
        return returned


class Process:
    def __init__(self, launch, source, calls):
        self._declared_launch = launch
        self._bound_input = source
        self._identity = launch.process
        self._startup_receipt = None
        self.calls = calls
        self.closed = 0

    declared_launch = property(lambda self: self._declared_launch)
    bound_input = property(lambda self: self._bound_input)
    identity = property(lambda self: self._identity)
    startup_receipt = property(lambda self: self._startup_receipt)

    def startup(self, *, deadline):
        self.calls.append(("startup", deadline))
        self._startup_receipt = SqlClientReady(
            1,
            launch_digest(self.declared_launch),
            self.identity,
            8 << 30,
            9,
            0,
            "8.0.31",
            536870912,
            False,
            "Disable",
        )

    def terminate(self, *, deadline):
        self.calls.append(("terminate", deadline))
        return TdsChildExit(self.identity, -9, True)

    def close(self):
        self.closed += 1


class Launcher:
    def __init__(self, process, calls, *, fail=False):
        self.process, self.calls, self.fail = process, calls, fail

    def spawn(self, *, startup_deadline, operation_deadline):
        self.calls.append(("spawn", startup_deadline, operation_deadline))
        if self.fail:
            raise OSError("spawn")
        return self.process


@pytest.fixture
def setup(tmp_path, monkeypatch):
    generator = admission_fixture.__wrapped__(tmp_path, monkeypatch)
    base = next(generator)
    lifecycle = Lifecycle()
    base.preparation.attempt._lifecycle = lifecycle
    captured = {}
    real_create = writer_admission_module._create_sqlclient_writer_admitted

    def capture_plan(token, plan):
        captured["plan"] = plan
        return real_create(token, plan)

    monkeypatch.setattr(writer_admission_module, "_create_sqlclient_writer_admitted", capture_plan)
    admitted = _admit(base)
    plan = captured["plan"]
    lifecycle.snapshot = plan.attempt
    base.preparation.attempt._directory = SimpleNamespace(observation=SimpleNamespace(snapshot=plan.directory))
    association = plan.p9_owner.retained._owner._association
    capture = (
        base.preparation.attempt,
        base.preparation,
        object(),
        object(),
        object(),
        lifecycle,
        base.preparation.attempt._directory,
    )
    association._capture = capture
    terminal_values = list(plan.terminal)
    terminal_values[9] = capture
    terminal = tuple.__new__(type(plan.terminal), terminal_values)
    plan.p9_owner._terminal = terminal
    proof = mint_writer_authority_proof(
        terminal,
        plan.p9_owner,
        plan.p9_owner.retained._owner,
        plan.transition,
        fence=plan.authority_fence,
    )
    plan = plan._replace(terminal=terminal, authority_proof=proof)
    admitted = _create_sqlclient_writer_admitted(_ADMITTED_TOKEN, plan)
    plan.terminal._preparation_origin._references = (
        None,
        None,
        None,
        None,
        None,
        lifecycle,
        base.preparation.attempt._directory,
        None,
        None,
        None,
    )
    source = plan.input_descriptor
    process_identity = TdsProcessIdentity("a" * 64, "11111111-1111-4111-8111-111111111111", 321, 4)
    descriptors = SqlClientDescriptors(30, 31, 32, 33, 34, source.fd)
    launch = SqlClientLaunch(
        1,
        "22222222-2222-4222-8222-222222222222",
        attempt_identity_digest(plan.attempt.state.identity),
        plan.build_sha256,
        input_descriptor_digest(source),
        process_identity,
        123,
        int(math.nextafter(plan.startup_deadline, 0.0) * 10**9),
        int(math.nextafter(plan.operation_deadline, 0.0) * 10**9),
        8 << 30,
        descriptors,
    )
    calls = []
    process = Process(launch, source, calls)
    evidence = Evidence(launch.attempt_sha256, calls)
    yield SimpleNamespace(
        admitted=admitted,
        lifecycle=lifecycle,
        process=process,
        evidence=evidence,
        plan=plan,
        calls=calls,
        launcher=Launcher(process, calls),
    )
    try:
        next(generator)
    except StopIteration:
        pass


def run(h, *, launcher=None, evidence=None, clock=lambda: 1.0):
    launcher = h.launcher if launcher is None else launcher
    evidence = h.evidence if evidence is None else evidence
    return launch_and_register_sqlclient_writer(
        h.admitted,
        clock=clock,
        open_evidence=lambda attempt, deadline: evidence,
        build_launcher=lambda **kwargs: launcher,
    )


def test_launch_intent_precedes_spawn_and_registration_precedes_process_cas(setup):
    registered = run(setup)

    assert repr(registered) == "SqlClientWriterRegistered(<opaque>)"
    assert setup.lifecycle.calls == [("LaunchIntent", 3.0), ("ProcessRegistered", 3.0)]
    assert setup.calls == [("spawn", 2.0, 3.0), ("startup", 2.0), ("evidence", 3.0)]
    assert setup.process.closed == 0
    cleanup = registered._prepare_p10c_cleanup(registered)
    claim = registered._claim_p10c_once(registered, cleanup)
    assert registered._assert_p10c_claim(registered, claim, cleanup).process is setup.process
    with pytest.raises(ValueError):
        registered._claim_p10c_once(registered, cleanup)


def test_registration_rebinds_equal_actor_ack_to_exact_observed_receipt(setup):
    evidence = DecodingEvidence(setup.process.declared_launch.attempt_sha256, setup.calls)
    registered = run(setup, evidence=evidence)
    cleanup = registered._prepare_p10c_cleanup(registered)
    claim = registered._claim_p10c_once(registered, cleanup)
    refs = registered._assert_p10c_claim(registered, claim, cleanup)

    assert refs.receipt is evidence.observation.receipt


def test_postclaim_spawn_failure_is_sticky_and_closes_original_actors_once(setup):
    failing = Launcher(setup.process, setup.calls, fail=True)

    with pytest.raises(SqlClientWriterLaunchUnknown):
        run(setup, launcher=failing)

    assert setup.evidence.closed == [6.0]
    assert setup.lifecycle.closed == [6.0]
    assert setup.process.closed == 0
    with pytest.raises(ValueError):
        run(setup)


def test_actor_open_unknown_retains_and_closes_exact_orphan(setup):
    orphan = Evidence(setup.process.declared_launch.attempt_sha256, setup.calls)

    def fail_open(attempt, deadline):
        raise SqlClientEvidenceOpenUnknown(orphan)

    with pytest.raises(SqlClientWriterLaunchUnknown):
        launch_and_register_sqlclient_writer(
            setup.admitted,
            clock=lambda: 1.0,
            open_evidence=fail_open,
            build_launcher=lambda **kwargs: setup.launcher,
        )

    assert orphan.closed == [6.0]
    assert setup.lifecycle.closed == [6.0]
    assert not setup.calls


def test_clock_failure_allocates_nothing_and_signals_existing_lifecycle_with_zero_budget(setup):
    allocated = []

    with pytest.raises(SqlClientWriterLaunchUnknown):
        launch_and_register_sqlclient_writer(
            setup.admitted,
            clock=lambda: float("nan"),
            open_evidence=lambda *args: allocated.append(args),
            build_launcher=lambda **kwargs: allocated.append(kwargs),
        )

    assert not allocated
    assert setup.lifecycle.closed == [0.0]


def test_first_postclaim_dereference_failure_is_sticky_unknown(setup):
    setup.plan.transition.attempt = None

    with pytest.raises(SqlClientWriterLaunchUnknown):
        run(setup)

    with pytest.raises(ValueError):
        run(setup)
    assert not setup.calls


def test_cancellation_after_claim_is_normalized_and_retains_cleanup(setup, monkeypatch):
    def cancel(_self, _claim):
        raise KeyboardInterrupt

    monkeypatch.setattr(type(setup.admitted), "_assert_p10b_claim", cancel)

    with pytest.raises(SqlClientWriterLaunchUnknown):
        run(setup)

    assert setup.lifecycle.closed == [0.0]
    assert not setup.calls
    with pytest.raises(ValueError):
        run(setup)


def test_live_grant_receipt_substitution_fails_before_launch_intent(setup):
    plan = setup.plan
    association = plan.p9_owner.retained._owner._association
    association._verify_owner_ref.local.held_owner.result_receipt = object()

    with pytest.raises(SqlClientWriterLaunchUnknown):
        run(setup)

    assert setup.lifecycle.calls == []
    assert setup.lifecycle.closed == [6.0]
    assert not setup.calls


def test_policy_mutation_fails_before_launch_intent(setup):
    object.__setattr__(setup.plan.policy, "batch_rows", 7)

    with pytest.raises(SqlClientWriterLaunchUnknown):
        run(setup)

    assert setup.lifecycle.calls == []
    assert not setup.calls


def test_mismatched_process_binding_is_contained_after_startup(setup):
    setup.process._declared_launch = replace(setup.process.declared_launch, attempt_sha256="f" * 64)

    with pytest.raises(SqlClientWriterLaunchUnknown):
        run(setup)

    assert setup.lifecycle.calls == [("LaunchIntent", 3.0)]
    assert setup.calls[-1] == ("terminate", 6.0)
    assert setup.process.closed == 1
    assert setup.evidence.closed == [6.0]


def test_unresolved_spawn_is_contained_once(setup):
    class Unresolved:
        def __init__(self):
            self.calls = []

        def contain(self, *, deadline):
            self.calls.append(("contain", deadline))

        def close(self):
            self.calls.append(("close",))

    unresolved = Unresolved()

    class UnknownLauncher:
        def spawn(self, **kwargs):
            raise TdsLaunchUnknown(unresolved)

    with pytest.raises(SqlClientWriterLaunchUnknown):
        run(setup, launcher=UnknownLauncher())

    assert unresolved.calls == [("contain", 6.0), ("close",)]
    assert setup.evidence.closed == [6.0]
    assert setup.lifecycle.closed == [6.0]


def test_registered_capability_reconstruction_cannot_claim(setup):
    registered = run(setup)
    reconstructed = object.__new__(type(registered))

    with pytest.raises(ValueError):
        reconstructed._prepare_p10c_cleanup(reconstructed)
    cleanup = registered._prepare_p10c_cleanup(registered)
    claim = registered._claim_p10c_once(registered, cleanup)
    assert registered._assert_p10c_claim(registered, claim, cleanup).process is setup.process


def test_registered_capability_exposes_no_custody_and_rejects_new_instances(setup):
    registered = run(setup)
    assert not isinstance(registered, tuple)
    assert not hasattr(registered, "owner")
    assert not hasattr(registered, "process")
    with pytest.raises(TypeError):
        registered[0]
    for reconstructed in (object.__new__(type(registered)), type(registered)()):
        with pytest.raises(ValueError):
            reconstructed._prepare_p10c_cleanup(reconstructed)


def test_containment_clock_is_called_exactly_once(setup):
    calls = []

    def clock():
        calls.append("clock")
        return 1.0

    run(setup, clock=clock)

    assert calls == ["clock"]


def test_instance_method_shadows_never_execute(setup):
    callbacks = []

    def shadow(*args, **kwargs):
        callbacks.append((args, kwargs))
        raise AssertionError("instance callback executed")

    setup.launcher.spawn = shadow
    setup.process.startup = shadow
    setup.evidence.write = shadow
    setup.lifecycle.advance = shadow

    run(setup)

    assert callbacks == []


def test_foreign_writer_authority_proof_is_rejected(setup):
    foreign = replace(setup.plan.authority_proof, terminal=object())
    replay = setup.plan._replace(authority_proof=foreign)

    with pytest.raises(ValueError, match="writer_authority_unknown"):
        _assert_authority(replay)


def test_mutated_writer_authority_proof_is_rejected(setup):
    object.__setattr__(setup.plan.authority_proof, "lineage_sha256", "0" * 64)

    with pytest.raises(ValueError, match="writer_authority_unknown"):
        _assert_authority(setup.plan)


def test_writer_authority_proof_cannot_replay_under_foreign_fence(setup):
    replay = setup.plan._replace(authority_fence=object())

    with pytest.raises(ValueError, match="writer_authority_unknown"):
        _assert_authority(replay)
