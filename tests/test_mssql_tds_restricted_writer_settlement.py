"""P9b consumes one exact retained P9a owner and settles once."""

from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
from threading import Event, Thread
from types import SimpleNamespace

import pytest

from dpone.app.mssql_sqlclient_restricted_writer_settlement_composition import make_restricted_writer_departure_plan
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceKind,
    SqlClientDepartureEvidenceReceipt,
    evidence_name,
)
from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantPrincipal
from dpone.contracts.mssql_sqlclient_restricted_session_departure import (
    RestrictedDepartureSample,
    RestrictedDepartureSampleKind,
    RestrictedObserverRequest,
    RestrictedSessionDeparture,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import (
    RestrictedWriterDepartureCompletion,
    RestrictedWriterDepartureRequest,
    RestrictedWriterDepartureResult,
    RestrictedWriterSettlementOperations,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement_codec import (
    encode_request,
    remote_settlement_payload,
    validate_result,
)
from dpone.contracts.mssql_sqlclient_writer_settlement import SqlClientStageContentExpectation
from dpone.contracts.mssql_tds_directory import TdsDirectorySnapshot
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_session import TdsRestrictedRemoteSessionIdentity
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.services.mssql_tds_original_continuation import PreparationTransition
from dpone.services.mssql_tds_preparation_origin import PreparationOrigin, PreparationWriterInputs
from dpone.services.mssql_tds_restricted_writer_settlement import (
    RestrictedWriterSettlementUnknown,
    RestrictedWriterVerified,
    _claim_restricted_writer,
    settle_restricted_writer,
)
from dpone.services.mssql_tds_restricted_writer_verification import RestrictedWriterVerifyLocalUnknown
from dpone.services.mssql_tds_restricted_writer_verify_coordinator import VerifyCoordinatorCustody
from tests.test_mssql_sqlclient_permission_grant_departure import grant_departure_fixture
from tests.test_mssql_sqlclient_restricted_writer_verify_request import launch_request
from tests.test_mssql_tds_directory import initial
from tests.test_mssql_tds_directory_journal import OWNER
from tests.test_mssql_tds_restricted_writer_verification import Association, execute

OPERATIONS = RestrictedWriterSettlementOperations(encode_request, validate_result, remote_settlement_payload)


def claim_for(retained, plan):
    claim = _claim_restricted_writer(retained, OPERATIONS)
    claim.bind_plan(plan, OPERATIONS)
    return claim


def setup_settlement(monkeypatch, *, fail=None):
    _, grant_request, observed = grant_departure_fixture()
    launch = launch_request()
    launch = replace(
        launch,
        request=replace(
            launch.request,
            implementation_sha256=grant_request.plan.grant_evidence.operation.implementation_sha256,
        ),
    )
    association = Association()
    association._verify_identity = grant_request.plan.grant_evidence.operation
    retained, association, _, _ = execute(launch=launch, association=association)
    preparation = object.__new__(PreparationTransition)
    preparation_origin = object.__new__(PreparationOrigin)
    preparation_inputs = tuple.__new__(
        PreparationWriterInputs,
        (
            preparation,
            object(),
            object(),
            object(),
            b"{}",
            b"{}",
            "0" * 64,
            1,
            object,
            SqlClientStageContentExpectation(0, "0" * 64, "1" * 64),
        ),
    )
    preparation._origin = preparation_origin
    preparation_origin._writer_inputs = preparation_inputs
    association._capture = (object(), preparation)
    verify = retained._owner
    observed_session = observed.absence.original
    restricted_session = TdsRestrictedRemoteSessionIdentity(
        observed_session.connection_id,
        observed_session.session_id,
        observed_session.login_time,
        observed_session.nonce,
        observed_session.authority_sha256,
    )
    restricted_absence = RestrictedSessionDeparture(
        original=restricted_session,
        database=observed.absence.database,
        admission=observed.absence.admission,
        principal=observed.absence.principal,
        observer=observed.absence.observer,
        samples=tuple(
            RestrictedDepartureSample(
                kind=RestrictedDepartureSampleKind(value.kind.value),
                raw_count=value.raw_count,
                own_count=value.own_count,
                original_epoch_count=0 if value.kind.value == "sessions" else None,
                request=None
                if value.request is None
                else RestrictedObserverRequest(
                    connection_id=value.request.connection_id,
                    session_id=value.request.session_id,
                    request_id=value.request.request_id,
                    start_time=value.request.start_time,
                ),
                before_sha256=value.before_sha256,
                after_sha256=value.after_sha256,
            )
            for value in observed.absence.samples
        ),
    )
    context = replace(verify._result.opening, session=restricted_session)
    verify._result = replace(verify._result, opening=context, closing=context)
    snapshot = verify._coordinator_custody.snapshot
    state = replace(
        snapshot.state,
        session=restricted_session,
        authority_sha256=restricted_session.authority_sha256.hex(),
    )
    verify._coordinator_custody.snapshot = replace(snapshot, state=state)
    verify._coordinator_custody.gateway.observation.snapshot = verify._coordinator_custody.snapshot
    grant = grant_request.plan.grant_evidence
    association._verify_grant_ref = grant
    plan = make_restricted_writer_departure_plan(
        retained,
        grant,
        grant_request.plan.management_admission,
        grant_request.plan.writer_admission,
        implementation_sha256=verify._request.implementation_sha256,
        package_root="/tmp/dpone",
        admission_sha256="a" * 64,
        startup_deadline=1.0,
        operation_deadline=2.0,
        max_address_space_bytes=1024,
        operations=OPERATIONS,
    )
    request = RestrictedWriterDepartureRequest(plan=plan, startup=grant_request.startup)
    writer = SqlClientGrantPrincipal(
        verify._request.writer.principal_id,
        verify._request.writer.name,
        verify._request.writer.sid,
        "SQL_USER",
        "INSTANCE",
    )
    result = RestrictedWriterDepartureResult(
        request_sha256=sha256(encode_request(request)).hexdigest(),
        absence=restricted_absence,
        catalog_observer=observed.catalog_observer,
        principals=(writer, SqlClientGrantPrincipal(0, "public", "00", "DATABASE_ROLE", "NONE")),
        direct_permissions=observed.direct_permissions,
        stage=replace(observed.stage, before=verify._request.stage, after=verify._request.stage),
        row_count=0,
    )
    attempt_sha = attempt_identity_digest(plan.attempt)
    receipts = tuple(
        SqlClientDepartureEvidenceReceipt(
            plan.helper_id,
            attempt_sha,
            kind,
            evidence_name(plan.helper_id, attempt_sha, kind, str(index + 1) * 64),
            str(index + 1) * 64,
            1,
        )
        for index, kind in enumerate(SqlClientDepartureEvidenceKind)
    )
    completion = RestrictedWriterDepartureCompletion(
        request,
        result,
        TdsChildExit(grant_request.startup.process, 0, True),
        receipts,
    )
    coordinator = verify._coordinator_custody.gateway
    events = coordinator.events
    events.clear()

    class Origin:
        def assert_verify_settlement(self, exact, *, deadline):
            if exact is not retained:
                raise ValueError

        def record_verify_local_containment(self, proof, *, deadline):
            events.append("directory:local")
            if fail == "directory_local":
                raise OSError
            return directory

        def record_verify_remote_settlement(self, proof, *, deadline):
            events.append("directory:remote")
            if fail == "directory_remote":
                raise OSError
            return directory

    class Evidence:
        observation = SimpleNamespace(receipt=None)

        def write(self, record, *, deadline):
            events.append("evidence:remote")
            receipt = record.receipt
            self.observation.receipt = receipt
            return receipt

        def close(self, *, deadline):
            events.append("cleanup:evidence")

    directory = TdsDirectorySnapshot(initial(), OWNER, 1)
    return retained, Origin(), coordinator, Evidence(), completion, directory, events


def test_exact_happy_path_orders_remote_evidence_coordinator_and_directory(monkeypatch):
    retained, origin, coordinator, evidence, completion, directory, events = setup_settlement(monkeypatch)
    terminal = settle_restricted_writer(
        claim_for(retained, completion.request.plan),
        origin,
        coordinator,
        evidence,
        lambda: completion,
        lambda deadline: events.append("cleanup:verifier"),
        deadline=1.0,
        cleanup_deadline=2.0,
        clock=lambda: 0.0,
        operations=OPERATIONS,
    )
    assert terminal.directory is directory
    assert events[:5] == [
        "evidence:remote",
        "CoordinatorLocalObserved",
        "CoordinatorRemoteObserved",
        "directory:local",
        "directory:remote",
    ]
    assert events[-2:] == ["cleanup:verifier", "cleanup:evidence"]
    assert coordinator.close_count == 1


def test_gateway_method_shadow_cannot_redirect_p9b_or_suppress_exact_cleanup(monkeypatch):
    retained, origin, coordinator, evidence, completion, directory, events = setup_settlement(monkeypatch)
    replacement_effects = []
    object.__setattr__(
        coordinator,
        "execute",
        lambda *args, **kwargs: replacement_effects.append("execute"),
    )
    object.__setattr__(
        coordinator,
        "close",
        lambda *args, **kwargs: replacement_effects.append("close"),
    )

    terminal = settle_restricted_writer(
        claim_for(retained, completion.request.plan),
        origin,
        coordinator,
        evidence,
        lambda: completion,
        lambda deadline: events.append("cleanup:verifier"),
        deadline=1.0,
        cleanup_deadline=2.0,
        clock=lambda: 0.0,
        operations=OPERATIONS,
    )

    assert terminal.directory is directory
    assert replacement_effects == []
    assert coordinator.close_count == 1
    assert "CoordinatorLocalObserved" in events
    assert "CoordinatorRemoteObserved" in events
    assert not hasattr(terminal, "credentials") and not hasattr(terminal, "writer")
    with pytest.raises(ValueError):
        RestrictedWriterVerified(object(), directory, object(), (), "a" * 64)


def test_p10_claim_is_exact_one_shot_and_can_finish_admitted(monkeypatch):
    retained, origin, coordinator, evidence, completion, directory, events = setup_settlement(monkeypatch)
    terminal = settle_restricted_writer(
        claim_for(retained, completion.request.plan),
        origin,
        coordinator,
        evidence,
        lambda: completion,
        lambda deadline: events.append("cleanup:verifier"),
        deadline=1.0,
        cleanup_deadline=2.0,
        clock=lambda: 0.0,
        operations=OPERATIONS,
    )

    claim = terminal._claim_p10a_once()
    assert terminal._assert_p10a_claim(claim)._terminal is terminal
    with pytest.raises(ValueError):
        terminal._claim_p10a_once()
    terminal._mark_p10a_admitted(claim)
    with pytest.raises(ValueError):
        terminal._assert_p10a_claim(claim)


def test_foreign_thread_cannot_consume_p10_claim(monkeypatch):
    retained, origin, coordinator, evidence, completion, _, events = setup_settlement(monkeypatch)
    terminal = settle_restricted_writer(
        claim_for(retained, completion.request.plan),
        origin,
        coordinator,
        evidence,
        lambda: completion,
        lambda deadline: events.append("cleanup:verifier"),
        deadline=1.0,
        cleanup_deadline=2.0,
        clock=lambda: 0.0,
        operations=OPERATIONS,
    )
    failures = []

    def foreign_claim():
        try:
            terminal._claim_p10a_once()
        except ValueError:
            failures.append("rejected")

    thread = Thread(target=foreign_claim)
    thread.start()
    thread.join()

    assert failures == ["rejected"]
    claim = terminal._claim_p10a_once()
    assert terminal._assert_p10a_claim(claim)._terminal is terminal


def test_equal_value_coordinator_substitution_is_rejected_before_effects(monkeypatch):
    retained, origin, coordinator, evidence, completion, _, events = setup_settlement(monkeypatch)
    foreign = SimpleNamespace(observation=coordinator.observation, execute=coordinator.execute, close=coordinator.close)
    with pytest.raises(ValueError, match="remote_settlement_unknown"):
        settle_restricted_writer(
            claim_for(retained, completion.request.plan),
            origin,
            foreign,
            evidence,
            lambda: events.append("verifier") or completion,
            lambda deadline: events.append("cleanup:verifier"),
            deadline=1.0,
            cleanup_deadline=2.0,
            clock=lambda: 0.0,
            operations=OPERATIONS,
        )
    assert events == []


def test_dual_gateway_reference_mutation_cannot_reach_p9b_or_foreign_effects(monkeypatch):
    retained, _, coordinator, _, completion, _, events = setup_settlement(monkeypatch)
    custody = retained._owner._coordinator_custody
    foreign_effects = []
    foreign = SimpleNamespace(
        observation=coordinator.observation,
        execute=lambda *args, **kwargs: foreign_effects.append("execute"),
        close=lambda **kwargs: foreign_effects.append("close"),
    )
    for name in ("gateway", "_gateway_ref", "_VerifyCoordinatorCustody__gateway"):
        with pytest.raises(AttributeError):
            setattr(custody, name, foreign)
        with pytest.raises((AttributeError, TypeError)):
            object.__setattr__(custody, name, foreign)
    with pytest.raises(TypeError):
        deepcopy(custody)

    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        claim_for(retained, completion.request.plan)
    before = list(events)
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        claim_for(retained, completion.request.plan)

    assert foreign_effects == []
    assert events == ["cleanup"]
    assert coordinator.close_count == 1
    assert events == before


def test_whole_coordinator_custody_substitution_is_sticky_before_p9b_effects(monkeypatch):
    retained, _, _, _, completion, _, events = setup_settlement(monkeypatch)
    owner = retained._owner
    original = owner._coordinator_custody
    foreign_effects = []
    foreign_gateway = SimpleNamespace(
        observation=SimpleNamespace(snapshot=original.snapshot),
        execute=lambda *args, **kwargs: foreign_effects.append("execute"),
        close=lambda **kwargs: foreign_effects.append("close"),
    )
    foreign = VerifyCoordinatorCustody(lambda: foreign_gateway, deadline=1.0)
    foreign._state.contract = owner._contract
    foreign.snapshot = original.snapshot
    replacement_effects = []
    for name in ("_coordinator_custody", "_coordinator_custody_ref"):
        object.__setattr__(owner, name, foreign)
    for name in ("_unknown", "_release_custody", "_assert_complete", "_exact_custody"):
        object.__setattr__(owner, name, lambda *args, marker=name: replacement_effects.append(marker))

    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        claim_for(retained, completion.request.plan)
    before = list(events)
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        claim_for(retained, completion.request.plan)

    assert foreign_effects == []
    assert replacement_effects == []
    assert events == ["cleanup"]
    assert original.gateway.close_count == 1
    assert events == before


@pytest.mark.parametrize("failure", ["directory_local", "directory_remote"])
def test_effect_ambiguity_is_sticky_and_cleanup_is_still_attempted(monkeypatch, failure):
    retained, origin, coordinator, evidence, completion, _, events = setup_settlement(monkeypatch, fail=failure)
    calls = []
    with pytest.raises(RestrictedWriterSettlementUnknown):
        settle_restricted_writer(
            claim_for(retained, completion.request.plan),
            origin,
            coordinator,
            evidence,
            lambda: calls.append(True) or completion,
            lambda deadline: events.append("cleanup:verifier"),
            deadline=1.0,
            cleanup_deadline=2.0,
            clock=lambda: 0.0,
            operations=OPERATIONS,
        )
    with pytest.raises(ValueError, match="remote_settlement_unknown"):
        _claim_restricted_writer(retained, OPERATIONS)
    assert calls == [True]
    assert events[-2:] == ["cleanup:verifier", "cleanup:evidence"]
    assert coordinator.close_count == 1


def test_equal_grant_substitution_from_verifier_is_unknown_before_settlement_effects(monkeypatch):
    retained, origin, coordinator, evidence, completion, _, events = setup_settlement(monkeypatch)
    substituted_plan = replace(
        completion.request.plan,
        grant_evidence=deepcopy(completion.request.plan.grant_evidence),
    )
    substituted = replace(completion, request=replace(completion.request, plan=substituted_plan))

    with pytest.raises(RestrictedWriterSettlementUnknown):
        settle_restricted_writer(
            claim_for(retained, completion.request.plan),
            origin,
            coordinator,
            evidence,
            lambda: substituted,
            lambda deadline: events.append("cleanup:verifier"),
            deadline=1.0,
            cleanup_deadline=2.0,
            clock=lambda: 0.0,
            operations=OPERATIONS,
        )

    assert not any(
        event == "evidence:remote" or event.startswith("Coordinator") or event.startswith("directory:")
        for event in events
    )
    assert events == ["cleanup:verifier", "cleanup:evidence"]
    assert coordinator.close_count == 1


def test_concurrent_second_entry_has_zero_effects(monkeypatch):
    retained, origin, coordinator, evidence, completion, _, events = setup_settlement(monkeypatch)
    claimed, release = Event(), Event()
    outcome = []

    def first_run():
        claimed.set()
        assert release.wait(2.0)
        return completion

    def first():
        try:
            claim = claim_for(retained, completion.request.plan)
            outcome.append(
                settle_restricted_writer(
                    claim,
                    origin,
                    coordinator,
                    evidence,
                    first_run,
                    lambda deadline: events.append("cleanup:verifier"),
                    deadline=1.0,
                    cleanup_deadline=2.0,
                    clock=lambda: 0.0,
                    operations=OPERATIONS,
                )
            )
        except BaseException as error:
            outcome.append(error)

    thread = Thread(target=first)
    thread.start()
    assert claimed.wait(2.0)
    second_effects = []
    with pytest.raises(ValueError, match="remote_settlement_unknown"):
        _claim_restricted_writer(retained, OPERATIONS)
    release.set()
    thread.join(2.0)
    assert not thread.is_alive() and second_effects == []
    assert len(outcome) == 1 and not isinstance(outcome[0], BaseException)
