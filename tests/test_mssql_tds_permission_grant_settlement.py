"""P8b2 consumes one exact P8a owner and settles local then remote once."""

from dataclasses import replace
from types import MethodType, SimpleNamespace

import pytest

from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
    PermissionGrantDepartureCompletion,
    PermissionGrantDepartureEvidenceContext,
)
from dpone.contracts.mssql_sqlclient_permission_grant_settlement import (
    PermissionGrantRemoteSettlementEvidenceKind,
)
from dpone.contracts.mssql_tds_coordinator import coordinator_grant_digest
from dpone.contracts.mssql_tds_directory import TdsDirectorySnapshot
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.services.mssql_tds_permission_grant_settlement import (
    PermissionGrantRemoteSettlementUnknown,
    PermissionGrantSettled,
    settle_permission_grant_remotely,
)
from tests.test_mssql_sqlclient_permission_grant_departure import (
    grant_departure_fixture,
    grant_evidence_payloads,
)
from tests.test_mssql_tds_directory import initial
from tests.test_mssql_tds_directory_journal import OWNER
from tests.test_mssql_tds_permission_grant_release import held_setup, invoke_release


def setup_remote(monkeypatch, fail=None):
    held, evidence, events = held_setup()
    _, canonical_request, _ = grant_departure_fixture(held)
    canonical = canonical_request.plan.grant_evidence
    held.authority = held._authority_ref = canonical.authority
    held.grant = held._grant_ref = canonical.grant
    held.result = held._result_ref = canonical
    current = held._current
    state = current.state
    coordinator_result = replace(state.result, grant_sha256=coordinator_grant_digest(canonical.grant))
    current = replace(
        current,
        state=replace(
            state,
            session=canonical.authority.session,
            authority_sha256=canonical.grant.authority_sha256,
            grant=canonical.grant,
            result=coordinator_result,
        ),
    )
    held._current = current
    held.coordinator.snapshot = current
    held.coordinator.observation.snapshot = current
    local = invoke_release(held, evidence, events)
    _, request, result = grant_departure_fixture(held)
    request = replace(request, plan=replace(request.plan, grant_evidence=held.result))
    _, _, payloads = grant_evidence_payloads()
    receipts = tuple(
        SqlClientDepartureEvidenceRecord(
            request.plan.helper_id,
            attempt_identity_digest(request.plan.attempt),
            kind,
            payloads[kind],
            permission_grant_context=PermissionGrantDepartureEvidenceContext(grant_departure_fixture()[1].plan),
        ).receipt
        for kind in Kind
    )
    completion = PermissionGrantDepartureCompletion(
        request, result, TdsChildExit(request.startup.process, 0, True), receipts
    )
    final = TdsDirectorySnapshot(initial(), OWNER, 1)
    association = local.association

    def local_record(self, proof):
        events.append("directory:local")
        if fail == "directory_local":
            raise OSError("lost")
        return final

    def remote_record(self, proof):
        events.append("directory:remote")
        if fail == "directory_remote":
            raise OSError("lost")
        return final

    association.record_local_containment = MethodType(local_record, association)
    association.record_remote_settlement = MethodType(remote_record, association)
    evidence.close = lambda *, deadline: events.append("settlement_evidence_close")
    held.coordinator.close = lambda *, deadline: events.append("coordinator_close")
    if fail == "remote_evidence":
        evidence.fail = PermissionGrantRemoteSettlementEvidenceKind.REMOTE_SETTLEMENT
    return local, completion, final, events


def test_happy_trace_settles_coordinator_and_directory_local_then_remote(monkeypatch):
    local, completion, final, events = setup_remote(monkeypatch)
    result = settle_permission_grant_remotely(
        local,
        lambda: completion,
        lambda deadline: events.append("verifier_close"),
        deadline=1.0,
        cleanup_deadline=2.0,
        clock=lambda: 0.0,
    )
    assert result.directory is final and result.verifier_receipts == completion.receipts
    assert events.index("evidence:remote_settlement") < events.index("coordinator:CoordinatorLocalObserved")
    assert events.index("coordinator:CoordinatorLocalObserved") < events.index("coordinator:CoordinatorRemoteObserved")
    assert events.index("directory:local") < events.index("directory:remote")
    assert not hasattr(result, "credentials") and not hasattr(result, "writer")
    association = local.association
    settlement_owner = association._remote_settlement_owner
    assert association._settled_capability is result
    assert settlement_owner._settled_ref is result
    assert replace(result) == result and replace(result) is not association._settled_capability


@pytest.mark.parametrize("failure", ["remote_evidence", "directory_local", "directory_remote"])
def test_effect_ambiguity_is_sticky_and_never_replayed(monkeypatch, failure):
    local, completion, _, events = setup_remote(monkeypatch, failure)
    calls = []

    def run():
        calls.append(True)
        return completion

    with pytest.raises(PermissionGrantRemoteSettlementUnknown) as caught:
        settle_permission_grant_remotely(
            local,
            run,
            lambda deadline: None,
            deadline=1.0,
            cleanup_deadline=2.0,
            clock=lambda: 0.0,
        )
    with pytest.raises(PermissionGrantRemoteSettlementUnknown):
        settle_permission_grant_remotely(
            local,
            run,
            lambda deadline: None,
            deadline=1.0,
            cleanup_deadline=2.0,
            clock=lambda: 0.0,
        )
    assert caught.value.owner.phase == "UNKNOWN" and calls == [True]
    assert events.count("directory:local") <= 1 and events.count("directory:remote") <= 1


@pytest.mark.parametrize("failed", ["verifier", "coordinator", "evidence"])
def test_cleanup_failures_attempt_all_and_never_report_success(monkeypatch, failed):
    local, completion, _, events = setup_remote(monkeypatch)

    def verifier(deadline):
        events.append("cleanup:verifier")
        if failed == "verifier":
            raise OSError("lost")

    def coordinator(*, deadline):
        events.append("cleanup:coordinator")
        if failed == "coordinator":
            raise OSError("lost")

    def evidence(*, deadline):
        events.append("cleanup:evidence")
        if failed == "evidence":
            raise OSError("lost")

    local.held_owner.coordinator.close = coordinator
    local.settlement_evidence.close = evidence
    with pytest.raises(PermissionGrantRemoteSettlementUnknown):
        settle_permission_grant_remotely(
            local, lambda: completion, verifier, deadline=1.0, cleanup_deadline=2.0, clock=lambda: 0.0
        )
    assert [item for item in events if item.startswith("cleanup:")] == [
        "cleanup:verifier",
        "cleanup:coordinator",
        "cleanup:evidence",
    ]


def test_execution_deadline_failure_still_uses_independent_cleanup_ceiling(monkeypatch):
    local, completion, _, events = setup_remote(monkeypatch)
    local.held_owner.coordinator.close = lambda *, deadline: events.append(("cleanup:coordinator", deadline))
    local.settlement_evidence.close = lambda *, deadline: events.append(("cleanup:evidence", deadline))
    with pytest.raises(PermissionGrantRemoteSettlementUnknown):
        settle_permission_grant_remotely(
            local,
            lambda: completion,
            lambda deadline: events.append(("cleanup:verifier", deadline)),
            deadline=1.0,
            cleanup_deadline=3.0,
            clock=lambda: 2.0,
        )
    assert [item for item in events if type(item) is tuple and item[0].startswith("cleanup:")] == [
        ("cleanup:verifier", 3.0),
        ("cleanup:coordinator", 3.0),
        ("cleanup:evidence", 3.0),
    ]


@pytest.mark.parametrize("mutation", ["coordinator", "receipt"])
def test_cleanup_callback_mutation_is_sticky_unknown(monkeypatch, mutation):
    local, completion, _, _ = setup_remote(monkeypatch)
    original = local.held_owner._current

    def mutate(deadline):
        if mutation == "coordinator":
            local.held_owner._current = original
        else:
            local.receipts = (*local.receipts[:-1], replace(local.receipts[-1]))

    with pytest.raises(PermissionGrantRemoteSettlementUnknown):
        settle_permission_grant_remotely(
            local, lambda: completion, mutate, deadline=1.0, cleanup_deadline=2.0, clock=lambda: 0.0
        )


def test_terminal_contract_rejects_substituted_records_and_digest(monkeypatch):
    local, completion, _, events = setup_remote(monkeypatch)
    settled = settle_permission_grant_remotely(
        local,
        lambda: completion,
        lambda deadline: events.append("verifier_close"),
        deadline=1.0,
        cleanup_deadline=2.0,
        clock=lambda: 0.0,
    )
    invalid = (
        {"directory": object()},
        {"local_evidence": (object(),) * 4},
        {"verifier_receipts": (object(),) * 6},
        {"verifier_authority_sha256": "A" * 64},
    )
    for change in invalid:
        with pytest.raises(ValueError):
            replace(settled, **change)
    assert type(settled) is PermissionGrantSettled


@pytest.mark.parametrize("event", ["CoordinatorLocalObserved", "CoordinatorRemoteObserved"])
def test_lost_coordinator_ack_is_unknown_and_cleanup_still_runs(monkeypatch, event):
    local, completion, _, events = setup_remote(monkeypatch)
    local.held_owner.coordinator.fail_on = event
    with pytest.raises(PermissionGrantRemoteSettlementUnknown):
        settle_permission_grant_remotely(
            local,
            lambda: completion,
            lambda deadline: events.append("verifier_close"),
            deadline=1.0,
            cleanup_deadline=2.0,
            clock=lambda: 0.0,
        )
    assert events[-3:] == ["verifier_close", "coordinator_close", "settlement_evidence_close"]


@pytest.mark.parametrize("event", ["CoordinatorLocalObserved", "CoordinatorRemoteObserved"])
def test_mismatched_coordinator_ack_is_unknown(monkeypatch, event):
    local, completion, _, events = setup_remote(monkeypatch)
    coordinator = local.held_owner.coordinator
    original = coordinator.execute

    def execute(self, request, *, deadline):
        observed = original(request, deadline=deadline)
        if type(getattr(request, "event", None)).__name__ == event:
            return replace(observed, revision=observed.revision + 1)
        return observed

    coordinator.execute = MethodType(execute, coordinator)
    with pytest.raises(PermissionGrantRemoteSettlementUnknown):
        settle_permission_grant_remotely(
            local,
            lambda: completion,
            lambda deadline: events.append("verifier_close"),
            deadline=1.0,
            cleanup_deadline=2.0,
            clock=lambda: 0.0,
        )


@pytest.mark.parametrize("substitution", ["coordinator", "evidence"])
def test_cleanup_closes_admitted_capabilities_after_public_substitution(monkeypatch, substitution):
    local, completion, _, events = setup_remote(monkeypatch)
    original_coordinator = local.held_owner.coordinator
    original_evidence = local.settlement_evidence
    cleanup = []
    original_coordinator.close = lambda *, deadline: cleanup.append("original_coordinator")
    original_evidence.close = lambda *, deadline: cleanup.append("original_evidence")
    substitute = SimpleNamespace(close=lambda **kwargs: cleanup.append("substitute"))

    def run():
        if substitution == "coordinator":
            local.held_owner = SimpleNamespace(coordinator=substitute)
        else:
            local.settlement_evidence = substitute
        return completion

    with pytest.raises(PermissionGrantRemoteSettlementUnknown):
        settle_permission_grant_remotely(
            local,
            run,
            lambda deadline: cleanup.append("verifier"),
            deadline=1.0,
            cleanup_deadline=2.0,
            clock=lambda: 0.0,
        )
    assert cleanup == ["verifier", "original_coordinator", "original_evidence"]
