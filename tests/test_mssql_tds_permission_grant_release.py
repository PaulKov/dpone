"""P8a consumes one exact P7 owner and stops at proven local exit."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_sqlclient_permission_grant_settlement import PermissionGrantSettlementEvidenceKind as E
from dpone.contracts.mssql_sqlclient_permission_grant_wire import PermissionWireKind as K
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.services.mssql_tds_permission_grant_release import (
    PermissionGrantLocalReleaseUnknown,
    release_permission_grant_locally,
)
from tests.test_mssql_tds_permission_grant import setup_run


class SettlementEvidence:
    def __init__(self, events, fail=None):
        self.events, self.fail, self.records = events, fail, []
        self.observation = SimpleNamespace(receipt=None)
        self.after_write = None

    def write(self, record, *, deadline):
        self.events.append("evidence:" + record.kind.value)
        if record.kind is self.fail:
            raise OSError("lost ack")
        self.records.append(record)
        self.observation.receipt = record.receipt
        if self.after_write is not None:
            self.after_write(record)
        return record.receipt


def held_setup(*, evidence_failure=None):
    invoke, association, coordinator, process, parent_evidence, events = setup_run()
    owner = invoke()
    parent_evidence.close = lambda *, deadline: events.append("parent_evidence_close")
    result_sha = owner.result_receipt.payload_sha256
    released_body = {"evidence_sha256": result_sha}
    original_receive = process.receive_public

    def receive(kind, ordinal, **kwargs):
        if kind is K.RELEASED:
            events.append("receive:RELEASED")
            process.calls.append(("receive", kind))
            return SimpleNamespace(kind=K.RELEASED, ordinal=5, body=canonical_json_bytes(released_body))
        return original_receive(kind, ordinal, **kwargs)

    process.receive_public = receive
    process.observe_eof = lambda **kwargs: events.append("eof")
    process.settle = lambda **kwargs: events.append("settle") or TdsChildExit(owner.binding.startup.process, 0, True)
    settlement = SettlementEvidence(events, evidence_failure)
    return owner, settlement, events


def invoke_release(owner, evidence, events):
    return release_permission_grant_locally(
        owner,
        lambda subject, binding: events.append("settlement_evidence_open") or evidence,
        deadline=1.0,
        containment_deadline=2.0,
        clock=lambda: 0.0,
    )


def test_exact_release_order_and_retained_identity():
    owner, evidence, events = held_setup()
    result = invoke_release(owner, evidence, events)
    assert result.held_owner is owner and result.process is owner.process
    assert result.association is owner.association and result.coordinator is owner.coordinator
    assert result.phase == "LOCAL_EXIT" and len(result.receipts) == 3
    assert [record.kind for record in evidence.records] == list(E)
    assert events.index("parent_evidence_close") < events.index("settlement_evidence_open")
    assert events.index("evidence:release_intent") < events.index("send:RELEASE")
    assert events.index("receive:RELEASED") < events.index("evidence:released") < events.index("eof")
    assert events.index("eof") < events.index("settle") < events.index("evidence:local_exit")
    assert [call for call in owner.process.calls if call == ("send", K.RELEASE)] == [("send", K.RELEASE)]
    assert "PRIVATE_CANARY" not in repr(result)


@pytest.mark.parametrize("kind", list(E))
def test_every_lost_evidence_ack_is_sticky_and_release_is_never_replayed(kind):
    owner, evidence, events = held_setup(evidence_failure=kind)
    with pytest.raises(PermissionGrantLocalReleaseUnknown) as caught:
        invoke_release(owner, evidence, events)
    assert caught.value.owner.phase == "UNKNOWN"
    sends = [call for call in owner.process.calls if call == ("send", K.RELEASE)]
    assert len(sends) <= 1
    with pytest.raises(PermissionGrantLocalReleaseUnknown):
        invoke_release(owner, evidence, events)
    assert [call for call in owner.process.calls if call == ("send", K.RELEASE)] == sends


@pytest.mark.parametrize("change", ["owner", "process", "reaped", "code"])
def test_substitution_or_invalid_exit_is_unknown(change):
    owner, evidence, events = held_setup()
    if change == "owner":
        owner.association._held_ready_owner = object()
    elif change == "process":
        owner.process = SimpleNamespace(**owner.process.__dict__)
    else:
        identity = owner.binding.startup.process
        if change == "exit":
            identity = SimpleNamespace(**identity.__dict__)
        code = 1 if change == "code" else 0
        reaped = change != "reaped"
        owner.process.settle = lambda **kwargs: TdsChildExit(identity, code, reaped)
    with pytest.raises(PermissionGrantLocalReleaseUnknown):
        invoke_release(owner, evidence, events)


def test_equal_adapter_exit_identity_is_rebound_to_exact_held_identity():
    owner, evidence, events = held_setup()
    identity = replace(owner.binding.startup.process)
    owner.process.settle = lambda **kwargs: TdsChildExit(identity, 0, True)

    released = invoke_release(owner, evidence, events)

    assert released.local_exit.identity is owner.binding.startup.process


def test_callback_reentry_poison_is_sticky():
    owner, evidence, events = held_setup()

    def factory(subject, binding):
        try:
            invoke_release(owner, evidence, events)
        except PermissionGrantLocalReleaseUnknown:
            pass
        return evidence

    with pytest.raises(PermissionGrantLocalReleaseUnknown):
        release_permission_grant_locally(owner, factory, deadline=1.0, containment_deadline=2.0, clock=lambda: 0.0)


def test_expired_deadline_precedes_parent_evidence_close_and_protocol_io():
    owner, evidence, events = held_setup()
    before = tuple(owner.process.calls)
    with pytest.raises(PermissionGrantLocalReleaseUnknown):
        release_permission_grant_locally(
            owner,
            lambda subject, binding: evidence,
            deadline=1.0,
            containment_deadline=2.0,
            clock=lambda: 2.0,
        )
    assert "parent_evidence_close" not in events
    assert tuple(owner.process.calls) == before


@pytest.mark.parametrize("kind", list(E))
def test_each_settlement_evidence_callback_cannot_replace_an_early_p7_receipt(kind):
    owner, evidence, events = held_setup()
    original = owner.receipts

    def mutate(record):
        if record.kind is kind:
            owner.receipts = (replace(original[0]), *original[1:])

    evidence.after_write = mutate
    with pytest.raises(PermissionGrantLocalReleaseUnknown) as caught:
        invoke_release(owner, evidence, events)
    assert caught.value.owner.phase == "UNKNOWN"
    with pytest.raises(PermissionGrantLocalReleaseUnknown):
        caught.value.owner.assert_local_exit()
    assert len([call for call in owner.process.calls if call == ("send", K.RELEASE)]) <= 1


@pytest.mark.parametrize("change", ["subject", "local_exit"])
def test_final_evidence_callback_rejects_equal_capability_substitution(change):
    owner, evidence, events = held_setup()
    local_result = None

    def mutate(record):
        nonlocal local_result
        if record.kind is not E.LOCAL_EXIT:
            return
        local_result = owner.association._local_release_owner
        if change == "subject":
            local_result.subject = replace(local_result.subject)
        else:
            local_result.local_exit = replace(local_result.local_exit)

    evidence.after_write = mutate
    with pytest.raises(PermissionGrantLocalReleaseUnknown) as caught:
        invoke_release(owner, evidence, events)
    assert caught.value.owner is local_result and caught.value.owner.phase == "UNKNOWN"
    with pytest.raises(PermissionGrantLocalReleaseUnknown):
        caught.value.owner.assert_local_exit()
