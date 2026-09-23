"""Create-only actor preserves exact settlement evidence order."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_permission_grant_settlement_evidence_actor import (
    SqlClientPermissionGrantSettlementEvidenceActor,
)
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.contracts.mssql_sqlclient_permission_grant_settlement import PermissionGrantSettlementEvidenceContext
from tests.test_mssql_sqlclient_permission_grant_settlement import fixture, remote_record


@contextmanager
def sink(writes):
    yield SimpleNamespace(write=lambda name, payload: writes.append((name, payload)))


def test_actor_persists_three_records_in_order():
    binding, records, _ = fixture()
    writes = []
    pool = TdsActorPool(capacity=1, clock=lambda: 0.0)
    actor = pool.open(
        lambda deadline, clock: SqlClientPermissionGrantSettlementEvidenceActor(
            lambda: sink(writes), PermissionGrantSettlementEvidenceContext(records[0].subject, binding), deadline, clock
        ),
        deadline=1.0,
    )
    for record in records:
        assert actor.write(record, deadline=1.0) == record.receipt
    assert [payload for _, payload in writes] == [record.payload for record in records]
    actor.close(deadline=1.0)


def test_actor_persists_remote_settlement_as_fourth_record():
    binding, records, _ = fixture()
    records = (*records, remote_record())
    writes = []
    pool = TdsActorPool(capacity=1, clock=lambda: 0.0)
    actor = pool.open(
        lambda deadline, clock: SqlClientPermissionGrantSettlementEvidenceActor(
            lambda: sink(writes),
            PermissionGrantSettlementEvidenceContext(records[0].subject, binding),
            deadline,
            clock,
        ),
        deadline=1.0,
    )
    for record in records:
        assert actor.write(record, deadline=1.0) == record.receipt
    assert [payload for _, payload in writes] == [record.payload for record in records]
    actor.close(deadline=1.0)


def test_actor_persists_compact_names_through_real_filesystem_backend(tmp_path):
    binding, records, _ = fixture()

    @contextmanager
    def filesystem_sink():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    pool = TdsActorPool(capacity=1, clock=lambda: 0.0)
    actor = pool.open(
        lambda deadline, clock: SqlClientPermissionGrantSettlementEvidenceActor(
            filesystem_sink,
            PermissionGrantSettlementEvidenceContext(records[0].subject, binding),
            deadline,
            clock,
        ),
        deadline=1.0,
    )
    for record in records:
        actor.write(record, deadline=1.0)
        assert (tmp_path / record.receipt.relative_name).read_bytes() == record.payload
    actor.close(deadline=1.0)


def test_wrong_order_is_effect_free_and_lost_ack_is_sticky(monkeypatch):
    binding, records, _ = fixture()
    writes = []
    pool = TdsActorPool(capacity=1, clock=lambda: 0.0)
    actor = pool.open(
        lambda deadline, clock: SqlClientPermissionGrantSettlementEvidenceActor(
            lambda: sink(writes), PermissionGrantSettlementEvidenceContext(records[0].subject, binding), deadline, clock
        ),
        deadline=1.0,
    )
    with pytest.raises(ValueError):
        actor.write(records[1], deadline=1.0)
    original = actor._dispatch

    def lost(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError("lost ack")

    monkeypatch.setattr(actor, "_dispatch", lost)
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(records[0], deadline=1.0)
    with pytest.raises(TdsJournalActorUnknown):
        actor.write(records[0], deadline=1.0)
    actor.close(deadline=1.0)
