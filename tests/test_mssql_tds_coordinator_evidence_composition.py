"""Concrete evidence roots stay off the supervisor's blocking path."""

from threading import Event, get_ident
from time import monotonic

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.app import mssql_tds_coordinator_composition as composition
from tests.test_mssql_tds_coordinator_evidence_actor import record


def test_concrete_root_validation_and_write_run_on_actor(tmp_path, monkeypatch):
    observed = []
    real = composition.DescriptorPinnedCreateOnlyEvidenceWriter

    def factory(root):
        observed.append(get_ident())
        return real(root)

    monkeypatch.setattr(composition, "DescriptorPinnedCreateOnlyEvidenceWriter", factory)
    pool = TdsActorPool(capacity=1)
    item = record()
    gateway = composition.open_tds_coordinator_evidence(pool, tmp_path, item.operation_sha256, deadline=monotonic() + 2)
    receipt = gateway.write(item, deadline=monotonic() + 2)
    gateway.close(deadline=monotonic() + 2)
    assert observed and observed[0] != get_ident()
    assert (tmp_path / receipt.relative_name).read_bytes() == item.payload
    assert pool.live_count == 0


def test_closing_one_operation_does_not_close_shared_pool(tmp_path):
    pool = TdsActorPool(capacity=2)
    item = record()
    first = composition.open_tds_coordinator_evidence(pool, tmp_path, item.operation_sha256, deadline=monotonic() + 2)
    second = composition.open_tds_coordinator_evidence(pool, tmp_path, item.operation_sha256, deadline=monotonic() + 2)
    first.close(deadline=monotonic() + 2)
    assert second.write(item, deadline=monotonic() + 2) == item.receipt
    second.close(deadline=monotonic() + 2)
    pool.close(deadline=monotonic() + 2)


def test_missing_root_is_unknown_without_creating_directory(tmp_path):
    root = tmp_path / "missing"
    pool = TdsActorPool(capacity=1)
    with pytest.raises(TdsJournalActorUnknown) as caught:
        composition.open_tds_coordinator_evidence(pool, root, "a" * 64, deadline=monotonic() + 2)
    assert caught.value.gateway is not None
    pool.close(deadline=monotonic() + 2)
    assert not root.exists()


def test_stalled_root_validation_retains_capacity(tmp_path, monkeypatch):
    entered, release = Event(), Event()
    real = composition.DescriptorPinnedCreateOnlyEvidenceWriter

    def blocked(root):
        entered.set()
        assert release.wait(2)
        return real(root)

    monkeypatch.setattr(composition, "DescriptorPinnedCreateOnlyEvidenceWriter", blocked)
    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(TdsJournalActorUnknown) as caught:
            composition.open_tds_coordinator_evidence(pool, tmp_path, "a" * 64, deadline=monotonic() + 0.1)
        assert entered.is_set() and caught.value.gateway is not None
        assert pool.live_count == 1
        with pytest.raises(TdsJournalActorUnknown):
            composition.open_tds_coordinator_evidence(pool, tmp_path, "b" * 64, deadline=monotonic() + 1)
    finally:
        release.set()
        pool.close(deadline=monotonic() + 2)
    assert pool.live_count == 0


def test_relative_root_rejected_before_actor_admission():
    from pathlib import Path

    pool = TdsActorPool(capacity=1)
    with pytest.raises(ValueError, match="root"):
        composition.open_tds_coordinator_evidence(pool, Path("relative"), "a" * 64, deadline=monotonic() + 1)
    assert pool.live_count == 0
