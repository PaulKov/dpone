"""Durable stage-complete authority must reject partial and changed extraction."""

import json
from dataclasses import replace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeChunkPlan, NativeChunkReceipt


def test_complete_requires_verified_contiguous_receipts(tmp_path):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    with pytest.raises(WindowContractError, match="EOF"):
        journal.complete(source_eof=False)
    receipt = NativeChunkReceipt(0, "run-0-0", "stage", 1, 2, "a" * 64, "b" * 64)
    journal.attempt(0, 0)
    journal.verified(receipt)
    result = journal.complete(source_eof=True)
    assert result.rows == 1
    assert NativeChunkJournal(store, lease, plan).completed() == result
    with pytest.raises(WindowContractError, match="identity"):
        NativeChunkJournal(store, lease, replace(plan, source_query_id="other")).completed()


def test_publication_intent_is_durable_and_blocks_discard(tmp_path):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    journal.attempt(0, 0)
    journal.verified(NativeChunkReceipt(0, "run-0-0", "stage", 0, 0, "a" * 64, "b" * 64))
    journal.complete(source_eof=True)
    journal.publication.prepared({"stage": "prepared"})
    journal.publication.publication_started({"stage": "prepared"})
    loaded = NativeChunkJournal(store, lease, plan)
    assert loaded.publication.state()["phase"] == "publishing"
    with pytest.raises(WindowContractError):
        loaded.reextract_required()
    loaded.publication.publication_confirmed({"generation": "receipt"})
    loaded.publication.evidence_complete()
    loaded.publication.succeeded()
    assert NativeChunkJournal(store, lease, plan).publication.state()["phase"] == "succeeded"


def test_stale_journal_writer_cannot_reload_and_overwrite_revision(tmp_path):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    first, stale = NativeChunkJournal(store, lease, plan), NativeChunkJournal(store, lease, plan)
    first.begin()
    with pytest.raises(WindowContractError):
        stale.begin()


def test_detached_audit_snapshot_cannot_mutate_durable_authority(tmp_path):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    journal.data["phase"] = "stage_complete"
    assert journal.completed() is None


def test_publication_view_shares_owner_revision_and_rejects_stale_writer(tmp_path):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    journal = NativeChunkJournal(store, lease, plan)
    publication = journal.publication
    journal.begin()
    journal.attempt(0, 0)
    journal.verified(NativeChunkReceipt(0, "run-0-0", "stage", 0, 0, "a" * 64, "b" * 64))
    journal.complete(source_eof=True, completion_metadata={"source": "closed"})
    stale = NativeChunkJournal(store, lease, plan)
    publication.preparation_started({"stage": "prepared"})
    with pytest.raises(WindowContractError):
        stale.publication.preparation_started({"stage": "other"})
    publication.prepared({"stage": "prepared", "artifact": "bound"})
    publication.publication_started({"stage": "prepared", "artifact": "bound"})
    publication.publication_rolled_back({"transaction": "rolled_back"})
    snapshot = journal.data
    assert snapshot["rollback_history"] == [{"transaction": "rolled_back"}]
    assert snapshot["publication"] == {
        "phase": "prepared",
        "prepared": {"stage": "prepared", "artifact": "bound"},
        "receipt": None,
    }
    assert journal.completed_metadata() == {"source": "closed"}
    publication.state()["prepared"]["stage"] = "tampered"
    assert NativeChunkJournal(store, lease, plan).data == snapshot


@pytest.mark.parametrize(
    "phase", ["preparing", "prepared", "publishing", "published", "evidence-complete", "succeeded"]
)
def test_existing_v1_publication_records_round_trip_without_state_changes(tmp_path, phase):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    journal.attempt(0, 0)
    journal.verified(NativeChunkReceipt(0, "run-0-0", "stage", 0, 0, "a" * 64, "b" * 64))
    journal.complete(source_eof=True)
    legacy = journal.data
    legacy["publication"] = {
        "phase": phase,
        "prepared": {"stage": "bound"},
        "receipt": {"generation": "receipt"} if phase in ("published", "evidence-complete", "succeeded") else None,
    }
    store.save(journal.key, journal.revision, json.dumps(legacy), lease)
    loaded = NativeChunkJournal(store, lease, plan)
    assert loaded.data == legacy
    assert loaded.publication.state() == legacy["publication"]


def test_publication_cannot_bypass_complete_staging_or_evidence_order(tmp_path):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    with pytest.raises(WindowContractError, match="preparation"):
        journal.publication.preparation_started({"stage": "bound"})
    with pytest.raises(WindowContractError, match="phase_order"):
        journal.publication.succeeded()
    assert NativeChunkJournal(store, lease, plan).data == journal.data
