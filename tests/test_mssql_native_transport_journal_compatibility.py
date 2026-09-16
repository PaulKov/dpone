"""Versioned transport admission never rewrites incompatible durable identity."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy, NativeChunkLimits, NativeChunkPlan
from dpone.runtime.mssql_native_chunks import BoundedNativeChunks


@pytest.fixture
def durable_plan(tmp_path):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    policy = NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire", transport=policy)
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    return store, lease, plan, journal


def test_explicit_transport_round_trips_as_v3_without_rewrite(durable_plan):
    store, lease, plan, journal = durable_plan
    before = store.load(journal.key)
    assert journal.data["version"] == 3
    assert NativeChunkJournal(store, lease, plan).data == journal.data
    assert store.load(journal.key) == before


@pytest.mark.parametrize("field,value", [("batch_rows", 1), ("operation_timeout_seconds", 301)])
def test_changed_resolved_policy_rejects_resume_without_write(durable_plan, field, value):
    store, lease, plan, journal = durable_plan
    before = store.load(journal.key)
    changed = replace(plan, transport=replace(plan.transport, **{field: value}))
    with pytest.raises(WindowContractError, match="journal_identity_changed"):
        NativeChunkJournal(store, lease, changed)
    assert store.load(journal.key) == before


@pytest.mark.parametrize("mutation", ["v2", "raw_v3", "extra", "unresolved", "null", "boolean_version"])
def test_unsupported_identity_is_rejected_without_rewrite(durable_plan, mutation):
    store, lease, plan, journal = durable_plan
    data = journal.data
    if mutation == "v2":
        data["version"] = 2
        data["identity"].pop("transport")
        data["identity"]["source_read_mode"] = "raw_single_query"
    elif mutation == "raw_v3":
        data["identity"]["source_read_mode"] = "raw_single_query"
    elif mutation == "extra":
        data["identity"]["extra"] = "unsupported"
    elif mutation == "unresolved":
        data["identity"]["transport"].pop("batch_rows")
    elif mutation == "null":
        data["identity"]["transport"] = None
    else:
        data["version"] = True
    record = store.save(journal.key, journal.revision, json.dumps(data), lease)
    with pytest.raises(WindowContractError, match="invalid_journal"):
        NativeChunkJournal(store, lease, plan)
    assert store.load(journal.key) == record


@pytest.mark.parametrize("operation", ["stage", "recover"])
def test_invalid_journal_precedes_source_advancement_and_recovery_effects(
    durable_plan, tmp_path, monkeypatch, operation
):
    store, lease, plan, journal = durable_plan
    data = journal.data
    data["identity"]["source_read_mode"] = "raw_single_query"
    record = store.save(journal.key, journal.revision, json.dumps(data), lease)
    monkeypatch.setattr(store, "save", lambda *a, **k: pytest.fail("write before identity admission"))
    events = []

    class OwnedRows:
        def __iter__(self):
            return self

        def __next__(self):
            pytest.fail("source advanced before identity admission")

        def close(self):
            events.append("closed")

    work = tmp_path / "untouched-spool"
    work.mkdir()
    marker = work / "retained.native"
    marker.write_bytes(b"synthetic-preserved")
    service = BoundedNativeChunks(
        store=store,
        importer_factory=lambda: pytest.fail("importer acquired before identity admission"),
        work_dir=work,
        limits=NativeChunkLimits(1 << 30, 1 << 30),
    )
    with pytest.raises(WindowContractError, match="invalid_journal"):
        if operation == "stage":
            service.stage(plan, OwnedRows(), SimpleNamespace(type_layout_hash=plan.wire_fingerprint), lease)
        else:
            service.recover(plan, lease)
    assert events == (["closed"] if operation == "stage" else [])
    assert store.load(journal.key) == record
    assert marker.read_bytes() == b"synthetic-preserved"
