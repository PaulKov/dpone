"""Durable stage-complete authority must reject partial and changed extraction."""

import json
from dataclasses import replace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy, NativeChunkPlan, NativeChunkReceipt
from dpone.contracts.mssql_native_parent_journal import (
    NativeCheckpointReceipt,
    NativeChunkRetirementReceipt,
    NativeParentAuthority,
    canonical_digest,
)


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
@pytest.mark.parametrize(
    "mode,transport",
    [
        (None, None),
        ("raw_single_query", None),
        (None, NativeBulkTransportPolicy("mssql_python", "rows", 64 << 20)),
        (None, NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30)),
    ],
)
def test_existing_v1_v2_v3_publication_records_round_trip_without_state_changes(tmp_path, phase, mode, transport):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan(
        "run", "target", "query", "window", "schema", "wire", source_read_mode=mode, transport=transport
    )
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


def test_attempt_coordinates_survive_retry_and_completed_recovery_without_parsing_run_id(tmp_path):
    store = SQLiteWindowStore(tmp_path / "coordinates.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run-with-3-dashes-Ю", "target", "query", "window", "schema", "wire")
    journal = NativeChunkJournal(store, lease, plan)
    assert journal.attempt_coordinates() == ()
    journal.begin()
    for ordinal in range(2):
        journal.attempt(ordinal, 0)
        journal.attempt(ordinal, 1)
        journal.verified(
            NativeChunkReceipt(ordinal, journal.attempt_id(ordinal, 1), f"stage-{ordinal}", 0, 0, "a" * 64, "b" * 64)
        )
    expected = ((0, 0), (0, 1), (1, 0), (1, 1))
    assert journal.attempt_coordinates() == expected
    journal.complete(source_eof=True)
    loaded = NativeChunkJournal(store, lease, plan)
    before = store.load(loaded.key)
    assert loaded.attempt_coordinates() == expected
    assert loaded.attempts() == tuple(loaded.attempt_id(*pair) for pair in expected)
    assert store.load(loaded.key) == before


def _v4_complete(tmp_path):
    store = SQLiteWindowStore(tmp_path / "v4.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan(
        "run",
        "target",
        "query",
        "window",
        "schema",
        "wire",
        transport=NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30),
    )
    journal = NativeChunkJournal(store, lease, plan, parent_schema_version=4)
    journal.begin()
    journal.attempt(0, 0)
    journal.verified(NativeChunkReceipt(0, "run-0-0", "stage", 1, 2, "a" * 64, "b" * 64))
    journal.complete(source_eof=True)
    journal.publication.prepared({"stage": "prepared"})
    return store, lease, plan, journal


def _retirement(authority_digest):
    verification = canonical_digest(
        {
            "ordinal": 0,
            "attempt_id": "run-0-0",
            "stage_id": "stage",
            "rows": 1,
            "encoded_bytes": 2,
            "file_sha256": "a" * 64,
            "typed_digest": "b" * 64,
            "consumed_part_evidence": {},
        }
    )
    return NativeChunkRetirementReceipt(
        0,
        "run-0-0",
        authority_digest,
        "1" * 64,
        verification,
        "3" * 64,
        "4" * 64,
        "5" * 64,
        "6" * 64,
        "7" * 64,
        "8" * 64,
    )


@pytest.mark.parametrize("invalid_version", [4.0, True])
def test_v4_reload_rejects_non_exact_version(tmp_path, invalid_version):
    store, lease, plan, journal = _v4_complete(tmp_path)
    changed = journal.data
    changed["version"] = invalid_version
    store.save(journal.key, journal.revision, json.dumps(changed), lease)
    with pytest.raises(WindowContractError, match="invalid_journal"):
        NativeChunkJournal(store, lease, plan, parent_schema_version=4)


def test_v4_reload_rejects_consistently_rehashed_empty_completion(tmp_path):
    store, lease, plan, journal = _v4_complete(tmp_path)
    changed = journal.data
    changed["chunks"] = {}
    changed["complete"] = {
        "rows": 0,
        "receipt_digest": canonical_digest([]),
        "metadata_digest": canonical_digest({}),
    }
    store.save(journal.key, journal.revision, json.dumps(changed), lease)
    with pytest.raises(WindowContractError, match="noncontiguous_receipts"):
        NativeChunkJournal(store, lease, plan, parent_schema_version=4)


@pytest.mark.parametrize("outcome", ["published", "aborted"])
def test_v4_settled_authority_retires_chunks_then_checkpoints(tmp_path, outcome):
    store, lease, plan, journal = _v4_complete(tmp_path)
    if outcome == "published":
        journal.publication.publication_started({"stage": "prepared"})
        authority = journal.publication.publication_confirmed({"generation": "receipt"})
    else:
        journal.publication.abort_required()
        authority = journal.publication.abort_confirmed({"rollback": "proved"})
    assert authority.kind == outcome
    assert journal.data["version"] == 4
    journal.publication.retirement_required(authority)
    journal.publication.retiring()
    journal.publication.chunk_retired(_retirement(authority.digest))
    retirement = journal.publication.retired()
    assert retirement.authority_digest == authority.digest
    journal.publication.checkpoint_required()
    journal.publication.succeeded(
        NativeCheckpointReceipt(retirement.digest, "target", "window", lease.fence, 7, "9" * 64)
    )
    loaded = NativeChunkJournal(store, lease, plan, parent_schema_version=4)
    assert loaded.publication.state()["phase"] == "succeeded"
    assert loaded.publication.retirement_receipt() == retirement


def test_v4_abort_is_terminal_and_never_returns_to_prepared(tmp_path):
    _, _, _, journal = _v4_complete(tmp_path)
    journal.publication.abort_required()
    authority = journal.publication.abort_confirmed({"rollback": "proved"})
    with pytest.raises(WindowContractError):
        journal.publication.prepared({"stage": "prepared"})
    with pytest.raises(WindowContractError):
        journal.publication.publication_started({"stage": "prepared"})
    assert journal.publication.authority() == authority


def test_v4_rejects_out_of_order_or_changed_retirement_receipts(tmp_path):
    _, _, _, journal = _v4_complete(tmp_path)
    journal.publication.abort_required()
    authority = journal.publication.abort_confirmed({"rollback": "proved"})
    journal.publication.retirement_required(authority)
    journal.publication.retiring()
    with pytest.raises(WindowContractError, match="retirement_order"):
        journal.publication.chunk_retired(replace(_retirement(authority.digest), ordinal=1))
    journal.publication.chunk_retired(_retirement(authority.digest))
    with pytest.raises(WindowContractError, match="retirement_receipt_changed"):
        journal.publication.chunk_retired(replace(_retirement(authority.digest), absence_sha256="9" * 64))


def test_unknown_v4_cas_grants_no_cleanup_or_checkpoint(tmp_path):
    store, lease, plan, owner = _v4_complete(tmp_path)
    stale = NativeChunkJournal(store, lease, plan, parent_schema_version=4)
    owner.publication.abort_required()
    with pytest.raises(WindowContractError):
        stale.publication.abort_required()
    assert stale.publication.authority() is None
    with pytest.raises(WindowContractError):
        stale.publication.retirement_required(
            # An invented digest cannot turn an unknown CAS into authority.
            NativeParentAuthority("aborted", "a" * 64, lease.fence)
        )
    with pytest.raises(WindowContractError):
        stale.publication.checkpoint_required()


@pytest.mark.parametrize("mutation", ["authority", "prepared", "chunk", "parent"])
def test_v4_reload_rejects_changed_nested_authority_or_retirement(tmp_path, mutation):
    store, lease, plan, journal = _v4_complete(tmp_path)
    journal.publication.abort_required()
    authority = journal.publication.abort_confirmed({"rollback": "proved"})
    journal.publication.retirement_required(authority)
    journal.publication.retiring()
    journal.publication.chunk_retired(_retirement(authority.digest))
    journal.publication.retired()
    changed = journal.data
    if mutation == "authority":
        changed["publication"]["authority"]["digest"] = "9" * 64
    elif mutation == "prepared":
        changed["publication"]["prepared"]["stage"] = "other"
    elif mutation == "chunk":
        changed["publication"]["chunk_retirements"][0]["attempt_id"] = "other"
    else:
        changed["publication"]["retirement_receipt"]["chunks"][0]["absence_sha256"] = "9" * 64
    store.save(journal.key, journal.revision, json.dumps(changed), lease)
    with pytest.raises(WindowContractError, match="invalid_journal"):
        NativeChunkJournal(store, lease, plan, parent_schema_version=4)


def test_v4_reload_rejects_retirement_receipt_before_retiring(tmp_path):
    store, lease, plan, journal = _v4_complete(tmp_path)
    journal.publication.publication_started({"stage": "prepared"})
    authority = journal.publication.publication_confirmed({"generation": "receipt"})
    changed = journal.data
    changed["publication"]["chunk_retirements"] = [_retirement(authority.digest).to_dict()]
    store.save(journal.key, journal.revision, json.dumps(changed), lease)
    with pytest.raises(WindowContractError, match="invalid_journal"):
        NativeChunkJournal(store, lease, plan, parent_schema_version=4)


def test_v4_succeeded_requires_exact_checkpoint_bindings_and_replay(tmp_path):
    _, lease, _, journal = _v4_complete(tmp_path)
    journal.publication.abort_required()
    authority = journal.publication.abort_confirmed({"rollback": "proved"})
    journal.publication.retirement_required(authority)
    journal.publication.retiring()
    journal.publication.chunk_retired(_retirement(authority.digest))
    retirement = journal.publication.retired()
    journal.publication.checkpoint_required()
    valid = NativeCheckpointReceipt(retirement.digest, "target", "window", lease.fence, 7, "9" * 64)
    for changed in (
        replace(valid, parent_retirement_digest="8" * 64),
        replace(valid, target_id="other"),
        replace(valid, window_fingerprint="other"),
        replace(valid, fence=lease.fence + 1),
    ):
        with pytest.raises(WindowContractError, match="checkpoint"):
            journal.publication.succeeded(changed)
    journal.publication.succeeded(valid)
    journal.publication.succeeded(valid)
    with pytest.raises(WindowContractError, match="checkpoint"):
        journal.publication.succeeded(replace(valid, checkpoint_sha256="8" * 64))


def test_unknown_checkpoint_cas_never_grants_local_success(tmp_path):
    store, lease, plan, owner = _v4_complete(tmp_path)
    owner.publication.abort_required()
    authority = owner.publication.abort_confirmed({"rollback": "proved"})
    owner.publication.retirement_required(authority)
    owner.publication.retiring()
    owner.publication.chunk_retired(_retirement(authority.digest))
    retirement = owner.publication.retired()
    owner.publication.checkpoint_required()
    stale = NativeChunkJournal(store, lease, plan, parent_schema_version=4)
    receipt = NativeCheckpointReceipt(retirement.digest, "target", "window", lease.fence, 7, "9" * 64)
    owner.publication.succeeded(receipt)
    with pytest.raises(WindowContractError):
        stale.publication.succeeded(receipt)
    assert stale.publication.state()["phase"] == "checkpoint_required"


def test_v4_recovery_requires_explicit_activation(tmp_path):
    store, lease, plan, _ = _v4_complete(tmp_path)
    with pytest.raises(WindowContractError, match="parent_schema_changed"):
        NativeChunkJournal(store, lease, plan)
    assert NativeChunkJournal(store, lease, plan, parent_schema_version=4).data["version"] == 4


@pytest.mark.parametrize("field", ["fence", "checkpoint_cas_revision", "checkpoint_sha256"])
def test_v4_reload_rejects_changed_checkpoint_receipt_fields(tmp_path, field):
    store, lease, plan, journal = _v4_complete(tmp_path)
    journal.publication.abort_required()
    authority = journal.publication.abort_confirmed({"rollback": "proved"})
    journal.publication.retirement_required(authority)
    journal.publication.retiring()
    journal.publication.chunk_retired(_retirement(authority.digest))
    retirement = journal.publication.retired()
    journal.publication.checkpoint_required()
    journal.publication.succeeded(
        NativeCheckpointReceipt(retirement.digest, "target", "window", lease.fence, 7, "9" * 64)
    )
    changed = journal.data
    changed["publication"]["checkpoint_receipt"][field] = (
        8 if field in {"fence", "checkpoint_cas_revision"} else "8" * 64
    )
    store.save(journal.key, journal.revision, json.dumps(changed), lease)
    with pytest.raises(WindowContractError, match="invalid_journal"):
        NativeChunkJournal(store, lease, plan, parent_schema_version=4)


@pytest.mark.parametrize("phase", ["preparing", "prepared", "publishing", "abort_required"])
def test_v4_reload_rejects_premature_outcome_receipt(tmp_path, phase):
    store, lease, plan, journal = _v4_complete(tmp_path)
    changed = journal.data
    changed["publication"]["phase"] = phase
    changed["publication"]["receipt"] = {"invented": "premature"}
    store.save(journal.key, journal.revision, json.dumps(changed), lease)
    with pytest.raises(WindowContractError, match="invalid_journal"):
        NativeChunkJournal(store, lease, plan, parent_schema_version=4)


@pytest.mark.parametrize("outcome", ["published", "aborted"])
def test_settled_authority_replay_is_immutable_across_fence_takeover(tmp_path, outcome):
    store, old_lease, plan, journal = _v4_complete(tmp_path)
    if outcome == "published":
        journal.publication.publication_started({"stage": "prepared"})
        receipt = {"generation": "receipt"}
        authority = journal.publication.publication_confirmed(receipt)
    else:
        journal.publication.abort_required()
        receipt = {"rollback": "proved"}
        authority = journal.publication.abort_confirmed(receipt)
    store.release(old_lease)
    new_lease = store.acquire("target", "new-owner", 60)
    recovered = NativeChunkJournal(store, new_lease, plan, parent_schema_version=4)
    before = store.load(recovered.key)
    replay = (
        recovered.publication.publication_confirmed(receipt)
        if outcome == "published"
        else recovered.publication.abort_confirmed(receipt)
    )
    assert replay == authority
    assert replay.fence == old_lease.fence
    assert store.load(recovered.key) == before


def test_succeeded_checkpoint_replay_is_immutable_across_fence_takeover(tmp_path):
    store, old_lease, plan, journal = _v4_complete(tmp_path)
    journal.publication.abort_required()
    authority = journal.publication.abort_confirmed({"rollback": "proved"})
    journal.publication.retirement_required(authority)
    journal.publication.retiring()
    journal.publication.chunk_retired(_retirement(authority.digest))
    retirement = journal.publication.retired()
    journal.publication.checkpoint_required()
    receipt = NativeCheckpointReceipt(retirement.digest, "target", "window", old_lease.fence, 7, "9" * 64)
    journal.publication.succeeded(receipt)
    store.release(old_lease)
    new_lease = store.acquire("target", "new-owner", 60)
    recovered = NativeChunkJournal(store, new_lease, plan, parent_schema_version=4)
    before = store.load(recovered.key)
    recovered.publication.succeeded(receipt)
    assert store.load(recovered.key) == before
    with pytest.raises(WindowContractError, match="checkpoint"):
        recovered.publication.succeeded(replace(receipt, checkpoint_sha256="8" * 64))
