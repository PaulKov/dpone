"""Policy/replay contracts with real SQLite journals and mocked target authority.

Risk matrix: positive/compatibility = both read modes; negative = cross-mode
identity; boundary = zero-row EOF versus incomplete staging; retry/replay =
completed staging and receipt reconciliation; idempotency = repeated committed
run; failure = absent/mismatching/unavailable receipt and evidence failure.
SQLite persistence is local integration. SQL operations remain mocked contract
checks; live integration and route certification are UNVERIFIED.

No SQL connection is opened: receipt objects are real contract values, while
receipt probes/publication callbacks are test doubles, never live certification.
"""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeChunkPlan, NativeChunkReceipt
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime, NativeRuntimeBindings
from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import MssqlGenericCommitOutcomeUnknown
from tests.test_mssql_native_policy import config
from tests.test_mssql_native_staged_recovery import prepared_fixture, receipt_fixture

RAW = "raw_single_query"


def poison(*args, **kwargs):
    pytest.fail("forbidden source/publication/evidence/checkpoint/cleanup side effect")


def raw_config(mode):
    value = config()
    if mode is not None:
        value.options["native_transfer"]["source_read"] = {"mode": mode}
    return value


def plan_for(mode):
    return NativeChunkPlan("run", "target", "query", "window", "schema", "wire", source_read_mode=mode)


@pytest.mark.parametrize("stored,requested", [(None, RAW), (RAW, None)])
@pytest.mark.parametrize("complete", [False, True])
def test_cross_mode_journal_rejected_without_rewriting_authority(tmp_path, stored, requested, complete):
    store = SQLiteWindowStore(tmp_path / "journal.sqlite", clock=lambda: 1)
    lease = store.acquire("target", "owner", 60)
    original = NativeChunkJournal(store, lease, plan_for(stored))
    original.begin()
    if complete:
        original.attempt(0, 0)
        original.verified(NativeChunkReceipt(0, "run-0-0", "stage", 0, 0, "a" * 64, "b" * 64))
        original.complete(source_eof=True)
    before = store.load(original.key)
    with pytest.raises(WindowContractError, match="journal_identity_changed"):
        NativeChunkJournal(store, lease, plan_for(requested))
    assert store.load(original.key) == before
    assert NativeChunkJournal(store, lease, plan_for(stored)).data == original.data


@pytest.mark.parametrize("bound,requested", [(None, RAW), (RAW, None)])
def test_runtime_policy_mismatch_precedes_resume_and_all_business_side_effects(tmp_path, bound, requested):
    def bindings(cfg, owner, lease, cancelled):
        context = SimpleNamespace(plan=plan_for(bound), lease=lease, cancelled=cancelled)
        return NativeRuntimeBindings(SimpleNamespace(resume=poison), context, None)

    runtime = NativeMssqlRuntime(
        store=SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1),
        target_id="target",
        bindings=bindings,
        source=poison,
        preflight=lambda _: None,
        quality=poison,
        evidence=poison,
        advance_state=poison,
    )
    with pytest.raises(WindowContractError, match="source_policy_mismatch"):
        runtime.run(raw_config(requested), owner="invocation")


@pytest.mark.parametrize("mode", [None, RAW])
@pytest.mark.parametrize("complete", [False, True])
def test_completed_stage_recovery_is_source_free_but_partial_requires_reextraction(tmp_path, mode, complete):
    store = SQLiteWindowStore(tmp_path / "stage.sqlite", clock=lambda: 1)
    lease = store.acquire("target", "owner", 60)
    plan = plan_for(mode)
    journal = NativeChunkJournal(store, lease, plan)
    journal.begin()
    if complete:
        journal.attempt(0, 0)
        journal.verified(NativeChunkReceipt(0, "run-0-0", "stage", 2, 8, "a" * 64, "b" * 64))
        journal.complete(source_eof=True)
    events = []
    prepared = prepared_fixture()

    def recover_stage(*args):
        events.append("recover-verified-stage")
        return prepared

    context = SimpleNamespace(
        plan=plan,
        lease=lease,
        journal_factory=lambda: NativeChunkJournal(store, lease, plan),
        row_source=poison,
        executor=SimpleNamespace(recover=lambda *args: events.append("settle-partial")),
    )
    service = MssqlNativeStagedLoadService(object(), SimpleNamespace(recover_stage=recover_stage, stage=poison))
    if complete:
        handle = service.resume(raw_config(mode), context, prepared.admission)
        assert isinstance(handle, StagedLoadHandle)
        assert handle.staged_rows == 2
        assert events == ["recover-verified-stage"]
    else:
        with pytest.raises(ValueError, match="reextract_required"):
            service.resume(raw_config(mode), context, prepared.admission)
        assert events == ["settle-partial"]


@pytest.mark.parametrize("mode", [None, RAW])
@pytest.mark.parametrize("outcome", ["match", "missing", "mismatch", "unavailable"])
def test_unknown_commit_probes_exact_receipt_before_replay_or_cleanup(tmp_path, monkeypatch, mode, outcome):
    from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState

    prepared = prepared_fixture()
    receipt = receipt_fixture(prepared)
    events = []

    def probe(operation):
        assert operation is prepared.admission.operation
        events.append("receipt-probe")
        if outcome == "unavailable":
            raise MssqlGenericCommitOutcomeUnknown()
        if outcome == "missing":
            return None
        if outcome == "mismatch":
            return replace(receipt, payload_evidence=replace(receipt.payload_evidence, manifest_sha256=b"z" * 32))
        return receipt

    monkeypatch.setattr(
        MssqlGenericTransactionState,
        "from_state_storage",
        classmethod(lambda cls, storage: SimpleNamespace(probe_receipt_fresh=probe)),
    )
    publication = SimpleNamespace(
        state=lambda: {"phase": "publishing", "prepared": {"operation_key": receipt.operation_key.hex()}},
        publication_confirmed=lambda result: events.append(("confirmed", result)),
    )
    service = MssqlNativeStagedLoadService(
        SimpleNamespace(state_storage=object()),
        SimpleNamespace(restore=lambda *args: prepared, reverify=poison, cleanup=poison, stage=poison),
    )
    context = SimpleNamespace(
        plan=plan_for(mode), journal_factory=lambda: SimpleNamespace(publication=publication), row_source=poison
    )
    if outcome == "match":
        result = service.resume(raw_config(mode), context, prepared.admission)
        assert result.commit_receipt_id == receipt.receipt_id
        assert events == ["receipt-probe", ("confirmed", {"receipt_id": receipt.receipt_id})]
    else:
        with pytest.raises(MssqlGenericCommitOutcomeUnknown):
            service.resume(raw_config(mode), context, prepared.admission)
        assert events == ["receipt-probe"]


@pytest.mark.parametrize("mode", [None, RAW])
def test_committed_replay_retries_evidence_before_checkpoint_without_republication(tmp_path, mode):
    prepared = prepared_fixture()
    receipt = receipt_fixture(prepared)
    admission = MssqlTransactionAdmission(replay_receipt=receipt)
    prepared = replace(prepared, admission=admission)
    events = []
    phase = {"value": "published"}
    publication = SimpleNamespace(
        state=lambda: {"phase": phase["value"], "prepared": {"operation_key": receipt.operation_key.hex()}},
        evidence_complete=lambda: phase.update(value="evidence-complete"),
        succeeded=lambda: phase.update(value="succeeded"),
    )
    service = MssqlNativeStagedLoadService(
        object(),
        SimpleNamespace(
            restore=lambda *args: prepared,
            reverify=poison,
            stage=poison,
            cleanup=lambda *args: events.append("cleanup"),
        ),
    )

    def bindings(cfg, owner, lease, cancelled):
        return NativeRuntimeBindings(
            service,
            SimpleNamespace(
                plan=plan_for(mode),
                lease=lease,
                cancelled=cancelled,
                journal_factory=lambda: SimpleNamespace(publication=publication),
                row_source=poison,
            ),
            admission,
        )

    def evidence(*args):
        events.append("evidence")
        if events.count("evidence") == 1:
            raise RuntimeError("synthetic evidence failure")

    runtime = NativeMssqlRuntime(
        store=SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1),
        target_id="target",
        bindings=bindings,
        source=poison,
        preflight=lambda _: None,
        quality=poison,
        evidence=evidence,
        advance_state=lambda *args: events.append("checkpoint"),
    )
    with pytest.raises(RuntimeError, match="synthetic evidence failure"):
        runtime.run(raw_config(mode), owner="invocation")
    assert phase["value"] == "published" and events == ["evidence"]
    assert runtime.run(raw_config(mode), owner="invocation").status == "success"
    assert events == ["evidence", "evidence", "checkpoint", "cleanup"]
    assert runtime.run(raw_config(mode), owner="invocation").status == "success"
    assert events == ["evidence", "evidence", "checkpoint", "cleanup", "cleanup"]
