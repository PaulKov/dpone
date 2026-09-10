"""Synthetic transaction/recovery proofs; this does not certify live SWITCH."""

import copy
from dataclasses import replace
from datetime import date
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.contracts.native_mssql_switch import NativeSwitchRejected
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.sinks.mssql_native_staged_load import MssqlNativeStagedLoadService
from dpone.runtime.sinks.mssql_native_switch import NativeSwitchCatalog, execute_native_switch, plan_native_switch
from dpone.runtime.sinks.mssql_native_switch.catalog_sql import TRANSACTION_SQL
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import MssqlGenericCommitOutcomeUnknown
from tests.test_mssql_generic_transaction_governance import (
    _config,
    _finalizer,
    _FinalizerConnector,
    _FinalizerState,
    _operation,
    _source_lifecycle_receipt,
    _staging,
)
from tests.test_mssql_native_partition_switch_catalog import CatalogSql
from tests.test_mssql_native_staged_recovery import prepared_fixture, receipt_fixture


class RowTransaction(CatalogSql):
    """Exact multiset fixture with unrelated target partitions and duplicates."""

    def __init__(self, *, empty=False):
        super().__init__(prepared_rows=0 if empty else 2)
        self.content = {
            "target": [
                (date(2025, 12, 1), "outside-before"),
                (date(2026, 1, 2), "old"),
                (date(2026, 1, 2), "old"),
                (date(2026, 2, 1), "outside-after"),
            ],
            "prepared": [] if empty else [(date(2026, 1, 1), "duplicate")] * 2,
            "old": [],
        }
        self.rows = {name: len(rows) for name, rows in self.content.items()}

    def plan(self):
        self.expected_prepared = copy.deepcopy(self.content["prepared"])
        return super().plan()

    def verify_prepared(self, plan):
        super().verify_prepared(plan)
        if self.content["prepared"] != self.expected_prepared:
            raise ValueError("typed prepared digest mismatch")

    def selected(self, row):
        return row[0] is not None and self.interval.start <= row[0] < self.interval.end

    def query(self, sql, parameters=()):
        if "COUNT_BIG(*) AS row_count FROM" not in sql:
            return super().query(sql, parameters)
        self.events.append((sql, parameters))
        name = next(name for name in self.content if f"[{name}]" in sql)
        rows = self.content[name]
        if " IS NULL OR " in sql:
            result = sum(not self.selected(row) for row in rows)
        elif " WHERE " in sql:
            result = sum(self.selected(row) for row in rows)
        else:
            result = len(rows)
        return [{"row_count": result}]

    def execute(self, sql):
        super().execute(sql)
        if self.switches == 1:
            self.content["old"] = [row for row in self.content["target"] if self.selected(row)]
            self.content["target"] = [row for row in self.content["target"] if not self.selected(row)]
        else:
            self.content["target"] += self.content["prepared"]
            self.content["prepared"] = []


@pytest.mark.parametrize("empty", [False, True])
def test_two_switches_preserve_outside_rows_and_count_replaced_rows(empty):
    tx = RowTransaction(empty=empty)
    before = copy.deepcopy(tx.content)
    result = execute_native_switch(tx.plan(), transaction=tx)
    assert result.replaced_rows == 2
    assert result.inserted_rows == (0 if empty else 2)
    assert tx.content["target"] == [row for row in before["target"] if not tx.selected(row)] + before["prepared"]
    assert tx.content["old"] == [row for row in before["target"] if tx.selected(row)]
    assert tx.content["prepared"] == []
    queries = [query for query, _ in tx.events]
    assert not any(
        query.startswith(("BEGIN", "COMMIT", "ROLLBACK", "DELETE", "INSERT", "DROP", "TRUNCATE")) for query in queries
    )
    locks = [query for query in queries if "TABLOCKX, HOLDLOCK" in query]
    assert [next(name for name in ("target", "prepared", "old") if f"[{name}]" in query) for query in locks] == [
        "target",
        "prepared",
        "old",
    ]
    assert queries.index(locks[-1]) < max(i for i, query in enumerate(queries) if "AS metadata" in query)


@pytest.mark.parametrize(
    "field,value",
    [
        ("state", 0),
        ("state", -1),
        ("depth", 2),
        ("database_id", 9),
        ("xact_abort", 0),
        ("isolation", 2),
        ("session_id", None),
    ],
)
def test_transaction_rejection_precedes_locks_and_mutation(field, value):
    tx = CatalogSql()
    plan = tx.plan()
    tx.events.clear()
    tx.state[field] = value
    with pytest.raises(NativeSwitchRejected, match="transaction_authority_invalid"):
        execute_native_switch(plan, transaction=tx)
    assert tx.switches == 0
    assert not any("TABLOCKX" in query for query, _ in tx.events)


def test_owner_authority_is_mandatory():
    tx = CatalogSql()
    plan = tx.plan()
    tx.events.clear()
    tx.assert_authority = lambda binding: (_ for _ in ()).throw(RuntimeError("stale fence"))
    with pytest.raises(RuntimeError, match="stale fence"):
        execute_native_switch(plan, transaction=tx)
    assert tx.events == []


def test_transaction_observations_preserve_authority_lock_integrity_and_switch_order():
    tx = RowTransaction()
    plan = tx.plan()
    tx.events.clear()
    execute_native_switch(plan, transaction=tx)
    events = [query for query, _ in tx.events]
    observations = [index for index, query in enumerate(events) if query == TRANSACTION_SQL]
    authority = [index for index, query in enumerate(events) if query == "authority"]
    locks = [index for index, query in enumerate(events) if "TABLOCKX" in query]
    mutations = [index for index, query in enumerate(events) if query.startswith("ALTER TABLE")]
    assert len(observations) == len(authority) == len(mutations) == 2
    assert len(locks) == 3
    assert authority[0] < observations[0] < locks[0]
    assert locks[-1] < events.index("verify_prepared") < authority[1] < observations[1] < mutations[0]
    assert mutations == [len(events) - 2, len(events) - 1]


@pytest.mark.parametrize("failure", [1, 2])
def test_original_failure_propagates_for_complete_caller_rollback(failure):
    tx = RowTransaction()
    plan = tx.plan()
    before = copy.deepcopy(tx.content)
    tx.fail_switch = failure
    with pytest.raises(OSError, match="switch failed"):
        execute_native_switch(plan, transaction=tx)
    assert tx.switches == failure
    if failure == 2:
        assert tx.content["old"] and not any(tx.selected(row) for row in tx.content["target"])
    # Transaction fixture owner performs the rollback; executor cannot do so.
    tx.content = before
    assert tx.content["prepared"] and not tx.content["old"]
    assert len(tx.content["target"]) == 4


@pytest.mark.parametrize("lost_ack,probe", [(False, False), (True, True), (True, False)])
def test_existing_finalizer_settles_switch_handler_commit_without_replay(lost_ack, probe):
    """Real finalizer, injected catalog/SQL double; no production bridge implied."""
    tx = CatalogSql(target_rows=0, prepared_rows=2)
    plan = tx.plan()
    connector = _FinalizerConnector(commit_error=OSError("lost acknowledgement") if lost_ack else None)
    state = _FinalizerState(probe_after_commit=probe)
    finalizer = _finalizer(connector, state)

    def handler(staging):
        result = execute_native_switch(plan, transaction=tx)
        return LoadResult(
            inserted_rows=result.inserted_rows,
            updated_rows=0,
            total_rows=result.inserted_rows,
            replaced_rows=result.replaced_rows,
        )

    def publish():
        return finalizer.finalize(
            _config(),
            MssqlTransactionAdmission(operation=_operation()),
            handler,
            _staging(2),
            staging_rows=2,
            load_id="load-A",
            source_lifecycle_receipt=_source_lifecycle_receipt(),
        )

    if lost_ack and not probe:
        with pytest.raises(MssqlGenericCommitOutcomeUnknown):
            publish()
    else:
        result = publish()
        expected = AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE if lost_ack else AtomicCommitOutcome.COMMITTED
        assert result.commit_outcome == expected
        assert result.replaced_rows == 0
        assert result.inserted_rows == 2
    assert tx.rows == {"target": 2, "prepared": 0, "old": 0}
    assert tx.switches == 2
    assert connector.commits == 1 and connector.rollbacks == 0
    assert state.receipt_inserts == 1 and state.fresh_probes == int(lost_ack)


def test_existing_finalizer_rolls_back_if_second_switch_fails():
    tx = CatalogSql()
    tx.fail_switch = 2
    plan = tx.plan()
    connector, state = _FinalizerConnector(), _FinalizerState()
    with pytest.raises(OSError, match="switch failed"):
        _finalizer(connector, state).finalize(
            _config(),
            MssqlTransactionAdmission(operation=_operation()),
            lambda staging: execute_native_switch(plan, transaction=tx),
            _staging(3),
            staging_rows=3,
            load_id="load-A",
            source_lifecycle_receipt=_source_lifecycle_receipt(),
        )
    assert connector.rollbacks == 1 and connector.commits == 0
    assert state.receipt_inserts == 0 and tx.switches == 2


@pytest.mark.parametrize("mismatch", [False, True])
def test_receipt_first_resume_after_switch_emptied_prepared(mismatch):
    """Real resume service rejects wrong receipts without re-reading moved rows."""
    tx = CatalogSql(target_rows=0, prepared_rows=2)
    execute_native_switch(tx.plan(), transaction=tx)
    assert tx.rows["prepared"] == tx.rows["old"] == 0
    prepared = prepared_fixture()
    receipt = receipt_fixture(prepared)
    if mismatch:
        receipt = replace(receipt, payload_evidence=replace(receipt.payload_evidence, manifest_sha256=b"z" * 32))
    admission = MssqlTransactionAdmission(replay_receipt=receipt)
    prepared = replace(prepared, admission=admission)
    confirmed = []

    class Preparer:
        def restore(self, *args):
            return prepared

        def reverify(self, *args):
            pytest.fail("receipt must precede reads of the emptied prepared table")

    journal = SimpleNamespace(
        publication=SimpleNamespace(
            state=lambda: {"phase": "publishing", "prepared": {"operation_key": receipt.operation_key.hex()}},
            publication_confirmed=confirmed.append,
        )
    )
    context = SimpleNamespace(journal_factory=lambda: journal, row_source=lambda: pytest.fail("source reopened"))
    service = MssqlNativeStagedLoadService(object(), Preparer())
    if mismatch:
        with pytest.raises(MssqlGenericCommitOutcomeUnknown):
            service.resume(object(), context, admission)
        assert confirmed == []
    else:
        for _ in range(2):
            assert service.resume(object(), context, admission).commit_receipt_id == receipt.receipt_id
    assert tx.switches == 2 and tx.rows["target"] == 2


def test_same_count_value_tampering_is_reverified_under_held_locks():
    tx = RowTransaction()
    plan = tx.plan()
    tx.content["prepared"][0] = (date(2026, 1, 1), "tampered business or framework value")
    with pytest.raises(ValueError, match="typed prepared digest mismatch"):
        execute_native_switch(plan, transaction=tx)
    assert tx.switches == 0
    queries = [query for query, _ in tx.events]
    assert max(i for i, query in enumerate(queries) if "TABLOCKX" in query) < queries.index("verify_prepared")


@pytest.mark.parametrize("day", [None, date(2025, 12, 31), date(2026, 2, 1)])
def test_prepared_null_and_outside_boundaries_rejected_with_real_row_double(day):
    tx = RowTransaction()
    tx.content["prepared"].append((day, "outside"))
    snapshot = NativeSwitchCatalog(tx).snapshot(tx.binding, interval=tx.interval)
    result = plan_native_switch(snapshot, interval=tx.interval, owner_binding=tx.binding)
    assert result.plan is None
    assert result.reasons == ("prepared_rows_outside_interval",)
    assert tx.switches == 0
