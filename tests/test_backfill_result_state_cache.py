"""Truthful terminal backfill state-cache references."""

from __future__ import annotations

import json
from types import SimpleNamespace

from dpone.backfill.runtime_results import build_backfill_result
from dpone.backfill.state import BackfillChunkRecord, BackfillLedger, FileBackfillStateStore


class _AuditStore(FileBackfillStateStore):
    dialect = "mssql"

    def __init__(self, root_dir) -> None:
        super().__init__(root_dir)
        self.sync_calls = 0

    def sync_local_cache(self, ledger: BackfillLedger):
        self.sync_calls += 1
        return self.sync_committed_snapshot(ledger)


def _ledger(*indexes: int) -> BackfillLedger:
    records = [
        BackfillChunkRecord(
            index=index,
            start=str(index),
            end=str(index + 1),
            idempotency_key=f"chunk:{index}",
            status="success",
            rows_extracted=10,
            rows_loaded=10,
        )
        for index in indexes
    ]
    return BackfillLedger(
        run_key="campaign-a",
        dataset="dbo.orders",
        inner_mode="partition_replace",
        chunk_config={"column": "id"},
        chunks=records,
    )


def _result(store: FileBackfillStateStore, ledger: BackfillLedger, *indexes: int):
    return build_backfill_result(
        ledger=ledger,
        store=store,
        chunks=tuple(SimpleNamespace(index=index) for index in indexes),
        retry_policy="resume",
        selected=len(indexes),
        skipped=0,
        errors=[],
        duration_seconds=1.0,
    )


def test_terminal_result_exposes_an_existing_sql_ledger_cache_without_rewriting_it(tmp_path) -> None:
    store = _AuditStore(tmp_path)
    ledger = _ledger(1, 2)
    store.sync_committed_snapshot(ledger)

    result = _result(store, ledger, 1, 2)

    state_path = result["backfill"]["state_path"]
    assert state_path is not None
    assert result["backfill"]["state_cache_status"] == "present_non_authoritative"
    assert result["backfill"]["state_authority"] == {
        "backend": "audit_schema",
        "dialect": "mssql",
        "schema": None,
        "campaigns_table": None,
        "chunks_table": None,
        "run_key": ledger.run_key,
    }
    assert store.sync_calls == 0
    assert json.loads(store.path_for(ledger.run_key).read_text(encoding="utf-8"))["run_key"] == ledger.run_key


def test_terminal_result_never_advertises_a_nonexistent_sql_cache(tmp_path) -> None:
    store = _AuditStore(tmp_path)

    result = _result(store, _ledger(1), 1, 2)

    assert result["backfill"]["state_path"] is None
    assert result["backfill"]["state_cache_status"] == "unavailable"
    assert store.sync_calls == 0


def test_local_file_state_is_labelled_authoritative(tmp_path) -> None:
    store = FileBackfillStateStore(tmp_path)
    ledger = _ledger(1)
    store.save(ledger)

    result = _result(store, ledger, 1)

    assert result["backfill"]["state_cache_status"] == "authoritative"
    assert result["backfill"]["state_authority"] == {
        "backend": "local_file",
        "run_key": ledger.run_key,
    }
