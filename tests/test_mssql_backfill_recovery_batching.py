"""Bounded MSSQL commit-unknown recovery contracts."""

from __future__ import annotations

from dpone.backfill.sql_state_mssql_mutations import (
    MSSQLLedgerMutation,
    MSSQLLedgerMutationCoordinator,
)
from dpone.backfill.sql_state_mssql_recovery import (
    MSSQL_CHUNK_RECOVERY_BATCH_SIZE,
    iter_mssql_parameter_batches,
)
from dpone.backfill.state import BackfillChunkRecord, BackfillLedger


def _chunk(index: int) -> BackfillChunkRecord:
    return BackfillChunkRecord(
        index=index,
        start=str(index),
        end=str(index + 1),
        idempotency_key=f"chunk:{index}",
    )


def _ledger(chunks: tuple[BackfillChunkRecord, ...]) -> BackfillLedger:
    return BackfillLedger(
        run_key="campaign-a",
        dataset="dbo.orders",
        inner_mode="partition_replace",
        chunk_config={"column": "id"},
        plan_hash="plan-a",
        config_hash="config-a",
        chunks=list(chunks),
    )


def test_mssql_parameter_batches_cover_authored_million_chunk_limit() -> None:
    count = 0
    largest = 0
    last: tuple[int, ...] = ()

    for batch in iter_mssql_parameter_batches(range(1, 1_000_001)):
        count += 1
        largest = max(largest, len(batch))
        last = batch

    assert count == 1_000
    assert largest == MSSQL_CHUNK_RECOVERY_BATCH_SIZE
    assert last == tuple(range(999_001, 1_000_001))


def test_unknown_commit_verifies_chunk_revisions_in_bounded_point_reads() -> None:
    expected_chunks = tuple(_chunk(index) for index in range(1, 2_502))
    candidate = _ledger(expected_chunks)
    point_reads: list[tuple[int, ...]] = []

    class Connector:
        def begin(self) -> None:
            return None

        def execute_query(self, _statement: str) -> None:
            return None

        def commit_transaction(self) -> None:
            raise OSError("injected acknowledgement loss")

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    loads = 0

    def load_for_update(_run_key, _connector):
        nonlocal loads
        loads += 1
        return None if loads == 1 else candidate

    by_index = {record.index: record for record in expected_chunks}

    def load_chunks(_run_key, _connector, indexes):
        point_reads.append(indexes)
        return tuple(by_index[index] for index in reversed(indexes))

    coordinator = MSSQLLedgerMutationCoordinator(
        Connector(),
        ensure_tables=lambda: None,
        load_for_update=load_for_update,
        load_chunks=load_chunks,
        append_snapshot=lambda _connector, _ledger, _chunks: None,
    )

    committed = coordinator.run(
        candidate.run_key,
        lambda _current: MSSQLLedgerMutation(candidate, expected_chunks, "ok"),
    )

    assert committed.result == "ok"
    assert committed.ledger is candidate
    assert [len(indexes) for indexes in point_reads] == [1_000, 1_000, 501]
    assert all(len(indexes) + 1 < 2_100 for indexes in point_reads)
