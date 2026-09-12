"""Transaction execution preserves shared-handle and bounded-fetch guarantees."""

from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from threading import Barrier, local
from types import SimpleNamespace
from uuid import UUID

import pytest

from tests import test_postgres_mssql_r1_v3_quality_xmin as fixtures

OWNER = "dpone.adapters.mssql_r1_v3_transaction_execution"


@pytest.mark.parametrize("row_count", [0, 1, 2, 3])
def test_quality_query_stops_after_two_rows_on_the_shared_mutation_handle(row_count):
    owner = import_module(OWNER)
    fixture = fixtures._fixture()

    class Cursor(fixtures._Cursor):
        reads = 0

        def fetchone(self):
            self.reads += 1
            return super().fetchone()

    supplied = tuple((index,) for index in range(row_count))
    cursor = Cursor("same-session", supplied)
    binder = owner.MssqlR1V3TransactionHandleBinder(lambda _: cursor, lambda handle: handle.session_id)
    query = next(item for item in fixture.statements if item.step_kind is fixtures.R1MutationStepV1.QUALITY)
    mutation = next(item for item in fixture.statements if item.step_kind is fixtures.R1MutationStepV1.DELETE)
    transaction = fixtures._Transaction()
    owner.MssqlR1V3OdbcMutationCommandAdapter(binder).execute(transaction, mutation)
    rows = owner.MssqlR1V3OdbcQualityQueryAdapter(binder).query(transaction, query)
    assert rows == supplied[:2]
    assert cursor.reads == min(row_count + 1, 2)
    assert cursor.sql == [mutation.statement_utf8_bytes.decode(), query.statement_utf8_bytes.decode()]


@pytest.mark.parametrize("same_handle", [False, True])
def test_racing_binding_admits_only_one_handle_per_transaction(same_handle):
    owner = import_module(OWNER)
    context = local()
    ready = Barrier(2)

    def handle_for(_):
        ready.wait(timeout=3)
        return context.handle

    binder = owner.MssqlR1V3TransactionHandleBinder(handle_for, lambda _: "same-session")
    transaction = SimpleNamespace(transaction_id=UUID(int=1))
    first = object()
    handles = [first, first if same_handle else object()]

    def resolve(handle):
        context.handle = handle
        try:
            return "bound", binder.resolve(transaction)
        except owner.MssqlR1V3ProviderAdmissionError as error:
            return "rejected", str(error)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(resolve, handle) for handle in handles]
        results = [future.result(timeout=5) for future in futures]
    if same_handle:
        assert results == [("bound", first), ("bound", first)]
    else:
        assert sorted(state for state, _ in results) == ["bound", "rejected"]
        assert (
            next(value for state, value in results if state == "rejected")
            == "postgres_mssql_r1.transaction_handle_changed"
        )


def test_distinct_transactions_may_bind_distinct_handles():
    owner = import_module(OWNER)
    handles = {UUID(int=1): object(), UUID(int=2): object()}
    binder = owner.MssqlR1V3TransactionHandleBinder(
        lambda transaction: handles[transaction.transaction_id], lambda _: "same-session"
    )
    for identity, handle in handles.items():
        assert binder.resolve(SimpleNamespace(transaction_id=identity)) is handle
