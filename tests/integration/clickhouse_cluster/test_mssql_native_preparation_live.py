"""Real SQL origin/locking proof, never ClickHouse recovery certification.

Uses only the explicit loopback-only, unique retained SQL catalog fixture. All
business generations and retirement observations are synthetic.
"""

import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import replace
from datetime import UTC
from threading import Event
from uuid import uuid4

import pytest

from dpone.contracts.clickhouse_cluster_publication import AuthorityMutationStatus as Status
from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase as Phase
from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError
from dpone.contracts.publication_authority_binding import publication_binding_digest, publication_slot_key
from tests.integration.clickhouse_cluster.test_mssql_publication_authority_live import (
    authority,
    connect,
    record,
    retirement_plan,
    retirement_store,
)
from tests.integration.clickhouse_cluster.test_mssql_publication_authority_live import (
    catalog as catalog,
)
from tests.test_mssql_publication_fresh_cas import preparation

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_MSSQL_PUBLICATION_LIVE") != "1", reason="explicit local SQL Server opt-in required"
    ),
]


def advance(store, current, phase):
    desired = current.record
    if phase is Phase.DISPATCHING:
        desired = desired.dispatching(token=uuid4().hex, query_digest="d" * 64)
    elif phase is Phase.COMMITTED:
        desired = replace(desired, phase=phase, ddl_entry="synthetic-entry")
    elif phase is Phase.CLEANUP_DISPATCHING:
        desired = replace(
            desired,
            phase=phase,
            dispatch_epoch=desired.dispatch_epoch + 1,
            cleanup_correlation_token=uuid4().hex,
            cleanup_query_digest="e" * 64,
        )
    elif phase is Phase.COMPLETED:
        desired = replace(
            desired,
            phase=phase,
            cleanup_entry="synthetic-cleanup" if desired.phase is Phase.CLEANUP_DISPATCHING else None,
        )
    result = store.compare_and_swap(current, desired)
    assert result.status is Status.VERIFIED
    return result.observed


def prepare(store, catalog, kind):
    if kind == "retired":
        plan = retirement_plan(catalog)
        assert retirement_store(catalog).retire_if_absent(plan) == "acknowledged"
        old = store.read_versioned(plan.original.record.target_key)
        result = store.compare_and_swap(old, preparation(old.record))
    elif kind == "later":
        old = store.create_if_absent(record()).observed
        for phase in (Phase.DISPATCHING, Phase.COMMITTED, Phase.COMPLETED):
            old = advance(store, old, phase)
        desired = record()
        desired = replace(
            desired,
            target_key=old.record.target_key,
            target=old.record.target,
            dispatch_epoch=old.record.dispatch_epoch + 1,
            predecessor=old.record.desired,
        )
        result = store.compare_and_swap(old, desired)
    else:
        result = store.create_if_absent(record())
    assert result.status is Status.VERIFIED
    return result.observed


@pytest.mark.parametrize("kind,revision", [("initial", 1), ("later", 5), ("retired", 2)])
@pytest.mark.parametrize("final_phase", list(Phase)[:5])
def test_real_origin_remains_exact_after_native_transitions(catalog, kind, revision, final_phase):
    store = authority(catalog)
    prepared = current = prepare(store, catalog, kind)
    if final_phase is not Phase.PREPARED:
        for phase in (Phase.DISPATCHING, Phase.COMMITTED, Phase.CLEANUP_DISPATCHING, Phase.COMPLETED):
            current = advance(store, current, phase)
            if phase is final_phase:
                break
    key = publication_slot_key(catalog[1], current.record.target_key)
    history_before = catalog[0].get_records(
        "SELECT revision,payload,created_utc FROM dbo.dpone_cluster_publication_events "
        "WHERE slot_key=? ORDER BY revision",
        (key,),
    )
    observation = store.read_native_preparation(current.record.target_key, current.record.operation_id)
    assert observation.binding_digest == publication_binding_digest(catalog[1], endpoint_identity=catalog[2])
    assert observation.prepared == prepared and observation.prepared.version == revision
    assert observation.current == current
    assert observation.prepared_at == history_before[revision - 1][2].replace(tzinfo=UTC)
    assert (
        catalog[0].get_records(
            "SELECT revision,payload,created_utc FROM dbo.dpone_cluster_publication_events "
            "WHERE slot_key=? ORDER BY revision",
            (key,),
        )
        == history_before
    )


def test_real_retired_id_stays_banned_after_fresh_completion(catalog):
    store = authority(catalog)
    plan = retirement_plan(catalog)
    assert retirement_store(catalog).retire_if_absent(plan) == "acknowledged"
    old = store.read_versioned(plan.original.record.target_key)
    current = store.compare_and_swap(old, preparation(old.record)).observed
    for phase in (Phase.DISPATCHING, Phase.COMMITTED, Phase.COMPLETED):
        current = advance(store, current, phase)
    with pytest.raises(ClusterPublicationError, match="RETIRED_OPERATION"):
        store.read_native_preparation(current.record.target_key, old.record.operation_id)


def test_native_observation_does_not_treat_absence_as_origin(catalog):
    value = record()
    with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
        authority(catalog).read_native_preparation(value.target_key, value.operation_id)


def test_operation_ids_differing_in_trailing_space_have_distinct_origins(catalog):
    store = authority(catalog)
    current = store.create_if_absent(record()).observed
    for phase in (Phase.DISPATCHING, Phase.COMMITTED, Phase.COMPLETED):
        current = advance(store, current, phase)
    desired = replace(
        record(),
        target_key=current.record.target_key,
        target=current.record.target,
        operation_id=current.record.operation_id + " ",
        dispatch_epoch=current.record.dispatch_epoch + 1,
    )
    prepared = store.compare_and_swap(current, desired).observed
    observed = store.read_native_preparation(prepared.record.target_key, desired.operation_id)
    assert observed.prepared == prepared and observed.prepared.version == 5


def test_origin_and_current_read_share_lock_until_transaction_ack(catalog):
    store = authority(catalog)
    prepared = store.create_if_absent(record()).observed
    read_locked, release_read, writer_started = Event(), Event(), Event()

    class PausedCursor:
        def __init__(self, cursor):
            self.cursor = cursor
            self.executions = 0

        def __getattr__(self, name):
            return getattr(self.cursor, name)

        def execute(self, query, params):
            self.executions += 1
            if self.executions == 2:
                read_locked.set()
                assert release_read.wait(10)
            return self.cursor.execute(query, params)

    class PausedSession:
        def __init__(self):
            self.connection = connect(catalog[1].database).connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        @property
        def autocommit(self):
            return self.connection.autocommit

        @autocommit.setter
        def autocommit(self, value):
            self.connection.autocommit = value

        def cursor(self):
            return PausedCursor(self.connection.cursor())

    reader = authority(catalog, factory=PausedSession)

    def write():
        writer_started.set()
        return advance(store, prepared, Phase.DISPATCHING)

    with ThreadPoolExecutor(max_workers=2) as pool:
        observed = pool.submit(reader.read_native_preparation, prepared.record.target_key, prepared.record.operation_id)
        try:
            assert read_locked.wait(10)
            written = pool.submit(write)
            assert writer_started.wait(10)
            with pytest.raises(TimeoutError):
                written.result(timeout=0.25)
        finally:
            release_read.set()
        assert observed.result(timeout=10).current == prepared
        assert written.result(timeout=10).record.phase is Phase.DISPATCHING
