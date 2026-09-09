"""Synthetic PostgreSQL snapshot -> bounded RowBinary -> ClickHouse route.

The target fixture owns a disposable database and is the exclusive SQL authority.
SQLite additionally fences local journal/evidence/state writes; it is never used
as proof that outside ClickHouse writers are excluded.
"""

from __future__ import annotations

import json
import time
import uuid
from collections import Counter
from dataclasses import replace
from datetime import timedelta

import pytest

from dpone.adapters.bounded_window_journal import WindowJournal
from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.runtime.bounded_window_execution import BoundedWindowExecutor
from dpone.runtime.sources.postgres_window_source import PostgresWindowSource
from tests.integration.clickhouse.test_clickhouse_window_target_integration import SCHEMA, START, read, seed
from tests.integration.clickhouse.test_clickhouse_window_target_integration import window_target as window_target

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse, pytest.mark.integration_postgres]


@pytest.fixture
def postgres_rows(postgres_settings):
    psycopg = pytest.importorskip("psycopg")
    settings = postgres_settings

    def connection():
        return psycopg.connect(
            host=settings.host,
            port=settings.port,
            dbname=settings.database,
            user=settings.user,
            password=settings.password,
            autocommit=True,
        )

    table = "window_source_" + uuid.uuid4().hex[:16]
    with connection() as conn:
        conn.execute(f'CREATE TABLE "{table}" (at timestamptz, value text, id bigint)')
    try:
        yield connection, table
    finally:
        with connection() as conn:
            conn.execute(f'DROP TABLE "{table}"')


def source_context(factory, table, fingerprint):
    return PostgresWindowSource(
        connection_factory=factory,
        schema_name="public",
        table_name=table,
        columns=[name for name, _ in SCHEMA],
        window_column="at",
        schema_fingerprint=fingerprint,
        batch_rows=1,
    )


def insert(factory, table, rows):
    with factory() as conn, conn.cursor() as cursor:
        cursor.executemany(f'INSERT INTO "{table}" (at, value, id) VALUES (%s, %s, %s)', rows)


def callbacks(store, target, expected):
    """Durable fenced/CAS callbacks recording only metadata, not row payloads."""

    def evidence(plan, generation, receipts, lease):
        with target.io.guard.hold(lease):
            store.assert_lease(lease)
            assert Counter(read(target)) == Counter(expected)
            key = "evidence:" + plan.run_id
            record = store.load(key)
            payload = json.dumps({"generation": generation, "rows": sum(r.row_count for r in receipts)})
            if record is None:
                store.save(key, None, payload, lease)
            else:
                assert record.payload == payload

    def state(plan, lease):
        with target.io.guard.hold(lease):
            store.assert_lease(lease)
            assert store.load("evidence:" + plan.run_id) is not None
            key = "state:" + plan.target_id
            record = store.load(key)
            payload = json.dumps({"end": plan.end.isoformat(), "run_id": plan.run_id})
            if record is None:
                store.save(key, None, payload, lease)
            else:
                assert record.payload == payload

    return evidence, state


@pytest.mark.parametrize("empty", [False, True])
def test_real_snapshot_route_preserves_rows_and_orders_durable_completion(
    window_target, postgres_rows, tmp_path, empty
):
    target, template, _, _ = window_target
    factory, table = postgres_rows
    initial = seed(target)
    replacement = [] if empty else [(START, "повтор", 1), (START, "повтор", 1), (START, None, 2)]
    insert(
        factory,
        table,
        [
            *replacement,
            (None, "source-null-excluded", 90),
            (START - timedelta(days=1), "source-before-excluded", 91),
            (template.end, "source-end-excluded", 92),
        ],
    )
    expected = [initial[0], initial[1], initial[3], *replacement]
    store = SQLiteWindowStore(tmp_path / "journal.sqlite", clock=time.time)
    evidence, state = callbacks(store, target, expected)
    with source_context(factory, table, template.schema_fingerprint) as source:
        plan = replace(
            template, source_version=source.source_version, parameters_fingerprint=source.parameters_fingerprint
        )
        # A post-export insert must remain invisible to every worker, even though
        # the second physical chunk is empty in the frozen snapshot.
        insert(factory, table, [(START + timedelta(days=1), "late-excluded", 99)])
        executor = BoundedWindowExecutor(
            source=source,
            target=target,
            store=store,
            evidence=evidence,
            advance_state=state,
            sleeper=lambda _: None,
            journal_factory=lambda lease, run_id: WindowJournal(store, lease, run_id),
        )
        result = executor.execute(plan, "synthetic-route")
    assert [receipt.row_count for receipt in result.receipts] == [len(replacement), 0]
    assert Counter(read(target)) == Counter(expected)
    assert target.generation_total(plan, result.generation) == len(expected)
    assert store.load("state:" + plan.target_id) is not None


@pytest.mark.parametrize("fault_phase", [None, "evidence", "state"])
def test_wrapper_recovers_postpublication_without_reopening_expired_source(
    window_target, postgres_rows, tmp_path, fault_phase
):
    from types import SimpleNamespace
    from unittest.mock import patch

    from dpone.runtime.connectors.clickhouse import ClickHouseConnector
    from dpone.runtime.rolling_window_runtime import RollingWindowRuntime

    target, template, _, _ = window_target
    factory, table = postgres_rows
    initial = seed(target)
    replacement = [(START, "duplicate", 1), (START, "duplicate", 1), (START, None, 2)]
    insert(factory, table, replacement)
    expected = [initial[0], initial[1], initial[3], *replacement]
    store = SQLiteWindowStore(tmp_path / "wrapper.sqlite", clock=time.time)
    target.io.guard.lease_validator = store.assert_lease
    persist_evidence, persist_state = callbacks(store, target, expected)
    source_opens = []
    failures = []
    exchanges = []

    def source_factory(frozen):
        assert not source_opens, "postpublication recovery must never reopen the PostgreSQL source"
        source_opens.append(frozen)
        return source_context(factory, table, template.schema_fingerprint)

    def evidence(plan, generation, receipts, lease):
        if fault_phase == "evidence" and not failures:
            failures.append("evidence")
            raise RuntimeError("synthetic evidence storage failure after publication")
        persist_evidence(plan, generation, receipts, lease)

    def state(plan, lease):
        if fault_phase == "state" and not failures:
            failures.append("state")
            raise RuntimeError("synthetic state storage failure after evidence")
        persist_state(plan, lease)

    def executor_factory(source):
        return BoundedWindowExecutor(
            source=source,
            target=target,
            store=store,
            evidence=evidence,
            advance_state=state,
            sleeper=lambda _: None,
            journal_factory=lambda lease, run_id: WindowJournal(store, lease, run_id),
        )

    runtime = RollingWindowRuntime(
        source_factory=source_factory,
        executor_factory=executor_factory,
        route_id="synthetic-pg-ch",
        target_id=template.target_id,
        schema_fingerprint=template.schema_fingerprint,
        store=store,
        generation_total=lambda plan, result: target.generation_total(plan, result.generation),
    )
    config = SimpleNamespace(
        options={
            "rolling_window": {
                "column": "at",
                "anchor": "data_interval_end",
                "lookback": "P2D",
                "chunk_interval": "P1D",
            },
            "interval": {"interval_end": template.end.isoformat()},
            "native_transfer": {
                "execution": {"chunking": {"mode": "bounded_window", "parallelism": 2, "checkpointing": "resumable"}}
            },
        }
    )
    original = ClickHouseConnector.execute_query

    def track_exchange(connector, query, params=None):
        if query.startswith("EXCHANGE"):
            exchanges.append(query)
        return original(connector, query, params)

    with patch.object(ClickHouseConnector, "execute_query", track_exchange):
        if fault_phase is not None:
            with pytest.raises(RuntimeError, match="synthetic"):
                runtime.run(config, owner="stable-invocation")
            assert Counter(read(target)) == Counter(expected)
            assert store.load("state:" + template.target_id) is None
        result = runtime.run(config, owner="stable-invocation")
        # A completed invocation is also idempotent and source-free.
        repeated = runtime.run(config, owner="stable-invocation")
    assert result.status == "success" and repeated.status == "success"
    assert result.inserted_rows == len(replacement) and result.final_rows == len(expected)
    assert len(source_opens) == 1 and len(exchanges) == 1
    assert Counter(read(target)) == Counter(expected)
