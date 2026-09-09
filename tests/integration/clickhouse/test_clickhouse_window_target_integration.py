"""Synthetic local Docker certification of one-table atomic publication.

This fixture authority is valid ONLY for a disposable container controlled by
this test process. It creates a fresh random database, exposes no credentials to
other writers, runs no external jobs, and joins all its HTTP operations before
releasing its lock. It is not a production writer-exclusion implementation.
"""

from __future__ import annotations

import os
import threading
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from dpone.adapters.window_metadata_files import FileWindowMetadataStore
from dpone.contracts.bounded_window import (
    PublicationStatus,
    WindowContractError,
    WindowLease,
    WindowOutcomeUnknown,
    WindowPlan,
    WindowTransientError,
)
from dpone.runtime.connectors.clickhouse import ClickHouseConnector
from dpone.runtime.connectors.clickhouse_http_bulk import (
    ClickHouseHttpBulkRunner,
    ClickHouseHttpCredentials,
    ClickHouseHttpOptions,
)
from dpone.runtime.sinks.clickhouse_window_target import ClickHouseWindowTarget, window_schema_fingerprint

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if os.getenv("DPONE_RUN_INTEGRATION", "0").lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

SCHEMA = [("at", "Nullable(DateTime64(6, 'UTC'))"), ("value", "Nullable(String)"), ("id", "Int64")]
START = datetime(2026, 1, 1, tzinfo=UTC)


class DisposableDatabaseAuthority:
    """Exclusive test-owned database; no SQL writer exists outside this fixture."""

    def __init__(self, database):
        self.database = database
        self.lock = threading.RLock()

    def validate(self, target_id, physical_target):
        assert target_id == self.database and physical_target == f"{self.database}.target"

    def assert_lease(self, lease):
        assert lease.target_id == self.database and lease.fence > 0
        validator = getattr(self, "lease_validator", None)
        if validator is not None:
            validator(lease)

    @contextmanager
    def hold(self, lease):
        with self.lock:
            self.assert_lease(lease)
            yield

    def fence_attempt(self, lease, attempt_id):
        # Every fixture write is synchronous and the lock joins prior writers.
        self.assert_lease(lease)


@pytest.fixture
def window_target(clickhouse_connector, clickhouse_settings, tmp_path):
    settings = clickhouse_settings
    database = "window_it_" + uuid.uuid4().hex[:16]
    clickhouse_connector.execute_query(f"CREATE DATABASE `{database}` ENGINE=Atomic")
    clickhouse_connector.execute_query(
        f"CREATE TABLE `{database}`.target (at Nullable(DateTime64(6, 'UTC')), value Nullable(String), id Int64) ENGINE=MergeTree ORDER BY id"
    )

    def connector():
        return ClickHouseConnector(
            host=settings.host,
            port=settings.port,
            database=database,
            user=settings.user,
            password=settings.password,
            secure=settings.secure,
        )

    def runner():
        return ClickHouseHttpBulkRunner(
            ClickHouseHttpCredentials(
                settings.host,
                int(os.getenv("DPONE_IT_CH_HTTP_PORT", os.getenv("DPONE_CLICKHOUSE_HTTP_PORT", "8123"))),
                database,
                settings.user,
                settings.password,
                settings.secure,
            ),
            ClickHouseHttpOptions(input_format="RowBinary", timeout_seconds=30),
        )

    options = dict(
        schema=SCHEMA,
        database=database,
        table="target",
        window_column="at",
        target_id=database,
        connector_factory=connector,
        http_runner_factory=runner,
        work_dir=tmp_path,
        metadata_store=FileWindowMetadataStore(),
        max_encoded_bytes=256,
        writer_guard=DisposableDatabaseAuthority(database),
    )
    target = ClickHouseWindowTarget(**options)
    plan = WindowPlan(
        "route",
        database,
        START,
        START + timedelta(days=2),
        (START, START + timedelta(days=1), START + timedelta(days=2)),
        window_schema_fingerprint(SCHEMA),
        "synthetic-snapshot-1",
        "parameters",
        workers=2,
        window_column="at",
    )
    try:
        yield target, plan, WindowLease(database, "test", 1), options
    finally:
        clickhouse_connector.execute_query(f"DROP DATABASE `{database}` SYNC")


def read(target, name="target"):
    with target.io.connection() as connection:
        return connection.get_records(f"SELECT at, value, id FROM {target.io.qualified(name)}")


def seed(target):
    rows = [
        (None, "null-window", 10),
        (START - timedelta(days=1), "before", 11),
        (START, "old-window", 12),
        (START + timedelta(days=2), "end-boundary", 13),
    ]
    target.io.http_runner_factory().insert_stream(
        target.io.qualified("target"), [name for name, _ in SCHEMA], target.io.encoder().iter_batches(rows)
    )
    return rows


@pytest.mark.parametrize("phase", ["prepare", "prepared_retry", "publish"])
def test_schema_drift_preserves_target_before_publication(window_target, phase):
    target, plan, lease, _ = window_target
    initial = seed(target)
    target.validate(plan)
    receipts = [target.stage(plan, chunk, "empty", iter(()), lease) for chunk in plan.chunks]
    generation = target.io.name(plan, "generation")
    if phase != "prepare":
        assert target.prepare(plan, receipts, lease) == generation
    with target.io.guard.hold(lease), target.io.connection() as connection:
        original_uuid = target.io.uuid(connection, "target")
        connection.execute_query(
            f"ALTER TABLE {target.io.qualified('target')} ADD COLUMN extra String DEFAULT 'retain-me'"
        )
    with pytest.raises(WindowContractError, match="Physical schema"):
        if phase == "publish":
            target.publish(plan, generation, lease)
        else:
            target.prepare(plan, receipts, lease)
    with target.io.connection() as connection:
        assert target.io.uuid(connection, "target") == original_uuid
        assert connection.get_records(f"SELECT extra FROM {target.io.qualified('target')}") == [("retain-me",)] * len(
            initial
        )
        if phase == "prepare":
            assert target.io.uuid(connection, generation) is None
    assert Counter(read(target)) == Counter(initial)


def test_published_replay_survives_later_schema_change(window_target):
    target, plan, lease, _ = window_target
    seed(target)
    target.validate(plan)
    receipts = [target.stage(plan, chunk, "empty", iter(()), lease) for chunk in plan.chunks]
    generation = target.prepare(plan, receipts, lease)
    target.publish(plan, generation, lease)
    with target.io.guard.hold(lease), target.io.connection() as connection:
        published_uuid = target.io.uuid(connection, "target")
        connection.execute_query(
            f"ALTER TABLE {target.io.qualified('target')} ADD COLUMN extra String DEFAULT 'retain-me'"
        )
    with patch("dpone.runtime.sinks.clickhouse_window_target.validate_target", side_effect=AssertionError):
        assert target.prepare(plan, receipts, lease) == generation
        target.publish(plan, generation, lease)
        assert target.inspect_publication(plan, generation, lease) == PublicationStatus.PUBLISHED
    with target.io.connection() as connection:
        assert target.io.uuid(connection, "target") == published_uuid
        assert connection.get_records(f"SELECT extra FROM {target.io.qualified('target')}") == [("retain-me",)] * 3


@pytest.mark.parametrize("empty", [False, True])
def test_exact_multiset_window_atomicity_and_restart(window_target, empty):
    target, plan, lease, options = window_target
    initial = seed(target)
    target.validate(plan)
    first = [] if empty else [(START, "дубликат", 1), (START, "дубликат", 1), (START, None, 2)]
    second = [] if empty else [(START + timedelta(days=1), "", 3)]
    receipts = [
        target.stage(plan, chunk, f"attempt-{index}", iter(rows), lease)
        for index, (chunk, rows) in enumerate(zip(plan.chunks, [first, second], strict=True))
    ]
    restarted = ClickHouseWindowTarget(**options)
    assert restarted.inspect_attempt(plan, plan.chunks[0], "attempt-0", lease) == receipts[0]
    generation = restarted.prepare(plan, receipts, lease)
    assert Counter(read(target)) == Counter(initial)
    assert restarted.prepare(plan, receipts, lease) == generation
    restarted.publish(plan, generation, lease)
    expected = [initial[0], initial[1], initial[3], *first, *second]
    assert Counter(read(target)) == Counter(expected)
    assert Counter(read(target, generation)) == Counter(initial)
    # Replaying publication must never exchange back to the old generation.
    restarted.publish(plan, generation, lease)
    assert Counter(read(target)) == Counter(expected)
    assert restarted.inspect_publication(plan, generation, lease) == PublicationStatus.PUBLISHED


def test_lost_exchange_reply_is_reconciled_without_second_exchange(window_target):
    target, plan, lease, _ = window_target
    initial = seed(target)
    target.validate(plan)
    receipts = [
        target.stage(plan, chunk, f"attempt-{index}", iter([]), lease) for index, chunk in enumerate(plan.chunks)
    ]
    generation = target.prepare(plan, receipts, lease)
    original = ClickHouseConnector.execute_query
    calls = []

    def lost_reply(connector, query, params=None):
        result = original(connector, query, params)
        if query.startswith("EXCHANGE"):
            calls.append(query)
            raise ConnectionResetError("synthetic lost response after server commit")
        return result

    with patch.object(ClickHouseConnector, "execute_query", lost_reply):
        with pytest.raises(WindowOutcomeUnknown):
            target.publish(plan, generation, lease)
        target.publish(plan, generation, lease)
    assert len(calls) == 1
    assert Counter(read(target)) == Counter([initial[0], initial[1], initial[3]])
    assert Counter(read(target, generation)) == Counter(initial)


@pytest.mark.parametrize("kind", ["oversize", "outside", "null"])
def test_bad_stream_leaves_target_untouched_and_can_be_discarded(window_target, kind):
    target, plan, lease, _ = window_target
    initial = seed(target)
    target.validate(plan)
    bad = {"oversize": (START, "x" * 1000, 1), "outside": (plan.end, "x", 1), "null": (None, "x", 1)}[kind]
    with pytest.raises((ValueError, WindowContractError)):
        target.stage(plan, plan.chunks[0], "bad", iter([bad]), lease)
    assert target.inspect_attempt(plan, plan.chunks[0], "bad", lease) is None
    target.discard_attempt(plan, plan.chunks[0], "bad", lease)
    target.discard_attempt(plan, plan.chunks[0], "bad", lease)
    assert Counter(read(target)) == Counter(initial)


def test_receipt_rejects_staging_tampering(window_target):
    target, plan, lease, _ = window_target
    target.validate(plan)
    receipt = target.stage(plan, plan.chunks[0], "one", iter([(START, "x", 1)]), lease)
    name = target.staging.name(plan, plan.chunks[0], receipt.attempt_id)
    target.io.http_runner_factory().insert_stream(
        target.io.qualified(name), [name for name, _ in SCHEMA], target.io.encoder().iter_batches([(START, "x", 1)])
    )
    with pytest.raises(WindowContractError, match="parity"):
        target.inspect_attempt(plan, plan.chunks[0], "one", lease)


def test_lost_stage_reply_requires_fenced_discard_before_reinsert(window_target):
    target, plan, lease, _ = window_target
    initial = seed(target)
    target.validate(plan)
    original = ClickHouseHttpBulkRunner.insert_stream

    def lost_reply(runner, table, columns, chunks):
        original(runner, table, columns, chunks)
        raise ConnectionResetError("synthetic stage response loss")

    with patch.object(ClickHouseHttpBulkRunner, "insert_stream", lost_reply):
        with pytest.raises(WindowTransientError):
            target.stage(plan, plan.chunks[0], "one", iter([(START, "x", 1)]), lease)
    assert target.inspect_attempt(plan, plan.chunks[0], "one", lease) is None
    with pytest.raises(WindowContractError, match="fence and discard"):
        target.stage(plan, plan.chunks[0], "one", iter([(START, "x", 1)]), lease)
    target.discard_attempt(plan, plan.chunks[0], "one", lease)
    assert target.stage(plan, plan.chunks[0], "one", iter([(START, "x", 1)]), lease).row_count == 1
    assert Counter(read(target)) == Counter(initial)


def test_interrupted_generation_build_restarts_without_duplicate_rows(window_target):
    target, plan, lease, _ = window_target
    initial = seed(target)
    target.validate(plan)
    receipts = [
        target.stage(plan, chunk, f"attempt-{index}", iter([]), lease) for index, chunk in enumerate(plan.chunks)
    ]
    original = ClickHouseConnector.execute_query

    def lost_reply(connector, query, params=None):
        result = original(connector, query, params)
        if query.startswith("INSERT INTO"):
            raise ConnectionResetError("synthetic prepare response loss")
        return result

    with patch.object(ClickHouseConnector, "execute_query", lost_reply):
        with pytest.raises(ConnectionResetError):
            target.prepare(plan, receipts, lease)
    generation = target.prepare(plan, receipts, lease)
    assert target.generation_total(plan, generation) == 3
    target.publish(plan, generation, lease)
    assert Counter(read(target)) == Counter([initial[0], initial[1], initial[3]])


def test_concurrent_chunk_connections_and_receipt_reuse(window_target):
    from concurrent.futures import ThreadPoolExecutor

    target, plan, lease, options = window_target
    target.validate(plan)
    rows = [[(chunk.start, "duplicate", index)] * 3 for index, chunk in enumerate(plan.chunks)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(target.stage, plan, chunk, str(index), iter(values), lease)
            for index, (chunk, values) in enumerate(zip(plan.chunks, rows, strict=True))
        ]
        receipts = [future.result() for future in futures]
    restarted = ClickHouseWindowTarget(**options)
    for index, chunk in enumerate(plan.chunks):
        # A verified attempt must return its receipt without consuming new rows.
        def forbidden():
            raise AssertionError("verified staging must not reread source")
            yield

        assert restarted.stage(plan, chunk, str(index), forbidden(), lease) == receipts[index]
    generation = restarted.prepare(plan, receipts, lease)
    restarted.publish(plan, generation, lease)
    assert Counter(read(target)) == Counter([row for chunk_rows in rows for row in chunk_rows])


def test_prepared_generation_receipts_cannot_be_replaced(window_target):
    from dataclasses import replace

    target, plan, lease, _ = window_target
    target.validate(plan)
    receipts = [
        target.stage(plan, chunk, f"attempt-{index}", iter([]), lease) for index, chunk in enumerate(plan.chunks)
    ]
    target.prepare(plan, receipts, lease)
    changed = [replace(receipts[0], attempt_id="different"), receipts[1]]
    with pytest.raises(WindowContractError, match="receipt identity"):
        target.prepare(plan, changed, lease)


@pytest.mark.parametrize("at", [None, START - timedelta(days=1), START, START + timedelta(days=2)])
def test_intervening_target_write_prevents_stale_generation_publication(window_target, at):
    target, plan, lease, _ = window_target
    initial = seed(target)
    target.validate(plan)
    receipts = [
        target.stage(plan, chunk, f"attempt-{index}", iter([]), lease) for index, chunk in enumerate(plan.chunks)
    ]
    generation = target.prepare(plan, receipts, lease)
    added = (at, "intervening-writer", 99)
    # Another correctly guarded writer can run between prepare and publish.
    with target.io.guard.hold(lease):
        target.io.http_runner_factory().insert_stream(
            target.io.qualified("target"), [name for name, _ in SCHEMA], target.io.encoder().iter_batches([added])
        )
    with pytest.raises(WindowContractError, match="Target changed"):
        target.publish(plan, generation, lease)
    assert Counter(read(target)) == Counter([*initial, added])
    assert target.inspect_publication(plan, generation, lease) == PublicationStatus.ABSENT
    # An idempotent prepare may return the stale generation, but must never
    # destructively replace it; publication remains refused on every retry.
    assert target.prepare(plan, receipts, lease) == generation
    with pytest.raises(WindowContractError, match="Target changed"):
        target.publish(plan, generation, lease)
    assert Counter(read(target)) == Counter([*initial, added])


def test_row_policy_is_rejected_before_preserving_filtered_rows(window_target):
    target, plan, _, _ = window_target
    policy = "policy_" + target.io.database
    with target.io.connection() as connector:
        connector.execute_query(
            f"CREATE ROW POLICY `{policy}` ON {target.io.qualified('target')} USING id != 10 TO default"
        )
    try:
        with pytest.raises(WindowContractError, match="Row policies"):
            target.validate(plan)
    finally:
        with target.io.connection() as connector:
            connector.execute_query(f"DROP ROW POLICY `{policy}` ON {target.io.qualified('target')}")


def test_materialized_view_dependency_is_rejected(window_target):
    target, plan, _, _ = window_target
    with target.io.connection() as connector:
        connector.execute_query(
            f"CREATE MATERIALIZED VIEW {target.io.qualified('dependent_view')} ENGINE=MergeTree ORDER BY id AS SELECT * FROM {target.io.qualified('target')}"
        )
    with pytest.raises(WindowContractError, match="dependencies"):
        target.validate(plan)


def test_generated_aggregate_evidence_contains_window_dq_and_measured_phases(window_target):
    target, plan, lease, _ = window_target
    target.validate(plan)
    rows = [(START, "same", 1), (START, "same", 1), (START, None, 2)]
    receipts = [
        target.stage(plan, chunk, str(index), iter(rows if index == 0 else []), lease)
        for index, chunk in enumerate(plan.chunks)
    ]
    generation = target.prepare(plan, receipts, lease)
    target.publish(plan, generation, lease)
    evidence = target.generation_evidence(plan, generation)
    window = evidence["target_window"]
    assert evidence["source_window_rows"] == evidence["target_total_rows"] == 3
    assert evidence["window_column"] == "at" and evidence["schema_fingerprint"] == plan.schema_fingerprint
    assert window["utc_day_counts"] == {"2026-01-01": 3}
    assert window["min_window_utc"] == window["max_window_utc"] == START.isoformat()
    assert window["outside_window"] == 0 and window["null_counts"] == {"at": 0, "value": 1, "id": 0}
    assert evidence["prepare_seconds"] >= 0 and evidence["encoded_bytes"] > 0
    assert all(chunk["stage_seconds"] >= 0 for chunk in evidence["chunks"])
    assert evidence["chunks"][1]["target"]["utc_day_counts"] == {}
    assert evidence["publish_timing"]["publish_attempt_seconds"] >= 0
    assert evidence["source_metric_parity"] == "inferred_from_verified_probabilistic_typed_multiset"


def test_day_metrics_preserve_pre_epoch_date(window_target):
    from dataclasses import replace

    target, template, lease, _ = window_target
    start = START.replace(year=1969, month=12, day=31)
    plan = replace(
        template,
        start=start,
        end=start + timedelta(days=2),
        boundaries=(start, start + timedelta(days=1), start + timedelta(days=2)),
    )
    target.validate(plan)
    rows = [(start + timedelta(days=1, microseconds=-1), "pre-epoch", 1)]
    receipts = [
        target.stage(plan, chunk, str(index), iter(rows if index == 0 else []), lease)
        for index, chunk in enumerate(plan.chunks)
    ]
    generation = target.prepare(plan, receipts, lease)
    target.publish(plan, generation, lease)
    assert target.generation_evidence(plan, generation)["target_window"]["utc_day_counts"] == {"1969-12-31": 1}
