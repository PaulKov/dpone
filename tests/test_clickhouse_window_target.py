"""Fail-closed boundaries of the bounded ClickHouse target."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dpone.adapters.window_metadata_files import load_metadata, save_metadata
from dpone.contracts.bounded_window import PublicationStatus, WindowLease, WindowPlan
from dpone.contracts.process_errors import WindowContractError, WindowOutcomeUnknown
from dpone.runtime.sinks.clickhouse_window_evidence import TypedMultiset
from dpone.runtime.sinks.clickhouse_window_staging import encoded_rows
from dpone.runtime.sinks.clickhouse_window_target import ClickHouseWindowTarget, window_schema_fingerprint


def test_missing_exclusion_is_rejected_before_connector_creation(tmp_path: Path) -> None:
    def forbidden():
        pytest.fail("preflight must reject missing authority before network I/O")

    target = ClickHouseWindowTarget(
        schema=[("at", "DateTime64(6, 'UTC')")],
        database="d",
        table="t",
        window_column="at",
        target_id="endpoint-d-t",
        connector_factory=forbidden,
        http_runner_factory=forbidden,
        work_dir=tmp_path,
        max_encoded_bytes=1024,
        writer_guard=None,
    )
    with pytest.raises(WindowContractError, match="exclusive"):
        target.validate(None)


SCHEMA = [("at", "Nullable(DateTime64(6, 'UTC'))"), ("value", "Nullable(String)")]
START = datetime(2026, 1, 1, tzinfo=UTC)


class Guard:
    """Unit-only authority; mock backend cannot admit external SQL writers."""

    def validate(self, target_id, physical_target):
        assert (target_id, physical_target) == ("endpoint-d-t", "d.t")

    def assert_lease(self, lease):
        assert lease.target_id == "endpoint-d-t"

    @contextmanager
    def hold(self, lease):
        self.assert_lease(lease)
        yield

    def fence_attempt(self, lease, attempt_id):
        self.assert_lease(lease)


def build(tmp_path, **kwargs):
    connector = MagicMock()
    connector.get_records.side_effect = [
        [("Atomic",)],
        [("MergeTree", "CREATE TABLE d.t ENGINE=MergeTree ORDER BY tuple()", [], [])],
        [(name, dtype, "") for name, dtype in SCHEMA],
        [(0,)],
        [(0,)],
    ]
    options = dict(
        schema=SCHEMA,
        database="d",
        table="t",
        window_column="at",
        target_id="endpoint-d-t",
        connector_factory=lambda: connector,
        http_runner_factory=MagicMock(),
        work_dir=tmp_path,
        max_encoded_bytes=128,
        writer_guard=Guard(),
    )
    options.update(kwargs)
    target = ClickHouseWindowTarget(**options)
    plan = WindowPlan(
        "route",
        "endpoint-d-t",
        START,
        START + timedelta(days=1),
        (START, START + timedelta(days=1)),
        window_schema_fingerprint(SCHEMA),
        "snapshot",
        "parameters",
        window_column="at",
    )
    return target, plan, WindowLease(plan.target_id, "worker", 1), connector


def test_valid_preflight_closes_connection(tmp_path):
    target, plan, _, connector = build(tmp_path)
    target.validate(plan)
    connector.close.assert_called_once()
    connector.execute_query.assert_not_called()


@pytest.mark.parametrize(
    "options",
    [
        {"database": "d;DROP"},
        {"table": "t`"},
        {"window_column": "unknown"},
        {"schema": [("at", "DateTime")]},
        {"schema": [("at", "DateTime64(9, 'UTC')")]},
        {"schema": [("at", "DateTime64(6, 'Europe/Moscow')")]},
        {"schema": []},
        {"schema": [("at", "DateTime64(6, 'UTC')")] * 2},
        {"max_encoded_bytes": 0},
        {"max_encoded_bytes": True},
        {"max_encoded_bytes": -1},
        {"target_id": ""},
    ],
)
def test_invalid_configuration(tmp_path, options):
    with pytest.raises(WindowContractError):
        build(tmp_path, **options)


@pytest.mark.parametrize(
    "metadata,match",
    [
        ([[("Ordinary",)]], "Atomic"),
        ([[("Atomic",)], [("ReplicatedMergeTree", "", [], [])]], "MergeTree"),
        ([[("Atomic",)], [("Distributed", "", [], [])]], "MergeTree"),
        ([[("Atomic",)], [("MergeTree", "", ["d"], ["mv"])]], "dependencies"),
        ([[("Atomic",)], [("MergeTree", "TTL at + INTERVAL 1 DAY", [], [])]], "TTL"),
        ([[("Atomic",)], [("MergeTree", "PROJECTION p", [], [])]], "projections"),
        ([[("Atomic",)], [("MergeTree", "", [], [])], [("at", SCHEMA[0][1], "DEFAULT")]], "schema"),
        ([[("Atomic",)], [("MergeTree", "", [], [])], [(n, t, "") for n, t in SCHEMA], [(1,)]], "mutations"),
    ],
)
def test_unsupported_topology_fails_without_mutation(tmp_path, metadata, match):
    target, plan, _, connector = build(tmp_path)
    connector.get_records.side_effect = metadata
    with pytest.raises(WindowContractError, match=match):
        target.validate(plan)
    connector.execute_query.assert_not_called()
    connector.close.assert_called_once()


def test_schema_and_target_identity_checked(tmp_path):
    target, plan, lease, connector = build(tmp_path)
    from dataclasses import replace

    for changed in (replace(plan, target_id="other"), replace(plan, schema_fingerprint="other")):
        with pytest.raises(WindowContractError, match="identity"):
            target.validate(changed)
    with pytest.raises(WindowContractError, match="Lease"):
        target.discard_attempt(plan, plan.chunks[0], "one", replace(lease, target_id="other"))
    connector.get_records.assert_not_called()


def test_multiset_order_duplicates_nulls_and_bounds(tmp_path):
    target, _, _, _ = build(tmp_path)
    rows = [(START, "a"), (START, None), (START, "a")]

    def digest(values):
        evidence = TypedMultiset(2)
        list(encoded_rows(values, target.io.encoder().iter_batches, evidence, 0, (START, START + timedelta(days=1))))
        return evidence.digest()

    assert digest(rows) == digest(rows[::-1])
    assert digest(rows) != digest(rows[:-1])
    assert digest(rows) != digest([(START, "a")] * 3)
    for bad in [None, START - timedelta(microseconds=1), START + timedelta(days=1)]:
        with pytest.raises(WindowContractError):
            digest([(bad, "a")])


def test_oversize_row_never_emitted(tmp_path):
    target, _, _, _ = build(tmp_path)
    evidence = TypedMultiset(2)
    with pytest.raises(WindowContractError, match="byte bounds"):
        list(encoded_rows([(START, "a" * 200)], target.io.encoder().iter_batches, evidence, 0, None))
    assert evidence.count == 0


def test_metadata_corruption_and_version_rejected(tmp_path):
    path = tmp_path / "receipt.json"
    assert load_metadata(path) is None
    for text in ["{", "[]", '{"version": 2}']:
        path.write_text(text)
        with pytest.raises(WindowContractError):
            load_metadata(path)
    save_metadata(path, {"version": 1, "digest": "abc"})
    assert load_metadata(path) == {"version": 1, "digest": "abc"}


@pytest.mark.parametrize(
    "pair,expected",
    [
        (("old", "new"), PublicationStatus.ABSENT),
        (("new", "old"), PublicationStatus.PUBLISHED),
        ((None, "new"), PublicationStatus.UNKNOWN),
        (("old", None), PublicationStatus.UNKNOWN),
        (("third", "new"), PublicationStatus.UNKNOWN),
        (("new", "new"), PublicationStatus.UNKNOWN),
    ],
)
def test_publication_uuid_pair_classification(tmp_path, pair, expected):
    target, plan, _, connector = build(tmp_path)
    generation = target.io.name(plan, "generation")
    save_metadata(
        target.io.path(generation),
        {
            "version": 1,
            "identity": [plan.run_id, target.io.schema_fingerprint, "d.t"],
            "original_uuid": "old",
            "generation_uuid": "new",
            "digest": "x",
        },
    )
    connector.get_records.side_effect = [[(value,)] if value is not None else [] for value in pair]
    assert target.inspect_publication(plan, generation) == expected
    connector.execute_query.assert_not_called()


def test_unknown_publication_never_exchanges(tmp_path):
    target, plan, lease, connector = build(tmp_path)
    generation = target.io.name(plan, "generation")
    with pytest.raises(WindowOutcomeUnknown):
        target.publish(plan, generation, lease)
    connector.execute_query.assert_not_called()


def test_new_run_blocked_by_pending_publication(tmp_path):
    target, plan, _, connector = build(tmp_path)
    save_metadata(target.io.path(target.io.name_for_target()), {"version": 1, "run_id": "previous"})
    with pytest.raises(WindowContractError, match="unresolved"):
        target.validate(plan)
    connector.get_records.assert_not_called()


def test_discard_requires_successful_old_writer_fencing(tmp_path):
    from dpone.contracts.process_errors import WindowLeaseLost

    authority = Guard()
    authority.fence_attempt = MagicMock(side_effect=WindowLeaseLost("stale"))
    target, plan, lease, connector = build(tmp_path, writer_guard=authority)
    with pytest.raises(WindowLeaseLost):
        target.discard_attempt(plan, plan.chunks[0], "one", lease)
    connector.execute_query.assert_not_called()


def test_stale_lease_cannot_start_a_writer(tmp_path):
    from dpone.contracts.process_errors import WindowLeaseLost

    authority = Guard()
    authority.assert_lease = MagicMock(side_effect=WindowLeaseLost("stale"))
    target, plan, lease, connector = build(tmp_path, writer_guard=authority)
    with pytest.raises(WindowLeaseLost):
        target.stage(plan, plan.chunks[0], "one", iter([]), lease)
    connector.execute_query.assert_not_called()


def test_attempt_receipt_cannot_survive_uuid_replacement(tmp_path):
    target, plan, lease, connector = build(tmp_path)
    chunk = plan.chunks[0]
    name = target.staging.name(plan, chunk, "one")
    save_metadata(
        target.io.path(name),
        {
            "version": 1,
            "identity": [plan.run_id, chunk.chunk_id, "one", target.io.schema_fingerprint, "d.t"],
            "uuid": "old",
            "count": 0,
            "digest": "unused",
        },
    )
    connector.get_records.side_effect = [[("replacement",)]]
    with pytest.raises(WindowContractError, match="UUID"):
        target.inspect_attempt(plan, chunk, "one", lease)
    connector.get_records_iterator.assert_not_called()


def test_source_closes_when_staging_rejects_unverified_attempt(tmp_path):
    target, plan, lease, connector = build(tmp_path)
    connector.get_records.side_effect = [[("existing",)]]
    source = MagicMock()
    with pytest.raises(WindowContractError, match="Unverified"):
        target.stage(plan, plan.chunks[0], "one", source, lease)
    source.close.assert_called_once()


@pytest.mark.parametrize("row", [(), (START,), (START, "x", "extra")])
def test_row_shape_rejected_before_encoding(tmp_path, row):
    target, _, _, _ = build(tmp_path)
    with pytest.raises(WindowContractError, match="shape"):
        list(encoded_rows([row], target.io.encoder().iter_batches, TypedMultiset(2), 0, None))


def test_row_policy_cannot_hide_preserved_data(tmp_path):
    target, plan, _, connector = build(tmp_path)
    connector.get_records.side_effect = [
        [("Atomic",)],
        [("MergeTree", "", [], [])],
        [(name, dtype, "") for name, dtype in SCHEMA],
        [(0,)],
        [(1,)],
    ]
    with pytest.raises(WindowContractError, match="Row policies"):
        target.validate(plan)
    connector.execute_query.assert_not_called()


def test_two_timestamp_columns_cannot_change_window_meaning(tmp_path):
    from dataclasses import replace

    schema = [("at", "DateTime64(6, 'UTC')"), ("other_at", "DateTime64(6, 'UTC')")]
    target, plan, _, connector = build(tmp_path, schema=schema)
    changed = replace(plan, schema_fingerprint=target.io.schema_fingerprint, window_column="other_at")
    with pytest.raises(WindowContractError, match="identity"):
        target.validate(changed)
    connector.get_records.assert_not_called()


@pytest.mark.parametrize(
    "rows,expected",
    [
        ([b"ab", b"cd", b"e", b"fg", b"hi"], [b"abcd", b"efg", b"hi"]),
        ([b"abcd", b"e"], [b"abcd", b"e"]),
        ([], []),
        ([b"", b"a", b""], [b"a"]),
    ],
)
def test_http_coalescing_preserves_exact_bytes_and_bound(rows, expected):
    from dpone.runtime.sinks.clickhouse_window_staging import coalesce_encoded_rows

    chunks = list(coalesce_encoded_rows(iter(rows), 4))
    assert chunks == expected
    assert b"".join(chunks) == b"".join(rows)
    assert all(0 < len(chunk) <= 4 for chunk in chunks)


@pytest.mark.parametrize("limit", [0, -1, True])
def test_http_coalescing_rejects_invalid_bound(limit):
    from dpone.runtime.sinks.clickhouse_window_staging import coalesce_encoded_rows

    with pytest.raises(WindowContractError, match="positive"):
        list(coalesce_encoded_rows(iter([]), limit))


def test_http_coalescing_rejects_oversized_row():
    from dpone.runtime.sinks.clickhouse_window_staging import coalesce_encoded_rows

    with pytest.raises(WindowContractError, match="exceeds"):
        list(coalesce_encoded_rows(iter([b"abcde"]), 4))


def test_http_coalescing_backpressure_and_cancellation():
    from dpone.runtime.sinks.clickhouse_window_staging import coalesce_encoded_rows

    pulls, closed = [], []

    def source():
        try:
            for index in range(100):
                pulls.append(index)
                yield b"abc"
        finally:
            closed.append(True)

    chunks = coalesce_encoded_rows(source(), 8)
    assert pulls == []
    assert next(chunks) == b"abcabc"
    assert pulls == [0, 1, 2]  # One complete next row beyond the buffered batch.
    chunks.close()
    assert closed == [True]


def test_http_coalescing_propagates_source_failure():
    from dpone.runtime.sinks.clickhouse_window_staging import coalesce_encoded_rows

    def source():
        yield b"abcd"
        raise RuntimeError("source failure")

    chunks = coalesce_encoded_rows(source(), 4)
    assert next(chunks) == b"abcd"
    with pytest.raises(RuntimeError, match="source failure"):
        next(chunks)


@pytest.mark.parametrize(
    "format_name,settings,database",
    [
        ("CSV", {}, "db"),
        ("RowBinary", {"async_insert": 1}, "db"),
        ("RowBinary", {}, "other"),
    ],
)
def test_http_window_admission_rejects_before_network(format_name, settings, database):
    from dpone.runtime.connectors.clickhouse_http_bulk import (
        ClickHouseHttpBulkRunner,
        ClickHouseHttpCredentials,
        ClickHouseHttpOptions,
    )

    runner = ClickHouseHttpBulkRunner(
        ClickHouseHttpCredentials("localhost", 8123, database, "default"),
        ClickHouseHttpOptions(input_format=format_name, settings=settings),
        connection_factory=lambda *args, **kwargs: pytest.fail("Network before admission"),
    )
    with pytest.raises(WindowContractError):
        runner.insert_window_stream("db", "db.staging", ["id"], iter([b"1"]), "attempt")


def test_http_window_admission_binds_query_id(monkeypatch):
    from dpone.runtime.connectors.clickhouse_http_bulk import (
        ClickHouseHttpBulkRunner,
        ClickHouseHttpCredentials,
        ClickHouseHttpOptions,
    )

    runner = ClickHouseHttpBulkRunner(
        ClickHouseHttpCredentials("localhost", 8123, "db", "default"),
        ClickHouseHttpOptions(input_format="RowBinary"),
    )
    monkeypatch.setattr(runner, "insert_stream", lambda *args: runner.options.query_id)
    assert runner.insert_window_stream("db", "db.staging", ["id"], iter([]), "attempt") == "attempt"
