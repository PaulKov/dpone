"""Fail-closed boundaries of the bounded ClickHouse target."""

from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dpone.adapters.window_metadata_files import FileWindowMetadataStore
from dpone.contracts.bounded_window import (
    PublicationStatus,
    WindowContractError,
    WindowLease,
    WindowOutcomeUnknown,
    WindowPlan,
)
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
        metadata_store=FileWindowMetadataStore(),
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
        metadata_store=FileWindowMetadataStore(),
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


def generation_metadata(target, plan, **changes):
    metadata = {
        "version": 1,
        "identity": [plan.run_id, target.io.schema_fingerprint, target.io.physical_target],
        "count": 2,
        "evidence": {"target_total_rows": 2},
    }
    metadata.update(changes)
    generation = target.io.name(plan, "generation")
    target.io.metadata_store.save(target.io.path(generation), metadata)
    return generation


def test_admission_compatibility_exports_and_fingerprint():
    from dpone.runtime.sinks import clickhouse_window_admission as old
    from dpone.runtime.sinks import clickhouse_window_target as current

    for name in ("validate_configuration", "validate_target", "window_schema_fingerprint"):
        assert getattr(old, name) is getattr(current, name)
    assert window_schema_fingerprint(SCHEMA) == "4f252714da3d36fefdf3119b6b416c5adaaa553bd26fb3dc1156510a35f374a5"


@pytest.mark.parametrize("count", [0, 2])
def test_io_generation_readback_without_network(tmp_path, count):
    target, plan, _, connector = build(tmp_path)
    generation = generation_metadata(target, plan, count=count)
    assert target.io.generation_total(plan, generation) == count
    assert target.io.generation_evidence(plan, generation) == {
        "target_total_rows": 2,
        "publish_timing": {"status": "unavailable", "reason": "not_recorded_or_process_loss"},
    }
    assert connector.mock_calls == []
    target.io.http_runner_factory.assert_not_called()


@pytest.mark.parametrize("count", [True, False, -1, 1.0, "1", None])
def test_io_generation_rejects_invalid_count(tmp_path, count):
    target, plan, _, connector = build(tmp_path)
    generation = generation_metadata(target, plan, count=count)
    with pytest.raises(WindowContractError, match="^Verified generation count is invalid$"):
        target.io.generation_total(plan, generation)
    assert connector.mock_calls == []


@pytest.mark.parametrize("identity", [None, [], ["foreign", "schema", "d.t"]])
def test_io_generation_rejects_foreign_metadata(tmp_path, identity):
    target, plan, _, connector = build(tmp_path)
    generation = generation_metadata(target, plan, identity=identity)
    with pytest.raises(WindowContractError, match="^Verified generation metadata is unavailable$"):
        target.io.generation_total(plan, generation)
    assert connector.mock_calls == []


def test_io_generation_rejects_missing_metadata_and_wrong_name(tmp_path):
    target, plan, _, connector = build(tmp_path)
    with pytest.raises(WindowContractError, match="^Verified generation metadata is unavailable$"):
        target.io.generation_total(plan, target.io.name(plan, "generation"))
    with pytest.raises(WindowContractError, match="^Generation does not belong to this run$"):
        target.io.generation_total(plan, "foreign")
    assert connector.mock_calls == []


@pytest.mark.parametrize("evidence", [None, [], "invalid"])
def test_io_generation_rejects_invalid_evidence(tmp_path, evidence):
    target, plan, _, connector = build(tmp_path)
    generation = generation_metadata(target, plan, evidence=evidence)
    with pytest.raises(WindowContractError, match="^Verified generation aggregate evidence is unavailable$"):
        target.io.generation_evidence(plan, generation)
    assert connector.mock_calls == []


def test_generation_wrapper_identity_precedes_metadata(tmp_path):
    store = MagicMock(spec=FileWindowMetadataStore)
    target, plan, _, connector = build(tmp_path, metadata_store=store)
    for changed in (replace(plan, target_id="foreign"), replace(plan, schema_fingerprint="foreign")):
        for read in (target.generation_total, target.generation_evidence):
            with pytest.raises(WindowContractError, match="^Plan physical target or schema identity mismatch$"):
                read(changed, "foreign")
    assert store.mock_calls == []
    assert connector.mock_calls == []


@pytest.mark.parametrize("disappears", [False, True])
def test_generation_evidence_preserves_two_reads_and_second_read_timing(tmp_path, disappears):
    store = MagicMock(spec=FileWindowMetadataStore)
    target, plan, _, connector = build(tmp_path, metadata_store=store)
    generation = target.io.name(plan, "generation")
    store.load.side_effect = [
        {"identity": [plan.run_id, target.io.schema_fingerprint, target.io.physical_target], "count": 2},
        None if disappears else {"evidence": {"target_total_rows": 2}, "publish_timing": {"elapsed": 3}},
    ]
    if disappears:
        with pytest.raises(WindowContractError, match="^Verified generation aggregate evidence is unavailable$"):
            target.generation_evidence(plan, generation)
    else:
        assert target.generation_evidence(plan, generation) == {
            "target_total_rows": 2,
            "publish_timing": {"elapsed": 3},
        }
    assert store.load.call_count == 2
    assert all(call.args == (target.io.path(generation),) for call in store.load.call_args_list)
    store.save.assert_not_called()
    store.remove.assert_not_called()
    assert connector.mock_calls == []


def test_generation_evidence_dispatches_overridden_total_before_readback(tmp_path):
    class SpecializedTarget(ClickHouseWindowTarget):
        def generation_total(self, plan, generation):
            raise WindowContractError("specialized total validation")

    store = MagicMock(spec=FileWindowMetadataStore)
    target, plan, _, connector = build(tmp_path, metadata_store=store)
    specialized = SpecializedTarget.__new__(SpecializedTarget)
    specialized.__dict__.update(target.__dict__)
    with pytest.raises(WindowContractError, match="^specialized total validation$"):
        specialized.generation_evidence(plan, "foreign")
    assert store.mock_calls == []
    assert connector.mock_calls == []


def test_generation_evidence_continues_after_successful_total_override(tmp_path):
    calls = []

    class SpecializedTarget(ClickHouseWindowTarget):
        def generation_total(self, plan, generation):
            calls.append("total")
            return super().generation_total(plan, generation)

    class TrackingStore(FileWindowMetadataStore):
        def load(self, path):
            calls.append("load")
            return super().load(path)

    target, plan, _, connector = build(tmp_path, metadata_store=TrackingStore())
    generation = generation_metadata(target, plan)
    specialized = SpecializedTarget.__new__(SpecializedTarget)
    specialized.__dict__.update(target.__dict__)
    assert specialized.generation_evidence(plan, generation)["target_total_rows"] == 2
    assert calls == ["total", "load", "load"]
    assert connector.mock_calls == []


def test_metrics_validates_identifiers_before_query_and_uses_half_open_predicate(tmp_path):
    target, _, _, connector = build(tmp_path)
    connector.get_records.side_effect = None
    connector.get_records.return_value = [(0, None, None, 0, 0, 0)]
    connector.get_records_iterator.return_value = iter(())
    with pytest.raises(WindowContractError, match="simple SQL identifiers"):
        target.io.metrics(connector, "invalid-table", START, START + timedelta(days=1))
    connector.get_records.assert_not_called()
    result = target.io.metrics(connector, "t", START, START + timedelta(days=1), window_only=True)
    assert result["row_count"] == 0
    assert result["null_counts"] == {"at": 0, "value": 0}
    assert (
        "WHERE (`at` >= toDateTime64('2026-01-01 00:00:00.000000', 6, 'UTC')" in connector.get_records.call_args[0][0]
    )
    assert "AND `at` < toDateTime64('2026-01-02 00:00:00.000000', 6, 'UTC'))" in connector.get_records.call_args[0][0]


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
    assert FileWindowMetadataStore().load(path) is None
    for text in ["{", "[]", '{"version": 2}']:
        path.write_text(text)
        with pytest.raises(WindowContractError):
            FileWindowMetadataStore().load(path)
    FileWindowMetadataStore().save(path, {"version": 1, "digest": "abc"})
    assert FileWindowMetadataStore().load(path) == {"version": 1, "digest": "abc"}


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
    target, plan, lease, connector = build(tmp_path)
    generation = target.io.name(plan, "generation")
    FileWindowMetadataStore().save(
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
    assert target.inspect_publication(plan, generation, lease) == expected
    connector.execute_query.assert_not_called()


def test_unknown_publication_never_exchanges(tmp_path):
    target, plan, lease, connector = build(tmp_path)
    generation = target.io.name(plan, "generation")
    with pytest.raises(WindowOutcomeUnknown):
        target.publish(plan, generation, lease)
    connector.execute_query.assert_not_called()


def test_new_run_blocked_by_pending_publication(tmp_path):
    target, plan, _, connector = build(tmp_path)
    FileWindowMetadataStore().save(target.io.path(target.io.name_for_target()), {"version": 1, "run_id": "previous"})
    with pytest.raises(WindowContractError, match="unresolved"):
        target.validate(plan)
    connector.get_records.assert_not_called()


def test_discard_requires_successful_old_writer_fencing(tmp_path):
    from dpone.contracts.bounded_window import WindowLeaseLost

    authority = Guard()
    authority.fence_attempt = MagicMock(side_effect=WindowLeaseLost("stale"))
    target, plan, lease, connector = build(tmp_path, writer_guard=authority)
    with pytest.raises(WindowLeaseLost):
        target.discard_attempt(plan, plan.chunks[0], "one", lease)
    connector.execute_query.assert_not_called()


def test_stale_lease_cannot_start_a_writer(tmp_path):
    from dpone.contracts.bounded_window import WindowLeaseLost

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
    FileWindowMetadataStore().save(
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


def test_metadata_remove_retries_directory_sync_after_unlink(tmp_path, monkeypatch):
    """An absent file on retry does not prove the previous deletion was durable."""
    from dpone.adapters import window_metadata_files

    path = tmp_path / "receipt.json"
    path.write_text('{"version": 1}')
    sync = MagicMock(side_effect=[OSError("storage unavailable"), None])
    monkeypatch.setattr(window_metadata_files.os, "fsync", sync)
    store = FileWindowMetadataStore()
    with pytest.raises(OSError, match="storage unavailable"):
        store.remove(path)
    assert not path.exists()
    store.remove(path)
    assert sync.call_count == 2
    store.remove(tmp_path / "absent-parent" / "absent.json")


def test_publication_marker_cleanup_requires_live_non_reentrant_authority(tmp_path):
    from dpone.contracts.bounded_window import WindowLeaseLost

    class Authority(Guard):
        held = False
        expired = False

        @contextmanager
        def hold(self, lease):
            assert not self.held
            if self.expired:
                raise WindowLeaseLost("expired")
            self.held = True
            try:
                yield
            finally:
                self.held = False

    authority = Authority()
    metadata = MagicMock()
    target, plan, lease, connector = build(tmp_path, writer_guard=authority, metadata_store=metadata)
    generation = target.io.name(plan, "generation")
    record = {
        "version": 1,
        "identity": [plan.run_id, target.io.schema_fingerprint, "d.t"],
        "original_uuid": "old",
        "generation_uuid": "new",
    }
    metadata.load.side_effect = [record, {"run_id": plan.run_id}]
    metadata.remove.side_effect = lambda path: authority.held or pytest.fail("unfenced metadata mutation")
    connector.get_records.side_effect = [[("new",)], [("old",)]]
    # Already-published path must not recursively acquire the injected authority.
    target.publish(plan, generation, lease)
    metadata.remove.assert_called_once_with(target.io.path(target.io.name_for_target()))
    metadata.reset_mock()
    authority.expired = True
    with pytest.raises(WindowLeaseLost, match="expired"):
        target.inspect_publication(plan, generation, lease)
    metadata.load.assert_not_called()
    metadata.remove.assert_not_called()
    authority.expired = False
    metadata.load.side_effect = [record, {"run_id": "successor"}]
    connector.get_records.side_effect = [[("new",)], [("old",)]]
    assert target.inspect_publication(plan, generation, lease) == PublicationStatus.PUBLISHED
    metadata.remove.assert_not_called()


@pytest.mark.parametrize("version", [True, 1.0, "1", None, 0, 2])
def test_metadata_version_requires_exact_integer_one(tmp_path, version):
    import json

    path = tmp_path / "invalid-version.json"
    path.write_text(json.dumps({"version": version}))
    with pytest.raises(WindowContractError, match="Invalid window receipt"):
        FileWindowMetadataStore().load(path)
