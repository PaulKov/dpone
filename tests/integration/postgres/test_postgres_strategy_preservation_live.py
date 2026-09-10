"""Real PostgreSQL proof of strategy choice, rollback and target preservation."""

from __future__ import annotations

import os
from threading import Timer

import psycopg
import pytest

from dpone.config import LoadStrategy
from dpone.runtime.artifacts import FileExportArtifact, InMemoryRowsArtifact
from dpone.runtime.etl.result_metrics import populate_success_result
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.strategies.postgres.target_table_manager import PostgresTargetTableManager
from tests.integration.postgres.strategy_preservation_support import BUSINESS_SCHEMA, PreservationLab

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_postgres]


@pytest.fixture
def lab(request):
    if os.getenv("DPONE_RUN_INTEGRATION") != "1":
        pytest.skip("Set DPONE_RUN_INTEGRATION=1 with an approved PostgreSQL environment")
    instance = PreservationLab(request.getfixturevalue("postgres_settings"))
    try:
        yield instance
    finally:
        instance.write_evidence(request.node.name)
        instance.close()


@pytest.mark.parametrize("mode", [None, "truncate_insert"])
@pytest.mark.parametrize("technical", ["required", "forbidden"])
def test_refresh_changed_rows_and_replay_preserve_catalog(lab, mode, technical):
    lab.constrained_target(technical=technical)
    baseline = lab.snapshot("before")
    cfg = lab.config(overwrite_type=mode, technical=technical)
    first = [(1, "first", "2026-01-01"), (2, "second", "2026-01-02")]
    changed = [(3, "changed", "2026-01-03")]
    for index, rows in enumerate([first, changed, changed]):
        lab.source(rows)
        result = lab.load(cfg, query=f"SELECT id,name,event_day FROM {lab.schema}.source WHERE id > %s", params=(0,))
        assert result.inserted_rows == result.total_rows == result.staging_rows == len(rows)
        after = lab.snapshot(f"refresh-{index}")
        assert after["rows"] == rows
        assert after["metadata"] == baseline["metadata"]
        assert lab.rows("target_view") == rows
        lab.assert_transaction(committed=True)
        lab.assert_no_staging()
        if technical == "required":
            assert lab.records(f"SELECT bool_and(__dpone__loaded_at IS NOT NULL) AS loaded FROM {lab.schema}.target")[
                0
            ]["loaded"]
    assert lab.records(f"SELECT count(*) AS n FROM {lab.schema}.audit")[0]["n"] == 5


@pytest.mark.parametrize(
    "rows,state",
    [
        ([(1, None, "2026-01-01")], "23502"),
        ([(1, "one", "2026-01-01"), (1, "two", "2026-01-01")], "23505"),
        ([(1, "invalid", "2026-01-01")], "23514"),
    ],
)
def test_real_insert_error_after_truncate_rolls_back_and_retry_succeeds(lab, rows, state):
    lab.constrained_target()
    before = lab.snapshot("before")
    lab.source(rows)
    with pytest.raises(psycopg.Error) as error:
        lab.load()
    assert error.value.sqlstate == state
    events = lab.last_events()
    truncate = next(i for i, e in enumerate(events) if e.get("sql", "").startswith("TRUNCATE TABLE"))
    failed = next(i for i, e in enumerate(events) if e.get("sqlstate") == state)
    assert truncate < failed
    assert lab.snapshot("rollback") == before
    lab.assert_transaction(committed=False)
    lab.assert_no_staging()
    lab.source([(2, "fixed", "2026-01-02")])
    lab.load()
    assert lab.rows() == [(2, "fixed", "2026-01-02")]


def test_source_query_failure_precedes_target_mutation(lab):
    lab.constrained_target()
    before = lab.snapshot("before")
    with pytest.raises(psycopg.errors.DivisionByZero):
        lab.load(query="SELECT 1/0 AS id, 'failure'::text AS name, CURRENT_DATE AS event_day")
    assert not any(e.get("sql", "").startswith("TRUNCATE") for e in lab.last_events())
    assert lab.snapshot("query-failure") == before
    lab.assert_transaction(committed=False)
    lab.assert_no_staging()
    lab.source([(2, "retry", "2026-01-02")])
    lab.load()
    assert lab.rows() == [(2, "retry", "2026-01-02")]


def test_empty_source_preserves_existing_object(lab):
    lab.constrained_target()
    before = lab.snapshot("before")
    result = lab.load()
    assert result.inserted_rows == result.staging_rows == 0
    after = lab.snapshot("empty")
    assert after["rows"] == []
    assert after["metadata"] == before["metadata"]


def test_absent_target_does_not_claim_source_constraint_copy(lab):
    lab.execute(f"ALTER TABLE {lab.schema}.source ADD PRIMARY KEY(id), ALTER name SET NOT NULL")
    lab.source([(1, "new", "2026-01-01")])
    lab.load()
    after = lab.snapshot("created")
    assert after["rows"] == [(1, "new", "2026-01-01")]
    assert after["metadata"]["constraints"] == []
    assert not any(c["attnotnull"] for c in after["metadata"]["columns"])


def test_incoming_fk_fails_closed(lab):
    lab.constrained_target()
    lab.execute(f"CREATE TABLE {lab.schema}.incoming (id integer REFERENCES {lab.schema}.target(id))")
    lab.execute(f"INSERT INTO {lab.schema}.incoming VALUES (90)")
    before = lab.snapshot("before")
    lab.source([(1, "new", "2026-01-01")])
    with pytest.raises(psycopg.errors.FeatureNotSupported):
        lab.load()
    assert lab.snapshot("fk-rejected") == before
    assert lab.records(f"SELECT id FROM {lab.schema}.incoming") == [{"id": 90}]
    assert all("CASCADE" not in e.get("sql", "") for e in lab.last_events())
    lab.assert_transaction(committed=False)


@pytest.mark.parametrize("cross_schema", [False, True])
@pytest.mark.parametrize("existing", [False, True])
def test_explicit_exchange_replaces_object_with_one_commit(lab, cross_schema, existing):
    before = None
    if existing:
        lab.constrained_target(view=False)
        before = lab.snapshot("before")
    lab.source([(1, "new", "2026-01-01")])
    result = lab.load(lab.config(overwrite_type="exchange", cross_schema=cross_schema, technical="required"))
    assert result.staging_rows == result.inserted_rows == 1
    after = lab.snapshot("exchanged")
    assert after["rows"] == [(1, "new", "2026-01-01")]
    if before:
        assert before["metadata"]["relation"][0]["oid"] != after["metadata"]["relation"][0]["oid"]
    assert {c["attname"] for c in after["metadata"]["columns"]} >= {"__dpone__loaded_at", "__dpone__deleted_at"}
    lab.assert_transaction(committed=True)
    lab.assert_no_staging()


def test_exchange_dependency_failure_restores_target_and_view(lab):
    lab.constrained_target()
    before = lab.snapshot("before")
    lab.source([(1, "new", "2026-01-01")])
    with pytest.raises(psycopg.errors.DependentObjectsStillExist):
        lab.load(lab.config(overwrite_type="exchange", cross_schema=True))
    assert lab.snapshot("exchange-rollback") == before
    assert lab.rows("target_view") == before["rows"]
    lab.assert_transaction(committed=False)
    assert lab.last_events()[-1]["operation"] == "rollback"
    lab.assert_no_staging()


@pytest.mark.parametrize("strategy", [LoadStrategy.REPLACE, LoadStrategy.PARTITION_REPLACE, LoadStrategy.BACKFILL])
def test_scoped_strategies_preserve_unrelated_rows(lab, strategy):
    lab.constrained_target()
    lab.source([(1, "new", "2026-01-02")])
    options = {"technical_columns": "forbidden"}
    if strategy is LoadStrategy.BACKFILL:
        options["backfill"] = {"inner_mode": "replace"}
    cfg = lab.config(
        load_strategy=strategy,
        custom_predicate="event_day = DATE '2026-01-02'",
        partition={"column": "event_day", "values_from_staging": True},
        options=options,
    )
    result = lab.load(cfg)
    assert lab.rows() == [(1, "new", "2026-01-02"), (90, "previous", "2026-01-01")]
    assert result.staging_rows == 1
    if strategy is LoadStrategy.PARTITION_REPLACE:
        assert result.replaced_rows == 1
        assert result.hard_deleted_rows == 0
    lab.snapshot("scoped")
    lab.assert_transaction(committed=True)


@pytest.mark.parametrize("native_mode", ["auto", "required"])
@pytest.mark.parametrize("wide", [False, True])
def test_native_partition_scope_and_fallback(lab, native_mode, wide):
    lab.execute(f"CREATE TABLE {lab.schema}.target(id integer,name text,event_day date) PARTITION BY RANGE(event_day)")
    lab.execute(
        f"CREATE TABLE {lab.schema}.january PARTITION OF {lab.schema}.target FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
    )
    lab.execute(
        f"INSERT INTO {lab.schema}.target VALUES (90,'old','2026-01-01'),(92,'old','2026-01-01'),(93,'old','2026-01-01')"
    )
    if wide:
        lab.execute(f"INSERT INTO {lab.schema}.target VALUES (91,'outside','2026-01-02')")
    before = lab.snapshot("before")
    new_rows = [(1, "new", "2026-01-01"), (2, "second", "2026-01-01")]
    lab.source(new_rows)
    cfg = lab.config(
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        partition={"column": "event_day", "values_from_staging": True, "native": True, "native_mode": native_mode},
    )
    if wide and native_mode == "required":
        with pytest.raises(ValueError, match="native_mode=required"):
            lab.load(cfg)
        assert lab.snapshot("native-rejected") == before
        return
    for attempt in range(2):
        result = lab.load(cfg)
        assert lab.rows() == new_rows + ([(91, "outside", "2026-01-02")] if wide else [])
        public = {}
        populate_success_result(public, result, validation_info=None, reconciliation_metrics=None)
        assert public["loaded_rows"] == public["inserted_rows"] == public["replaced_rows"] == 2
        assert public["hard_deleted_rows"] == (3 if attempt == 0 else 2)
        assert public["final_rows"] == (3 if wide else 2)
        events = lab.last_events()
        assert any("DETACH PARTITION" in e.get("sql", "") for e in events) is not wide
        lab.snapshot(f"native-or-fallback-{attempt}")


def test_multiple_values_in_same_native_child_preserve_rows(lab):
    lab.execute(f"CREATE TABLE {lab.schema}.target(id integer,name text,event_day date) PARTITION BY RANGE(event_day)")
    lab.execute(
        f"CREATE TABLE {lab.schema}.january PARTITION OF {lab.schema}.target FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
    )
    rows = [(1, "new", "2026-01-01"), (2, "second", "2026-01-02")]
    for row in rows:
        lab.execute(f"INSERT INTO {lab.schema}.target VALUES (%s,%s,%s)", row)
    lab.source(rows)
    result = lab.load(
        lab.config(
            load_strategy=LoadStrategy.PARTITION_REPLACE,
            partition={"column": "event_day", "values_from_staging": True, "native": True},
        )
    )
    assert lab.rows() == rows
    assert result.replaced_rows == 2
    assert not any("DETACH PARTITION" in e.get("sql", "") for e in lab.last_events())


def test_lock_timeout_preserves_previous_state(lab):
    lab.constrained_target()
    before = lab.snapshot("before")
    lab.source([(1, "new", "2026-01-01")])
    lab.connector.execute_query("SET lock_timeout = '100ms'")
    lab.execute("BEGIN")
    lab.execute(f"LOCK TABLE {lab.schema}.target IN ACCESS SHARE MODE")
    try:
        with pytest.raises(psycopg.errors.LockNotAvailable):
            lab.load()
    finally:
        lab.execute("ROLLBACK")
    assert lab.snapshot("lock-timeout") == before
    lab.assert_transaction(committed=False)


def test_query_cancellation_rolls_back(lab):
    lab.constrained_target()
    before = lab.snapshot("before")
    cancel = Timer(0.2, lab.connector.connection.cancel)
    cancel.start()
    try:
        with pytest.raises(psycopg.errors.QueryCanceled):
            lab.load(query="SELECT 1, 'waiting'::text, CURRENT_DATE FROM pg_sleep(10)")
    finally:
        cancel.join()
    assert lab.snapshot("cancelled") == before
    lab.assert_transaction(committed=False)


@pytest.mark.parametrize("kind", ["memory", "file"])
def test_file_and_memory_refresh_still_preserve_target(lab, kind, tmp_path):
    lab.constrained_target()
    before = lab.snapshot("before")
    if kind == "memory":
        artifact = InMemoryRowsArtifact([{"id": 1, "name": "new", "event_day": "2026-01-01"}])
    else:
        path = tmp_path / "rows.csv"
        path.write_text("1,new,2026-01-01\n")
        artifact = FileExportArtifact(str(path), columns=["id", "name", "event_day"], compressed=False, format="csv")
    result = lab.sink.load(lab.config(), LoadPayload(artifact, BUSINESS_SCHEMA))
    after = lab.snapshot(kind)
    assert after["metadata"] == before["metadata"]
    assert after["rows"] == [(1, "new", "2026-01-01")]
    assert result.staging_rows == 1


@pytest.mark.parametrize("entry", ["load", "load_standard", "load_with_truncate", "load_with_exchange"])
@pytest.mark.parametrize("valid", [True, False])
def test_legacy_file_loader_preserves_strategy_and_releases_file(lab, tmp_path, entry, valid):
    from dpone.runtime.sinks.strategies.postgres.file_export_loader import PostgresFileExportLoader

    lab.constrained_target()
    before = lab.snapshot("before")
    path = tmp_path / "legacy.csv"
    path.write_text(f"1,{'new' if valid else 'invalid'},2026-01-01\n")
    artifact = FileExportArtifact(str(path), columns=["id", "name", "event_day"], compressed=False, format="csv")
    batch = LoadPayload(artifact, BUSINESS_SCHEMA)
    calls = []

    class TargetPolicy(PostgresTargetTableManager):
        def ensure_target_table(self, load_config, schema):
            calls.append("target")
            return super().ensure_target_table(load_config, schema)

    manager = TargetPolicy(lab.connector, None, lambda _: False)
    loader = PostgresFileExportLoader(lab.connector, None, manager, lambda *_: calls.append("sample"))
    call = getattr(loader, entry)
    args = [lab.config(log_sample_rows=1), batch] + ([] if entry == "load" else [artifact])
    if valid:
        assert call(*args).inserted_rows == 1
        assert lab.snapshot("loaded")["metadata"] == before["metadata"]
    else:
        with pytest.raises(psycopg.errors.CheckViolation):
            call(*args)
        assert lab.snapshot("rejected") == before
    assert calls == (["target", "sample"] if valid else ["target"])
    assert not path.exists()


def test_null_partition_value_replay_replaces_previous_null_rows(lab):
    lab.constrained_target()
    lab.execute(f"INSERT INTO {lab.schema}.target VALUES (91,'old-null',NULL)")
    lab.source([(1, "new-null", None)])
    cfg = lab.config(
        load_strategy=LoadStrategy.PARTITION_REPLACE, partition={"column": "event_day", "values_from_staging": True}
    )
    for attempt in range(2):
        result = lab.load(cfg)
        assert lab.rows() == [(1, "new-null", None), (90, "previous", "2026-01-01")]
        assert result.replaced_rows == 1
        lab.snapshot(f"null-replay-{attempt}")


def test_sample_sql_failure_preserves_original_error_and_rows(lab):
    lab.constrained_target()
    before = lab.snapshot("before")
    lab.source([(1, "new", "2026-01-01")])
    get_records = lab.connector.get_records

    def sample_fault(query, params=None, as_dict=False):
        text = query.as_string() if hasattr(query, "as_string") else str(query)
        if text.startswith("SELECT *"):
            lab.connector.execute_query("SELECT 1/0")
        return get_records(query, params, as_dict)

    lab.connector.get_records = sample_fault
    cfg = lab.config()
    cfg.log_sample_rows = 1
    with pytest.raises(psycopg.errors.DivisionByZero):
        lab.load(cfg)
    assert lab.snapshot("sample-query-rollback") == before
    lab.assert_transaction(committed=False)


@pytest.mark.parametrize("native_mode", ["auto", "required"])
def test_native_null_partition_uses_fallback_or_rejects(lab, native_mode):
    lab.execute(f"CREATE TABLE {lab.schema}.target(id integer,name text,event_day date) PARTITION BY LIST(event_day)")
    lab.execute(f"CREATE TABLE {lab.schema}.null_rows PARTITION OF {lab.schema}.target FOR VALUES IN (NULL)")
    lab.execute(f"INSERT INTO {lab.schema}.target VALUES (90,'old-null',NULL)")
    before = lab.snapshot("before")
    lab.source([(1, "new-null", None)])
    cfg = lab.config(
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        partition={"column": "event_day", "values_from_staging": True, "native_mode": native_mode},
    )
    for attempt in range(2):
        if native_mode == "required":
            with pytest.raises(ValueError, match="native_mode=required"):
                lab.load(cfg)
            assert lab.snapshot(f"rejected-{attempt}") == before
        else:
            result = lab.load(cfg)
            assert lab.rows() == [(1, "new-null", None)]
            assert result.replaced_rows == 1
            lab.snapshot(f"fallback-{attempt}")
        assert not any("DETACH PARTITION" in e.get("sql", "") for e in lab.last_events())
