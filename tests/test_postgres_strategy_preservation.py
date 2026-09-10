"""Regressions at the PostgreSQL artifact/strategy and transaction boundaries."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.internal_query_artifact import InternalQueryArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.postgres import PostgresSink
from dpone.runtime.sinks.strategies.postgres.postgres_base import PostgresStrategyBase
from tests.test_runtime_postgres_strategy_split import StubLogger


class RecordingConnector:
    """SQL boundary double; live tests separately assert catalog and row truth."""

    def __init__(self, fail_on=None, rollback_error=None):
        self.operations = []
        self.fail_on = fail_on
        self.rollback_error = rollback_error
        self.primary = RuntimeError("original SQL failure")

    def execute_query(self, query, params=None):
        statement = query.as_string() if hasattr(query, "as_string") else str(query)
        self.operations.append((statement, params))
        if self.fail_on and self.fail_on in statement:
            raise self.primary
        return 2

    def get_records(self, query, params=None, as_dict=False):
        statement = query.as_string() if hasattr(query, "as_string") else str(query)
        self.operations.append((statement, params))
        return [(2,)] if "COUNT(*)" in statement else [(True,)]

    def get_table_column_types(self, *_args):
        return {"id": "integer"}

    def begin(self):
        self.operations.append(("BEGIN", None))

    def commit_transaction(self):
        self.operations.append(("COMMIT", None))
        if self.fail_on == "COMMIT":
            raise self.primary

    def rollback(self):
        self.operations.append(("ROLLBACK", None))
        if self.rollback_error:
            raise self.rollback_error


def config(**kwargs):
    return LoadConfig(
        source_conn_id="pg",
        target_conn_id="pg",
        source_schema="source",
        source_table="orders",
        target_schema="target",
        target_table="orders",
        staging_schema="stage",
        log_sample_rows=0,
        options={"technical_columns": "forbidden"},
        **kwargs,
    )


def payload():
    return LoadPayload(
        InternalQueryArtifact("SELECT id FROM source.orders WHERE id > %s", params=(10,)), [("id", "integer")]
    )


@pytest.mark.parametrize("mode", [None, "truncate_insert"])
def test_internal_query_uses_staging_then_selected_refresh(mode):
    connector = RecordingConnector()
    result = PostgresSink(connector, None, StubLogger()).load(config(overwrite_type=mode), payload())
    sql = [op[0] for op in connector.operations]
    stage_insert = next(i for i, stmt in enumerate(sql) if stmt.startswith('INSERT INTO "stage".'))
    truncate = sql.index('TRUNCATE TABLE "target"."orders"')
    assert stage_insert < truncate
    assert connector.operations[stage_insert][1] == (10,)
    assert not any("RENAME" in stmt or "CASCADE" in stmt or " AS SELECT" in stmt for stmt in sql)
    assert sql.count("BEGIN") == sql.count("COMMIT") == 1
    assert sql[-1] == "COMMIT"
    assert result == LoadResult(2, 0, 2, staging_rows=2)


def test_exchange_participates_in_sink_transaction_and_cleans_original_staging_name():
    connector = RecordingConnector()
    PostgresSink(connector, None, StubLogger()).load(config(overwrite_type="exchange"), payload())
    sql = [op[0] for op in connector.operations]
    assert sql.count("BEGIN") == sql.count("COMMIT") == 1
    assert any("SET SCHEMA" in stmt for stmt in sql)
    assert any(stmt.startswith('ALTER TABLE "target"."stg_') for stmt in sql)
    assert sql[-2].startswith('DROP TABLE IF EXISTS "stage"."stg_')
    assert sql[-1] == "COMMIT"
    assert not any("CASCADE" in stmt for stmt in sql)


@pytest.mark.parametrize(
    "mode,fail_on", [(None, 'INSERT INTO "target".'), ("exchange", 'DROP TABLE IF EXISTS "target".')]
)
def test_failure_rolls_back_once_without_compensating_target_ddl(mode, fail_on):
    connector = RecordingConnector(fail_on=fail_on)
    with pytest.raises(RuntimeError) as raised:
        PostgresSink(connector, None, StubLogger()).load(config(overwrite_type=mode), payload())
    assert raised.value is connector.primary
    sql = [op[0] for op in connector.operations]
    assert sql.count("ROLLBACK") == 1
    assert "COMMIT" not in sql
    assert sql[-1] == "ROLLBACK"


@pytest.mark.parametrize(
    "failure", ['INSERT INTO "stage".', 'INSERT INTO "target".', 'DROP TABLE IF EXISTS "stage".', "COMMIT"]
)
def test_primary_failure_survives_rollback_failure(failure):
    connector = RecordingConnector(fail_on=failure, rollback_error=OSError("rollback lost"))
    with pytest.raises(RuntimeError) as raised:
        PostgresSink(connector, None, StubLogger()).load(config(), payload())
    assert raised.value is connector.primary
    assert any("OSError" in note for note in getattr(raised.value, "__notes__", []))


class HandlerStrategy(PostgresStrategyBase):
    def load(self, load_config, payload):
        raise NotImplementedError


def test_staging_count_preserves_every_result_field():
    manager = SimpleNamespace(drop=lambda *_: None)
    staging = StagingTableArtifact("stage", "orders", ["id"], manager, row_count=2)
    artifact = SimpleNamespace(materialize=lambda *_: staging)
    # Distinct sentinels make any omitted existing or future field observable.
    expected = LoadResult(**{name: object() for name in LoadResult.__dataclass_fields__})
    strategy = HandlerStrategy(RecordingConnector(), StubLogger(), manager)
    actual = strategy._consume_with_staging(config(), LoadPayload(artifact, []), lambda _: expected)
    assert actual == replace(expected, staging_rows=2)


@pytest.mark.parametrize("primary", [RuntimeError("insert failed"), KeyboardInterrupt()])
def test_cleanup_never_masks_primary_handler_error(primary):
    def cleanup(_):
        raise OSError("cleanup failed")

    manager = SimpleNamespace(drop=cleanup)
    staging = StagingTableArtifact("stage", "orders", ["id"], manager)
    artifact = SimpleNamespace(materialize=lambda *_: staging)
    strategy = HandlerStrategy(RecordingConnector(), StubLogger(), manager)

    def handler(_):
        raise primary

    with pytest.raises(type(primary)) as raised:
        strategy._consume_with_staging(config(), LoadPayload(artifact, []), handler)
    assert raised.value is primary
    assert any("OSError" in note for note in getattr(primary, "__notes__", []))


def test_replace_internal_query_reaches_predicate_handler():
    connector = RecordingConnector()
    result = PostgresSink(connector, None, StubLogger()).load(
        config(load_strategy=LoadStrategy.REPLACE, custom_predicate="id > 10"), payload()
    )
    sql = [op[0] for op in connector.operations]
    assert 'DELETE FROM "target"."orders" WHERE id > 10' in sql
    assert not any("TRUNCATE" in stmt or "RENAME" in stmt for stmt in sql)
    assert result.updated_rows == 2


def test_long_multibyte_target_names_keep_unique_bounded_staging_identity():
    from dpone.runtime.sinks.staging_managers.postgres import PostgresStagingManager

    manager = PostgresStagingManager(RecordingConnector(), StubLogger())
    cfg = config()
    cfg.target_table = "orders_" + "я" * 28
    first = manager.create(cfg, [("id", "integer")])
    second = manager.create(cfg, [("id", "integer")])
    assert len(first.table.encode()) <= 63
    assert first.table != second.table
    assert all("IF NOT EXISTS" not in op[0] for op in manager.connector.operations)


def test_native_partition_replacement_rejects_child_with_out_of_scope_rows():
    from dpone.runtime.sinks.strategies.postgres.native_partition_replace import PostgresNativePartitionReplacer

    connector = RecordingConnector()
    replacer = PostgresNativePartitionReplacer(connector, StubLogger())
    replacer._existing_partition_for_value = lambda *_: ('"target"."january"', "FOR VALUES FROM ('1') TO ('32')")
    assert replacer._resolve_partition_plan(config(), "day", ["1"]) is None


@pytest.mark.parametrize("entry", ["internal"])
def test_standalone_legacy_loaders_delegate_selected_strategy(entry):
    from dpone.runtime.sinks.strategies.postgres.file_export_loader import PostgresFileExportLoader
    from dpone.runtime.sinks.strategies.postgres.internal_query_loader import PostgresInternalQueryLoader

    connector = RecordingConnector()
    cls = PostgresInternalQueryLoader if entry == "internal" else PostgresFileExportLoader
    loader = cls(connector, StubLogger(), SimpleNamespace(), lambda *_: None)
    cfg = config(load_strategy=LoadStrategy.REPLACE, custom_predicate="id > 10")
    batch = payload()
    if entry in {"internal", "file"}:
        loader.load(cfg, batch)
    else:
        method = {"standard": "load_standard", "truncate": "load_with_truncate", "exchange": "load_with_exchange"}[
            entry
        ]
        getattr(loader, method)(cfg, batch, batch.artifact)
    sql = [op[0] for op in connector.operations]
    assert 'DELETE FROM "target"."orders" WHERE id > 10' in sql
    assert sql.count("BEGIN") == sql.count("COMMIT") == 1
    assert not any("CASCADE" in q or "RENAME" in q for q in sql)


def test_internal_query_extraction_completes_before_strategy_commit():
    from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority

    lifecycle = ExtractionLifecycleAuthority()
    batch = payload()
    batch.artifact.bind_extraction_lifecycle(lifecycle)

    class Connector(RecordingConnector):
        def commit_transaction(self):
            assert lifecycle.require_completed().extraction_completed_at is not None
            super().commit_transaction()

    PostgresSink(Connector(), None, StubLogger()).load(config(), batch)
    assert lifecycle.require_completed().extraction_completed_at is not None


def test_append_micro_batch_keeps_existing_commit_boundary():
    from dpone.runtime.sinks.strategies.postgres.postgres_increment_append import PostgresIncrementAppendStrategy
    from dpone.runtime.streaming_rows import StreamingRowsArtifact

    connector = RecordingConnector()
    connector.connection = SimpleNamespace(commit=lambda: None)
    manager = SimpleNamespace(drop=lambda *_: None, insert_rows=lambda _, rows: len(rows))
    manager.create = lambda *_: StagingTableArtifact("stage", "rows", ["id"], manager)
    strategy = PostgresIncrementAppendStrategy(connector, StubLogger(), manager)
    strategy._ensure_target_table = lambda *_: False
    strategy._insert_new_only = lambda *_: 1
    cfg = config(only_new_rows=True, unique_key=["id"], micro_batch_commit=True)
    batch = LoadPayload(StreamingRowsArtifact(iter([{"id": 1}, {"id": 2}]), batch_size=1), [("id", "integer")])
    result = strategy.load(cfg, batch)
    assert result.staging_rows == result.inserted_rows == 2
    assert [op[0] for op in connector.operations].count("COMMIT") == 3


def test_sample_query_failure_survives_cleanup_and_rollback():
    class Connector(RecordingConnector):
        def get_records(self, query, params=None, as_dict=False):
            statement = query.as_string() if hasattr(query, "as_string") else str(query)
            if statement.startswith("SELECT *"):
                raise self.primary
            return super().get_records(query, params, as_dict)

    connector = Connector()
    cfg = config()
    cfg.log_sample_rows = 1
    with pytest.raises(RuntimeError) as error:
        PostgresSink(connector, None, StubLogger()).load(cfg, payload())
    assert error.value is connector.primary
    assert connector.operations[-1][0] == "ROLLBACK"


@pytest.mark.parametrize("native_mode,count_failure", [("required", False), ("fallback", False), ("required", True)])
def test_partition_row_metrics_distinguish_old_rows_new_rows_and_partitions(native_mode, count_failure):
    from dpone.runtime.etl.result_metrics import populate_success_result

    class Connector(RecordingConnector):
        def execute_query(self, query, params=None):
            result = super().execute_query(query, params)
            return 3 if self.operations[-1][0].lstrip().startswith("DELETE FROM") else result

        def get_records(self, query, params=None, as_dict=False):
            super().get_records(query, params, as_dict)
            statement = self.operations[-1][0]
            if "SELECT DISTINCT" in statement:
                return [("1",)]
            if "tableoid::regclass" in statement:
                return [('"target"."orders_one"',)]
            if "pg_get_expr" in statement:
                return [("FOR VALUES IN (1)",)]
            if "IS DISTINCT FROM" in statement:
                return [(False,)]
            if "COUNT(*)" in statement:
                if count_failure and '"orders_one"' in statement:
                    raise self.primary
                return [(3 if '"orders_one"' in statement else 9 if '"target"."orders"' in statement else 2,)]
            return [(True,)]

    cfg = config(
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        partition={"column": "id", "values_from_staging": True, "native_mode": native_mode},
    )
    connector = Connector()
    sink = PostgresSink(connector, None, StubLogger())
    if count_failure:
        with pytest.raises(RuntimeError) as raised:
            sink.load(cfg, payload())
        assert raised.value is connector.primary
        assert connector.operations[-1][0] == "ROLLBACK"
        assert not any("DETACH PARTITION" in op[0] or op[0] == "COMMIT" for op in connector.operations)
        return
    result = sink.load(cfg, payload())
    public = {}
    populate_success_result(public, result, validation_info=None, reconciliation_metrics=None)
    assert public["loaded_rows"] == public["inserted_rows"] == public["replaced_rows"] == 2
    assert public["hard_deleted_rows"] == 3
    assert public["staging_rows"] == 2
    assert public["final_rows"] == 9
