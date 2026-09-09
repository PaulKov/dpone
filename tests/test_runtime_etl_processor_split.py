from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.etl.load_config_runtime import LoadConfigRuntimeService
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.etl.processor_runtime import MssqlReplayQualityEvidenceRequired
from dpone.runtime.etl.reconciliation_service import ReconciliationService
from dpone.runtime.etl.run_state_tracker import RunStateTracker
from dpone.runtime.etl.source_state import SourceStateService
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.kafka.offsets import KafkaOffsetState
from dpone.runtime.sinks.base import LoadResult
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.state.xmin_storage import XMinState
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class StubLogger:
    def __init__(self):
        self.progress = []
        self.infos = []
        self.warnings = []
        self.errors = []
        self.started = []
        self.ended = []

    def log_etl_start(self, payload):
        self.started.append(payload)

    def log_etl_progress(self, event, payload):
        self.progress.append((event, payload))

    def info(self, message):
        self.infos.append(message)

    def warning(self, message):
        self.warnings.append(message)

    def log_etl_error(self, message, payload):
        self.errors.append((message, payload))

    def log_etl_end(self, payload):
        self.ended.append(payload)


class StubStorage:
    def __init__(self):
        self.saved = []
        self.updated = []

    def save_run_state(self, state):
        self.saved.append(state)

    def update_run_state(self, state):
        self.updated.append(state)


class FailingUpdateStorage(StubStorage):
    def update_run_state(self, state):
        self.updated.append(state)
        raise RuntimeError("run-state unavailable")


class StubSource:
    def __init__(self, extract_result):
        self.extract_result = extract_result

        class SourceConnector: ...

        self.connector = SourceConnector()
        self.received_state = None

    def get_incremental_state(self, load_config):
        return {"cursor": 123}

    def extract(self, load_config, state):
        self.received_state = state
        return self.extract_result


class StubSink:
    def __init__(self):
        class TargetConnector: ...

        self.connector = TargetConnector()
        self.received_load_config = None
        self.received_payload = None
        self.saved_states = []

    def load(self, load_config, payload):
        self.received_load_config = load_config
        self.received_payload = payload
        return LoadResult(inserted_rows=3, updated_rows=0, total_rows=3, staging_rows=3)

    def save_state(self, load_config, state):
        self.saved_states.append((load_config, state))


class ReceiptBackedStubSink(StubSink):
    def load(self, load_config, payload):
        self.received_load_config = load_config
        self.received_payload = payload
        return LoadResult(
            inserted_rows=3,
            updated_rows=0,
            total_rows=3,
            staging_rows=3,
            commit_receipt_id="receipt-1",
            commit_outcome=AtomicCommitOutcome.COMMITTED,
        )


def make_load_config(**overrides) -> LoadConfig:
    base = LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="public",
        source_table="orders",
        target_schema="landing__src__db",
        target_table="public__orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        options={},
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def test_load_config_runtime_service_prepares_isolated_copy_and_enriches() -> None:
    service = LoadConfigRuntimeService()
    logger = StubLogger()
    source = SimpleNamespace(connector=object())
    original = make_load_config(options={"incremental_column": "updated_at"})

    artifact = SimpleNamespace(
        lookback_partitions=["2024-01-02"],
        incremental_partitions=["2024-01-01", "2024-01-02"],
        incremental_column="updated_at",
        column_timezone="UTC",
        new_partitions=["2024-01-03"],
    )
    extract_result = SimpleNamespace(artifact=artifact)

    runtime_cfg = service.prepare(original)
    enriched = service.enrich_for_load(runtime_cfg, extract_result, source, logger)

    assert original.options == {"incremental_column": "updated_at"}
    assert enriched.options["_lookback_partitions"] == ["2024-01-01", "2024-01-02"]
    assert enriched.options["incremental_column"] == "updated_at"
    assert enriched.options["_column_timezone"] == "UTC"
    assert enriched.options["_new_partitions"] == ["2024-01-03"]
    assert enriched.options["date_column"] == "updated_at"
    assert enriched.options["partition_by"] == "day"
    assert enriched.options["_source_connector"] is source.connector


def test_source_state_service_loads_incremental_state_only_for_stateful_strategies() -> None:
    class CountingSource:
        def __init__(self):
            self.calls = 0

        def get_incremental_state(self, load_config):
            self.calls += 1
            return {"cursor": self.calls}

    service = SourceStateService()
    source = CountingSource()

    assert service.load_for_extract(source, make_load_config()) == {"cursor": 1}
    assert service.load_for_extract(source, make_load_config(load_strategy=LoadStrategy.FULL_REFRESH)) is None
    assert source.calls == 1


def test_source_state_service_persists_kafka_and_xmin_states_to_their_owners() -> None:
    class SavingSource:
        def __init__(self):
            self.saved = []

        def save_state(self, load_config, state):
            self.saved.append((load_config, state))

    source = SavingSource()
    sink = StubSink()
    service = SourceStateService()
    cfg = make_load_config()
    kafka_state = KafkaOffsetState(
        topic="orders",
        group_id="dpone.orders",
        partition_offsets={0: 10},
        high_watermarks={0: 10},
    )
    xmin_state = XMinState(xmin_value=123, timestamp=datetime(2026, 1, 1))

    service.persist_after_load(
        source=source,
        sink=sink,
        original_load_config=cfg,
        effective_load_config=cfg,
        extract_result=SimpleNamespace(state=kafka_state),
        should_persist=True,
    )
    service.persist_after_load(
        source=source,
        sink=sink,
        original_load_config=cfg,
        effective_load_config=cfg,
        extract_result=SimpleNamespace(state=xmin_state),
        should_persist=True,
    )
    service.persist_after_load(
        source=source,
        sink=sink,
        original_load_config=cfg,
        effective_load_config=cfg,
        extract_result=SimpleNamespace(state={"cursor": 123}),
        should_persist=True,
    )

    assert source.saved == [(cfg, kafka_state)]
    assert sink.saved_states == [(cfg, xmin_state)]


def test_run_state_tracker_updates_storage() -> None:
    storage = StubStorage()
    tracker = RunStateTracker(storage=storage)
    cfg = make_load_config()

    tracker.start(cfg, dag_id="dag", execution_date=__import__("datetime").datetime(2026, 1, 1))
    tracker.update_load_strategy("full_refresh")
    tracker.mark_success({"extracted_rows": 11, "inserted_rows": 7, "updated_rows": 2, "soft_deleted_rows": 1})

    assert len(storage.saved) == 1
    assert len(storage.updated) == 1
    saved = storage.saved[0]
    updated = storage.updated[0]
    assert saved.load_strategy == "full_refresh"
    assert saved.process_name == "public.orders->landing__src__db.public__orders"
    assert updated.rows_read == 11
    assert updated.rows_written == 9
    assert updated.rows_updated == 2
    assert updated.rows_deleted == 1


def test_run_state_tracker_uses_verified_pipeline_identity() -> None:
    storage = StubStorage()
    tracker = RunStateTracker(storage=storage)
    cfg = make_load_config(
        options={
            "state_identity": {
                "environment": "dev",
                "process": "sample_metrics_metrics_value",
            }
        }
    )

    tracker.start(cfg, dag_id="dag", execution_date=datetime(2026, 1, 1))

    assert storage.saved[0].process_name == "sample_metrics_metrics_value"


def test_reconciliation_service_skips_without_unique_key() -> None:
    logger = StubLogger()
    service = ReconciliationService(
        source=SimpleNamespace(),
        sink=SimpleNamespace(connector=type("TargetConnector", (), {})()),
        logger=logger,
        run_state_storage=None,
    )
    cfg = make_load_config(reconciliation=True, unique_key=None)
    result = service.process(cfg, SimpleNamespace(schema=[]))

    assert result == {"skipped": True, "reason": "unique_key not specified"}
    assert logger.warnings


def test_etl_processor_full_refresh_override_does_not_mutate_original_options() -> None:
    logger = StubLogger()
    artifact = SimpleNamespace(
        lookback_partitions=[],
        incremental_partitions=[],
        new_partitions=[],
        column_timezone=None,
    )
    extract_result = SimpleNamespace(
        artifact=artifact,
        schema=[],
        state={"cursor": 123},
        force_full_refresh=True,
    )
    source = StubSource(extract_result)
    sink = StubSink()
    processor = ETLProcessor(source=source, sink=sink, etl_logger=logger, run_state_storage=None)
    original = make_load_config(options={})

    result = processor.run(original)

    assert result["status"] == "success"
    assert source.received_state == {"cursor": 123}
    assert sink.received_load_config.load_strategy == LoadStrategy.FULL_REFRESH
    assert original.load_strategy == LoadStrategy.INCREMENTAL_MERGE
    assert original.options == {}
    assert sink.received_load_config.options["_source_connector"] is source.connector
    assert sink.received_load_config.options["__dpone_load_identity"]["run_id"] == result["run_id"]
    assert sink.received_load_config.options["__dpone_load_identity"]["load_id"] == result["load_id"]


def test_receipt_backed_commit_survives_post_commit_run_state_write_failure() -> None:
    logger = StubLogger()
    artifact = SimpleNamespace(
        lookback_partitions=[],
        incremental_partitions=[],
        new_partitions=[],
        column_timezone=None,
    )
    source = StubSource(
        SimpleNamespace(
            artifact=artifact,
            schema=[],
            state=None,
            force_full_refresh=False,
        )
    )
    storage = FailingUpdateStorage()
    processor = ETLProcessor(
        source=source,
        sink=ReceiptBackedStubSink(),
        etl_logger=logger,
        run_state_storage=storage,
    )

    result = processor.run(
        make_load_config(),
        dag_id="dag",
        execution_date=datetime(2026, 1, 1),
    )

    assert result["status"] == "success"
    assert result["commit_receipt_id"] == "receipt-1"
    assert len(storage.updated) == 1
    assert any("stage=run_state_post_commit" in warning for warning in logger.warnings)


def test_non_receipt_run_state_write_failure_remains_fail_closed() -> None:
    logger = StubLogger()
    artifact = SimpleNamespace(
        lookback_partitions=[],
        incremental_partitions=[],
        new_partitions=[],
        column_timezone=None,
    )
    source = StubSource(
        SimpleNamespace(
            artifact=artifact,
            schema=[],
            state=None,
            force_full_refresh=False,
        )
    )
    processor = ETLProcessor(
        source=source,
        sink=StubSink(),
        etl_logger=logger,
        run_state_storage=FailingUpdateStorage(),
    )

    with pytest.raises(RuntimeError, match="run-state unavailable"):
        processor.run(
            make_load_config(),
            dag_id="dag",
            execution_date=datetime(2026, 1, 1),
        )


def test_pre_extract_blocker_runs_before_hooks_route_and_source_reads() -> None:
    class CountingSource(StubSource):
        def __init__(self) -> None:
            super().__init__(SimpleNamespace(artifact=SimpleNamespace(), schema=[]))
            self.extract_calls = 0

        def extract(self, load_config, state):
            self.extract_calls += 1
            return super().extract(load_config, state)

    class BlockingSink(StubSink):
        def preflight_before_extract(self, **_kwargs):
            raise RuntimeError("governed target mutation plan blocked")

    class CountingGovernance(LoadGovernanceService):
        def __init__(self) -> None:
            super().__init__()
            self.pre_hook_calls = 0

        def run_pre_hooks(self, **kwargs):
            self.pre_hook_calls += 1
            return super().run_pre_hooks(**kwargs)

    source = CountingSource()
    governance = CountingGovernance()
    route = SimpleNamespace(prepare=lambda **_kwargs: pytest.fail("route preparation must not run"))
    processor = ETLProcessor(
        source=source,
        sink=BlockingSink(),
        etl_logger=StubLogger(),
        load_governance_service=governance,
        route_capability_orchestrator=route,
    )

    with pytest.raises(RuntimeError, match="mutation plan blocked"):
        processor.run(make_load_config())

    assert source.extract_calls == 0
    assert governance.pre_hook_calls == 0


def test_receipt_replay_completes_inert_post_hooks_without_source_or_dml() -> None:
    replay = LoadResult(
        inserted_rows=2,
        updated_rows=1,
        total_rows=3,
        staging_rows=3,
        commit_receipt_id="receipt-replay",
        commit_outcome=AtomicCommitOutcome.REPLAY_SUPPRESSED,
    )

    class ReplayAdmission:
        @staticmethod
        def prepare(load_config, **_kwargs):
            return load_config

        @staticmethod
        def replay_result(_load_config):
            return replay

    class ReplaySource(StubSource):
        def __init__(self) -> None:
            super().__init__(SimpleNamespace())
            self.extract_calls = 0

        def extract(self, _load_config, _state):
            self.extract_calls += 1
            raise AssertionError("replay must not read source")

    class ReplaySink(StubSink):
        def __init__(self) -> None:
            super().__init__()
            self.load_calls = 0

        def load(self, _load_config, _payload):
            self.load_calls += 1
            raise AssertionError("replay must not mutate target")

    class ReplayGovernance(LoadGovernanceService):
        def __init__(self) -> None:
            super().__init__()
            self.pre_hook_calls = 0
            self.post_hook_calls = 0

        def run_pre_hooks(self, **_kwargs):
            self.pre_hook_calls += 1

        def run_post_hooks(self, **_kwargs):
            self.post_hook_calls += 1

    source = ReplaySource()
    sink = ReplaySink()
    governance = ReplayGovernance()

    result = ETLProcessor(
        source,
        sink,
        etl_logger=StubLogger(),
        load_governance_service=governance,
        mssql_transaction_admission_service=ReplayAdmission(),
    ).run(make_load_config())

    assert result["status"] == "success"
    assert result["commit_outcome"] is AtomicCommitOutcome.REPLAY_SUPPRESSED
    assert source.extract_calls == 0
    assert sink.load_calls == 0
    assert governance.pre_hook_calls == 0
    assert governance.post_hook_calls == 1


def test_receipt_replay_with_non_inert_quality_fails_closed_without_source_or_dml() -> None:
    replay = LoadResult(
        inserted_rows=2,
        updated_rows=1,
        total_rows=3,
        commit_receipt_id="receipt-replay",
        commit_outcome=AtomicCommitOutcome.REPLAY_SUPPRESSED,
    )

    class ReplayAdmission:
        prepare = staticmethod(lambda load_config, **_kwargs: load_config)
        replay_result = staticmethod(lambda _load_config: replay)

    source = StubSource(SimpleNamespace())
    sink = StubSink()
    source.extract = lambda *_args: pytest.fail("replay must not read source")
    sink.load = lambda *_args: pytest.fail("replay must not mutate target")
    config = make_load_config(
        options={
            "quality": {
                "gates": [
                    {
                        "id": "rows",
                        "type": "row_count_reconciliation",
                        "severity": "error",
                    }
                ]
            }
        }
    )

    with pytest.raises(MssqlReplayQualityEvidenceRequired):
        ETLProcessor(
            source,
            sink,
            etl_logger=StubLogger(),
            mssql_transaction_admission_service=ReplayAdmission(),
        ).run(config)


def test_target_commit_callback_turns_secondary_failure_into_repairable_warning() -> None:
    artifact = SimpleNamespace(
        lookback_partitions=[],
        incremental_partitions=[],
        new_partitions=[],
        column_timezone=None,
        cleanup=lambda: None,
    )
    source = StubSource(
        SimpleNamespace(
            artifact=artifact,
            schema=[],
            state=None,
            force_full_refresh=False,
        )
    )
    committed = LoadResult(
        inserted_rows=4,
        updated_rows=0,
        total_rows=4,
        staging_rows=4,
        commit_receipt_id="receipt-secondary",
        commit_outcome=AtomicCommitOutcome.COMMITTED,
    )

    class FailingEvidenceLoadService:
        row_lineage_enricher = None
        nested_load_service = None
        strategy_metadata_enricher = None
        payload_load_service = None

        def load_extracted_payload(self, *, owned_payload_scope, **_kwargs):
            owned_payload_scope.mark_target_committed(committed)
            raise RuntimeError("runtime evidence writer unavailable")

    result = ETLProcessor(
        source,
        StubSink(),
        etl_logger=StubLogger(),
        extracted_payload_load_service=FailingEvidenceLoadService(),
    ).run(make_load_config())

    assert result["status"] == "success"
    assert result["commit_receipt_id"] == "receipt-secondary"
    assert result["artifact_terminal"]["outcome"] == "success"
    assert result["secondary_warnings"] == ["runtime evidence writer unavailable"]


@pytest.mark.parametrize("sink_name", ["BigQuerySink", "PostgresSink", "ClickHouseSink", "KafkaSink"])
@pytest.mark.parametrize("fail_after_consume", [False, True])
def test_top_level_scope_is_the_only_source_terminal_owner_across_sinks(
    sink_name: str,
    fail_after_consume: bool,
) -> None:
    terminal_calls: list[str] = []

    class Staging:
        def create(self, _config, schema):
            return StagingTableArtifact("stage", "payload", tuple(name for name, _ in schema), self)

        def insert_rows(self, _handle, rows):
            return len(list(rows))

        def drop(self, _handle):
            return None

    class ConsumingSink(StubSink):
        def load(self, load_config, payload):
            staged = payload.artifact.materialize(Staging(), load_config, payload.schema)
            staged.cleanup()
            if fail_after_consume:
                raise RuntimeError("target mutation rejected")
            return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1)

    sink_type = type(sink_name, (ConsumingSink,), {})
    artifact = StreamingRowsArtifact(
        iter(({"id": 1},)),
        on_success=lambda: terminal_calls.append("commit"),
        on_abort=lambda: terminal_calls.append("rollback"),
    )
    source = StubSource(
        SimpleNamespace(
            artifact=artifact,
            schema=[("id", "int")],
            state=None,
            force_full_refresh=False,
        )
    )
    processor = ETLProcessor(
        source=source,
        sink=sink_type(),
        etl_logger=StubLogger(),
    )

    if fail_after_consume:
        with pytest.raises(RuntimeError, match="target mutation rejected"):
            processor.run(make_load_config())
        assert terminal_calls == ["rollback"]
    else:
        result = processor.run(make_load_config())
        assert result["artifact_terminal"]["outcome"] == "success"
        assert terminal_calls == ["commit"]
