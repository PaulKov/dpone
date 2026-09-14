"""Public-DI cancellation matrix for truthful state and source evidence ownership.

Storage records actual port writes in memory; no live database or OS signal is
claimed. The final interaction test executes the real MSSQL strategy/finalizer.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.mssql_transaction_governance import MssqlCleanupDisposition
from dpone.runtime.bootstrap_state_factories import build_run_state_storage
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.state.mssql_generic_transaction_storage import MssqlGenericTransactionStateStorage
from tests import test_mssql_commit_interruption as mssql_support

# Reuse the public fixture so the interaction test has identical real MSSQL DI.
mssql_case = mssql_support.case


class RetainedKeyboardInterrupt(KeyboardInterrupt):
    cleanup_disposition = MssqlCleanupDisposition.PRESERVE_STAGING_EVIDENCE


class RetainedSystemExit(SystemExit):
    cleanup_disposition = MssqlCleanupDisposition.PRESERVE_STAGING_EVIDENCE


class RetainedError(RuntimeError):
    cleanup_disposition = MssqlCleanupDisposition.PRESERVE_STAGING_EVIDENCE


class Source:
    connector = object()

    def __init__(self, artifact, events, abort_failure=False):
        self.artifact, self.events, self.abort_failure = artifact, events, abort_failure

    def extract(self, load_config, state):
        self.events.append("source.extract")
        return ExtractResult(artifact=self.artifact, schema=())

    def abort_mssql_source_boundary(self, load_config):
        self.events.append("source.abort_boundary")
        if self.abort_failure:
            raise RuntimeError("synthetic.source_abort_failed")


class LoadService:
    def __init__(self, failure, events, committed=None):
        self.failure, self.events, self.scope = failure, events, None
        self.committed = committed

    def load_extracted_payload(self, **kwargs):
        self.scope = kwargs["owned_payload_scope"]
        self.scope.require_completed_extraction()
        if self.committed is not None:
            self.scope.mark_target_committed(self.committed)
        self.events.append("load.raise:" + type(self.failure).__name__)
        raise self.failure


class Storage:
    def __init__(self, events):
        self.events, self.rows = events, []
        self.fail_update = False
        self.attempts = []

    def save_run_state(self, state):
        self.record("save", state)

    def update_run_state(self, state):
        self.attempts.append(state.state.value)
        if self.fail_update:
            raise RuntimeError("synthetic state unavailable")
        self.record("update", state)

    def record(self, action, state):
        self.rows.append({"action": action, "state": state.state.value, "error_message": state.error_message})
        self.events.append("state." + action + ":" + state.state.value)


class Logger:
    def __init__(self, events, fail_end=False):
        self.events, self.fail_end, self.ended = events, fail_end, []
        self.end_error = None

    def log_etl_start(self, payload):
        pass

    def log_etl_progress(self, event, payload):
        pass

    def log_etl_error(self, message, payload):
        self.events.append("logger.error:" + message)

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def log_etl_end(self, payload):
        self.ended.append(deepcopy(payload))
        self.events.append("logger.end:" + payload["status"])
        if self.end_error is not None:
            raise self.end_error
        if self.fail_end:
            raise RuntimeError("synthetic.final_log_failed")


@pytest.fixture
def runtime_case(tmp_path):
    events = []
    path = tmp_path / "source.tsv"
    path.write_bytes(b"1\n")
    artifact = FileExportArtifact(str(path), ("id",), rows_exported=1)
    source = Source(artifact, events)
    storage = Storage(events)
    logger = Logger(events)
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"lineage": False},
    )
    return SimpleNamespace(
        events=events, path=path, source=source, artifact=artifact, storage=storage, logger=logger, config=config
    )


def execute(case, error, *, explicit=True, dag=True, date=True, committed=None, load_service=None):
    service = load_service or LoadService(error, case.events, committed)
    processor = ETLProcessor(
        case.source,
        SimpleNamespace(connector=object()),
        etl_logger=case.logger,
        extracted_payload_load_service=service,
        **({"run_state_storage": case.storage} if explicit else {}),
    )
    with pytest.raises(type(error)) as caught:
        processor.run(
            case.config,
            dag_id="synthetic-run" if dag else None,
            execution_date=datetime(2026, 9, 13, tzinfo=UTC) if date else None,
        )
    assert caught.value is error
    return service


@pytest.mark.parametrize("kind", [RetainedError, RetainedKeyboardInterrupt, RetainedSystemExit])
@pytest.mark.parametrize("construction", ["explicit", "default", "no_dag", "no_date"])
def test_uncommitted_failure_never_persists_success(runtime_case, kind, construction):
    error = kind(17) if kind is RetainedSystemExit else kind("password=synthetic-secret")
    cause = ValueError("original cause")
    error.__cause__ = cause
    service = execute(
        runtime_case,
        error,
        explicit=construction != "default",
        dag=construction != "no_dag",
        date=construction != "no_date",
    )
    assert error.__cause__ is cause
    if kind is RetainedSystemExit:
        assert error.code == 17
    expected = ["running", "failed"] if construction == "explicit" else []
    assert [row["state"] for row in runtime_case.storage.rows] == expected
    assert runtime_case.logger.ended[-1]["status"] == "error"
    assert runtime_case.logger.ended[-1]["errors"]
    assert "synthetic-secret" not in repr(runtime_case.storage.rows + runtime_case.logger.ended)
    assert service.scope.target_commit_result is None
    assert service.scope.terminal_receipt.outcome is ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN
    runtime_case.artifact.cleanup()
    assert runtime_case.path.read_bytes() == b"1\n"


@pytest.mark.parametrize("kind", [RetainedKeyboardInterrupt, RetainedSystemExit])
@pytest.mark.parametrize("secondary", ["logger", "source", "tracker"])
def test_secondary_exception_cannot_mask_primary_cancellation(runtime_case, kind, secondary):
    error = kind(17)
    runtime_case.logger.fail_end = secondary == "logger"
    runtime_case.source.abort_failure = secondary == "source"
    runtime_case.storage.fail_update = secondary == "tracker"
    service = execute(runtime_case, error)
    assert runtime_case.storage.attempts == ["failed"]
    expected = ["running"] if secondary == "tracker" else ["running", "failed"]
    assert [row["state"] for row in runtime_case.storage.rows] == expected
    assert service.scope.terminal_receipt.outcome is ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN
    assert runtime_case.path.exists()


@pytest.mark.parametrize("outcome", list(AtomicCommitOutcome))
@pytest.mark.parametrize("blocked", [False, True])
@pytest.mark.parametrize("kind", [RetainedKeyboardInterrupt, RetainedSystemExit])
def test_commit_receipt_remains_authoritative_during_cancellation(runtime_case, outcome, blocked, kind):
    error = kind(17)
    error.blocks_committed_success = blocked
    committed = LoadResult(1, 0, 1, staging_rows=1, commit_receipt_id="synthetic-receipt", commit_outcome=outcome)
    service = execute(runtime_case, error, committed=committed)
    assert service.scope.target_commit_result is committed
    expected_state = "failed" if blocked else "success"
    assert [row["state"] for row in runtime_case.storage.rows] == ["running", expected_state]
    terminal = ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN if blocked else ArtifactTerminalOutcome.SUCCESS
    assert service.scope.terminal_receipt.outcome is terminal
    assert runtime_case.path.exists() is blocked
    result = runtime_case.logger.ended[-1]
    assert result["status"] == ("error" if blocked else "success")
    if not blocked:
        assert result["commit_receipt_id"] == committed.commit_receipt_id
        assert result["commit_outcome"] is outcome


@pytest.mark.parametrize("secondary", ["logger", "source", "tracker"])
def test_committed_cancellation_survives_secondary_exception(runtime_case, secondary):
    runtime_case.logger.fail_end = secondary == "logger"
    runtime_case.source.abort_failure = secondary == "source"
    runtime_case.storage.fail_update = secondary == "tracker"
    committed = LoadResult(1, 0, 1, commit_receipt_id="synthetic-receipt", commit_outcome=AtomicCommitOutcome.COMMITTED)
    service = execute(runtime_case, RetainedSystemExit(17), committed=committed)
    assert service.scope.target_commit_result is committed
    assert runtime_case.storage.attempts == ["success"]
    assert service.scope.terminal_receipt.outcome is ArtifactTerminalOutcome.SUCCESS


def test_new_cleanup_cancellation_is_not_suppressed(runtime_case):
    primary = RetainedKeyboardInterrupt("primary")
    secondary = SystemExit(31)
    runtime_case.logger.end_error = secondary
    processor = ETLProcessor(
        runtime_case.source,
        SimpleNamespace(connector=object()),
        etl_logger=runtime_case.logger,
        extracted_payload_load_service=LoadService(primary, runtime_case.events),
    )
    with pytest.raises(SystemExit) as caught:
        processor.run(runtime_case.config)
    assert caught.value is secondary
    assert secondary.__context__ is primary


def test_callers_handled_cancellation_does_not_change_successful_run_error_policy(runtime_case):
    class SuccessfulLoad:
        def load_extracted_payload(self, **kwargs):
            result = LoadResult(1, 0, 1, staging_rows=1)
            kwargs["owned_payload_scope"].mark_target_committed(result)
            return result, None

    runtime_case.logger.fail_end = True
    processor = ETLProcessor(
        runtime_case.source,
        SimpleNamespace(connector=object()),
        etl_logger=runtime_case.logger,
        extracted_payload_load_service=SuccessfulLoad(),
    )
    try:
        raise KeyboardInterrupt("already handled by caller")
    except KeyboardInterrupt:
        with pytest.raises(RuntimeError, match="synthetic.final_log_failed"):
            processor.run(runtime_case.config)
    assert runtime_case.logger.ended[-1]["status"] == "success"


def test_generic_bootstrap_does_not_install_run_state_storage():
    connector = object()
    generic = MssqlGenericTransactionStateStorage(connector, database="synthetic", schema="etl_state")
    bindings = SimpleNamespace(state_type="mssql", shared_mssql_state_connector=connector, xmin_state_storage=generic)
    assert (
        build_run_state_storage(
            state_factory=object(), state_bindings=bindings, state_cfg={}, sink_obj=SimpleNamespace(connector=connector)
        )
        is None
    )


@pytest.mark.parametrize("kind", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("boundary", ["commit_attempted", "fresh_receipt_probe"])
def test_real_mssql_interrupt_retains_stage_and_source_with_failed_run_state(runtime_case, mssql_case, kind, boundary):
    error = kind(17)
    mssql_case.connector.faults = {"commit_attempted": OSError("lost ACK"), boundary: error}

    class MssqlLoadService:
        def load_extracted_payload(self, **kwargs):
            self.scope = kwargs["owned_payload_scope"]
            payload = LoadPayload(
                runtime_case.artifact,
                (("id", "bigint"),),
                owned_payload_scope=self.scope,
                extraction_lifecycle=self.scope.extraction_lifecycle,
                mssql_transaction_admission=mssql_case.payload.mssql_transaction_admission,
            ).rebind(artifact=mssql_support.SyntheticInput())
            return mssql_case.strategy.load(mssql_case.config, payload), None

    service = execute(runtime_case, error, load_service=MssqlLoadService())
    assert service.scope.terminal_receipt.outcome is ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN
    assert runtime_case.path.read_bytes() == b"1\n"
    assert not mssql_case.manager.dropped
    assert mssql_case.connector.events.count("commit_attempted") == 1
    assert [row["state"] for row in runtime_case.storage.rows] == ["running", "failed"]
