from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg import errors

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sources.strategies.postgres.postgres_base_strategy import (
    PostgresFetchedSchema,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_retry import (
    MAX_POSTGRES_SNAPSHOT_RETRIES,
    PostgresSnapshotRetryPolicy,
)
from dpone.runtime.sources.strategies.postgres.postgres_xmin_extract import (
    PostgresXMinExtractStrategy,
)
from dpone.runtime.state.xmin_storage import XMinState


class _Logger:
    def __init__(self, *, retry_log_failure: BaseException | None = None) -> None:
        self.progress: list[tuple[str, dict[str, object]]] = []
        self._retry_log_failure = retry_log_failure

    def log_xmin_state_info(self, *_args, **_kwargs) -> None:
        return None

    def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
        self.progress.append((event, payload))
        if event == "POSTGRES_SNAPSHOT_RETRY" and self._retry_log_failure is not None:
            raise self._retry_log_failure


class _Session:
    def __init__(self, identity: int) -> None:
        self.identity = identity


class _Connector:
    database = "sample-metrics"

    def __init__(
        self,
        failures: list[BaseException | None],
        *,
        snapshot_failures: list[BaseException | None] | None = None,
    ) -> None:
        self._failures = list(failures)
        self._snapshot_failures = list(snapshot_failures or ())
        self._connection: _Session | None = None
        self._next_session_identity = 0
        self.in_transaction = False
        self.close_count = 0
        self.copy_session_ids: list[int] = []
        self.copy_kinds: list[str] = []
        self.snapshot_session_ids: list[int] = []
        self.export_paths: list[Path] = []

    @property
    def connection(self) -> _Session:
        if self._connection is None:
            self._next_session_identity += 1
            self._connection = _Session(self._next_session_identity)
        return self._connection

    def begin(self) -> None:
        self.connection
        self.in_transaction = True

    def execute_query(self, _query, _params=None) -> int:
        return 0

    def get_records(self, query, params=None, as_dict=False):
        del params, as_dict
        rendered = str(query)
        if "txid_current_snapshot" in rendered:
            session_id = self.connection.identity
            self.snapshot_session_ids.append(session_id)
            failure = self._snapshot_failures.pop(0) if self._snapshot_failures else None
            if failure is not None:
                raise failure
            return [
                {
                    "snapshot_token": f"{session_id}00:{session_id}02:",
                    "extraction_horizon": session_id * 100 + 2,
                }
            ]
        return []

    def copy_to_file(
        self,
        *,
        output_path: str,
        **_kwargs,
    ) -> dict[str, object]:
        session_id = self.connection.identity
        self.copy_session_ids.append(session_id)
        query = str(_kwargs.get("query_sql") or "")
        is_incremental_delta = "__dpone__xmin" in query
        self.copy_kinds.append("delta" if is_incremental_delta else "key_or_baseline")
        path = Path(output_path)
        self.export_paths.append(path)
        failure = self._failures.pop(0) if self._failures else None
        if failure is not None:
            path.write_bytes(b"partial-row-without-a-receipt")
            raise failure
        payload = b"00000000-0000-0000-0000-000000000001"
        if is_incremental_delta:
            payload += b"\t101"
        payload += b"\n"
        path.write_bytes(payload)
        return {
            "total_bytes": len(payload),
            "copy_read_count": 1,
            "chunk_count": 1,
            "elapsed": 0.01,
            "throughput": 1.0,
            "rows_exported": 1,
            "sha256": hashlib.sha256(payload).hexdigest(),
        }

    def commit_transaction(self) -> None:
        self.in_transaction = False

    def rollback(self) -> None:
        self.in_transaction = False

    def close(self) -> None:
        self.close_count += 1
        self.in_transaction = False
        self._connection = None


class _TargetConnector:
    database = "DWH_Dev"

    def __init__(self, *, exists: bool = False) -> None:
        self.read_calls = 0
        self.write_calls = 0
        self._exists = exists

    def table_exists(self, _schema, _table, *, database=None) -> bool:
        del database
        self.read_calls += 1
        return self._exists

    def execute_query(self, *_args, **_kwargs) -> None:
        self.write_calls += 1
        raise AssertionError("snapshot extraction must not write to the sink")


class _StateStorage:
    def __init__(self, *, loaded_state: XMinState | None = None) -> None:
        self.load_calls = 0
        self.write_calls = 0
        self._loaded_state = loaded_state

    def load_state_by_key(self, _key):
        self.load_calls += 1
        return self._loaded_state

    def save_state(self, *_args, **_kwargs) -> None:
        self.write_calls += 1
        raise AssertionError("snapshot extraction must not promote state")


class _SourceAuthority:
    @staticmethod
    def verify_snapshot(*, connector, snapshot_lease, load_config):
        del load_config
        snapshot_lease.require_for(connector)
        return SimpleNamespace(verified=True)


class _XminManager:
    def __init__(self, connector: _Connector) -> None:
        self._connector = connector

    def get_snapshot_xmin_anchor(self) -> int:
        assert self._connector.in_transaction
        return self._connector.connection.identity * 100

    @staticmethod
    def calculate_safe_xmin(anchor: int, previous: XMinState | None) -> XMinState:
        return XMinState(
            xmin_value=anchor,
            timestamp=datetime.now(UTC),
            is_initial=previous is None,
        )

    @staticmethod
    def should_perform_full_refresh(state: XMinState) -> bool:
        return state.is_initial

    @staticmethod
    def build_incremental_query(*_args, **_kwargs) -> str:
        return "SELECT guid, xmin AS __dpone__xmin FROM public.metrics_value"


def _config(tmp_path: Path) -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres_sample_metrics_source",
        target_conn_id="mssql_sample_metrics_target",
        source_schema="public",
        source_table="metrics_value",
        source_database="sample-metrics",
        target_schema="sample_metrics",
        target_table="metrics_value",
        target_database="DWH_Dev",
        staging_schema="sample_metrics",
        staging_database="DWH_Dev",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["guid"],
        export_format="csv",
        compress_export=False,
        options={
            "incremental_strategy": "xmin",
            "batch_commit_mode": "whole",
            "sink_type": "mssql",
            "runtime_storage": {"work_dir": str(tmp_path)},
            "reconciliation": {
                "enabled": True,
                "mode": "key_snapshot",
                "consistency": "same_source_snapshot",
                "delete_policy": "soft_delete",
            },
            "state_identity": {
                "environment": "dev",
                "process": "platform.sample_metrics.metrics_value",
            },
            "schema_contract": {"columns": {"guid": {"nullable": False}}},
        },
    )


def _strategy(
    tmp_path: Path,
    *,
    failures: list[BaseException | None],
    sleeps: list[float],
    random_value: float = 0.0,
    snapshot_failures: list[BaseException | None] | None = None,
    retry_log_failure: BaseException | None = None,
    sleeper: Callable[[float], None] | None = None,
    target_exists: bool = False,
    loaded_state: XMinState | None = None,
) -> tuple[PostgresXMinExtractStrategy, _Connector, _TargetConnector, _StateStorage, _Logger]:
    connector = _Connector(failures, snapshot_failures=snapshot_failures)
    target = _TargetConnector(exists=target_exists)
    state = _StateStorage(loaded_state=loaded_state)
    logger = _Logger(retry_log_failure=retry_log_failure)
    strategy = PostgresXMinExtractStrategy(
        connector=connector,
        sink_connector=target,
        state_storage=state,
        logger=logger,
        snapshot_retry_sleeper=sleeper or sleeps.append,
        snapshot_retry_random=lambda: random_value,
    )
    strategy.bind_postgres_source_authority(_SourceAuthority())
    strategy.xmin_manager = _XminManager(connector)  # type: ignore[assignment]
    strategy._preflight_atomic_route = lambda _config: None  # type: ignore[assignment]
    strategy._physical_target_binding = ("DWH_Dev", "sample_metrics", "metrics_value")
    strategy._physical_target_identity = b"t" * 32
    strategy.fetch_schema_projection = lambda _config: PostgresFetchedSchema(  # type: ignore[assignment]
        relation_schema=(("guid", "uuid"),),
        projected_schema=(("guid", "uniqueidentifier"),),
        relation_metadata=(),
        target_projection=None,
    )
    strategy._snapshot_extractor._require_checkpoint_above_freeze_horizon = (  # type: ignore[method-assign]
        lambda *_args, **_kwargs: 1
    )
    return strategy, connector, target, state, logger


def _previous_state() -> XMinState:
    return XMinState(
        xmin_value=42,
        timestamp=datetime(2026, 8, 20, tzinfo=UTC),
        is_initial=False,
    )


class _SnapshotCancelled(BaseException):
    sqlstate = "40001"


def test_retry_policy_is_closed_to_sqlstate_40001() -> None:
    policy = PostgresSnapshotRetryPolicy()
    retryable = RuntimeError("redacted wrapper")
    retryable.__cause__ = errors.SerializationFailure("recovery conflict")

    assert policy.is_retryable(retryable) is True
    assert policy.is_retryable(errors.UniqueViolation("duplicate")) is False
    assert policy.is_retryable(ValueError("deterministic")) is False
    psycopg2_style = RuntimeError("legacy adapter")
    psycopg2_style.pgcode = "40001"  # type: ignore[attr-defined]
    assert policy.is_retryable(psycopg2_style) is False
    assert MAX_POSTGRES_SNAPSHOT_RETRIES == 2


def test_retry_policy_uses_bounded_exponential_backoff_with_jitter() -> None:
    policy = PostgresSnapshotRetryPolicy()

    assert policy.delay_seconds(retry_number=1, random_unit=0.0) == 2.0
    assert policy.delay_seconds(retry_number=2, random_unit=1.0) == 5.0
    with pytest.raises(ValueError, match="max_retries"):
        PostgresSnapshotRetryPolicy(max_retries=3)


def test_snapshot_retry_reopens_session_and_cleans_partial_artifact(tmp_path: Path) -> None:
    sleeps: list[float] = []
    strategy, connector, target, state, logger = _strategy(
        tmp_path,
        failures=[errors.SerializationFailure("recovery conflict"), None],
        sleeps=sleeps,
        random_value=0.5,
    )

    result = strategy.extract(_config(tmp_path), None)

    assert connector.copy_session_ids[0] != connector.copy_session_ids[1]
    assert connector.close_count == 1
    assert sleeps == [2.25]
    assert len(connector.export_paths) == 2
    assert connector.export_paths[0].exists() is False
    assert target.write_calls == 0
    assert state.write_calls == 0
    retry_events = [payload for event, payload in logger.progress if event == "POSTGRES_SNAPSHOT_RETRY"]
    assert retry_events == [
        {
            "SQLSTATE": "40001",
            "Retry": 1,
            "Max Retries": 2,
            "Backoff Seconds": 2.25,
        }
    ]
    assert result.snapshot_envelope is not None
    result.snapshot_envelope.cleanup()


def test_snapshot_retry_covers_40001_before_copy_inside_repeatable_read(tmp_path: Path) -> None:
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[None],
        sleeps=sleeps,
        # get_state owns the first RR; the envelope's first RR then conflicts.
        snapshot_failures=[None, errors.SerializationFailure("snapshot conflict"), None],
    )

    result = strategy.extract(_config(tmp_path), None)

    assert connector.snapshot_session_ids[-2] != connector.snapshot_session_ids[-1]
    assert connector.copy_session_ids == [connector.snapshot_session_ids[-1]]
    assert connector.close_count == 1
    assert sleeps == [2.0]
    assert target.write_calls == 0
    assert state.write_calls == 0
    assert result.snapshot_envelope is not None
    result.snapshot_envelope.cleanup()


def test_snapshot_retry_log_failure_does_not_mask_recovery(tmp_path: Path) -> None:
    sleeps: list[float] = []
    strategy, connector, target, state, logger = _strategy(
        tmp_path,
        failures=[errors.SerializationFailure("recovery conflict"), None],
        sleeps=sleeps,
        retry_log_failure=RuntimeError("retry logging unavailable"),
    )

    result = strategy.extract(_config(tmp_path), None)

    assert len(connector.copy_session_ids) == 2
    assert sleeps == [2.0]
    assert any(event == "POSTGRES_SNAPSHOT_RETRY" for event, _payload in logger.progress)
    assert target.write_calls == 0
    assert state.write_calls == 0
    assert result.snapshot_envelope is not None
    result.snapshot_envelope.cleanup()


def test_snapshot_retry_propagates_close_cancellation_without_sleep(tmp_path: Path) -> None:
    cancellation = _SnapshotCancelled("cancelled while closing")
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[errors.SerializationFailure("recovery conflict")],
        sleeps=sleeps,
    )

    def cancel_close() -> None:
        raise cancellation

    connector.close = cancel_close  # type: ignore[method-assign]

    with pytest.raises(_SnapshotCancelled) as exc_info:
        strategy.extract(_config(tmp_path), None)

    assert exc_info.value is cancellation
    assert len(connector.copy_session_ids) == 1
    assert sleeps == []
    assert target.write_calls == 0
    assert state.write_calls == 0


def test_snapshot_retry_propagates_logger_cancellation_without_sleep(tmp_path: Path) -> None:
    cancellation = _SnapshotCancelled("cancelled while logging")
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[errors.SerializationFailure("recovery conflict")],
        sleeps=sleeps,
        retry_log_failure=cancellation,
    )

    with pytest.raises(_SnapshotCancelled) as exc_info:
        strategy.extract(_config(tmp_path), None)

    assert exc_info.value is cancellation
    assert len(connector.copy_session_ids) == 1
    assert connector.close_count == 1
    assert sleeps == []
    assert target.write_calls == 0
    assert state.write_calls == 0


def test_snapshot_retry_propagates_sleeper_cancellation_without_another_attempt(tmp_path: Path) -> None:
    cancellation = _SnapshotCancelled("cancelled during backoff")
    sleeps: list[float] = []

    def cancel_sleep(delay: float) -> None:
        sleeps.append(delay)
        raise cancellation

    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[errors.SerializationFailure("recovery conflict")],
        sleeps=sleeps,
        sleeper=cancel_sleep,
    )

    with pytest.raises(_SnapshotCancelled) as exc_info:
        strategy.extract(_config(tmp_path), None)

    assert exc_info.value is cancellation
    assert len(connector.copy_session_ids) == 1
    assert connector.close_count == 1
    assert sleeps == [2.0]
    assert target.write_calls == 0
    assert state.write_calls == 0


def test_state_projection_retry_reopens_session_before_state_read(tmp_path: Path) -> None:
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[None],
        sleeps=sleeps,
        snapshot_failures=[errors.SerializationFailure("pre-read conflict"), None, None],
    )

    result = strategy.extract(_config(tmp_path), None)

    assert connector.snapshot_session_ids[0] != connector.snapshot_session_ids[1]
    assert connector.close_count == 1
    assert sleeps == [2.0]
    assert state.load_calls == 1
    assert target.write_calls == 0
    assert state.write_calls == 0
    assert result.snapshot_envelope is not None
    result.snapshot_envelope.cleanup()


def test_state_projection_retry_is_bounded_before_state_read(tmp_path: Path) -> None:
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[],
        sleeps=sleeps,
        snapshot_failures=[
            errors.SerializationFailure("first"),
            errors.SerializationFailure("second"),
            errors.SerializationFailure("third"),
        ],
    )

    with pytest.raises(errors.SerializationFailure, match="third"):
        strategy.extract(_config(tmp_path), None)

    assert len(connector.snapshot_session_ids) == 3
    assert len(set(connector.snapshot_session_ids)) == 3
    assert connector.close_count == 2
    assert sleeps == [2.0, 4.0]
    assert state.load_calls == 0
    assert connector.copy_session_ids == []
    assert target.write_calls == 0
    assert state.write_calls == 0


def test_state_projection_non_retryable_error_fails_before_state_read(tmp_path: Path) -> None:
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[],
        sleeps=sleeps,
        snapshot_failures=[errors.UniqueViolation("deterministic")],
    )

    with pytest.raises(errors.UniqueViolation, match="deterministic"):
        strategy.extract(_config(tmp_path), None)

    assert len(connector.snapshot_session_ids) == 1
    assert connector.close_count == 0
    assert sleeps == []
    assert state.load_calls == 0
    assert connector.copy_session_ids == []
    assert target.write_calls == 0
    assert state.write_calls == 0


def test_state_projection_cancellation_propagates_before_state_read(tmp_path: Path) -> None:
    cancellation = _SnapshotCancelled("cancelled in pre-read")
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[],
        sleeps=sleeps,
        snapshot_failures=[cancellation],
    )

    with pytest.raises(_SnapshotCancelled) as exc_info:
        strategy.extract(_config(tmp_path), None)

    assert exc_info.value is cancellation
    assert len(connector.snapshot_session_ids) == 1
    assert connector.close_count == 0
    assert sleeps == []
    assert state.load_calls == 0
    assert connector.copy_session_ids == []
    assert target.write_calls == 0
    assert state.write_calls == 0


def test_snapshot_retry_is_bounded_to_three_total_attempts(tmp_path: Path) -> None:
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[
            errors.SerializationFailure("first"),
            errors.SerializationFailure("second"),
            errors.SerializationFailure("third"),
        ],
        sleeps=sleeps,
    )

    with pytest.raises(errors.SerializationFailure, match="third"):
        strategy.extract(_config(tmp_path), None)

    assert len(connector.copy_session_ids) == 3
    assert len(set(connector.copy_session_ids)) == 3
    assert connector.close_count == 2
    assert sleeps == [2.0, 4.0]
    assert all(path.exists() is False for path in connector.export_paths)
    assert target.write_calls == 0
    assert state.write_calls == 0


def test_snapshot_retry_fails_fast_for_non_retryable_sqlstate(tmp_path: Path) -> None:
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[errors.UniqueViolation("deterministic")],
        sleeps=sleeps,
    )

    with pytest.raises(errors.UniqueViolation, match="deterministic"):
        strategy.extract(_config(tmp_path), None)

    assert len(connector.copy_session_ids) == 1
    assert connector.close_count == 0
    assert sleeps == []
    assert all(path.exists() is False for path in connector.export_paths)
    assert target.write_calls == 0
    assert state.write_calls == 0


def test_snapshot_retry_fails_closed_when_session_cannot_be_reset(tmp_path: Path) -> None:
    sleeps: list[float] = []
    primary = errors.SerializationFailure("recovery conflict")
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[primary],
        sleeps=sleeps,
    )

    def fail_close() -> None:
        raise RuntimeError("must remain redacted")

    connector.close = fail_close  # type: ignore[method-assign]

    with pytest.raises(errors.SerializationFailure) as exc_info:
        strategy.extract(_config(tmp_path), None)

    assert exc_info.value is primary
    assert len(connector.copy_session_ids) == 1
    assert sleeps == []
    assert target.write_calls == 0
    assert state.write_calls == 0
    assert any(
        note == "postgres_snapshot.retry_connection_reset_failed:builtins.RuntimeError"
        for note in getattr(primary, "__notes__", ())
    )
    assert all("must remain redacted" not in note for note in getattr(primary, "__notes__", ()))


def test_snapshot_retry_does_not_catch_cancellation(tmp_path: Path) -> None:
    cancellation = _SnapshotCancelled("cancelled")
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[cancellation],
        sleeps=sleeps,
    )

    with pytest.raises(_SnapshotCancelled) as exc_info:
        strategy.extract(_config(tmp_path), None)

    assert exc_info.value is cancellation
    assert len(connector.copy_session_ids) == 1
    assert connector.close_count == 0
    assert sleeps == []
    assert target.write_calls == 0
    assert state.write_calls == 0


@pytest.mark.parametrize(
    ("failures", "expected_kinds", "expected_sessions", "failed_path_indexes"),
    [
        (
            [errors.SerializationFailure("delta conflict"), None, None],
            ["delta", "delta", "key_or_baseline"],
            [1, 2, 2],
            [0],
        ),
        (
            [None, errors.SerializationFailure("key conflict"), None, None],
            ["delta", "key_or_baseline", "delta", "key_or_baseline"],
            [1, 1, 2, 2],
            [0, 1],
        ),
    ],
    ids=["delta-copy", "key-copy"],
)
def test_incremental_snapshot_retry_replays_delta_and_keys_from_one_fresh_snapshot(
    tmp_path: Path,
    failures: list[BaseException | None],
    expected_kinds: list[str],
    expected_sessions: list[int],
    failed_path_indexes: list[int],
) -> None:
    previous = _previous_state()
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=failures,
        sleeps=sleeps,
        target_exists=True,
        loaded_state=previous,
    )

    config = _config(tmp_path)
    loaded = strategy.get_state(config)
    assert loaded is previous
    result = strategy.extract(config, loaded)

    assert connector.copy_kinds == expected_kinds
    assert connector.copy_session_ids == expected_sessions
    assert connector.snapshot_session_ids == [1, 1, 2]
    assert connector.close_count == 1
    assert sleeps == [2.0]
    assert all(connector.export_paths[index].exists() is False for index in failed_path_indexes)
    assert state.load_calls == 1
    assert target.write_calls == 0
    assert state.write_calls == 0
    assert previous.xmin_value == 42
    assert result.snapshot_envelope is not None
    envelope = result.snapshot_envelope
    assert envelope.baseline is False
    assert envelope.previous_checkpoint is previous
    assert envelope.state_key == strategy._loaded_state_key
    assert envelope.safe_checkpoint is result.state
    assert envelope.safe_checkpoint.xmin_value == 200
    assert envelope.visible_horizon == 202
    assert envelope.delta_receipt.snapshot_token == envelope.snapshot_token
    assert envelope.key_receipt.snapshot_token == envelope.snapshot_token
    assert connector.copy_session_ids[-2:] == [connector.snapshot_session_ids[-1]] * 2
    final_paths = {Path(envelope.delta_artifact.file_path), Path(envelope.key_artifact.file_path)}
    assert {path for path in tmp_path.iterdir() if path.is_file()} == final_paths
    envelope.cleanup()
    assert list(tmp_path.iterdir()) == []


def test_incremental_key_copy_cancellation_survives_rollback_and_artifact_cleanup_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancellation = _SnapshotCancelled("cancelled during key COPY")
    previous = _previous_state()
    sleeps: list[float] = []
    strategy, connector, target, state, _logger = _strategy(
        tmp_path,
        failures=[None, cancellation],
        sleeps=sleeps,
        target_exists=True,
    )

    def rollback_failure() -> None:
        connector.in_transaction = False
        raise RuntimeError("rollback detail must remain redacted")

    original_cleanup = FileExportArtifact.cleanup

    def cleanup_failure(artifact: FileExportArtifact) -> None:
        original_cleanup(artifact)
        raise RuntimeError("cleanup detail must remain redacted")

    connector.rollback = rollback_failure  # type: ignore[method-assign]
    monkeypatch.setattr(FileExportArtifact, "cleanup", cleanup_failure)

    with pytest.raises(_SnapshotCancelled) as exc_info:
        strategy.extract(_config(tmp_path), previous)

    assert exc_info.value is cancellation
    assert connector.copy_kinds == ["delta", "key_or_baseline"]
    assert connector.copy_session_ids == [1, 1]
    assert connector.close_count == 0
    assert sleeps == []
    assert list(tmp_path.iterdir()) == []
    assert state.load_calls == 0
    assert target.write_calls == 0
    assert state.write_calls == 0
    notes = set(getattr(cancellation, "__notes__", ()))
    assert "postgres_snapshot.rollback_failed:builtins.RuntimeError" in notes
    assert "postgres_snapshot.raw_delta_cleanup_failed:builtins.RuntimeError" in notes
    assert all("detail must remain redacted" not in note for note in notes)
