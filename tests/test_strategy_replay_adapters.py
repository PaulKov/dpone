from __future__ import annotations

from pathlib import Path

import pytest

from dpone.strategy_intelligence.replay import ReplayExecutionRequest, ReplayExecutionService
from dpone.strategy_intelligence.replay_adapters import (
    DbReplayAdapter,
    KafkaReplayAdapter,
    ReplayAdapterRegistry,
    ReplayBackend,
    ReplayBackendResult,
)


class _Backend(ReplayBackend):
    def __init__(self, *, reconcile_passed: bool = True) -> None:
        self.operations: list[str] = []
        self._reconcile_passed = reconcile_passed

    def validate_staging(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        self.operations.append(f"validate_staging:{request.strategy_mode}")
        return ReplayBackendResult(True, "staging valid")

    def execute_finalizer(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        self.operations.append(f"execute_finalizer:{request.sink_type}:{request.strategy_mode}")
        return ReplayBackendResult(True, "finalizer executed")

    def produce_replay_events(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        self.operations.append(f"produce_replay_events:{request.sink_type}:{request.strategy_mode}")
        return ReplayBackendResult(True, "events produced")

    def reconcile(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        self.operations.append(f"reconcile:{request.sink_type}:{request.strategy_mode}")
        return ReplayBackendResult(self._reconcile_passed, "reconcile result")

    def commit_state(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        self.operations.append(f"commit_state:{request.run_id}")
        return ReplayBackendResult(True, "state committed")


def test_db_replay_adapter_executes_staging_first_and_commits_state_after_reconcile() -> None:
    backend = _Backend()
    result = DbReplayAdapter(backend=backend).execute(
        ReplayExecutionRequest(
            action="resync",
            run_id="RUN_ID",
            source_type="postgres",
            sink_type="mssql",
            strategy_mode="partition_replace",
            yes=True,
        )
    )

    assert result.status == "executed"
    assert result.state_committed is True
    assert result.operations == (
        "validate_staging:partition_replace",
        "execute_finalizer:mssql:partition_replace",
        "reconcile:mssql:partition_replace",
        "commit_state:RUN_ID",
    )
    assert backend.operations == list(result.operations)


def test_db_replay_adapter_does_not_commit_state_when_reconciliation_fails() -> None:
    backend = _Backend(reconcile_passed=False)
    result = DbReplayAdapter(backend=backend).execute(
        ReplayExecutionRequest(
            action="resync",
            run_id="RUN_ID",
            source_type="postgres",
            sink_type="mssql",
            strategy_mode="partition_replace",
            yes=True,
        )
    )

    assert result.status == "failed"
    assert result.state_committed is False
    assert "commit_state:RUN_ID" not in result.operations
    assert backend.operations == [
        "validate_staging:partition_replace",
        "execute_finalizer:mssql:partition_replace",
        "reconcile:mssql:partition_replace",
    ]


def test_kafka_replay_adapter_produces_events_and_commits_state_after_reconcile() -> None:
    backend = _Backend()
    result = KafkaReplayAdapter(backend=backend).execute(
        ReplayExecutionRequest(
            action="resume",
            run_id="RUN_ID",
            source_type="postgres",
            sink_type="kafka",
            strategy_mode="incremental_merge",
            yes=True,
        )
    )

    assert result.status == "executed"
    assert result.operations == (
        "validate_staging:incremental_merge",
        "produce_replay_events:kafka:incremental_merge",
        "reconcile:kafka:incremental_merge",
        "commit_state:RUN_ID",
    )


def test_replay_adapter_registry_resolves_db_and_kafka_adapters() -> None:
    registry = ReplayAdapterRegistry(backend=_Backend())

    assert isinstance(registry.resolve("mssql"), DbReplayAdapter)
    assert isinstance(registry.resolve("postgres"), DbReplayAdapter)
    assert isinstance(registry.resolve("clickhouse"), DbReplayAdapter)
    assert isinstance(registry.resolve("bigquery"), DbReplayAdapter)
    assert isinstance(registry.resolve("kafka"), KafkaReplayAdapter)

    with pytest.raises(ValueError, match="Unsupported replay sink"):
        registry.resolve("unknown")


def test_replay_execution_service_uses_adapter_when_yes_and_records_operations(tmp_path: Path) -> None:
    backend = _Backend()
    service = ReplayExecutionService(artifact_dir=tmp_path, adapter_registry=ReplayAdapterRegistry(backend=backend))

    result = service.execute(
        ReplayExecutionRequest(
            action="resync",
            run_id="RUN_ID",
            source_type="postgres",
            sink_type="mssql",
            strategy_mode="partition_replace",
            yes=True,
        )
    )

    assert result.status == "executed"
    assert result.operations == (
        "validate_staging:partition_replace",
        "execute_finalizer:mssql:partition_replace",
        "reconcile:mssql:partition_replace",
        "commit_state:RUN_ID",
    )
    assert result.state_committed is True
    assert "commit_state:RUN_ID" in result.artifact_path.read_text(encoding="utf-8")
