from __future__ import annotations

import os
from typing import Any

import pytest

from dpone.strategy_intelligence.live_backends import KafkaLiveReplayBackend, MssqlReplayBackend
from dpone.strategy_intelligence.replay import ReplayExecutionRequest
from dpone.strategy_intelligence.replay_adapters import DbReplayAdapter, KafkaReplayAdapter

pytestmark = [pytest.mark.integration, pytest.mark.integration_replay]


def _enabled() -> bool:
    return os.getenv("DPONE_RUN_INTEGRATION_REPLAY") == "1"


@pytest.mark.skipif(not _enabled(), reason="Set DPONE_RUN_INTEGRATION_REPLAY=1 to run replay integration gate.")
def test_mssql_replay_backend_runs_full_adapter_sequence_with_injected_client() -> None:
    client = _RecordingSqlClient(row_count=10_000)
    request = ReplayExecutionRequest(
        action="resync",
        run_id="01JREPLAY000000000000000001",
        source_type="postgres",
        sink_type="mssql",
        strategy_mode="incremental_merge",
        yes=True,
    )

    result = DbReplayAdapter(
        backend=MssqlReplayBackend(
            client=client,
            target_schema="dbo",
            target_table="orders",
            staging_schema="staging",
        )
    ).execute(request)

    assert result.status == "executed"
    assert result.state_committed is True
    assert result.operations == (
        "validate_staging:incremental_merge",
        "execute_finalizer:mssql:incremental_merge",
        "reconcile:mssql:incremental_merge",
        "commit_state:01JREPLAY000000000000000001",
    )
    assert client.exists_checks == [("staging", "orders__replay_staging")]
    assert "BEGIN TRANSACTION" in client.executed_statements
    assert any(statement.startswith("DELETE FROM [dbo].[orders]") for statement in client.executed_statements)
    assert any(statement.startswith("INSERT INTO [dbo].[orders]") for statement in client.executed_statements)
    assert "COMMIT TRANSACTION" in client.executed_statements
    assert any("etl_state.__dpone__loads" in statement for statement in client.executed_statements)


@pytest.mark.skipif(not _enabled(), reason="Set DPONE_RUN_INTEGRATION_REPLAY=1 to run replay integration gate.")
def test_kafka_replay_backend_runs_full_adapter_sequence_with_injected_client() -> None:
    client = _RecordingKafkaClient()
    request = ReplayExecutionRequest(
        action="resume",
        run_id="01JREPLAY000000000000000002",
        source_type="postgres",
        sink_type="kafka",
        strategy_mode="incremental_merge",
        yes=True,
    )

    result = KafkaReplayAdapter(backend=KafkaLiveReplayBackend(client=client, topic="dwh.orders")).execute(request)

    assert result.status == "executed"
    assert result.state_committed is True
    assert result.operations == (
        "validate_staging:incremental_merge",
        "produce_replay_events:kafka:incremental_merge",
        "reconcile:kafka:incremental_merge",
        "commit_state:01JREPLAY000000000000000002",
    )
    assert client.produced == [
        (
            "dwh.orders",
            {
                "op": "replay",
                "run_id": "01JREPLAY000000000000000002",
                "strategy": "incremental_merge",
                "partitions": [],
            },
        )
    ]
    assert client.flushed is True
    assert client.scalar_statements == [
        "kafka_reconcile:01JREPLAY000000000000000002",
        "state_commit:01JREPLAY000000000000000002",
    ]


class _RecordingSqlClient:
    def __init__(self, row_count: int) -> None:
        self.row_count = row_count
        self.exists_checks: list[tuple[str, str]] = []
        self.executed_statements: list[str] = []

    def exists(self, schema: str, table: str) -> bool:
        self.exists_checks.append((schema, table))
        return True

    def scalar(self, statement: str) -> Any:
        self.executed_statements.append(statement)
        return self.row_count

    def execute(self, statement: str) -> None:
        self.executed_statements.append(statement)


class _RecordingKafkaClient:
    def __init__(self) -> None:
        self.produced: list[tuple[str, dict[str, Any]]] = []
        self.flushed = False
        self.scalar_statements: list[str] = []

    def produce(self, topic: str, value: dict[str, Any]) -> None:
        self.produced.append((topic, value))

    def flush(self) -> None:
        self.flushed = True

    def scalar(self, statement: str) -> int:
        self.scalar_statements.append(statement)
        return 1
