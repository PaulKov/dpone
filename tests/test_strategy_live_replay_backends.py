from __future__ import annotations

from dataclasses import dataclass

from dpone.strategy_intelligence.live_backends import (
    ClickHouseReplayBackend,
    KafkaLiveReplayBackend,
    MssqlReplayBackend,
    PostgresReplayBackend,
)
from dpone.strategy_intelligence.replay import ReplayExecutionRequest
from dpone.strategy_intelligence.replay_adapters import DbReplayAdapter, KafkaReplayAdapter


@dataclass
class _SqlClient:
    staging_exists: bool = True
    reconcile_passed: bool = True

    def __post_init__(self) -> None:
        self.executed: list[str] = []
        self.scalars: list[str] = []

    def exists(self, schema: str, table: str) -> bool:
        self.scalars.append(f"exists:{schema}.{table}")
        return self.staging_exists

    def execute(self, statement: str) -> None:
        self.executed.append(statement)

    def scalar(self, statement: str) -> int:
        self.scalars.append(statement)
        return 1 if self.reconcile_passed else 0


class _KafkaClient:
    def __init__(self, *, reconcile_passed: bool = True) -> None:
        self.produced: list[tuple[str, dict]] = []
        self.executed: list[str] = []
        self._reconcile_passed = reconcile_passed

    def produce(self, topic: str, value: dict) -> None:
        self.produced.append((topic, value))

    def flush(self) -> None:
        self.executed.append("flush")

    def scalar(self, statement: str) -> int:
        self.executed.append(statement)
        return 1 if self._reconcile_passed else 0


def _request(*, sink: str, strategy: str = "partition_replace") -> ReplayExecutionRequest:
    return ReplayExecutionRequest(
        action="resync",
        run_id="RUN_ID",
        source_type="postgres",
        sink_type=sink,
        strategy_mode=strategy,
        partitions=("2026-01-01",),
        yes=True,
    )


def test_mssql_live_backend_executes_partition_replace_transaction_and_commits_state_after_reconcile() -> None:
    client = _SqlClient()
    backend = MssqlReplayBackend(
        client=client, target_schema="landing", target_table="orders", staging_schema="staging"
    )

    result = DbReplayAdapter(backend=backend).execute(_request(sink="mssql"))

    assert result.status == "executed"
    assert result.state_committed is True
    assert any("ALTER TABLE" in statement and "SWITCH" in statement for statement in client.executed)
    assert client.executed[-1].startswith("UPDATE etl_state.__dpone__loads")


def test_postgres_live_backend_uses_partition_exchange_or_delete_insert_fallback() -> None:
    client = _SqlClient()
    backend = PostgresReplayBackend(
        client=client, target_schema="public", target_table="orders", staging_schema="staging"
    )

    result = DbReplayAdapter(backend=backend).execute(_request(sink="postgres", strategy="incremental_merge"))

    assert result.status == "executed"
    assert any("DELETE FROM" in statement for statement in client.executed)
    assert any("INSERT INTO" in statement for statement in client.executed)


def test_clickhouse_live_backend_uses_replace_partition_for_partition_replace() -> None:
    client = _SqlClient()
    backend = ClickHouseReplayBackend(
        client=client, target_schema="analytics", target_table="orders", staging_schema="staging"
    )

    result = DbReplayAdapter(backend=backend).execute(_request(sink="clickhouse"))

    assert result.status == "executed"
    assert any("REPLACE PARTITION" in statement for statement in client.executed)


def test_live_backend_does_not_commit_state_when_reconciliation_fails() -> None:
    client = _SqlClient(reconcile_passed=False)
    backend = MssqlReplayBackend(
        client=client, target_schema="landing", target_table="orders", staging_schema="staging"
    )

    result = DbReplayAdapter(backend=backend).execute(_request(sink="mssql"))

    assert result.status == "failed"
    assert result.state_committed is False
    assert not any(statement.startswith("UPDATE etl_state.__dpone__loads") for statement in client.executed)


def test_kafka_live_backend_produces_replay_event_and_commits_state_after_reconcile() -> None:
    client = _KafkaClient()
    backend = KafkaLiveReplayBackend(client=client, topic="orders.replay")

    result = KafkaReplayAdapter(backend=backend).execute(_request(sink="kafka", strategy="incremental_merge"))

    assert result.status == "executed"
    assert client.produced == [
        (
            "orders.replay",
            {
                "op": "replay",
                "run_id": "RUN_ID",
                "strategy": "incremental_merge",
                "partitions": ["2026-01-01"],
            },
        )
    ]
    assert "flush" in client.executed
    assert any(statement.startswith("state_commit:") for statement in client.executed)
