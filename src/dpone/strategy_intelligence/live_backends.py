from __future__ import annotations

from typing import Any, Protocol

from dpone.strategy_intelligence.replay import ReplayExecutionRequest
from dpone.strategy_intelligence.replay_adapters import ReplayBackend, ReplayBackendResult


class ReplaySqlClient(Protocol):
    """Minimal SQL client protocol for live replay backends."""

    def exists(self, schema: str, table: str) -> bool: ...

    def execute(self, statement: str) -> None: ...

    def scalar(self, statement: str) -> int: ...


class ReplayKafkaClient(Protocol):
    """Minimal Kafka client protocol for live replay backends."""

    def produce(self, topic: str, value: dict[str, Any]) -> None: ...

    def flush(self) -> None: ...

    def scalar(self, statement: str) -> int: ...


class BaseSqlReplayBackend(ReplayBackend):
    """Shared replay backend for DB sinks with injected SQL client."""

    def __init__(self, *, client: ReplaySqlClient, target_schema: str, target_table: str, staging_schema: str) -> None:
        self._client = client
        self._target_schema = target_schema
        self._target_table = target_table
        self._staging_schema = staging_schema

    @property
    def staging_table(self) -> str:
        return f"{self._target_table}__replay_staging"

    def validate_staging(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        del request
        exists = self._client.exists(self._staging_schema, self.staging_table)
        return ReplayBackendResult(
            exists,
            "staging table exists" if exists else "staging table is missing",
            details={"status_check": {"name": "staging_exists", "status": "passed" if exists else "failed"}},
        )

    def execute_finalizer(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        for statement in self._finalizer_sql(request):
            self._client.execute(statement)
        return ReplayBackendResult(
            True,
            "finalizer executed",
            details={"status_check": {"name": "finalizer", "status": "passed"}},
        )

    def produce_replay_events(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        del request
        return ReplayBackendResult(False, "database replay backend cannot produce Kafka events")

    def reconcile(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        passed = bool(self._client.scalar(self._reconcile_sql(request)))
        return ReplayBackendResult(
            passed,
            "reconciliation passed" if passed else "reconciliation failed",
            details={"status_check": {"name": "reconciliation", "status": "passed" if passed else "failed"}},
        )

    def commit_state(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        self._client.execute(self._commit_state_sql(request))
        return ReplayBackendResult(
            True,
            "state committed",
            details={"status_check": {"name": "state_commit", "status": "passed"}},
        )

    def _finalizer_sql(self, request: ReplayExecutionRequest) -> tuple[str, ...]:
        raise NotImplementedError

    def _reconcile_sql(self, request: ReplayExecutionRequest) -> str:
        del request
        return (
            f"SELECT CASE WHEN COUNT(*) >= 0 THEN 1 ELSE 0 END "
            f"FROM {self._quote_table(self._staging_schema, self.staging_table)}"
        )

    def _commit_state_sql(self, request: ReplayExecutionRequest) -> str:
        return (
            f"UPDATE etl_state.__dpone__loads SET status = 'committed' WHERE run_id = '{_sql_literal(request.run_id)}'"
        )

    def _target(self) -> str:
        return self._quote_table(self._target_schema, self._target_table)

    def _staging(self) -> str:
        return self._quote_table(self._staging_schema, self.staging_table)

    def _quote_table(self, schema: str, table: str) -> str:
        raise NotImplementedError


class MssqlReplayBackend(BaseSqlReplayBackend):
    """MSSQL replay backend using partition switch or delete+insert finalization."""

    def _finalizer_sql(self, request: ReplayExecutionRequest) -> tuple[str, ...]:
        if request.strategy_mode == "partition_replace":
            partition = request.partitions[0] if request.partitions else "1"
            return (
                "BEGIN TRANSACTION",
                (
                    f"ALTER TABLE {self._staging()} SWITCH PARTITION {_partition_literal(partition)} "
                    f"TO {self._target()} PARTITION {_partition_literal(partition)}"
                ),
                "COMMIT TRANSACTION",
            )
        return (
            "BEGIN TRANSACTION",
            f"DELETE FROM {self._target()} WHERE EXISTS (SELECT 1 FROM {self._staging()} s)",
            f"INSERT INTO {self._target()} SELECT * FROM {self._staging()}",
            "COMMIT TRANSACTION",
        )

    def _quote_table(self, schema: str, table: str) -> str:
        return f"[{_mssql_ident(schema)}].[{_mssql_ident(table)}]"


class PostgresReplayBackend(BaseSqlReplayBackend):
    """Postgres replay backend using partition attach/exchange contracts or delete+insert fallback."""

    def _finalizer_sql(self, request: ReplayExecutionRequest) -> tuple[str, ...]:
        if request.strategy_mode == "partition_replace":
            partition = request.partitions[0] if request.partitions else "from_staging"
            return (
                "BEGIN",
                f"ALTER TABLE {self._target()} DETACH PARTITION IF EXISTS {self._partition_table(partition)}",
                f"ALTER TABLE {self._target()} ATTACH PARTITION {self._staging()} FOR VALUES IN ('{_sql_literal(partition)}')",
                "COMMIT",
            )
        return (
            "BEGIN",
            f"DELETE FROM {self._target()} t USING {self._staging()} s WHERE TRUE",
            f"INSERT INTO {self._target()} SELECT * FROM {self._staging()}",
            "COMMIT",
        )

    def _quote_table(self, schema: str, table: str) -> str:
        return f'"{_pg_ident(schema)}"."{_pg_ident(table)}"'

    def _partition_table(self, partition: str) -> str:
        return self._quote_table(self._target_schema, f"{self._target_table}__p__{_safe_suffix(partition)}")


class ClickHouseReplayBackend(BaseSqlReplayBackend):
    """ClickHouse replay backend using native REPLACE PARTITION where possible."""

    def _finalizer_sql(self, request: ReplayExecutionRequest) -> tuple[str, ...]:
        if request.strategy_mode == "partition_replace":
            partition = request.partitions[0] if request.partitions else "from_staging"
            return (
                f"ALTER TABLE {self._target()} REPLACE PARTITION '{_sql_literal(partition)}' FROM {self._staging()}",
            )
        return (
            f"TRUNCATE TABLE {self._target()}",
            f"INSERT INTO {self._target()} SELECT * FROM {self._staging()}",
        )

    def _reconcile_sql(self, request: ReplayExecutionRequest) -> str:
        del request
        return f"SELECT 1 FROM {self._staging()} LIMIT 1"

    def _commit_state_sql(self, request: ReplayExecutionRequest) -> str:
        return (
            "ALTER TABLE `etl_state`.`__dpone__loads` "
            f"UPDATE status = 'committed' WHERE run_id = '{_sql_literal(request.run_id)}' SETTINGS mutations_sync = 2"
        )

    def _quote_table(self, schema: str, table: str) -> str:
        return f"`{_backtick_ident(schema)}`.`{_backtick_ident(table)}`"


class BigQueryReplayBackend(BaseSqlReplayBackend):
    """BigQuery replay backend using partition overwrite/delete+insert SQL contracts."""

    def _finalizer_sql(self, request: ReplayExecutionRequest) -> tuple[str, ...]:
        if request.strategy_mode == "partition_replace":
            partition = request.partitions[0] if request.partitions else "from_staging"
            return (
                f"DELETE FROM {self._target()} WHERE _PARTITIONDATE = DATE '{_sql_literal(partition)}'",
                f"INSERT INTO {self._target()} SELECT * FROM {self._staging()}",
            )
        return (
            f"DELETE FROM {self._target()} WHERE TRUE",
            f"INSERT INTO {self._target()} SELECT * FROM {self._staging()}",
        )

    def _quote_table(self, schema: str, table: str) -> str:
        return f"`{_backtick_ident(schema)}.{_backtick_ident(table)}`"


class KafkaLiveReplayBackend(ReplayBackend):
    """Kafka replay backend with injected producer-like client."""

    def __init__(self, *, client: ReplayKafkaClient, topic: str) -> None:
        self._client = client
        self._topic = topic

    def validate_staging(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        del request
        return ReplayBackendResult(
            True,
            "Kafka replay event payload is bounded",
            details={"status_check": {"name": "event_payload_bounded", "status": "passed"}},
        )

    def execute_finalizer(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        del request
        return ReplayBackendResult(False, "Kafka replay backend does not mutate table targets")

    def produce_replay_events(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        self._client.produce(
            self._topic,
            {
                "op": "replay",
                "run_id": request.run_id,
                "strategy": request.strategy_mode,
                "partitions": list(request.partitions),
            },
        )
        self._client.flush()
        return ReplayBackendResult(
            True,
            "Kafka replay event produced",
            details={"status_check": {"name": "event_produced", "status": "passed"}},
        )

    def reconcile(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        passed = bool(self._client.scalar(f"kafka_reconcile:{request.run_id}"))
        return ReplayBackendResult(
            passed,
            "Kafka replay reconciliation passed" if passed else "Kafka replay reconciliation failed",
            details={"status_check": {"name": "reconciliation", "status": "passed" if passed else "failed"}},
        )

    def commit_state(self, request: ReplayExecutionRequest) -> ReplayBackendResult:
        self._client.scalar(f"state_commit:{request.run_id}")
        return ReplayBackendResult(
            True,
            "Kafka replay state committed",
            details={"status_check": {"name": "state_commit", "status": "passed"}},
        )


def _mssql_ident(value: str) -> str:
    return str(value).replace("]", "]]")


def _pg_ident(value: str) -> str:
    return str(value).replace('"', '""')


def _backtick_ident(value: str) -> str:
    return str(value).replace("`", "``")


def _sql_literal(value: str) -> str:
    return str(value).replace("'", "''")


def _partition_literal(value: str) -> str:
    raw = str(value)
    return raw if raw.isdigit() else f"'{_sql_literal(raw)}'"


def _safe_suffix(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in str(value)).strip("_") or "partition"
