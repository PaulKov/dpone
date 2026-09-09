"""Target-specific DDL executor contracts.

Executors expose a deterministic ``plan`` for tests and diagnostics, then use a
connector-like dependency for actual execution when provided.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dpone.readiness.physical_apply import DdlExecutionRequest


@dataclass(frozen=True, slots=True)
class DdlExecutionResult:
    statements: tuple[str, ...]
    executed: bool

    def to_dict(self) -> dict[str, object]:
        return {"statements": list(self.statements), "executed": self.executed}


class _ConnectorExecutor:
    def __init__(self, connector: Any | None = None) -> None:
        self._connector = connector

    def execute(self, request: DdlExecutionRequest) -> None:
        statements = self.plan(request)
        if self._connector is None:
            return
        for statement in statements:
            self._execute_statement(statement)

    def _execute_statement(self, statement: str) -> None:
        if hasattr(self._connector, "execute"):
            self._connector.execute(statement)
            return
        if hasattr(self._connector, "query"):
            self._connector.query(statement)
            return
        raise RuntimeError("DDL connector must expose execute() or query()")

    def plan(self, request: DdlExecutionRequest) -> tuple[str, ...]:
        raise NotImplementedError


class MSSQLDdlExecutor(_ConnectorExecutor):
    def __init__(self, connector: Any | None = None, *, lock_timeout_ms: int = 15000) -> None:
        super().__init__(connector)
        self.lock_timeout_ms = lock_timeout_ms

    def plan(self, request: DdlExecutionRequest) -> tuple[str, ...]:
        return (f"SET LOCK_TIMEOUT {self.lock_timeout_ms};", request.sql)


class PostgresDdlExecutor(_ConnectorExecutor):
    def __init__(
        self,
        connector: Any | None = None,
        *,
        lock_timeout_seconds: int = 15,
        statement_timeout_seconds: int = 300,
    ) -> None:
        super().__init__(connector)
        self.lock_timeout_seconds = lock_timeout_seconds
        self.statement_timeout_seconds = statement_timeout_seconds

    def plan(self, request: DdlExecutionRequest) -> tuple[str, ...]:
        return (
            f"SET lock_timeout = '{self.lock_timeout_seconds}s';",
            f"SET statement_timeout = '{self.statement_timeout_seconds}s';",
            request.sql,
        )


class ClickHouseDdlExecutor(_ConnectorExecutor):
    def __init__(self, connector: Any | None = None, *, mutations_sync: int = 2) -> None:
        super().__init__(connector)
        self.mutations_sync = mutations_sync

    def plan(self, request: DdlExecutionRequest) -> tuple[str, ...]:
        return (f"SET mutations_sync = {self.mutations_sync};", request.sql)


class BigQueryDdlExecutor(_ConnectorExecutor):
    def plan(self, request: DdlExecutionRequest) -> tuple[str, ...]:
        return (request.sql,)


class KafkaSchemaRegistryDdlExecutor:
    """Schema Registry compatibility executor for Kafka sink schema changes."""

    def __init__(self, client: Any | None = None) -> None:
        self._client = client

    def plan(self, request: DdlExecutionRequest) -> tuple[str, ...]:
        return (f"-- Schema Registry compatibility check for {request.table}", request.sql)

    def execute(self, request: DdlExecutionRequest) -> None:
        if self._client is None:
            return
        if hasattr(self._client, "test_compatibility"):
            compatible = self._client.test_compatibility(request.table, request.sql)
            if not compatible:
                raise RuntimeError("Kafka Schema Registry compatibility check failed")


def render_statements(executor: Any, request: DdlExecutionRequest) -> Sequence[str]:
    return tuple(executor.plan(request))


__all__ = [
    "BigQueryDdlExecutor",
    "ClickHouseDdlExecutor",
    "DdlExecutionResult",
    "KafkaSchemaRegistryDdlExecutor",
    "MSSQLDdlExecutor",
    "PostgresDdlExecutor",
    "render_statements",
]
