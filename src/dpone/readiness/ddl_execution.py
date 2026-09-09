"""Governed DDL execution adapters for online schema evolution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dpone.readiness.ddl_governance import GovernedDdlAction, GovernedSchemaPlan


class OnlineDdlAdapter(Protocol):
    def execute(self, *, connector: object, action: GovernedDdlAction) -> None: ...


@dataclass(frozen=True, slots=True)
class SqlOnlineDdlAdapter:
    """Executes governed SQL DDL through a connector exposing execute_query."""

    def execute(self, *, connector: object, action: GovernedDdlAction) -> None:
        if action.decision != "apply":
            return
        execute_query = getattr(connector, "execute_query", None)
        if execute_query is None:
            raise TypeError("online DDL connector must expose execute_query(statement)")
        for statement in action.ddl:
            execute_query(statement)


class PostgresOnlineDdlAdapter(SqlOnlineDdlAdapter):
    """PostgreSQL online DDL adapter."""


class MssqlOnlineDdlAdapter(SqlOnlineDdlAdapter):
    """MSSQL online DDL adapter."""


class ClickHouseOnlineDdlAdapter(SqlOnlineDdlAdapter):
    """ClickHouse online DDL adapter."""


class BigQuerySchemaUpdateAdapter(SqlOnlineDdlAdapter):
    """BigQuery schema update adapter through existing connector query/API facade."""


@dataclass(frozen=True, slots=True)
class KafkaSchemaRegistryCompatibilityAdapter:
    """Validates Kafka schema compatibility instead of table DDL."""

    def execute(self, *, connector: object, action: GovernedDdlAction) -> None:
        if action.decision != "apply":
            return
        check = getattr(connector, "check_schema_compatibility", None)
        if check is not None:
            check(action.to_dict())


class GovernedDdlExecutor:
    """Applies governed DDL actions with a sink-specific adapter."""

    def __init__(self, adapter: OnlineDdlAdapter | None = None) -> None:
        self.adapter = adapter

    def apply(self, *, connector: object, governed_plan: GovernedSchemaPlan) -> None:
        adapter = self.adapter or _adapter_for_dialect(governed_plan.dialect)
        for action in governed_plan.actions:
            adapter.execute(connector=connector, action=action)


def _adapter_for_dialect(dialect: str) -> OnlineDdlAdapter:
    if dialect == "postgres":
        return PostgresOnlineDdlAdapter()
    if dialect == "mssql":
        return MssqlOnlineDdlAdapter()
    if dialect == "clickhouse":
        return ClickHouseOnlineDdlAdapter()
    if dialect == "bigquery":
        return BigQuerySchemaUpdateAdapter()
    if dialect == "kafka":
        return KafkaSchemaRegistryCompatibilityAdapter()
    return SqlOnlineDdlAdapter()
