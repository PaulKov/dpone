"""ClickHouse migration operation execution adapter."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any


@dataclass(slots=True)
class ClickHouseMigrationOperationExecutor:
    """Execute reviewed migration pack operations through ClickHouse."""

    connector: Any

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        connector_factory: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> ClickHouseMigrationOperationExecutor:
        factory = connector_factory or _default_connector_factory
        return cls(factory(config))

    def execute(self, operation: dict[str, Any]) -> dict[str, Any]:
        sql = str(operation.get("sql", ""))
        if str(operation.get("operation_type", "sql")) == "validation":
            rows = self.connector.get_records(sql)
            return {"status": "executed", "result_rows": rows, "row_count": len(rows)}
        self.connector.execute_query(sql)
        return {"status": "executed"}

    def close(self) -> None:
        close = getattr(self.connector, "close", None)
        if callable(close):
            close()


__all__ = ["ClickHouseMigrationOperationExecutor"]


def _default_connector_factory(config: Mapping[str, Any]) -> Any:
    module = import_module("dpone.runtime.connectors.clickhouse")
    connector_cls = module.ClickHouseConnector
    return connector_cls(
        host=str(config.get("host", "127.0.0.1")),
        port=int(config.get("port", 9000)),
        database=str(config.get("database", "default")),
        user=str(config.get("user", config.get("username", "default"))),
        password=str(config.get("password", "")),
        secure=bool(config.get("secure", False)),
        compression=bool(config.get("compression", True)),
    )
