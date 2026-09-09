"""Connectors for runtime execution.

IMPORTANT
---------
Some connectors require optional heavyweight dependencies (e.g. `google-cloud-bigquery`).
To keep lightweight CLI tooling usable in minimal environments, this package exposes
symbols **lazily**.

Use explicit submodules for strict imports:
- :mod:`dpone.runtime.connectors.postgres`
- :mod:`dpone.runtime.connectors.bigquery`
- :mod:`dpone.runtime.connectors.clickhouse`

The package-level imports are safe in environments without optional deps, as long as
you don't access the missing connector symbols.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "AbstractConnector",
    "PostgresConnector",
    "BigQueryConnector",
    "ClickHouseConnector",
    "MSSQLConnector",
    "MySQLConnector",
    "KafkaConnector",
    "CSVLoadConfig",
]

# name -> "module:attribute"
_EXPORTS: dict[str, str] = {
    "AbstractConnector": "dpone.runtime.connectors.base:AbstractConnector",
    "PostgresConnector": "dpone.runtime.connectors.postgres:PostgresConnector",
    "ClickHouseConnector": "dpone.runtime.connectors.clickhouse:ClickHouseConnector",
    "MSSQLConnector": "dpone.runtime.connectors.mssql:MSSQLConnector",
    "MySQLConnector": "dpone.runtime.connectors.mysql:MySQLConnector",
    "KafkaConnector": "dpone.runtime.connectors.kafka:KafkaConnector",
    "BigQueryConnector": "dpone.runtime.connectors.bigquery:BigQueryConnector",
    "CSVLoadConfig": "dpone.runtime.connectors.bigquery:CSVLoadConfig",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    mod = import_module(module_name)
    value = getattr(mod, attr)
    globals()[name] = value  # cache
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))
