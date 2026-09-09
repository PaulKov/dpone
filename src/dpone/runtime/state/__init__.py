"""Runtime state storage.

IMPORTANT
---------
State storages are optional-runtime integrations. Importing this package must
not import BigQuery, PostgreSQL, MSSQL or Kafka client dependencies eagerly.

This package exposes its symbols **lazily** so that metadata-only tooling can
import dpone without optional runtime dependencies.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "XMinState",
    "XMinStateStorage",
    "RunState",
    "RunStateStatus",
    "RunStateStorage",
    "PostgresXMinStateStorage",
    "PostgresRunStateStorage",
    "PostgresLoadAuditStorage",
    "MSSQLXMinStateStorage",
    "MSSQLRunStateStorage",
    "MSSQLLoadAuditStorage",
    "BigQueryLoadAuditStorage",
    "ClickHouseLoadAuditStorage",
    "ClickHouseLoadStepAuditStorage",
    "ClickHouseBackfillStateStore",
    "MSSQLBackfillStateStore",
    "PostgresBackfillStateStore",
    "StateFactory",
    "MSSQLCDCOffsetStorage",
]

_EXPORTS: dict[str, str] = {
    "XMinState": "dpone.runtime.state.xmin_storage:XMinState",
    "XMinStateStorage": "dpone.runtime.state.xmin_storage:XMinStateStorage",
    "RunState": "dpone.runtime.state.models:RunState",
    "RunStateStatus": "dpone.runtime.state.models:RunStateStatus",
    "RunStateStorage": "dpone.runtime.state.run_state:RunStateStorage",
    "PostgresXMinStateStorage": "dpone.runtime.state.postgres:PostgresXMinStateStorage",
    "PostgresRunStateStorage": "dpone.runtime.state.postgres:PostgresRunStateStorage",
    "PostgresLoadAuditStorage": "dpone.runtime.state.postgres:PostgresLoadAuditStorage",
    "MSSQLXMinStateStorage": "dpone.runtime.state.mssql:MSSQLXMinStateStorage",
    "MSSQLRunStateStorage": "dpone.runtime.state.mssql:MSSQLRunStateStorage",
    "MSSQLLoadAuditStorage": "dpone.runtime.state.mssql:MSSQLLoadAuditStorage",
    "BigQueryLoadAuditStorage": "dpone.runtime.state.load_audit:BigQueryLoadAuditStorage",
    "ClickHouseLoadAuditStorage": "dpone.runtime.state.clickhouse:ClickHouseLoadAuditStorage",
    "ClickHouseLoadStepAuditStorage": "dpone.runtime.state.clickhouse:ClickHouseLoadStepAuditStorage",
    "ClickHouseBackfillStateStore": "dpone.backfill.sql_state:ClickHouseBackfillStateStore",
    "MSSQLBackfillStateStore": "dpone.backfill.sql_state:MSSQLBackfillStateStore",
    "PostgresBackfillStateStore": "dpone.backfill.sql_state:PostgresBackfillStateStore",
    "StateFactory": "dpone.runtime.state.factory:StateFactory",
    "MSSQLCDCOffsetStorage": "dpone.runtime.state.cdc:MSSQLCDCOffsetStorage",
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
