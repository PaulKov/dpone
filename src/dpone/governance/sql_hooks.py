"""SQL hook provider implementations for runtime governance."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import Any

from dpone.governance.hooks import HookDefinition, HookExecutionContext


class SqlHookProvider:
    """Execute SQL hook actions through source/sink connector-like objects."""

    def __init__(self, connectors: Mapping[str, Any]) -> None:
        self._connectors = dict(connectors)

    def execute(self, action: HookDefinition, context: HookExecutionContext) -> Mapping[str, Any]:
        del context
        if not action.sql:
            raise ValueError(f"sql hook {action.id} requires sql")
        connector = self._connectors.get(action.connector)
        if connector is None:
            raise ValueError(f"sql hook {action.id} connector {action.connector!r} is not available")
        _execute_sql(connector, action.sql, autocommit=action.autocommit)
        return {"connector": action.connector, "sql_hash": _stable_hash(action.sql)}


def _execute_sql(connector: Any, sql: str, *, autocommit: bool) -> None:
    for method_name in ("execute_query", "execute", "run"):
        method = getattr(connector, method_name, None)
        if callable(method):
            _call_sql_method(method, sql, autocommit=autocommit)
            return
    connection = getattr(connector, "connection", None)
    execute = getattr(connection, "execute", None)
    if callable(execute):
        _call_sql_method(execute, sql, autocommit=autocommit)
        return
    raise ValueError("sql hook connector does not expose execute_query/execute/run")


def _call_sql_method(method: Any, sql: str, *, autocommit: bool) -> None:
    if _accepts_autocommit(method):
        method(sql, autocommit=autocommit)
        return
    method(sql)


def _accepts_autocommit(method: Any) -> bool:
    try:
        parameters = inspect.signature(method).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD or parameter.name == "autocommit" for parameter in parameters
    )


def _stable_hash(value: str) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()
