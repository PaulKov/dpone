"""Read-only ClickHouse cost evidence adapter."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any


class ClickHouseCostProbe:
    """Collects table/query cost metrics without persisting query text or secrets."""

    def collect(self, connection: Mapping[str, Any]) -> dict[str, Any]:
        database = str(connection.get("database") or "")
        table = str(connection.get("table") or connection.get("cost_table") or "")
        if not database or not table:
            return {"metrics": {}, "blockers": ["data_product_cost.clickhouse_table_required"], "warnings": []}
        try:
            client = import_module("clickhouse_connect").get_client(**_client_kwargs(connection))
            rows, bytes_ = _table_metrics(client, database, table)
            duration_ms, read_bytes = _query_metrics(client, database, table)
        except Exception as exc:  # pragma: no cover - defensive adapter boundary
            return {
                "metrics": {},
                "blockers": [f"data_product_cost.clickhouse_probe_failed:{type(exc).__name__}"],
                "warnings": [],
            }
        return {
            "metrics": {
                "table_rows": rows,
                "table_bytes": bytes_,
                "query_duration_ms": duration_ms,
                "query_bytes_read": read_bytes,
            },
            "blockers": [],
            "warnings": [],
        }


def _client_kwargs(connection: Mapping[str, Any]) -> dict[str, Any]:
    allowed = ("host", "port", "username", "user", "password", "database", "secure", "verify")
    return {key: connection[key] for key in allowed if key in connection}


def _table_metrics(client: Any, database: str, table: str) -> tuple[int, int]:
    result = client.query(
        f"""
        SELECT sum(rows) AS rows, sum(bytes_on_disk) AS bytes
        FROM system.parts
        WHERE active AND database = {_quote(database)} AND table = {_quote(table)}
        """
    )
    row = _first_row(result)
    return (int(row[0] or 0), int(row[1] or 0)) if row else (0, 0)


def _query_metrics(client: Any, database: str, table: str) -> tuple[int, int]:
    result = client.query(
        f"""
        SELECT max(query_duration_ms) AS duration_ms, sum(read_bytes) AS read_bytes
        FROM system.query_log
        WHERE event_date >= today() - 1
          AND current_database = {_quote(database)}
          AND has(tables, concat({_quote(database)}, '.', {_quote(table)}))
        """
    )
    row = _first_row(result)
    return (int(row[0] or 0), int(row[1] or 0)) if row else (0, 0)


def _first_row(result: Any) -> tuple[Any, ...] | None:
    rows = getattr(result, "result_rows", None) or []
    return tuple(rows[0]) if rows else None


def _quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


__all__ = ["ClickHouseCostProbe"]
