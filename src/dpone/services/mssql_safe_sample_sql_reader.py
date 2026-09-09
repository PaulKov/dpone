"""MSSQL safe-sample SQL reader adapter.

This module builds a secret-free, bounded read plan from a certified copy
request and delegates physical SQL execution to an injected client. Runtime
code owns database-driver selection and credential handling.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.services.safe_sample_execution_common import MssqlSafeSampleBatch, non_negative_int, redact_mapping


@dataclass(frozen=True, slots=True)
class MssqlSafeSampleReadPlan:
    """Secret-free MSSQL read plan for a bounded safe sample."""

    sql: str
    parameters: Mapping[str, Any]
    sample_rows: int
    max_bytes: int
    timeout_seconds: int
    source_read_only: bool
    source: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.mssql-safe-sample-read-plan.v1",
            "sql": self.sql,
            "parameters": dict(self.parameters),
            "sample_rows": self.sample_rows,
            "max_bytes": self.max_bytes,
            "timeout_seconds": self.timeout_seconds,
            "source_read_only": self.source_read_only,
            "source": dict(self.source),
        }


class MssqlSafeSampleSqlClient(Protocol):
    """Execute a secret-free MSSQL safe-sample read plan."""

    def fetch(self, plan: MssqlSafeSampleReadPlan) -> Mapping[str, Any]:
        """Return rows, byte count, and safe diagnostics for ``plan``."""


class MssqlSafeSampleSqlReader:
    """Build a bounded MSSQL read plan and delegate execution to an injected client."""

    def __init__(self, *, client: MssqlSafeSampleSqlClient) -> None:
        self._client = client

    def read(self, request: dict[str, Any]) -> MssqlSafeSampleBatch:
        plan = _mssql_read_plan(request)
        result = dict(self._client.fetch(plan))
        rows = _rows(result.get("rows"))
        client_diagnostics = redact_mapping(result.get("diagnostics"))
        diagnostics = {
            "query_template": "mssql_top_sample_v1",
            "source_table": dict(_source_table(plan.source)),
        }
        if client_diagnostics:
            diagnostics["client"] = client_diagnostics
        return MssqlSafeSampleBatch(
            rows=rows,
            rows_read=non_negative_int(result.get("rows_read")) or len(rows),
            bytes_read=non_negative_int(result.get("bytes_read")),
            diagnostics=diagnostics,
        )


def _mssql_read_plan(request: Mapping[str, Any]) -> MssqlSafeSampleReadPlan:
    source = _source_section(request)
    if source.get("type") != "mssql":
        raise ValueError("MSSQL safe sample reader requires source.type=mssql")
    if request.get("source_read_only") is not True:
        raise ValueError("MSSQL safe sample reader requires source_read_only=true")
    table = _source_table(source)
    sample_rows = non_negative_int(request.get("sample_rows"))
    max_bytes = non_negative_int(request.get("max_bytes"))
    timeout_seconds = non_negative_int(request.get("timeout_seconds"))
    if not table["name"]:
        raise ValueError("MSSQL safe sample reader requires source.table.name")
    if sample_rows <= 0 or max_bytes <= 0 or timeout_seconds <= 0:
        raise ValueError("MSSQL safe sample reader requires positive row, byte, and timeout budgets")
    return MssqlSafeSampleReadPlan(
        sql=f"SELECT TOP (@sample_rows) * FROM {_mssql_table_name(table)}",
        parameters={"sample_rows": sample_rows},
        sample_rows=sample_rows,
        max_bytes=max_bytes,
        timeout_seconds=timeout_seconds,
        source_read_only=True,
        source=source,
    )


def _source_section(request: Mapping[str, Any]) -> dict[str, Any]:
    source = request.get("source")
    return dict(source) if isinstance(source, Mapping) else {}


def _source_table(source: Mapping[str, Any]) -> dict[str, str]:
    table = source.get("table")
    if not isinstance(table, Mapping):
        return {"schema": "", "name": ""}
    return {"schema": str(table.get("schema") or ""), "name": str(table.get("name") or "")}


def _mssql_table_name(table: Mapping[str, str]) -> str:
    schema = str(table.get("schema") or "")
    name = str(table.get("name") or "")
    if schema:
        return f"{_quote_mssql_identifier(schema)}.{_quote_mssql_identifier(name)}"
    return _quote_mssql_identifier(name)


def _quote_mssql_identifier(identifier: str) -> str:
    return f"[{identifier.replace(']', ']]')}]"


def _rows(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


__all__ = ["MssqlSafeSampleReadPlan", "MssqlSafeSampleSqlClient", "MssqlSafeSampleSqlReader"]
