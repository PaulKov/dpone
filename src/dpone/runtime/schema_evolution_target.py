"""Target identity helpers shared by schema-evolution runtime paths."""

from __future__ import annotations

from typing import Any

from dpone.contracts.mssql_object_name import MSSQLObjectName


def sink_dialect(sink: Any) -> str:
    """Return the schema-planner dialect for a runtime sink."""

    name = sink.__class__.__name__.lower()
    for dialect in ("mssql", "postgres", "clickhouse", "bigquery"):
        if dialect in name:
            return dialect
    return "postgres"


def qualified_target(load_config: Any, sink: Any) -> str:
    """Resolve the exact target identity used by planning, DDL, and evidence."""

    dialect = sink_dialect(sink)
    if dialect == "mssql":
        return MSSQLObjectName.from_parts(
            database=getattr(load_config, "target_database", None),
            schema=load_config.target_schema,
            table=load_config.target_table,
        ).dataset
    if dialect == "bigquery":
        project_id = getattr(getattr(sink, "connector", None), "project_id", None)
        if project_id:
            return f"{project_id}.{load_config.target_schema}.{load_config.target_table}"
    return f"{load_config.target_schema}.{load_config.target_table}"


__all__ = ["qualified_target", "sink_dialect"]
