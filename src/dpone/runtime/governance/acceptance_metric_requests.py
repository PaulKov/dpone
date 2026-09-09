"""Build connector-aware requests for runtime acceptance metric probes."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from dpone.runtime.governance.acceptance_snapshot import (
    AcceptanceMetricRequest,
    business_columns,
    selected_columns,
)
from dpone.runtime.sink_dialect import is_mssql_dialect
from dpone.runtime.support.mssql_object_name import MSSQLObjectName


class AcceptanceMetricPolicyView(Protocol):
    """Minimal policy surface needed to construct a probe request."""

    row_count: bool
    null_counts: str | tuple[str, ...]
    distinct_counts: str | tuple[str, ...]


def build_acceptance_metric_request(
    policy: AcceptanceMetricPolicyView,
    *,
    side: str,
    load_config: Any,
    extract_result: Any,
    payload_schema: Sequence[tuple[str, str]] | Sequence[str],
    staged_handle: Any | None,
) -> AcceptanceMetricRequest:
    """Resolve a canonical relation or query identity for one metric side."""

    if side == "source":
        columns = business_columns(getattr(extract_result, "schema", ()) or ())
        sql = getattr(getattr(extract_result, "artifact", None), "sql", None)
        if isinstance(sql, str) and sql.strip():
            database = schema = table = None
            identity = f"source_query:{_hash(sql)}"
        else:
            sql = None
            database = str(getattr(load_config, "source_database", "") or "") or None
            schema = str(getattr(load_config, "source_schema", "") or "")
            table = str(getattr(load_config, "source_table", "") or "")
            database, schema, identity = _relation_identity(
                load_config,
                side="source",
                database=database,
                schema=schema,
                table=table,
            )
    else:
        relation = getattr(staged_handle, "staging_config", None) if side == "staged" else load_config
        schema_values = getattr(staged_handle, "payload_schema", ()) if side == "staged" else payload_schema
        columns = business_columns(schema_values or ())
        database = str(getattr(relation, "target_database", "") or "") or None
        schema = str(getattr(relation, "target_schema", "") or "")
        table = str(getattr(relation, "target_table", "") or "")
        database, schema, identity = _relation_identity(
            load_config,
            side="target",
            database=database,
            schema=schema,
            table=table,
        )
        sql = None
    return AcceptanceMetricRequest(
        side=side,
        columns=columns,
        dataset_identity=identity,
        database=database,
        schema=schema,
        table=table,
        sql=sql,
        sql_hash=_hash(sql) if sql else None,
        include_row_count=policy.row_count,
        null_count_columns=selected_columns(policy.null_counts, columns),
        distinct_count_columns=selected_columns(policy.distinct_counts, columns),
    )


def _relation_identity(
    load_config: Any,
    *,
    side: str,
    database: str | None,
    schema: str,
    table: str,
) -> tuple[str | None, str, str]:
    options = getattr(load_config, "options", None)
    options = options if isinstance(options, Mapping) else {}
    dialect = options.get("source_type" if side == "source" else "sink_type")
    if is_mssql_dialect(dialect):
        name = MSSQLObjectName.from_parts(database=database, schema=schema, table=table, strict=True)
        return name.database, name.schema, name.dataset
    return database, schema, ".".join(part for part in (schema, table) if part)


def _hash(value: str | None) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


__all__ = ["AcceptanceMetricPolicyView", "build_acceptance_metric_request"]
