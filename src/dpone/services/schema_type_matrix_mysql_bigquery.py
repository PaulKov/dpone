"""MySQL -> BigQuery pair type-matrix builder."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from dpone.type_system.source_sink.certification import matrix_entry_for_mysql_bigquery

_DEFAULT_MYSQL_BIGQUERY_TYPES: tuple[str, ...] = (
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "decimal(18,4)",
    "tinyint(1)",
    "float",
    "double",
    "date",
    "datetime",
    "timestamp",
    "time",
    "varchar(255)",
    "text",
    "json",
    "blob",
)


def build_mysql_bigquery_matrix(
    source: str,
    sink: str,
    source_types: Sequence[str] | None,
    *,
    split_source_spec: Callable[[str], tuple[str | None, str]],
) -> dict[str, Any]:
    """Build the explainable mysql→bigquery type matrix payload."""

    entries = []
    for source_spec in source_types or _DEFAULT_MYSQL_BIGQUERY_TYPES:
        column, source_type = split_source_spec(source_spec)
        entries.append(matrix_entry_for_mysql_bigquery(column=column, source_type=source_type))
    return {
        "source": source,
        "sink": sink,
        "profile": "mysql_to_bigquery_analytics_v1",
        "entries": entries,
        "certification": {
            "suite": "mysql_to_bigquery",
            "artifact_names": [
                "type_matrix_decisions.json",
                "physical_ddl_plan.sql",
                "schema_evolution_rerun.json",
                "typed_reconciliation.json",
                "live_fixture_summary.md",
            ],
        },
        "runbook": "docs/source-sink/mysql-to-bigquery.md#schema-evolution-and-type-mapping",
    }


__all__ = ["build_mysql_bigquery_matrix"]
