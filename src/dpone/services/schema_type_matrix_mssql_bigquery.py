"""MSSQL -> BigQuery pair type-matrix builder."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from dpone.type_system.source_sink.certification_payloads import matrix_entry_for_mssql_bigquery

_DEFAULT_MSSQL_BIGQUERY_TYPES: tuple[str, ...] = (
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "decimal(18,4)",
    "bit",
    "real",
    "float",
    "date",
    "datetime2(3)",
    "datetimeoffset(6)",
    "time(0)",
    "nvarchar(255)",
    "nvarchar(max)",
    "uniqueidentifier",
    "varbinary(16)",
)


def build_mssql_bigquery_matrix(
    source: str,
    sink: str,
    source_types: Sequence[str] | None,
    *,
    split_source_spec: Callable[[str], tuple[str | None, str]],
) -> dict[str, Any]:
    """Build the explainable mssql→bigquery type matrix payload."""

    entries = []
    for source_spec in source_types or _DEFAULT_MSSQL_BIGQUERY_TYPES:
        column, source_type = split_source_spec(source_spec)
        entries.append(matrix_entry_for_mssql_bigquery(column=column, source_type=source_type))
    return {
        "source": source,
        "sink": sink,
        "profile": "mssql_to_bigquery_analytics_v1",
        "entries": entries,
        "certification": {
            "suite": "mssql_to_bigquery",
            "artifact_names": [
                "type_matrix_decisions.json",
                "physical_ddl_plan.sql",
                "schema_evolution_rerun.json",
                "typed_reconciliation.json",
                "live_fixture_summary.md",
            ],
        },
        "runbook": "docs/source-sink/mssql-to-bigquery.md#schema-evolution-and-type-mapping",
    }


__all__ = ["build_mssql_bigquery_matrix"]
