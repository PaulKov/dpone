"""PostgreSQL -> BigQuery pair type-matrix builder."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from dpone.type_system.source_sink.certification_payloads import (
    matrix_entry_for_postgres_bigquery,
)

_DEFAULT_POSTGRES_BIGQUERY_TYPES: tuple[str, ...] = (
    "smallint",
    "integer",
    "bigint",
    "numeric(18,4)",
    "boolean",
    "real",
    "double precision",
    "date",
    "timestamp without time zone",
    "timestamp with time zone",
    "time without time zone",
    "character varying(255)",
    "text",
    "jsonb",
    "bytea",
    "uuid",
)


def build_postgres_bigquery_matrix(
    source: str,
    sink: str,
    source_types: Sequence[str] | None,
    *,
    split_source_spec: Callable[[str], tuple[str | None, str]],
) -> dict[str, Any]:
    """Build the explainable postgres→bigquery type matrix payload."""

    entries = []
    for source_spec in source_types or _DEFAULT_POSTGRES_BIGQUERY_TYPES:
        column, source_type = split_source_spec(source_spec)
        entries.append(matrix_entry_for_postgres_bigquery(column=column, source_type=source_type))
    return {
        "source": source,
        "sink": sink,
        "profile": "postgres_to_bigquery_analytics_v1",
        "entries": entries,
        "certification": {
            "suite": "postgres_to_bigquery",
            "artifact_names": [
                "type_matrix_decisions.json",
                "physical_ddl_plan.sql",
                "schema_evolution_rerun.json",
                "typed_reconciliation.json",
                "live_fixture_summary.md",
            ],
        },
        "runbook": "docs/source-sink/postgres-to-bigquery.md#schema-evolution-and-type-mapping",
    }


__all__ = ["build_postgres_bigquery_matrix"]
