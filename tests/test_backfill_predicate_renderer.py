from __future__ import annotations

from dpone.backfill.models import chunk_spec_from_options
from dpone.backfill.planner import plan_chunks
from dpone.backfill.predicates import BackfillPredicateRenderer


def _chunk(kind: str = "date"):
    if kind == "integer":
        spec = chunk_spec_from_options({"chunk": {"column": "order_id", "from": "1", "to": "10", "step": "5"}})
    elif kind == "timestamp":
        spec = chunk_spec_from_options(
            {
                "chunk": {
                    "column": "created_at",
                    "from": "2025-01-01T00:00:00",
                    "to": "2025-01-02T00:00:00",
                    "step": "12h",
                }
            }
        )
    else:
        spec = chunk_spec_from_options(
            {"chunk": {"column": "business_date", "from": "2025-01-01", "to": "2025-01-03", "step": "1d"}}
        )
    assert spec is not None
    return spec, plan_chunks(spec, run_key="k")[0]


def test_clickhouse_renderer_quotes_column_and_types_date_literals() -> None:
    spec, chunk = _chunk()

    predicate = BackfillPredicateRenderer.for_dialect("clickhouse").render(spec, chunk)

    assert predicate == "`business_date` >= toDate('2025-01-01') AND `business_date` < toDate('2025-01-02')"


def test_mssql_renderer_quotes_column_and_types_timestamp_literals() -> None:
    spec, chunk = _chunk("timestamp")

    predicate = BackfillPredicateRenderer.for_dialect("mssql").render(spec, chunk)

    assert (
        predicate == "[created_at] >= CAST('2025-01-01 00:00:00' AS datetime2) "
        "AND [created_at] < CAST('2025-01-01 12:00:00' AS datetime2)"
    )


def test_postgres_renderer_quotes_column_and_keeps_integer_literals_numeric() -> None:
    spec, chunk = _chunk("integer")

    predicate = BackfillPredicateRenderer.for_dialect("postgres").render(spec, chunk)

    assert predicate == '"order_id" >= 1 AND "order_id" <= 5'


def test_generic_renderer_is_compatibility_only_and_not_planner_authority() -> None:
    spec, chunk = _chunk()

    predicate = BackfillPredicateRenderer.for_dialect("unknown").render(spec, chunk)

    assert predicate == "business_date >= '2025-01-01' AND business_date < '2025-01-02'"
    assert "predicate" not in chunk.to_jsonable()
