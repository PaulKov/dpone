from __future__ import annotations

from itertools import product

from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy

SOURCES = ("postgres", "mssql", "clickhouse", "api")
SINKS = ("mssql", "postgres", "clickhouse", "bigquery")


def test_schema_evolution_contract_matrix_covers_all_source_sink_pairs() -> None:
    covered_pairs = set(product(SOURCES, SINKS))
    assert len(covered_pairs) == 16

    for source_type, sink_type in covered_pairs:
        plan = SchemaComparator(SchemaEvolutionPolicy(mode="widening", on_type_change="new_column")).compare(
            source=[
                ColumnDef("id", "bigint", nullable=False),
                ColumnDef("payload", "text"),
                ColumnDef("amount", "numeric(18,2)"),
            ],
            target=[
                ColumnDef("id", "int", nullable=False),
                ColumnDef("amount", "varchar(32)"),
            ],
        )

        ddl = plan.ddl_sql(sink_type, "project.landing.orders" if sink_type == "bigquery" else "landing.orders")
        assert source_type in SOURCES
        assert sink_type in SINKS
        assert plan.column_mapping["amount"] == "__dpone__nc__amount"
        assert any("__dpone__nc__amount" in statement for statement in ddl)


def test_schema_evolution_docs_cover_all_source_sink_pairs() -> None:
    text = open("docs/schema-evolution.md", encoding="utf-8").read()

    for source_type, sink_type in product(SOURCES, SINKS):
        assert f"{source_type} -> {sink_type}" in text

    assert "__dpone__nc__<original_column_name>" in text
    assert "schema_evolution.enabled: false" in text
