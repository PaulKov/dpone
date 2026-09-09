from __future__ import annotations

import pytest

from dpone.commands.schema_plan_cmd import _render_schema_explain_text, _render_type_matrix_md
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.schema_evolution import ColumnDef, SchemaEvolutionPolicy
from dpone.services.schema_evolution_explain import SchemaEvolutionExplainService
from dpone.type_system.models import ColumnProfile
from dpone.type_system.source_sink.certification import (
    TypeCertificationSuiteRegistry,
    assert_certification_suite_matches_runtime_decisions,
)

pytestmark = pytest.mark.type_matrix_certification


def test_mssql_clickhouse_certification_suite_covers_practical_type_families() -> None:
    suite = TypeCertificationSuiteRegistry.default().suite("mssql", "clickhouse")

    by_name = {case.name: case for case in suite.cases}

    assert by_name["int_nullable"].expected_target_type == "Nullable(Int32)"
    assert by_name["nvarchar_nullable"].expected_target_type == "Nullable(String)"
    assert by_name["datetime_epoch"].expected_target_type == "DateTime64(3)"
    assert by_name["datetime2_7_epoch"].expected_target_type == "DateTime64(7)"
    assert by_name["datetimeoffset_preserve_offset"].expected_target_type == "DateTime64(7, 'UTC')"
    assert by_name["rowversion_hex"].expected_target_type == "String"
    assert {case.decision_category for case in suite.cases} >= {
        "auto_inferred",
        "incompatible_requires_policy",
    }


def test_postgres_mssql_certification_suite_covers_practical_type_families() -> None:
    suite = TypeCertificationSuiteRegistry.default().suite("postgres", "mssql")

    by_name = {case.name: case for case in suite.cases}

    assert by_name["integer"].expected_target_type == "int"
    assert by_name["jsonb"].expected_target_type == "nvarchar(max)"
    assert by_name["timestamptz"].expected_target_type == "datetimeoffset(6)"
    assert by_name["array_contract_required"].expected_target_type == "nvarchar(max)"
    assert by_name["enum_contract_required"].decision_category == "incompatible_requires_policy"


def test_clickhouse_mssql_certification_suite_covers_event_landing_type_families() -> None:
    suite = TypeCertificationSuiteRegistry.default().suite("clickhouse", "mssql")

    by_name = {case.name: case for case in suite.cases}

    assert by_name["uint64"].expected_target_type == "decimal(20,0)"
    assert by_name["datetime64_3"].expected_target_type == "datetime2(3)"
    assert by_name["string_nullable"].expected_target_type == "nvarchar(max)"
    assert by_name["decimal_18_4"].expected_target_type == "decimal(18,4)"
    assert by_name["array_contract_required"].decision_category == "incompatible_requires_policy"
    assert by_name["datetime64_9_contract_required"].lossless is False


def test_certification_suites_match_runtime_type_matrix_decisions() -> None:
    registry = TypeCertificationSuiteRegistry.default()

    assert_certification_suite_matches_runtime_decisions(registry.suite("mssql", "clickhouse"))
    assert_certification_suite_matches_runtime_decisions(registry.suite("postgres", "mssql"))
    assert_certification_suite_matches_runtime_decisions(registry.suite("clickhouse", "mssql"))


def test_type_matrix_service_exposes_decision_category_and_source() -> None:
    suite = TypeCertificationSuiteRegistry.default().build_matrix_payload("mssql", "clickhouse")

    int_entry = next(entry for entry in suite["entries"] if entry["source_type"] == "int nullable")

    assert int_entry["decision_category"] == "auto_inferred"
    assert int_entry["decision_source"] == "source_metadata"
    assert int_entry["canonical_type"] == "integer"


def test_schema_explain_uses_certified_matrix_categories_for_expected_mappings() -> None:
    payload = SchemaEvolutionExplainService().explain(
        source_system="mssql",
        sink_system="clickhouse",
        source=[
            ColumnDef("doc_movement_id", "int", nullable=True),
            ColumnDef("dm_base_zone_name", "nvarchar(510)", nullable=True),
            ColumnDef("created_at", "datetime", nullable=True),
        ],
        target=[
            ColumnDef("doc_movement_id", "Nullable(Int32)", nullable=True),
            ColumnDef("dm_base_zone_name", "Nullable(String)", nullable=True),
            ColumnDef("created_at", "Nullable(DateTime64(3))", nullable=True),
        ],
        policy=SchemaEvolutionPolicy(),
    )

    decisions = {item["column"]: item for item in payload["type_decisions"]}

    assert decisions["doc_movement_id"]["matrix_matches_target"] is True
    assert payload["schema_plan"]["changes"] == []
    assert payload["has_breaking_changes"] is False
    assert decisions["doc_movement_id"]["matrix_decision_category"] == "auto_inferred"
    assert decisions["dm_base_zone_name"]["matrix_expected_target_type"] == "Nullable(String)"
    assert decisions["created_at"]["matrix_canonical_type"] == "timestamp"


def test_cli_renderers_surface_decision_category_and_source() -> None:
    matrix_payload = TypeCertificationSuiteRegistry.default().build_matrix_payload("mssql", "clickhouse")
    explain_payload = SchemaEvolutionExplainService().explain(
        source_system="mssql",
        sink_system="clickhouse",
        source=[ColumnDef("id", "int", nullable=True)],
        target=[ColumnDef("id", "Nullable(Int32)", nullable=True)],
        policy=SchemaEvolutionPolicy(),
    )

    matrix_markdown = _render_type_matrix_md(matrix_payload)
    explain_text = _render_schema_explain_text(explain_payload)

    assert "Decision" in matrix_markdown
    assert "auto_inferred" in matrix_markdown
    assert "category=auto_inferred" in explain_text
    assert "source=source_metadata" in explain_text


def test_physical_plan_reports_decision_precedence_categories() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("amount", "int"), ("status", "text")],
        options=PhysicalDesignOptions.from_config(
            {
                "columns": {
                    "amount": {
                        "target_type": {
                            "clickhouse": "String",
                        }
                    }
                },
                "storage": {
                    "clickhouse": {
                        "low_cardinality": {
                            "mode": "explicit",
                            "columns": ["status"],
                        }
                    }
                },
            }
        ),
    )

    assert plan.columns["amount"].target_type == "String"
    assert plan.columns["amount"].decision_category == "explicit_physical_override"
    assert plan.columns["status"].target_type == "LowCardinality(String)"
    assert plan.columns["status"].decision_source == "physical_design"


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("off", "String"),
        ("auto", "LowCardinality(String)"),
        ("explicit", "LowCardinality(String)"),
        ("force", "LowCardinality(String)"),
        ("preserve", "String"),
    ],
)
def test_clickhouse_low_cardinality_certification_modes(mode: str, expected: str) -> None:
    profiles = {
        "status": ColumnProfile(
            name="status",
            row_count=10000,
            null_count=0,
            empty_string_count=0,
            distinct_count=5,
            distinct_ratio=0.0005,
            observed_types=("string",),
            max_length=16,
        )
    }
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("status", "text")],
        profiles=profiles,
        options=PhysicalDesignOptions.from_config(
            {
                "storage": {
                    "clickhouse": {
                        "low_cardinality": {
                            "mode": mode,
                            "columns": ["status"],
                        }
                    }
                }
            }
        ),
    )

    assert plan.columns["status"].target_type == expected


def test_low_cardinality_rejects_non_string_explicit_contract() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("amount", "int")],
        options=PhysicalDesignOptions.from_config(
            {
                "storage": {
                    "clickhouse": {
                        "low_cardinality": {
                            "mode": "force",
                            "columns": ["amount"],
                        }
                    }
                }
            }
        ),
    )

    assert plan.columns["amount"].target_type == "Int64"
    assert "LowCardinality" not in plan.columns["amount"].target_type
