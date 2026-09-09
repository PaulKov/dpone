from pathlib import Path

FIXTURE = Path("tests/fixtures/mssql-clickhouse-wide-dbt")


def test_wide_dbt_fixture_materializes_one_calculated_mssql_relation() -> None:
    project = (FIXTURE / "dbt_project.yml").read_text(encoding="utf-8")
    model = (FIXTURE / "models/wide_dbt_result.sql").read_text(encoding="utf-8")

    assert "+materialized: table" in project
    assert "source.*" in model
    assert "dbt_calculated_amount" in model
    assert "decimal(38, 8)" in model
    assert "var('source_schema')" in model
    assert "var('source_table')" in model
