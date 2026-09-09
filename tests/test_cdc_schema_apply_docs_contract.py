from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_schema_apply_user_docs_cover_cli_artifacts_and_runbook() -> None:
    text = (DOCS / "cdc-schema-apply.md").read_text(encoding="utf-8")

    required = [
        "# CDC schema apply",
        "dpone ops cdc-schema-apply",
        "--schema-change-json",
        "--mode dry_run",
        "--mode apply",
        "--typed-refresh",
        "--require-approval",
        "cdc_schema_apply_plan.json",
        "cdc_schema_apply_result.json",
        "cdc_typed_materialization.json",
        "DPONE_RUN_INTEGRATION=1",
        "## Runbook",
    ]
    for item in required:
        assert item in text


def test_cdc_schema_apply_developer_docs_cover_boundaries_and_extension_rules() -> None:
    text = (DOCS / "developer-cdc-schema-apply.md").read_text(encoding="utf-8")

    required = [
        "CdcSchemaEvolutionApplyService",
        "CdcSchemaApplyPolicy",
        "CdcSchemaApplyPlan",
        "CdcSchemaApplyReport",
        "ClickHouseCdcSchemaDdlPlanner",
        "DDL planner",
        "typed materialization refresh",
        "Do not add DDL apply logic to CdcRuntimeOrchestrator",
        "Do not mutate CDC offsets",
    ]
    for item in required:
        assert item in text


def test_cdc_schema_apply_docs_are_linked_from_nav_architecture_ci_and_route_docs() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    dev_cicd = (DOCS / "developer-ci-cd.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-schema-apply.md" in index
    assert "CDC schema apply" in architecture
    assert "cdc-schema-apply" in cicd
    assert "cdc-schema-apply.md" in dev_cicd
    assert "cdc-schema-apply.md" in mkdocs
    assert "dpone ops cdc-schema-apply" in source_sink
