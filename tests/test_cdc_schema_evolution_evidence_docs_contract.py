from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_schema_evolution_user_docs_cover_cli_domains_and_runbook() -> None:
    text = (DOCS / "cdc-schema-evolution-evidence.md").read_text(encoding="utf-8")

    required = [
        "# CDC schema evolution evidence",
        "## User workflow",
        "## Schema change contract",
        "## CLI examples",
        "mssql -> clickhouse",
        "cdc_schema_change_capture",
        "cdc_schema_compatibility",
        "cdc_type_widening_safety",
        "cdc_target_ddl_dry_run",
        "cdc_backfill_requirement",
        "cdc_breaking_change_gate",
        "cdc_offset_schema_ordering",
        "## Runbook",
        "dpone ops cdc-schema-evolution-evidence",
    ]
    for item in required:
        assert item in text


def test_developer_cdc_schema_evolution_docs_cover_interfaces_and_no_live_io_rule() -> None:
    text = (DOCS / "developer-cdc-schema-evolution-evidence.md").read_text(encoding="utf-8")

    required = [
        "CdcSchemaEvolutionEvidenceService",
        "CdcSchemaChangeEvent",
        "CdcSchemaEvolutionPlan",
        "CdcSchemaEvolutionDecision",
        "Do not add route-specific branches to the service",
        "does not execute live DDL",
    ]
    for item in required:
        assert item in text


def test_index_architecture_ci_source_sink_and_nav_link_cdc_schema_evolution() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-schema-evolution-evidence.md" in index
    assert "CDC schema evolution evidence" in architecture
    assert "cdc-schema-evolution-evidence" in cicd
    assert "cdc-schema-evolution-evidence.md" in mkdocs
    assert "dpone ops cdc-schema-evolution-evidence" in source_sink
