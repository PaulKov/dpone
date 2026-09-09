from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_compare_repair_user_docs_cover_cli_artifacts_and_runbook() -> None:
    text = (DOCS / "cdc-compare-repair.md").read_text(encoding="utf-8")

    required = [
        "# CDC compare and repair",
        "dpone ops cdc-compare-repair",
        "dpone ops cdc-repair-execute",
        "cdc_compare_repair.json",
        "cdc_repair_plan.json",
        "cdc_repair_execution.json",
        "Do not mutate CDC offsets from repair execution",
        "ClickHouse CDC log",
        "DPONE_RUN_INTEGRATION=1",
        "## Runbook",
    ]
    for item in required:
        assert item in text


def test_cdc_compare_repair_developer_docs_cover_boundaries_and_extension_rules() -> None:
    text = (DOCS / "developer-cdc-compare-repair.md").read_text(encoding="utf-8")

    required = [
        "CdcCompareRepairService",
        "CdcRepairExecutionService",
        "MssqlCdcCompareReader",
        "ClickHouseCdcLogCompareReader",
        "CdcCompareRepairOpsService",
        "CdcRepairExecutionOpsService",
        "Do not put route-specific compare rules in CdcCompareRepairService",
        "Do not mutate CDC offsets from repair execution",
    ]
    for item in required:
        assert item in text


def test_cdc_compare_repair_docs_are_linked_from_nav_architecture_ci_and_route_docs() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    dev_cicd = (DOCS / "developer-ci-cd.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    matrix = (DOCS / "source-sink-matrix.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-compare-repair.md" in index
    assert "CDC compare and repair" in architecture
    assert "cdc-compare-repair" in cicd
    assert "cdc-compare-repair.md" in dev_cicd
    assert "cdc-compare-repair.md" in mkdocs
    assert "dpone ops cdc-compare-repair" in source_sink
    assert "CDC compare and repair" in matrix
