from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_clickhouse_cdc_typed_materialization_user_docs_cover_workflow_and_runbook() -> None:
    text = (DOCS / "cdc-clickhouse-typed-materialization.md").read_text(encoding="utf-8")

    required = [
        "# ClickHouse CDC typed materialization",
        "dpone ops cdc-materialize-clickhouse-typed",
        "--cdc-dataset",
        "--target-dataset",
        "--column order_id=Int32",
        "--column amount=Decimal(18,2)",
        "--delete-mode exclude_deleted",
        "--delete-mode tombstone",
        "cdc_typed_materialization.json",
        "cdc_typed_materialization.md",
        "cdc_typed_parse_quarantine.json",
        "--fail-on-parse-errors",
        "--quarantine-dataset",
        "--schema-drift-mode strict",
        "DPONE_RUN_INTEGRATION=1",
        "test_mssql_clickhouse_live_cdc_runtime_integration.py",
        "## Runbook",
    ]
    for item in required:
        assert item in text


def test_developer_clickhouse_cdc_typed_materialization_docs_cover_boundaries_and_extension_rules() -> None:
    text = (DOCS / "developer-cdc-clickhouse-typed-materialization.md").read_text(encoding="utf-8")

    required = [
        "ClickHouseCdcTypedColumn",
        "ClickHouseCdcPayloadProjector",
        "ClickHouseCdcTypedMaterializationPlan",
        "ClickHouseCdcTypedMaterializationPolicy",
        "ClickHouseCdcTypedMaterializationReport",
        "ClickHouseCdcTypedMaterializationService",
        "ClickHouseCdcTypedQualityPolicy",
        "ClickHouseCdcTypedQualityEvidence",
        "CdcTypedMaterializationService",
        "parse quarantine",
        "schema drift",
        "Do not add route-specific branches to CdcRuntimeOrchestrator",
        "shadow-table replace",
        "injected connector",
    ]
    for item in required:
        assert item in text


def test_clickhouse_cdc_typed_materialization_docs_are_linked_from_nav_architecture_ci_and_matrix() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    dev_cicd = (DOCS / "developer-ci-cd.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    matrix = (DOCS / "source-sink-matrix.md").read_text(encoding="utf-8")
    json_materialization = (DOCS / "cdc-clickhouse-materialization.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-clickhouse-typed-materialization.md" in index
    assert "CDC typed serving materialization" in architecture
    assert "cdc-materialize-clickhouse-typed" in cicd
    assert "cdc-clickhouse-typed-materialization.md" in dev_cicd
    assert "cdc-clickhouse-typed-materialization.md" in mkdocs
    assert "dpone ops cdc-materialize-clickhouse-typed" in source_sink
    assert "CDC typed serving materialization" in matrix
    assert "ClickHouse CDC typed materialization" in json_materialization
