from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_live_runtime_adapters_user_docs_cover_live_mssql_clickhouse_workflow() -> None:
    text = (DOCS / "cdc-live-runtime-adapters.md").read_text(encoding="utf-8")

    required = [
        "# CDC live runtime adapters",
        "mssql -> clickhouse",
        "dpone ops cdc-runtime-run",
        "--mode live",
        "--source-connection-id",
        "--sink-connection-id",
        "DPONE_RUN_INTEGRATION=1",
        "test_mssql_clickhouse_live_cdc_runtime_integration.py",
        "commit offset only after durable sink apply",
        "ClickHouseCdcSinkApplier",
        "SQL-backed offset store",
        "## Runbook",
    ]
    for item in required:
        assert item in text


def test_developer_cdc_live_runtime_adapters_docs_cover_boundaries_and_extension_rules() -> None:
    text = (DOCS / "developer-cdc-live-runtime-adapters.md").read_text(encoding="utf-8")

    required = [
        "CdcRuntimeLiveAdapterFactory",
        "SqlCdcOffsetStoreAdapter",
        "MSSQLCDCReader",
        "MSSQLChangeTrackingReader",
        "ClickHouseCdcSinkApplier",
        "CdcSinkApplier",
        "Do not add route-specific branches to CdcRuntimeOrchestrator",
        "live adapters are injected",
    ]
    for item in required:
        assert item in text


def test_live_cdc_docs_are_linked_from_nav_architecture_ci_and_route_matrix() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    dev_cicd = (DOCS / "developer-ci-cd.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    matrix = (DOCS / "source-sink-matrix.md").read_text(encoding="utf-8")
    runtime = (DOCS / "cdc-runtime-orchestrator.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-live-runtime-adapters.md" in index
    assert "CDC live runtime adapters" in architecture
    assert "cdc-runtime-run --mode live" in cicd
    assert "cdc-live-runtime-adapters.md" in dev_cicd
    assert "cdc-live-runtime-adapters.md" in mkdocs
    assert "dpone ops cdc-runtime-run" in source_sink and "--mode live" in source_sink
    assert "live CDC runtime adapters" in matrix
    assert "CDC live runtime adapters" in runtime
