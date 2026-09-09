from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_runtime_orchestrator_user_docs_cover_runtime_loop_and_runbook() -> None:
    text = (DOCS / "cdc-runtime-orchestrator.md").read_text(encoding="utf-8")

    required = [
        "# CDC runtime orchestrator",
        "## User workflow",
        "## Runtime loop",
        "## CLI examples",
        "mssql -> clickhouse",
        "dpone ops cdc-runtime-run",
        "commit offset only after durable sink apply",
        "cdc_runtime.duplicate_events",
        "cdc_runtime.sink_not_durable",
        "## Runbook",
    ]
    for item in required:
        assert item in text


def test_developer_cdc_runtime_orchestrator_docs_cover_interfaces_and_boundaries() -> None:
    text = (DOCS / "developer-cdc-runtime-orchestrator.md").read_text(encoding="utf-8")

    required = [
        "CdcRuntimeOrchestrator",
        "CdcRuntimeStream",
        "CdcRuntimePolicy",
        "CdcOffsetStore",
        "CdcSinkApplier",
        "CdcApplyReceipt",
        "Do not add route-specific branches to the orchestrator",
        "live adapters are injected",
    ]
    for item in required:
        assert item in text


def test_index_architecture_ci_source_sink_and_nav_link_cdc_runtime_orchestrator() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-runtime-orchestrator.md" in index
    assert "CDC runtime orchestrator" in architecture
    assert "cdc-runtime-run" in cicd
    assert "cdc-runtime-orchestrator.md" in mkdocs
    assert "dpone ops cdc-runtime-run" in source_sink
