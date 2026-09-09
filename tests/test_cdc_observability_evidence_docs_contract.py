from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_observability_evidence_user_docs_cover_cli_slos_and_runbook() -> None:
    text = (DOCS / "cdc-observability-evidence.md").read_text(encoding="utf-8")

    required = [
        "# CDC observability evidence",
        "## User workflow",
        "## Metrics contract",
        "## CLI examples",
        "mssql -> clickhouse",
        "cdc_lag_slo",
        "cdc_freshness_slo",
        "cdc_retention_risk",
        "cdc_offset_commit_health",
        "cdc_duplicate_replay_rate",
        "cdc_throughput_slo",
        "## Runbook",
        "dpone ops cdc-observability-evidence",
    ]
    for item in required:
        assert item in text


def test_developer_cdc_observability_docs_cover_interfaces_and_no_route_branches() -> None:
    text = (DOCS / "developer-cdc-observability-evidence.md").read_text(encoding="utf-8")

    required = [
        "CdcObservabilityEvidenceService",
        "CdcTelemetrySnapshot",
        "CdcSloProfile",
        "CdcObservabilityDecision",
        "Do not add route-specific branches to the service",
        "does not execute heavy tests",
    ]
    for item in required:
        assert item in text


def test_index_architecture_ci_source_sink_and_nav_link_cdc_observability() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-observability-evidence.md" in index
    assert "CDC observability evidence" in architecture
    assert "cdc-observability-evidence" in cicd
    assert "cdc-observability-evidence.md" in mkdocs
    assert "dpone ops cdc-observability-evidence" in source_sink
