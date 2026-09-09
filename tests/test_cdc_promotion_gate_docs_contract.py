from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_promotion_gate_user_docs_cover_cli_decision_and_runbook() -> None:
    text = (DOCS / "cdc-promotion-gate.md").read_text(encoding="utf-8")

    required = [
        "# CDC promotion gate",
        "## User workflow",
        "## Required upstream artifacts",
        "## CLI examples",
        "mssql -> clickhouse",
        "cdc_apply_gate",
        "cdc_handoff_gate",
        "cdc_observability_gate",
        "cdc_recovery_gate",
        "cdc_schema_evolution_gate",
        "cdc_stream_identity_consistency",
        "cdc_offset_promotion_decision",
        "promote_offsets",
        "production_ready",
        "## Runbook",
        "dpone ops cdc-promotion-gate",
    ]
    for item in required:
        assert item in text


def test_developer_cdc_promotion_gate_docs_cover_interfaces_and_no_live_io_rule() -> None:
    text = (DOCS / "developer-cdc-promotion-gate.md").read_text(encoding="utf-8")

    required = [
        "CdcPromotionGateService",
        "CdcPromotionEvidenceItem",
        "CdcPromotionDecision",
        "CdcPromotionReport",
        "Do not add route-specific branches to the service",
        "does not promote offsets",
    ]
    for item in required:
        assert item in text


def test_index_architecture_ci_source_sink_and_nav_link_cdc_promotion_gate() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-promotion-gate.md" in index
    assert "CDC promotion gate" in architecture
    assert "cdc-promotion-gate" in cicd
    assert "cdc-promotion-gate.md" in mkdocs
    assert "dpone ops cdc-promotion-gate" in source_sink
