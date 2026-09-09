from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_recovery_evidence_user_docs_cover_cli_scenarios_and_runbook() -> None:
    text = (DOCS / "cdc-recovery-evidence.md").read_text(encoding="utf-8")

    required = [
        "# CDC recovery evidence",
        "## User workflow",
        "## Failure scenario contract",
        "## CLI examples",
        "mssql -> clickhouse",
        "cdc_restart_resume",
        "cdc_offset_commit_ordering",
        "cdc_idempotent_replay_window",
        "cdc_partial_commit_repair",
        "cdc_poison_event_quarantine",
        "cdc_retention_recovery_margin",
        "## Runbook",
        "dpone ops cdc-recovery-evidence",
    ]
    for item in required:
        assert item in text


def test_developer_cdc_recovery_docs_cover_interfaces_and_no_live_io_rule() -> None:
    text = (DOCS / "developer-cdc-recovery-evidence.md").read_text(encoding="utf-8")

    required = [
        "CdcRecoveryEvidenceService",
        "CdcFailureScenario",
        "CdcRecoveryPolicy",
        "CdcRecoveryDecision",
        "Do not add route-specific branches to the service",
        "does not execute live CDC reads",
    ]
    for item in required:
        assert item in text


def test_index_architecture_ci_source_sink_and_nav_link_cdc_recovery() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-recovery-evidence.md" in index
    assert "CDC recovery evidence" in architecture
    assert "cdc-recovery-evidence" in cicd
    assert "cdc-recovery-evidence.md" in mkdocs
    assert "dpone ops cdc-recovery-evidence" in source_sink
