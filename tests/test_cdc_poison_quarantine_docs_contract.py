from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_poison_quarantine_user_docs_cover_cli_artifacts_and_runbook() -> None:
    text = (DOCS / "cdc-poison-quarantine.md").read_text(encoding="utf-8")

    required = [
        "# CDC poison quarantine and replay",
        "dpone ops cdc-runtime-run",
        "--poison-mode quarantine_and_continue",
        "dpone ops cdc-quarantine-inspect",
        "dpone ops cdc-replay-execute",
        "cdc_poison_quarantine.json",
        "cdc_quarantine_inspection.json",
        "cdc_replay_execution.json",
        "duplicate_events_skipped",
        "DPONE_RUN_INTEGRATION=1",
        "## Runbook",
    ]
    for item in required:
        assert item in text


def test_cdc_poison_quarantine_developer_docs_cover_boundaries_and_extension_rules() -> None:
    text = (DOCS / "developer-cdc-poison-quarantine.md").read_text(encoding="utf-8")

    required = [
        "CdcPoisonClassifier",
        "FileCdcPoisonQuarantine",
        "CdcReplayExecutionService",
        "CdcReplayExecutionOpsService",
        "CdcQuarantineInspectionService",
        "ClickHouseCdcSinkApplier",
        "Do not mutate CDC offsets from replay execution",
        "Do not put route-specific poison rules in CdcRuntimeOrchestrator",
    ]
    for item in required:
        assert item in text


def test_cdc_poison_quarantine_docs_are_linked_from_nav_architecture_ci_and_route_docs() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    dev_cicd = (DOCS / "developer-ci-cd.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    matrix = (DOCS / "source-sink-matrix.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-poison-quarantine.md" in index
    assert "CDC poison quarantine" in architecture
    assert "cdc-replay-execute" in cicd
    assert "cdc-poison-quarantine.md" in dev_cicd
    assert "cdc-poison-quarantine.md" in mkdocs
    assert "dpone ops cdc-replay-execute" in source_sink
    assert "CDC poison quarantine" in matrix
