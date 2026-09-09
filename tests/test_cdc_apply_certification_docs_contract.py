from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_apply_certification_user_docs_cover_cli_fixture_and_runbook() -> None:
    text = (DOCS / "cdc-apply-certification.md").read_text(encoding="utf-8")

    required = [
        "# CDC apply certification",
        "## User workflow",
        "## Fixture contract",
        "## CLI examples",
        "mssql -> clickhouse",
        "cdc_apply_correctness",
        "typed_cdc_hash",
        "delete_semantics",
        "## Runbook",
        "dpone ops cdc-apply-certification",
        "dpone ops cdc-handoff",
    ]
    for item in required:
        assert item in text


def test_developer_cdc_apply_docs_cover_interfaces_and_no_live_io_rule() -> None:
    text = (DOCS / "developer-cdc-apply-certification.md").read_text(encoding="utf-8")

    required = [
        "CdcApplyCertificationService",
        "CdcApplyFixture",
        "CdcApplyEvent",
        "CdcApplyStrategy",
        "InMemoryCdcApplyStrategy",
        "Do not add route-specific branches to the service",
        "does not open live database connections",
    ]
    for item in required:
        assert item in text


def test_index_architecture_source_sink_and_nav_link_cdc_apply_certification() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-apply-certification.md" in index
    assert "CDC apply certification" in architecture
    assert "cdc-apply-certification.md" in mkdocs
    assert "dpone ops cdc-apply-certification" in source_sink
