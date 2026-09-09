from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_cdc_handoff_user_docs_cover_cli_examples_and_runbook() -> None:
    text = (DOCS / "cdc-handoff.md").read_text(encoding="utf-8")

    required = [
        "# CDC snapshot handoff",
        "## User workflow",
        "## CLI examples",
        "mssql -> clickhouse",
        "cdc_snapshot_boundary",
        "cdc_apply_correctness",
        "typed_cdc_hash",
        "## Runbook",
        "dpone ops cdc-handoff",
    ]
    for item in required:
        assert item in text


def test_developer_cdc_handoff_docs_cover_generic_interfaces() -> None:
    text = (DOCS / "developer-cdc-handoff.md").read_text(encoding="utf-8")

    required = [
        "SnapshotCdcHandoffService",
        "CdcHandoffCatalog",
        "CdcHandoffPolicy",
        "CdcStreamKey",
        "Do not add route-specific branches to the service",
        "CDC apply evidence",
    ]
    for item in required:
        assert item in text


def test_index_architecture_source_sink_and_nav_link_cdc_handoff() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    matrix = (DOCS / "source-sink-matrix.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink" / "mssql-to-clickhouse.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "cdc-handoff.md" in index
    assert "CDC snapshot handoff" in architecture
    assert "cdc-handoff.md" in mkdocs
    assert "CDC apply evidence" in matrix
    assert "dpone ops cdc-handoff" in source_sink
