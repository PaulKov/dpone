from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_cdc_retention_resync_docs_exist_and_cover_user_and_developer_contracts() -> None:
    user_doc = ROOT / "docs/cdc-retention-resync.md"
    developer_doc = ROOT / "docs/developer-cdc-retention-resync.md"

    assert user_doc.exists()
    assert developer_doc.exists()

    user_text = user_doc.read_text(encoding="utf-8")
    developer_text = developer_doc.read_text(encoding="utf-8")

    for expected in (
        "CDC retention gap auto-resync",
        "cdc-retention-check",
        "cdc-resync-plan",
        "cdc-resync-execute",
        "mssql -> clickhouse",
        "Docker-live",
        "runbook",
    ):
        assert expected in user_text

    for expected in (
        "CdcRetentionGapService",
        "CdcResyncPlanner",
        "CdcResyncExecutionService",
        "CdcRetentionProbe",
        "CdcSinkApplier",
        "SOLID",
    ):
        assert expected in developer_text


def test_cdc_retention_resync_docs_are_linked_from_navigation_and_quality_gates() -> None:
    checks = {
        "mkdocs.yml": (
            "cdc-retention-resync.md",
            "developer-cdc-retention-resync.md",
        ),
        "docs/README.md": ("CDC retention gap auto-resync",),
        "docs/architecture.md": (
            "CdcRetentionGapService",
            "CdcResyncPlanner",
            "CdcResyncExecutionService",
        ),
        "docs/ci-cd.md": (
            "cdc-retention-check",
            "cdc-resync-plan",
            "cdc-resync-execute",
        ),
        "docs/developer-ci-cd.md": ("CDC retention gap auto-resync",),
        "docs/ops-cli.md": (
            "cdc-retention-check",
            "cdc-resync-plan",
            "cdc-resync-execute",
        ),
        "docs/source-sink-matrix.md": ("CDC retention gap auto-resync", "mssql -> clickhouse"),
        "docs/source-sink/mssql-to-clickhouse.md": ("CDC retention gap auto-resync",),
    }
    for path, expected_values in checks.items():
        text = _read(path)
        for expected in expected_values:
            assert expected in text, f"{path} must mention {expected}"
