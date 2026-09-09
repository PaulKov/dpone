from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_route_readiness_user_docs_cover_examples_and_runbook() -> None:
    text = (DOCS / "route-readiness.md").read_text(encoding="utf-8")

    required = [
        "# Route readiness",
        "## User workflow",
        "## CLI examples",
        "postgres -> mssql",
        "mssql -> clickhouse",
        "## Evidence taxonomy",
        "## Runbook",
        "dpone ops route-readiness",
    ]
    for item in required:
        assert item in text


def test_route_readiness_developer_docs_cover_boundaries_and_extension_rules() -> None:
    text = (DOCS / "developer-route-readiness.md").read_text(encoding="utf-8")

    required = [
        "# Developer guide: route readiness",
        "## Architecture boundaries",
        "RouteKey",
        "RouteProfile",
        "RouteEvidenceReader",
        "RouteReadinessPolicy",
        "RouteReadinessService",
        "## Adding a new route",
        "Do not add route-specific logic to the CLI",
    ]
    for item in required:
        assert item in text


def test_architecture_and_index_docs_link_route_readiness() -> None:
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    index = (DOCS / "README.md").read_text(encoding="utf-8")

    assert "Route readiness" in architecture
    assert "developer-route-readiness.md" in index
    assert "route-readiness.md" in index
