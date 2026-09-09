from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_route_certification_pack_user_docs_cover_cli_examples_and_runbook() -> None:
    text = (DOCS / "route-certification-pack.md").read_text(encoding="utf-8")

    required = [
        "# Route certification pack",
        "## User workflow",
        "## CLI examples",
        "postgres -> mssql",
        "mssql -> clickhouse",
        "## Generated artifacts",
        "## Runbook",
        "dpone ops route-certification-pack",
        "dpone ops route-readiness",
    ]
    for item in required:
        assert item in text


def test_developer_route_readiness_docs_cover_pack_boundaries() -> None:
    text = (DOCS / "developer-route-readiness.md").read_text(encoding="utf-8")

    required = [
        "RouteCertificationPackService",
        "RouteEvidenceProbe",
        "RouteEvidenceProbeResult",
        "Do not execute heavy certification tests inside probes",
        "route-certification-pack",
    ]
    for item in required:
        assert item in text


def test_index_architecture_and_nav_link_route_certification_pack() -> None:
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "route-certification-pack.md" in index
    assert "Route certification pack" in architecture
    assert "route-certification-pack.md" in mkdocs
