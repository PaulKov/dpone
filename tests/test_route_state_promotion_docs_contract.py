from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_route_state_promotion_user_docs_cover_workflow_examples_and_runbook() -> None:
    text = _read(DOCS / "route-state-promotion.md")

    for expected in (
        "# Route state promotion",
        "dpone ops route-state-promote",
        "state_promotion.json",
        "state_promotion.md",
        "commit receipt",
        "fencing token",
        "source state",
        "state_promotion.boundary_mismatch",
        "Operator runbook",
    ):
        assert expected in text


def test_developer_route_state_promotion_docs_cover_boundaries_and_extension_rules() -> None:
    text = _read(DOCS / "developer-route-state-promotion.md")

    for expected in (
        "# Developer route state promotion",
        "RouteStatePromotionService",
        "RouteCommitReceipt",
        "RouteStateStore",
        "LocalRouteStateStore",
        "SqliteRouteStateStore",
        "RouteStatePromotionPolicy",
        "dependency injection",
        "No connector imports",
        "Extension rules",
    ):
        assert expected in text


def test_route_state_promotion_docs_are_linked_from_nav_architecture_ci_and_matrix() -> None:
    mkdocs = _read(ROOT / "mkdocs.yml")
    architecture = _read(DOCS / "architecture.md")
    cicd = _read(DOCS / "ci-cd.md")
    developer_cicd = _read(DOCS / "developer-ci-cd.md")
    ops_cli = _read(DOCS / "ops-cli.md")
    source_sink_matrix = _read(DOCS / "source-sink-matrix.md")
    route_readiness = _read(DOCS / "route-readiness.md")

    for text in (mkdocs, architecture, cicd, developer_cicd, ops_cli, source_sink_matrix, route_readiness):
        assert "route-state-promotion" in text
    assert "Route state promotion" in architecture
    assert "state_promotion" in route_readiness
    assert "dpone ops route-state-promote" in ops_cli
