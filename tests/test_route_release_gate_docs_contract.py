from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_route_release_gate_user_docs_cover_cli_artifacts_and_runbook() -> None:
    text = _read(DOCS / "route-release-gate.md")

    for expected in (
        "# Route release gate",
        "dpone ops route-release-gate",
        "route_release_gate.json",
        "route_release_gate.md",
        "route_readiness",
        "route_certification_pack",
        "route_execution_ledger",
        "state_promotion",
        "cdc_apply_certification",
        "Operator runbook",
        "go/no-go",
    ):
        assert expected in text


def test_developer_route_release_gate_docs_cover_boundaries_and_extension_rules() -> None:
    text = _read(DOCS / "developer-route-release-gate.md")

    for expected in (
        "# Developer route release gate",
        "RouteReleaseGateService",
        "RouteReleaseGatePolicy",
        "RouteReleaseGateReport",
        "RouteReleaseGateEvidence",
        "RouteProfileCatalog",
        "dependency injection",
        "No connector imports",
        "Extension rules",
        "Stable JSON contract",
    ):
        assert expected in text


def test_route_release_gate_docs_are_linked_from_nav_architecture_ci_and_matrix() -> None:
    mkdocs = _read(ROOT / "mkdocs.yml")
    architecture = _read(DOCS / "architecture.md")
    cicd = _read(DOCS / "ci-cd.md")
    developer_cicd = _read(DOCS / "developer-ci-cd.md")
    ops_cli = _read(DOCS / "ops-cli.md")
    route_readiness = _read(DOCS / "route-readiness.md")
    source_sink_matrix = _read(DOCS / "source-sink-matrix.md")
    index = _read(DOCS / "README.md")

    for text in (mkdocs, architecture, cicd, developer_cicd, ops_cli, route_readiness, source_sink_matrix, index):
        assert "route-release-gate" in text
    assert "Route release gate" in architecture
    assert "dpone ops route-release-gate" in ops_cli
