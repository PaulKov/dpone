from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_route_release_finalize_user_docs_cover_runbook() -> None:
    text = _read(DOCS / "route-release-finalize.md")

    for expected in (
        "# Route release finalize",
        "dpone ops route-release-finalize",
        "bundle discovery",
        "freshness",
        "provenance",
        "regression",
        "route_release_finalizer.json",
        "route_certification_history_index.json",
        "Operator runbook",
    ):
        assert expected in text


def test_route_release_finalize_developer_docs_cover_boundaries() -> None:
    text = _read(DOCS / "developer-route-release-finalize.md")

    for expected in (
        "RouteCertificationReleaseFinalizerService",
        "RouteCertificationBundleDiscovery",
        "RouteCertificationReleaseFinalizerPolicy",
        "dependency injection",
        "does not run Docker",
        "does not open database connections",
        "No route-specific branches",
    ):
        assert expected in text


def test_route_release_finalize_is_linked_from_docs_cli_and_workflow() -> None:
    files = {
        "mkdocs": _read(ROOT / "mkdocs.yml"),
        "readme": _read(DOCS / "README.md"),
        "architecture": _read(DOCS / "architecture.md"),
        "ci": _read(DOCS / "ci-cd.md"),
        "developer_ci": _read(DOCS / "developer-ci-cd.md"),
        "ops_cli": _read(DOCS / "ops-cli.md"),
        "source_sink_matrix": _read(DOCS / "source-sink-matrix.md"),
        "route_certify_release": _read(DOCS / "route-certify-release.md"),
        "cli_reference": _read(DOCS / "cli-reference.md"),
        "workflow": _read(ROOT / ".github/workflows/route-release-finalize.yml"),
    }

    for name, text in files.items():
        assert "route-release-finalize" in text, name

    assert "Route release finalize" in files["mkdocs"]
    assert "actions/upload-artifact" in files["workflow"]
    assert "route_release_finalizer.json" in files["workflow"]
