from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_route_certify_release_user_docs_cover_release_gate_and_runbook() -> None:
    text = _read(DOCS / "route-certify-release.md")

    for expected in (
        "# Route certify release",
        "dpone ops route-certify-release",
        "route_certification_release.json",
        "route_certification_release_notes.md",
        "postgres_to_mssql__incremental_merge",
        "mssql_to_clickhouse__incremental_merge",
        "OSS-safe mode",
        "vendor-live mode",
        "Operator runbook",
    ):
        assert expected in text


def test_developer_route_certify_release_docs_cover_boundaries() -> None:
    text = _read(DOCS / "developer-route-certify-release.md")

    for expected in (
        "RouteCertificationReleaseService",
        "RouteCertificationReleasePolicy",
        "RouteCertificationReleaseReport",
        "dependency injection",
        "does not run route-certify",
        "does not run Docker",
        "No route-specific branches",
    ):
        assert expected in text


def test_route_certify_release_is_linked_from_docs_cli_and_workflow() -> None:
    files = {
        "mkdocs": _read(ROOT / "mkdocs.yml"),
        "readme": _read(DOCS / "README.md"),
        "architecture": _read(DOCS / "architecture.md"),
        "ci": _read(DOCS / "ci-cd.md"),
        "developer_ci": _read(DOCS / "developer-ci-cd.md"),
        "ops_cli": _read(DOCS / "ops-cli.md"),
        "source_sink_matrix": _read(DOCS / "source-sink-matrix.md"),
        "route_certify": _read(DOCS / "route-certify.md"),
        "cli_reference": _read(DOCS / "cli-reference.md"),
        "workflow": _read(ROOT / ".github/workflows/route-certification-release.yml"),
    }

    for name, text in files.items():
        assert "route-certify-release" in text, name

    assert "Route certify release" in files["mkdocs"]
    assert "actions/upload-artifact" in files["workflow"]
    assert "route_certification_release.json" in files["workflow"]


def test_route_certify_release_docs_are_decomposed() -> None:
    text = _read(DOCS / "route-certify-release.md")

    assert len(text.splitlines()) < 220
    assert "[Route certify](route-certify.md)" in text
