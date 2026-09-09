from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_route_certify_user_docs_cover_modes_outputs_and_runbook() -> None:
    text = _read(DOCS / "route-certify.md")

    for expected in (
        "dpone ops route-certify",
        "OSS-safe mode",
        "vendor-live mode",
        "route_certification_bundle.json",
        "route_promotion_gate",
        "release_evidence_pack",
        "route_refresh_snapshot_capture",
        "Operator runbook",
    ):
        assert expected in text


def test_route_certify_developer_docs_cover_boundaries_and_extension_rules() -> None:
    text = _read(DOCS / "developer-route-certify.md")

    for expected in (
        "RouteCertificationService",
        "RouteCertificationPolicy",
        "RouteCertificationBundleReport",
        "dependency injection",
        "No route-specific branches",
        "does not run Docker",
        "does not open database connections",
        "generated evidence",
        "promotion gate",
    ):
        assert expected in text


def test_route_certify_is_linked_from_navigation_and_operational_docs() -> None:
    files = {
        "mkdocs": _read(ROOT / "mkdocs.yml"),
        "readme": _read(DOCS / "README.md"),
        "architecture": _read(DOCS / "architecture.md"),
        "ci": _read(DOCS / "ci-cd.md"),
        "developer_ci": _read(DOCS / "developer-ci-cd.md"),
        "ops_cli": _read(DOCS / "ops-cli.md"),
        "source_sink_matrix": _read(DOCS / "source-sink-matrix.md"),
        "cli_reference": _read(DOCS / "cli-reference.md"),
        "postgres_mssql": _read(DOCS / "source-sink/postgres-to-mssql.md"),
        "mssql_clickhouse": _read(DOCS / "source-sink/mssql-to-clickhouse.md"),
    }

    for name, text in files.items():
        assert "route-certify" in text, name

    assert "Route certify" in files["mkdocs"]


def test_route_certify_docs_are_decomposed_from_rc_orchestrator_docs() -> None:
    text = _read(DOCS / "route-certify.md")

    assert len(text.splitlines()) < 220
    assert "[Route release candidate orchestrator](route-rc-orchestrator.md)" in text
