from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_route_live_certification_user_docs_cover_cli_harness_bundle_and_runbook() -> None:
    text = (DOCS / "route-live-certification.md").read_text(encoding="utf-8")

    for expected in (
        "# Route live certification",
        "dpone ops route-live-certification",
        "route_live_certification.json",
        "route_live_certification.md",
        "route_live_evidence_bundle",
        "MSSQL -> ClickHouse",
        "Postgres -> MSSQL",
        "CDC apply certification",
        "Docker-live",
        "vendor-live",
        "route-release-gate",
        "Operator runbook",
    ):
        assert expected in text


def test_developer_route_live_certification_docs_cover_interfaces_and_no_live_io_rule() -> None:
    text = (DOCS / "developer-route-live-certification.md").read_text(encoding="utf-8")

    for expected in (
        "# Developer route live certification",
        "RouteLiveCertificationService",
        "RouteLiveCertificationReport",
        "RouteLiveCertificationPolicy",
        "RouteLiveCertificationEvidence",
        "RouteProfileCatalog",
        "dependency injection",
        "does not open live database connections",
        "Extension rules",
        "Stable JSON contract",
    ):
        assert expected in text


def test_route_live_certification_is_linked_from_docs_ci_and_cli_reference() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    ci_cd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    developer_ci = (DOCS / "developer-ci-cd.md").read_text(encoding="utf-8")
    ops_cli = (DOCS / "ops-cli.md").read_text(encoding="utf-8")
    source_sink = (DOCS / "source-sink-matrix.md").read_text(encoding="utf-8")
    release_evidence = (DOCS / "release-evidence.md").read_text(encoding="utf-8")
    live_certification = (DOCS / "live-certification.md").read_text(encoding="utf-8")
    cli_reference = (DOCS / "cli-reference.md").read_text(encoding="utf-8")

    assert "route-live-certification.md" in mkdocs
    assert "Route live certification" in index
    assert "Route live certification" in architecture
    assert "route-live-certification" in ci_cd
    assert "route-live-certification" in developer_ci
    assert "dpone ops route-live-certification" in ops_cli
    assert "route_live_evidence_bundle" in source_sink
    assert "route_live_evidence_bundle" in release_evidence
    assert "route-live-certification" in live_certification
    assert "route-live-certification" in cli_reference
