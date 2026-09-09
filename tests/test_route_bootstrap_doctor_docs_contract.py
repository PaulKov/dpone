from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_route_bootstrap_doctor_user_docs_cover_workflow_artifacts_and_runbook() -> None:
    text = (DOCS / "route-bootstrap-doctor.md").read_text(encoding="utf-8")

    required = [
        "# Route bootstrap and doctor",
        "dpone ops connection-doctor",
        "dpone ops source-discover",
        "dpone ops route-bootstrap",
        "dpone ops route-doctor",
        "connection_doctor.json",
        "source_discovery.json",
        "route_bootstrap.json",
        "route_doctor.json",
        "## Runbook",
    ]
    for item in required:
        assert item in text


def test_route_bootstrap_doctor_developer_docs_cover_boundaries_and_extension_rules() -> None:
    text = (DOCS / "developer-route-bootstrap-doctor.md").read_text(encoding="utf-8")

    required = [
        "# Developer guide: route bootstrap and doctor",
        "ConnectionDoctorService",
        "SourceDiscoveryService",
        "RouteBootstrapService",
        "RouteDoctorService",
        "dpone.ops.routes.bootstrap_models",
        "dpone.ops.routes.bootstrap_policy",
        "Do not add route-specific logic to CLI handlers",
        "RouteProfileCatalog",
    ]
    for item in required:
        assert item in text


def test_route_bootstrap_doctor_docs_are_linked_from_nav_architecture_ci_matrix_and_cli() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    ci_cd = (DOCS / "developer-ci-cd.md").read_text(encoding="utf-8")
    matrix = (DOCS / "source-sink-matrix.md").read_text(encoding="utf-8")
    ops_cli = (DOCS / "ops-cli.md").read_text(encoding="utf-8")
    cli_reference = (DOCS / "cli-reference.md").read_text(encoding="utf-8")

    assert "Route bootstrap and doctor: route-bootstrap-doctor.md" in mkdocs
    assert "Developer route bootstrap and doctor" in index
    assert "route-bootstrap-doctor.md" in index
    assert "Route bootstrap and doctor" in architecture
    assert "connection-doctor" in ci_cd
    assert "route-doctor" in matrix
    assert "dpone ops route-bootstrap" in ops_cli
    assert "dpone ops connection-doctor" in cli_reference
    assert "dpone ops route-doctor" in cli_reference
