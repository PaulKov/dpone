from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_route_conformance_user_docs_cover_workflow_artifacts_and_runbook() -> None:
    text = (DOCS / "route-conformance-lab.md").read_text(encoding="utf-8")

    required = [
        "# Route Conformance Lab",
        "dpone ops route-conformance run",
        "dpone ops route-conformance live-run",
        "dpone ops route-conformance summarize",
        "dpone ops route-conformance release-gate",
        "route_conformance.json",
        "source_snapshot.json",
        "sink_snapshot.json",
        "route_conformance_summary.json",
        "route_conformance_release_gate.json",
        "route_conformance_live.json",
        "live_source_snapshot.json",
        "live_sink_snapshot.json",
        "## Runbook",
        "10,000 rows",
        "200 columns",
        "DPONE_LIVE_ROUTE_CONFORMANCE=1",
        "DPONE_VENDOR_LIVE=1",
        "--adapter vendor_live",
        "--adapter docker",
        "postgres -> mssql",
        "mssql -> clickhouse",
    ]
    for item in required:
        assert item in text


def test_route_conformance_developer_docs_cover_boundaries_and_extension_rules() -> None:
    text = (DOCS / "developer-route-conformance-lab.md").read_text(encoding="utf-8")

    required = [
        "# Developer guide: Route Conformance Lab",
        "RouteConformanceService",
        "SyntheticRouteDatasetFactory",
        "RouteConformanceVerifier",
        "RouteConformanceLiveService",
        "RouteConformanceSourceSeeder",
        "RouteConformanceRouteExecutor",
        "RouteConformanceSnapshotReader",
        "RouteConformancePolicy",
        "dpone.ops.routes.conformance_models",
        "dpone.ops.routes.conformance_dataset",
        "dpone.ops.routes.conformance_verifier",
        "dpone.ops.routes.conformance_live_ports",
        "dpone.ops.routes.conformance_live_adapters",
        "dpone.ops.routes.conformance_vendor_live",
        "VendorRouteConformanceLiveAdapter",
        "VendorLiveRouteBinding",
        "RouteConformanceSchemaEvolutionApplier",
        "Do not add route-specific logic to CLI handlers",
        "RouteProfileCatalog",
    ]
    for item in required:
        assert item in text


def test_route_conformance_docs_are_linked_from_nav_architecture_ci_matrix_and_cli() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    ci_cd = (DOCS / "developer-ci-cd.md").read_text(encoding="utf-8")
    matrix = (DOCS / "source-sink-matrix.md").read_text(encoding="utf-8")
    ops_cli = (DOCS / "ops-cli.md").read_text(encoding="utf-8")
    cli_reference = (DOCS / "cli-reference.md").read_text(encoding="utf-8")

    assert "Route Conformance Lab: route-conformance-lab.md" in mkdocs
    assert "Developer Route Conformance Lab" in index
    assert "route-conformance-lab.md" in index
    assert "Route Conformance Lab" in architecture
    assert "route-conformance release-gate" in ci_cd
    assert "route-conformance" in matrix
    assert "dpone ops route-conformance run" in ops_cli
    assert "dpone ops route-conformance" in cli_reference
