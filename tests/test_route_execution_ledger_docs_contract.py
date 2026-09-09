from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_route_execution_ledger_user_docs_cover_workflow_examples_and_runbook() -> None:
    text = _read(DOCS / "route-execution-ledger.md")

    for expected in (
        "# Route execution ledger",
        "dpone ops route-execution-ledger",
        "route_execution_ledger.json",
        "route_commit_protocol.md",
        "idempotency key",
        "lease TTL",
        "--store-backend sqlite",
        "route_execution_ledger.sqlite3",
        "atomic compare-and-swap",
        "state_committed",
        "CDC window",
        "resync chunk",
        "Operator runbook",
    ):
        assert expected in text


def test_developer_route_execution_ledger_docs_cover_boundaries_and_extension_rules() -> None:
    text = _read(DOCS / "developer-route-execution-ledger.md")

    for expected in (
        "# Developer route execution ledger",
        "RouteExecutionService",
        "dpone.ops.routes.execution_models",
        "dpone.ops.routes.execution_policy",
        "dpone.ops.routes.execution_store",
        "RouteExecutionLedgerStore",
        "LocalRouteExecutionLedgerStore",
        "SqliteRouteExecutionLedgerStore",
        "RouteCommitProtocolPolicy",
        "compare-and-swap",
        "dependency injection",
        "No connector imports",
        "Extension rules",
    ):
        assert expected in text


def test_route_execution_ledger_docs_are_linked_from_nav_architecture_ci_and_matrix() -> None:
    mkdocs = _read(ROOT / "mkdocs.yml")
    architecture = _read(DOCS / "architecture.md")
    cicd = _read(DOCS / "ci-cd.md")
    developer_cicd = _read(DOCS / "developer-ci-cd.md")
    ops_cli = _read(DOCS / "ops-cli.md")
    source_sink_matrix = _read(DOCS / "source-sink-matrix.md")
    route_readiness = _read(DOCS / "route-readiness.md")

    for text in (mkdocs, architecture, cicd, developer_cicd, ops_cli, source_sink_matrix, route_readiness):
        assert "route-execution-ledger" in text
    assert "Route execution ledger" in architecture
    assert "route_execution_ledger" in route_readiness
    assert "dpone ops route-execution-ledger" in ops_cli
