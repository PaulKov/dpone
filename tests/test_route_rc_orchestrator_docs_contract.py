from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_route_rc_orchestrator_user_docs_cover_release_train_and_runbook() -> None:
    text = (DOCS / "route-rc-orchestrator.md").read_text(encoding="utf-8")

    for expected in (
        "# Route release candidate orchestrator",
        "dpone ops route-rc-orchestrator",
        "route_rc_orchestration.json",
        "route_certification_pack",
        "route_live_evidence_bundle",
        "route_release_gate",
        "release_evidence_pack",
        "fail-closed",
        "MSSQL -> ClickHouse",
        "Postgres -> MSSQL",
        "Operator runbook",
    ):
        assert expected in text


def test_developer_route_rc_orchestrator_docs_cover_boundaries_and_interfaces() -> None:
    text = (DOCS / "developer-route-rc-orchestrator.md").read_text(encoding="utf-8")

    for expected in (
        "# Developer route release candidate orchestrator",
        "RouteReleaseCandidateOrchestratorService",
        "RouteRcOrchestrationReport",
        "RouteRcOrchestrationPolicy",
        "RouteRcOrchestrationStep",
        "dependency injection",
        "does not start Docker",
        "does not run pytest",
        "Stable JSON contract",
        "Extension rules",
    ):
        assert expected in text


def test_route_rc_orchestrator_is_linked_from_docs_workflow_and_cli_reference() -> None:
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
    workflow = (ROOT / ".github/workflows/live-certification.yml").read_text(encoding="utf-8")

    assert "route-rc-orchestrator.md" in mkdocs
    assert "Route release candidate orchestrator" in index
    assert "Route release candidate orchestrator" in architecture
    assert "route-rc-orchestrator" in ci_cd
    assert "route-rc-orchestrator" in developer_ci
    assert "dpone ops route-rc-orchestrator" in ops_cli
    assert "route_rc_orchestration" in source_sink
    assert "route_rc_orchestration" in release_evidence
    assert "route-rc-orchestrator" in live_certification
    assert "route-rc-orchestrator" in cli_reference
    assert "route-rc-orchestrator" in workflow
