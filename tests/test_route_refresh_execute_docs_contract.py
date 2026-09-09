from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_route_refresh_execute_docs_are_self_service() -> None:
    required_docs = [
        "docs/route-refresh-execute.md",
        "docs/developer-route-refresh-execute.md",
    ]
    for relative_path in required_docs:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "Runbook" in text, relative_path
        assert "source -> sink" in text, relative_path
        assert "dpone ops route-refresh-execute" in text, relative_path
        assert "route_refresh_execution.json" in text, relative_path
        assert "dry_run" in text, relative_path
        assert "RouteRefreshExecutor" in text, relative_path
        assert "mssql_clickhouse" in text, relative_path
        assert "executor config" in text, relative_path

    ops_cli = (ROOT / "docs/ops-cli.md").read_text(encoding="utf-8")
    assert "dpone ops route-refresh-execute" in ops_cli
    assert "--executor mssql_clickhouse" in ops_cli

    architecture = (ROOT / "docs/architecture.md").read_text(encoding="utf-8")
    assert "route-refresh-execute" in architecture
    assert "MssqlClickHouseRouteRefreshExecutor" in architecture

    control_plane = (ROOT / "docs/operational-control-plane.md").read_text(encoding="utf-8")
    assert "route-refresh-execute" in control_plane

    ci_cd = (ROOT / "docs/ci-cd.md").read_text(encoding="utf-8")
    assert "route_refresh_execution" in ci_cd

    release_gate = (ROOT / "docs/route-release-gate.md").read_text(encoding="utf-8")
    assert "route_refresh_execution" in release_gate

    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    assert "Route refresh execute: route-refresh-execute.md" in mkdocs
