from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_route_run_supervisor_docs_are_self_service() -> None:
    required_docs = [
        "docs/route-run-supervisor.md",
        "docs/developer-route-run-supervisor.md",
    ]
    for relative_path in required_docs:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "Runbook" in text, relative_path
        assert "source -> sink" in text, relative_path
        assert "dpone ops route-run-supervisor" in text, relative_path
        assert "--run-mode route_refresh" in text, relative_path
        assert "execution_contract" in text, relative_path

    ops_cli = (ROOT / "docs/ops-cli.md").read_text(encoding="utf-8")
    assert "dpone ops route-run-supervisor" in ops_cli
    assert "--run-mode" in ops_cli

    architecture = (ROOT / "docs/architecture.md").read_text(encoding="utf-8")
    assert "route run execution contract" in architecture
    assert "dpone.ops.routes.run_supervisor_contract" in architecture

    control_plane = (ROOT / "docs/operational-control-plane.md").read_text(encoding="utf-8")
    assert "route-run-supervisor" in control_plane
    assert "--run-mode route_refresh" in control_plane

    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    assert "Route run supervisor: route-run-supervisor.md" in mkdocs
