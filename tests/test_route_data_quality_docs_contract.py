from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_route_data_quality_docs_are_self_service() -> None:
    required_docs = [
        "docs/route-data-quality.md",
        "docs/developer-route-data-quality.md",
    ]
    for relative_path in required_docs:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "Runbook" in text, relative_path
        assert "source -> sink" in text, relative_path
        assert "dpone ops route-data-quality" in text, relative_path
        assert "route_data_quality.json" in text, relative_path

    ops_cli = (ROOT / "docs/ops-cli.md").read_text(encoding="utf-8")
    assert "dpone ops route-data-quality" in ops_cli

    control_plane = (ROOT / "docs/operational-control-plane.md").read_text(encoding="utf-8")
    assert "route-data-quality" in control_plane

    ci_cd = (ROOT / "docs/ci-cd.md").read_text(encoding="utf-8")
    assert "route_data_quality" in ci_cd

    release_gate = (ROOT / "docs/route-release-gate.md").read_text(encoding="utf-8")
    assert "route_data_quality" in release_gate

    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    assert "Route data quality: route-data-quality.md" in mkdocs
