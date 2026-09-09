from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_route_schema_evolution_and_repair_docs_are_self_service() -> None:
    required_docs = [
        "docs/route-schema-evolution.md",
        "docs/route-reconciliation-repair.md",
        "docs/developer-route-schema-evolution-repair.md",
    ]
    for relative_path in required_docs:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "Runbook" in text, relative_path
        assert "dpone ops route-" in text, relative_path
        assert "source -> sink" in text, relative_path

    ops_cli = (ROOT / "docs/ops-cli.md").read_text(encoding="utf-8")
    assert "dpone ops route-schema-evolution" in ops_cli
    assert "dpone ops route-reconciliation-repair" in ops_cli

    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    assert "Route schema evolution: route-schema-evolution.md" in mkdocs
    assert "Route reconciliation repair: route-reconciliation-repair.md" in mkdocs
