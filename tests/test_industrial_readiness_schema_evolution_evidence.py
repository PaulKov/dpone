from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.industrial_readiness import DEFAULT_INDUSTRIAL_DOMAINS, IndustrialReadinessService


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_industrial_readiness_requires_schema_evolution_evidence_by_default(tmp_path: Path) -> None:
    assert "schema_evolution" in DEFAULT_INDUSTRIAL_DOMAINS
    artifacts = {
        domain: _write_json(tmp_path / f"{domain}.json", {"passed": True})
        for domain in DEFAULT_INDUSTRIAL_DOMAINS
        if domain != "schema_evolution"
    }

    report = IndustrialReadinessService().evaluate(
        output_dir=tmp_path / "report",
        release="v0.5.1",
        artifacts=artifacts,
    )

    assert report.passed is False
    assert "schema_evolution.missing" in report.blockers
