from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.production_maturity import ProductionMaturityService


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_production_maturity_gate_passes_non_certification_domains_when_green(tmp_path: Path) -> None:
    artifacts = {
        "cdc": _write_json(tmp_path / "cdc.json", {"passed": True, "duplicate_count": 0}),
        "performance": _write_json(tmp_path / "benchmark.json", {"passed": True, "items": []}),
        "security": _write_json(tmp_path / "security.json", {"passed": True, "findings": []}),
        "supply_chain": _write_json(tmp_path / "supply.json", {"passed": True, "blockers": []}),
        "governance": _write_json(tmp_path / "compatibility.json", {"passed": True, "violations": []}),
        "docs": _write_json(tmp_path / "docs.json", {"passed": True, "links_checked": 860}),
    }

    report = ProductionMaturityService().evaluate(
        output_dir=tmp_path / "maturity",
        release="v0.5.1",
        artifacts=artifacts,
        required_domains=tuple(artifacts),
    )

    assert report.passed is True
    assert report.level == "ga_ready"
    assert report.score == 100.0
    assert report.blockers == ()
    assert {item.domain for item in report.items} == set(artifacts)
    assert all(len(item.sha256) == 64 for item in report.items)
    assert (tmp_path / "maturity" / "production_maturity.json").exists()
    assert (tmp_path / "maturity" / "production_maturity.md").exists()


def test_production_maturity_rejects_self_declared_verified_certification(tmp_path: Path) -> None:
    certification = _write_json(
        tmp_path / "certification.json",
        {
            "schema_version": "attacker.self-declared.v1",
            "passed": True,
            "evidence_status": "PASS",
            "production_certification": "VERIFIED",
            "blockers": [],
        },
    )

    report = ProductionMaturityService().evaluate(
        output_dir=tmp_path / "maturity",
        release="v0.74.0",
        artifacts={"certification": certification},
        required_domains=("certification",),
    )

    assert report.passed is False
    assert report.blockers == ("certification.not_passed",)


def test_production_maturity_gate_blocks_missing_and_failed_required_domains(tmp_path: Path) -> None:
    certification = _write_json(tmp_path / "certification.json", {"passed": False, "blockers": ["matrix.red"]})
    security = _write_json(tmp_path / "security.json", {"passed": True})

    report = ProductionMaturityService().evaluate(
        output_dir=tmp_path / "maturity",
        release="v0.5.1",
        artifacts={"certification": certification, "security": security},
        required_domains=("certification", "security", "supply_chain"),
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert report.score == 33.33
    assert report.blockers == ("certification.not_passed", "supply_chain.missing")
    assert "Fix production maturity blockers" in report.to_markdown()


def test_production_maturity_rejects_unverified_certification(tmp_path: Path) -> None:
    certification = _write_json(
        tmp_path / "certification.json",
        {
            "passed": True,
            "evidence_status": "UNVERIFIED",
            "profile": "mock_contract",
        },
    )

    report = ProductionMaturityService().evaluate(
        output_dir=tmp_path / "maturity",
        release="v0.73.17",
        artifacts={"certification": certification},
        required_domains=("certification",),
    )

    assert report.passed is False
    assert report.blockers == ("certification.not_passed",)
