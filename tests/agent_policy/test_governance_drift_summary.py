from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


governance_drift_summary = _load(
    "dpone_agent_governance_drift_summary",
    "tools/agent_policy/governance_drift_summary.py",
)


def _write_receipt(path: Path, *, status: str, warnings: list[str] | None = None) -> None:
    path.write_text(
        json.dumps({"errors": [], "status": status, "warnings": warnings or []}),
        encoding="utf-8",
    )


def test_governance_drift_summary_passes_when_all_components_pass(tmp_path: Path) -> None:
    _write_receipt(tmp_path / "workflow-security.json", status="passed")
    _write_receipt(tmp_path / "github-ruleset-drift.json", status="passed")
    _write_receipt(tmp_path / "github-classic-branch-protection-drift.json", status="passed")

    summary = governance_drift_summary.build_summary(tmp_path)

    assert summary["overall_status"] == "passed"
    assert summary["errors"] == []
    assert summary["warnings"] == []
    assert summary["components"]["classic_branch_protection"]["status"] == "passed"


def test_governance_drift_summary_is_unverified_when_classic_receipt_is_unverified(tmp_path: Path) -> None:
    _write_receipt(tmp_path / "workflow-security.json", status="passed")
    _write_receipt(tmp_path / "github-ruleset-drift.json", status="passed")
    _write_receipt(
        tmp_path / "github-classic-branch-protection-drift.json",
        status="unverified",
        warnings=["DPONE_GOVERNANCE_GITHUB_TOKEN is not configured."],
    )

    summary = governance_drift_summary.build_summary(tmp_path)

    assert summary["overall_status"] == "unverified"
    assert summary["errors"] == []
    assert summary["warnings"] == ["classic_branch_protection: DPONE_GOVERNANCE_GITHUB_TOKEN is not configured."]


def test_governance_drift_summary_fails_when_any_component_fails(tmp_path: Path) -> None:
    (tmp_path / "workflow-security.json").write_text(
        json.dumps({"errors": ["workflow-security broke"], "status": "failed", "warnings": []}),
        encoding="utf-8",
    )
    _write_receipt(tmp_path / "github-ruleset-drift.json", status="passed")
    _write_receipt(tmp_path / "github-classic-branch-protection-drift.json", status="passed")

    summary = governance_drift_summary.build_summary(tmp_path)

    assert summary["overall_status"] == "failed"
    assert summary["errors"] == ["workflow_security: workflow-security broke"]


def test_governance_drift_summary_cli_writes_summary(tmp_path: Path) -> None:
    output = tmp_path / "summary.json"
    _write_receipt(tmp_path / "workflow-security.json", status="passed")
    _write_receipt(tmp_path / "github-ruleset-drift.json", status="passed")
    _write_receipt(tmp_path / "github-classic-branch-protection-drift.json", status="passed")

    code = governance_drift_summary.main(["--evidence-dir", str(tmp_path), "--output", str(output)])
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["overall_status"] == "passed"


def test_governance_drift_summary_cli_fails_closed_when_unverified(tmp_path: Path) -> None:
    output = tmp_path / "summary.json"
    _write_receipt(tmp_path / "workflow-security.json", status="passed")
    _write_receipt(tmp_path / "github-ruleset-drift.json", status="passed")
    _write_receipt(
        tmp_path / "github-classic-branch-protection-drift.json",
        status="unverified",
        warnings=["DPONE_GOVERNANCE_GITHUB_TOKEN is not configured."],
    )

    code = governance_drift_summary.main(["--evidence-dir", str(tmp_path), "--output", str(output)])
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert code == 1
    assert payload["overall_status"] == "unverified"
