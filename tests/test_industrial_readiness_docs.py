from __future__ import annotations

from pathlib import Path


def test_industrial_readiness_docs_and_workflow_are_discoverable() -> None:
    workflow = Path(".github/workflows/industrial-readiness.yml").read_text(encoding="utf-8")
    guide = Path("docs/industrial-readiness.md").read_text(encoding="utf-8")
    ops_cli = Path("docs/ops-cli.md").read_text(encoding="utf-8")

    assert "uv run dpone ops industrial-readiness" in workflow
    assert "industrial-readiness-report" in workflow
    assert "local matrix, correctness, reliability, performance lab, UX, governance, and schema evolution" in guide
    assert "dpone ops industrial-readiness" in guide
    assert "dpone ops industrial-readiness" in ops_cli


def test_industrial_readiness_workflow_creates_report_dir_before_tee() -> None:
    workflow = Path(".github/workflows/industrial-readiness.yml").read_text(encoding="utf-8")

    mkdir_report = "mkdir -p test_artifacts/industrial_readiness/report"
    stdout_report = "tee test_artifacts/industrial_readiness/report/industrial_readiness.stdout.json"
    mkdir_index = "mkdir -p test_artifacts/industrial_readiness/index"
    stdout_index = "tee test_artifacts/industrial_readiness/index/artifact_index.stdout.json"

    assert mkdir_report in workflow
    assert workflow.index(mkdir_report) < workflow.index(stdout_report)
    assert mkdir_index in workflow
    assert workflow.index(mkdir_index) < workflow.index(stdout_index)
