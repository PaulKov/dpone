from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_route_rc_executor_user_docs_cover_dry_run_execute_and_runbook() -> None:
    text = (DOCS / "route-rc-executor.md").read_text(encoding="utf-8")

    for needle in (
        "dpone ops route-rc-execute",
        "route_rc_execution.json",
        "--execute",
        "dry-run",
        "timeout",
        "retry",
        "redaction",
        "artifact collection",
        "operator runbook",
        "mssql -> clickhouse",
        "postgres -> mssql",
    ):
        assert needle in text


def test_developer_route_rc_executor_docs_cover_boundaries_and_interfaces() -> None:
    text = (DOCS / "developer-route-rc-executor.md").read_text(encoding="utf-8")

    for needle in (
        "dpone.ops.route_rc_executor",
        "dpone.ops.routes.rc_executor_models",
        "dpone.ops.routes.rc_executor_runner",
        "dpone.ops.routes.rc_executor_redaction",
        "dpone.ops.routes.rc_executor_policy",
        "CommandProcessRunner",
        "RouteReleaseCandidateExecutorService",
        "no route-specific branches",
        "does not open database connections",
    ):
        assert needle in text


def test_route_rc_executor_is_linked_from_docs_workflow_and_cli_reference() -> None:
    files = {
        "index": DOCS / "README.md",
        "architecture": DOCS / "architecture.md",
        "ci_cd": DOCS / "ci-cd.md",
        "developer_ci": DOCS / "developer-ci-cd.md",
        "ops_cli": DOCS / "ops-cli.md",
        "source_sink": DOCS / "source-sink-matrix.md",
        "release_evidence": DOCS / "release-evidence.md",
        "live_certification": DOCS / "live-certification.md",
        "cli_reference": DOCS / "cli-reference.md",
        "mkdocs": ROOT / "mkdocs.yml",
        "workflow": ROOT / ".github" / "workflows" / "live-certification.yml",
    }

    for name, path in files.items():
        text = path.read_text(encoding="utf-8")
        assert "route-rc-execute" in text or "route_rc_execution" in text, name

    assert "route-rc-executor.md" in files["mkdocs"].read_text(encoding="utf-8")
