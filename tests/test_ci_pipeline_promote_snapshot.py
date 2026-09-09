from __future__ import annotations

from pathlib import Path

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_promote_snapshot_job_and_stage() -> None:
    data = load_gitlab_ci_config()
    assert "promote" in data["stages"]
    assert "promote_dev_snapshot_to_argocd_mr" in data


def test_ci_promote_snapshot_job_references_tool_and_snapshot_file() -> None:
    text = load_gitlab_ci_text()
    assert "tools/promote_snapshot_to_argocd_mr.py" in text
    assert ".helm/overrides/airflow-dev-dpone-snapshot.yaml" in text
    assert "promote_argocd_result.json" in text
