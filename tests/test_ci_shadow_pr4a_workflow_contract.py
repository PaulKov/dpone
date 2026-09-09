from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "pr-gate-shadow.yml"


def test_shadow_plan_workflow_is_read_only_and_non_authoritative() -> None:
    payload = yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert payload["name"] == "PR Gate shadow"
    assert payload["permissions"] == {"contents": "read"}
    jobs = payload["jobs"]
    assert set(jobs) == {
        "plan",
        "static",
        "contracts",
        "docs",
        "python_311",
        "python_312",
        "packaging",
        "postgresql",
        "runtime_wheel_smoke",
        "airflow",
        "collector",
    }
    assert jobs["plan"]["name"] == "PR Gate shadow plan"
    workflow_text = WORKFLOW.read_text(encoding="utf-8")
    assert "name: PR Gate\n" not in workflow_text
    assert "persist-credentials: false" in workflow_text
    assert "submodules: false" in workflow_text
    assert "overwrite: false" in workflow_text
    assert "retention-days: 90" in workflow_text
    assert "id-token: write" not in workflow_text
    assert "contents: write" not in workflow_text
    assert "PR Gate shadow static" in workflow_text
    assert "PR Gate shadow docs" in workflow_text
    assert "PR Gate shadow contracts" in workflow_text
    assert "PR Gate shadow packaging" in workflow_text
    assert "PR Gate shadow PostgreSQL XMin" in workflow_text
    assert "PR Gate shadow runtime wheel smoke" in workflow_text
    runtime_wheel_smoke = jobs["runtime_wheel_smoke"]
    smoke_command = next(
        str(step["run"])
        for step in runtime_wheel_smoke["steps"]
        if step.get("name") == "Build and install one exact runtime wheel"
    )
    assert "uv build packages/dpone-airflow-pack --wheel --out-dir dist" in smoke_command
    assert "pack_wheel=(dist/dpone_airflow_pack-*.whl)" in smoke_command
    assert '"${pack_wheel[0]}" "${wheel[0]}"' in smoke_command
    assert "PR Gate shadow Airflow" in workflow_text
    assert "fail-fast: false" in workflow_text
    assert jobs["collector"]["name"] == "PR Gate shadow"
    assert "actions: read" in workflow_text
    assert "REPOSITORY_ID: ${{ needs.plan.outputs.repository_id }}" in workflow_text
    assert "PR_NUMBER: ${{ needs.plan.outputs.pr_number }}" in workflow_text
    assert "BASE_SHA: ${{ needs.plan.outputs.base_sha }}" in workflow_text
    assert "HEAD_SHA: ${{ needs.plan.outputs.head_sha }}" in workflow_text
    assert "MERGE_SHA: ${{ needs.plan.outputs.merge_sha }}" in workflow_text
    assert "PR Gate shadow plan identity is incomplete or invalid" in workflow_text
    assert "PR Gate shadow event identity does not match the exact plan" in workflow_text
    assert "uv run --frozen python - <<'PY' >> \"${GITHUB_OUTPUT}\"" in workflow_text
