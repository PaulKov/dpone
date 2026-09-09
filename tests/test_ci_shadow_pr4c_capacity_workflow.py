from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "pr-gate-shadow-capacity.yml"


def test_capacity_workflow_is_default_branch_only_read_only_and_immutable() -> None:
    payload = yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    text = WORKFLOW.read_text(encoding="utf-8")

    assert payload["name"] == "PR Gate shadow capacity"
    assert payload["on"] == {"workflow_dispatch": ""}
    assert payload["permissions"] == {"actions": "read", "contents": "read", "pull-requests": "read"}
    job = payload["jobs"]["calibrate"]
    assert job["if"] == "github.ref == 'refs/heads/master'"
    assert job["runs-on"] == "ubuntu-24.04"
    assert job["permissions"] == payload["permissions"]
    assert "persist-credentials: false" in text
    assert "submodules: false" in text
    assert "ref: ${{ github.sha }}" in text
    assert "actions/cache" not in text
    assert "pull_request" not in text
    assert "archive: true" in text
    assert "overwrite: false" in text
    assert "retention-days: 90" in text
    assert "continue-on-error: true" in text
    assert "Preserve diagnostic unverified outcome" in text
