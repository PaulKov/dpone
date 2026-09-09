from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/pr-gate-shadow-audit.yml"


def test_trusted_audit_workflow_is_read_only_data_only_and_create_only() -> None:
    payload = yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    text = WORKFLOW.read_text(encoding="utf-8")

    assert payload["name"] == "PR Gate shadow audit"
    assert payload["permissions"] == {"actions": "read", "contents": "read", "pull-requests": "read"}
    assert payload["on"]["workflow_run"]["workflows"] == ["PR Gate shadow"]
    assert "conclusion != 'action_required'" in text
    assert "persist-credentials: false" in text
    assert "submodules: false" in text
    assert "astral-sh/setup-uv@37802adc94f370d6bfd71619e3f0bf239e1f3b78" in text
    assert "actions/download-artifact" in text
    assert "github.paginate(github.rest.actions.listWorkflowRunArtifacts" in text
    assert "digest-mismatch: error" in text
    assert "overwrite: false" in text
    assert "retention-days: 90" in text
    assert "contents: write" not in text
    assert "id-token: write" not in text
    assert "checkout@" in text
    assert "github.event.workflow_run.head_sha" not in text
