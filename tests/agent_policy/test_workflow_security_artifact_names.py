"""Audit-artifact names remain reviewed literals, not mutable step output."""

from __future__ import annotations

from pathlib import Path

from tests.agent_policy.test_workflow_security import _policy_payload, workflow_security


def test_workflow_security_rejects_indirect_name_for_literal_audit_artifact(tmp_path: Path) -> None:
    workflow = tmp_path / "audit.yml"
    workflow.write_text(
        """
name: Audit artifact contract
on:
  workflow_dispatch:
permissions:
  contents: read
jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - id: authorization-identity
        run: echo artifact_name=unexpected >> "${GITHUB_OUTPUT}"
      - name: Upload authorization evidence
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a
        with:
          name: ${{ steps.authorization-identity.outputs.artifact_name }}
          path: evidence/
          retention-days: 90
""".lstrip(),
        encoding="utf-8",
    )
    literal_name = "audit-evidence-${{ github.run_id }}-${{ github.run_attempt }}"
    policy = workflow_security.WorkflowSecurityPolicy.from_mapping(
        _policy_payload(
            required_audit_artifacts={
                "audit.yml": {
                    literal_name: {
                        "retention_days": 90,
                        "reason": "Keep one exact attempt-bound audit artifact.",
                    }
                }
            }
        ),
        label="policy.yml",
    )

    result = workflow_security.validate_workflow_file(workflow, policy=policy)

    assert result.errors == [f"{workflow}: required audit artifact {literal_name} is not uploaded"]
