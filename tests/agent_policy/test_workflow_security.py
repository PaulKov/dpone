from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


workflow_security = _load("dpone_agent_workflow_security", "tools/agent_policy/workflow_security.py")


def _policy_payload(
    *,
    required_audit_artifacts: dict[str, dict[str, dict[str, object]]] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "owner": "PaulKov",
        "last_reviewed": "2026-07-12",
        "purpose": "Test policy for documented workflow controls.",
        "required_controls": {
            "forbid_pull_request_target": True,
            "require_top_level_permissions": True,
            "require_pinned_external_actions": True,
            "forbid_pr_workflow_secrets": True,
            "require_audit_artifact_retention": True,
            "require_hidden_ci_artifact_transport": True,
            "isolate_dbt_execution_from_oidc": True,
            "require_platform_owned_prod_trust_policy": True,
        },
        "allowed_workflow_write_permissions": {},
        "required_audit_artifacts": required_audit_artifacts or {},
    }


def test_current_github_workflows_follow_security_policy() -> None:
    result = workflow_security.validate_repository(
        ROOT,
        policy_path=ROOT / ".agents/policy/workflow-security.yml",
        workflows_dir=ROOT / ".github/workflows",
    )

    assert result.errors == []


def test_agent_pr_receipt_runs_for_exact_head_and_merged_close() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/agent-pr-receipt.yml").read_text(encoding="utf-8"))
    events = workflow.get("on", workflow.get(True))
    assert isinstance(events, dict)
    pull_request = events["pull_request"]

    assert pull_request["types"] == ["opened", "reopened", "synchronize", "edited", "closed"]
    assert "workflow_dispatch" not in events
    assert workflow["permissions"]["checks"] == "read"
    assert workflow["jobs"]["merge-closure"]["permissions"]["checks"] == "write"


def test_workflow_security_rejects_missing_top_level_permissions(tmp_path: Path) -> None:
    workflow = tmp_path / "unsafe.yml"
    workflow.write_text(
        """
name: Unsafe
on:
  pull_request:
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo unsafe
""".lstrip(),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(workflow, policy=workflow_security.WorkflowSecurityPolicy.empty())

    assert any("top-level permissions must be a mapping" in error for error in result.errors)


def test_workflow_security_rejects_pull_request_target(tmp_path: Path) -> None:
    workflow = tmp_path / "unsafe.yml"
    workflow.write_text(
        """
name: Unsafe
on:
  pull_request_target:
permissions:
  contents: read
env:
  RELEASE_TOKEN: ${{ secrets.RELEASE_TOKEN }}
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo unsafe
""".lstrip(),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(workflow, policy=workflow_security.WorkflowSecurityPolicy.empty())

    assert any("pull_request_target is forbidden" in error for error in result.errors)


def test_workflow_security_rejects_unpinned_action_reference(tmp_path: Path) -> None:
    workflow = tmp_path / "unsafe.yml"
    workflow.write_text(
        """
name: Unsafe
on:
  push:
permissions:
  contents: read
env:
  RELEASE_TOKEN: ${{ secrets.RELEASE_TOKEN }}
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
""".lstrip(),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(workflow, policy=workflow_security.WorkflowSecurityPolicy.empty())

    assert any("must pin external actions to a full commit SHA" in error for error in result.errors)


def test_workflow_security_rejects_direct_input_interpolation_in_run(
    tmp_path: Path,
) -> None:
    workflow = tmp_path / "unsafe.yml"
    workflow.write_text(
        """
name: Unsafe
on:
  workflow_call:
    inputs:
      project-dir:
        type: string
        required: true
permissions:
  contents: read
env:
  RELEASE_TOKEN: ${{ secrets.RELEASE_TOKEN }}
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: tool --project "${{ inputs.project-dir }}"
""".lstrip(),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(
        workflow,
        policy=workflow_security.WorkflowSecurityPolicy.empty(),
    )

    assert any("interpolates an untrusted GitHub expression" in error for error in result.errors)


def test_workflow_security_allows_input_bound_through_environment(
    tmp_path: Path,
) -> None:
    workflow = tmp_path / "safe.yml"
    workflow.write_text(
        """
name: Safe
on:
  workflow_call:
    inputs:
      project-dir:
        type: string
        required: true
permissions:
  contents: read
env:
  RELEASE_TOKEN: ${{ secrets.RELEASE_TOKEN }}
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - env:
          PROJECT_DIR: ${{ inputs.project-dir }}
        run: tool --project "${PROJECT_DIR}"
""".lstrip(),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(
        workflow,
        policy=workflow_security.WorkflowSecurityPolicy.empty(),
    )

    assert result.errors == []


def test_workflow_security_allows_documented_job_write_permissions(tmp_path: Path) -> None:
    workflow = tmp_path / "pages.yml"
    workflow.write_text(
        """
name: Pages
on:
  push:
permissions: {}
jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      pages: write
      id-token: write
    steps:
      - run: echo deploy
""".lstrip(),
        encoding="utf-8",
    )
    policy = workflow_security.WorkflowSecurityPolicy.from_mapping(
        _policy_payload()
        | {
            "allowed_workflow_write_permissions": {
                "pages.yml": {
                    "jobs": {"deploy": ["id-token", "pages"]},
                    "reason": "GitHub Pages deployment requires these write scopes.",
                }
            }
        },
        label="policy.yml",
    )

    result = workflow_security.validate_workflow_file(workflow, policy=policy)

    assert result.errors == []


def test_workflow_security_rejects_missing_audit_artifact_retention(tmp_path: Path) -> None:
    workflow = tmp_path / "agent-pr-receipt.yml"
    workflow.write_text(
        """
name: Agent PR receipt
on:
  pull_request:
permissions:
  contents: read
jobs:
  receipt:
    runs-on: ubuntu-latest
    steps:
      - name: Upload PR receipt evidence
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a
        with:
          name: agent-pr-receipt
          path: test_artifacts/agent-policy/
""".lstrip(),
        encoding="utf-8",
    )
    policy = workflow_security.WorkflowSecurityPolicy.from_mapping(
        _policy_payload(
            required_audit_artifacts={
                "agent-pr-receipt.yml": {
                    "agent-pr-receipt": {
                        "retention_days": 90,
                        "reason": "Keep PR receipt evidence long enough for governance audits.",
                    }
                }
            }
        ),
        label="policy.yml",
    )

    result = workflow_security.validate_workflow_file(workflow, policy=policy)

    assert any("agent-pr-receipt" in error and "retention-days must be 90" in error for error in result.errors)


def test_workflow_security_allows_required_audit_artifact_retention(tmp_path: Path) -> None:
    workflow = tmp_path / "agent-pr-receipt.yml"
    workflow.write_text(
        """
name: Agent PR receipt
on:
  pull_request:
permissions:
  contents: read
jobs:
  receipt:
    runs-on: ubuntu-latest
    steps:
      - name: Upload PR receipt evidence
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a
        with:
          name: agent-pr-receipt
          path: test_artifacts/agent-policy/
          retention-days: 90
""".lstrip(),
        encoding="utf-8",
    )
    policy = workflow_security.WorkflowSecurityPolicy.from_mapping(
        _policy_payload(
            required_audit_artifacts={
                "agent-pr-receipt.yml": {
                    "agent-pr-receipt": {
                        "retention_days": 90,
                        "reason": "Keep PR receipt evidence long enough for governance audits.",
                    }
                }
            }
        ),
        label="policy.yml",
    )

    result = workflow_security.validate_workflow_file(workflow, policy=policy)

    assert result.errors == []


def test_workflow_security_resolves_bounded_audit_artifact_input_default(
    tmp_path: Path,
) -> None:
    workflow = tmp_path / "evidence.yml"
    workflow.write_text(
        """
name: Evidence
on:
  workflow_call:
    inputs:
      evidence-artifact-name:
        type: string
        default: dpone-dbt-dev-evidence
permissions:
  contents: read
jobs:
  evidence:
    runs-on: ubuntu-latest
    steps:
      - name: Upload evidence
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a
        with:
          name: ${{ inputs.evidence-artifact-name }}
          path: evidence/
          retention-days: 180
""".lstrip(),
        encoding="utf-8",
    )
    policy = workflow_security.WorkflowSecurityPolicy.from_mapping(
        _policy_payload(
            required_audit_artifacts={
                "evidence.yml": {
                    "dpone-dbt-dev-evidence": {
                        "retention_days": 180,
                        "reason": "Keep trusted evidence long enough for promotion audits.",
                    }
                }
            }
        ),
        label="policy.yml",
    )

    result = workflow_security.validate_workflow_file(workflow, policy=policy)

    assert result.errors == []
