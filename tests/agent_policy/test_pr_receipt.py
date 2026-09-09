from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


pr_receipt = _load("dpone_agent_pr_receipt", "tools/agent_policy/pr_receipt.py")
governance_artifact_content = _load(
    "dpone_agent_governance_artifact_content_pr_receipt_test",
    "tools/agent_policy/governance_artifact_content.py",
)


COMPLETE_BODY = """
## Summary

- Problem: agent governance controls changed.

## Design and scope

- Approved specification or issue: #275
- In scope: PR receipt traceability evidence.
- Non-goals: Runtime ETL behavior.

## Validation evidence

| Check | Status | Command/workflow | Artifact/notes |
|---|---|---|---|
| Focused tests | PASS | `uv run pytest tests/agent_policy/test_pr_receipt.py -q` | test output |
| Agent governance gate | PASS | `uv run python tools/agent_policy/governance_gate.py --base-ref origin/master` | agent_governance_gate.json |

## Owner attestation

- [x] Owner reviewed the final diff and accepts the change.
- [x] Required GitHub checks are green on the reviewed head commit.
- [x] Admin bypass was not used.
- [x] Agent governance receipt is attached when agent controls changed.

Evidence: agent_governance_gate.json
""".strip()


def _governance_content(changed_paths: list[str]) -> object:
    return governance_artifact_content.content_from_payload(
        {
            "schema_version": 1,
            "status": "PASS",
            "control_surface_changed": True,
            "changed_paths": changed_paths,
            "head_commit": "abc1234",
            "checks": [{"name": "changed_control_surface_red_team", "status": "PASS"}],
        }
    )


def test_pr_receipt_is_not_applicable_without_control_surface_changes() -> None:
    result = pr_receipt.validate_pr_receipt(
        body="",
        changed_paths=["docs/run.md"],
    )

    assert result.status == "N/A"
    assert result.errors == []
    assert pr_receipt.result_payload(result)["traceability"] is None


def test_pr_receipt_passes_for_control_surface_with_complete_attestation() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
    )
    payload = pr_receipt.result_payload(result)

    assert result.status == "PASS"
    assert result.errors == []
    assert payload["traceability"] == {
        "approved_source": "#275",
        "approved_source_kind": "issue",
        "validation_rows": [
            {
                "check": "Focused tests",
                "status": "PASS",
                "command": "uv run pytest tests/agent_policy/test_pr_receipt.py -q",
                "notes": "test output",
            },
            {
                "check": "Agent governance gate",
                "status": "PASS",
                "command": "uv run python tools/agent_policy/governance_gate.py --base-ref origin/master",
                "notes": "agent_governance_gate.json",
            },
        ],
        "validation_statuses": ["PASS"],
        "non_pass_reasons": [],
        "owner_attestation": {
            "owner_review": True,
            "required_checks": True,
            "admin_bypass": True,
            "governance_receipt": True,
        },
        "governance_receipt_referenced": True,
    }
    Draft202012Validator(json.loads((ROOT / "evals/agent/pr-receipt.schema.json").read_text())).validate(payload)


def test_pr_receipt_fails_when_live_required_check_is_not_green() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=pr_receipt.GitHubEvidence(
            head_sha="abc1234",
            required_checks=["Agent PR receipt", "Quality checks (3.11)"],
            check_runs=[
                pr_receipt.GitHubCheckEvidence(
                    name="Quality checks (3.11)",
                    source="check_run",
                    status="completed",
                    conclusion="failure",
                    url="https://github.example/check",
                )
            ],
            statuses=[],
            artifacts=[
                pr_receipt.GitHubArtifactEvidence(
                    name="agent-governance-gate",
                    artifact_id=42,
                    workflow_run_head_sha="abc1234",
                    workflow_run_id=10,
                    digest="sha256:" + "a" * 64,
                    size_in_bytes=200,
                    archive_sha256="sha256:" + "a" * 64,
                    archive_size_bytes=200,
                    expired=False,
                    url="https://github.example/artifact",
                    content=_governance_content(["tools/agent_policy/governance_gate.py"]),
                )
            ],
        ),
        require_github_evidence=True,
    )

    assert result.status == "FAIL"
    assert any("Quality checks (3.11)" in error and "not successful" in error for error in result.errors)


def test_pr_receipt_ignores_self_check_when_validating_live_required_checks() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        github_evidence=pr_receipt.GitHubEvidence(
            head_sha="abc1234",
            required_checks=["Agent PR receipt", "Quality checks (3.11)"],
            check_runs=[
                pr_receipt.GitHubCheckEvidence(
                    name="Quality checks (3.11)",
                    source="check_run",
                    status="completed",
                    conclusion="success",
                    url="https://github.example/check",
                )
            ],
            statuses=[],
            artifacts=[
                pr_receipt.GitHubArtifactEvidence(
                    name="agent-governance-gate",
                    artifact_id=42,
                    workflow_run_head_sha="abc1234",
                    workflow_run_id=10,
                    digest="sha256:" + "a" * 64,
                    size_in_bytes=200,
                    archive_sha256="sha256:" + "a" * 64,
                    archive_size_bytes=200,
                    expired=False,
                    url="https://github.example/artifact",
                    content=_governance_content(["tools/agent_policy/governance_gate.py"]),
                )
            ],
        ),
        require_github_evidence=True,
    )

    assert result.status == "PASS"
    assert result.errors == []


def test_pr_receipt_fails_when_governance_artifact_is_missing_on_head() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=[".github/workflows/ci.yml"],
        github_evidence=pr_receipt.GitHubEvidence(
            head_sha="abc1234",
            required_checks=["Quality checks (3.11)"],
            check_runs=[
                pr_receipt.GitHubCheckEvidence(
                    name="Quality checks (3.11)",
                    source="check_run",
                    status="completed",
                    conclusion="success",
                    url="https://github.example/check",
                )
            ],
            statuses=[],
            artifacts=[],
        ),
        require_github_evidence=True,
    )

    assert result.status == "FAIL"
    assert any("agent-governance-gate" in error and "abc1234" in error for error in result.errors)


def test_pr_receipt_fails_when_governance_artifact_content_has_stale_paths() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/pr_receipt.py"],
        github_evidence=pr_receipt.GitHubEvidence(
            head_sha="abc1234",
            required_checks=["Quality checks (3.11)"],
            check_runs=[
                pr_receipt.GitHubCheckEvidence(
                    name="Quality checks (3.11)",
                    source="check_run",
                    status="completed",
                    conclusion="success",
                    url="https://github.example/check",
                )
            ],
            statuses=[],
            artifacts=[
                SimpleNamespace(
                    name="agent-governance-gate",
                    artifact_id=42,
                    workflow_run_head_sha="abc1234",
                    workflow_run_id=10,
                    digest="sha256:" + "a" * 64,
                    archive_sha256="sha256:" + "a" * 64,
                    expired=False,
                    url="https://github.example/artifact",
                    size_in_bytes=1719,
                    archive_size_bytes=1719,
                    created_at="2026-07-13T00:00:00Z",
                    expires_at="2026-10-11T00:00:00Z",
                    content=governance_artifact_content.content_from_payload(
                        {
                            "schema_version": 1,
                            "status": "PASS",
                            "control_surface_changed": True,
                            "changed_paths": [],
                            "head_commit": "abc1234",
                            "checks": [
                                {
                                    "name": "changed_control_surface_red_team",
                                    "status": "PASS",
                                }
                            ],
                        }
                    ),
                )
            ],
        ),
        require_github_evidence=True,
    )

    assert result.status == "FAIL"
    assert any("changed_paths" in error and "missing" in error for error in result.errors)


def test_pr_receipt_requires_live_github_evidence_when_enforced() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY,
        changed_paths=["tools/agent_policy/governance_gate.py"],
        require_github_evidence=True,
    )

    assert result.status == "FAIL"
    assert any("Live GitHub evidence is required" in error for error in result.errors)


def test_pr_receipt_requires_owner_attestation_for_control_surface() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY.replace("[x] Owner reviewed", "[ ] Owner reviewed"),
        changed_paths=[".github/workflows/ci.yml"],
    )

    assert result.status == "FAIL"
    assert any("owner review attestation" in error for error in result.errors)


def test_pr_receipt_requires_governance_receipt_reference_for_control_surface() -> None:
    body = COMPLETE_BODY.replace("Evidence: agent_governance_gate.json", "").replace(
        "agent_governance_gate.json",
        "governance receipt artifact",
    )

    result = pr_receipt.validate_pr_receipt(
        body=body,
        changed_paths=[".agents/policy/tool-registry.yml"],
    )

    assert result.status == "FAIL"
    assert any("agent_governance_gate.json" in error for error in result.errors)


def test_pr_receipt_cli_writes_json(tmp_path: Path) -> None:
    body_path = tmp_path / "body.md"
    paths_path = tmp_path / "changed.txt"
    output_path = tmp_path / "receipt.json"
    body_path.write_text(COMPLETE_BODY, encoding="utf-8")
    paths_path.write_text("docs/agent-governance.md\n", encoding="utf-8")

    code = pr_receipt.main(
        [
            "--body-file",
            str(body_path),
            "--changed-paths-file",
            str(paths_path),
            "--output",
            str(output_path),
            "--format",
            "json",
        ]
    )
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["schema_version"] == 2
    assert payload["status"] == "PASS"
    assert payload["control_surface_changed"] is True
    assert payload["github_evidence"] is None
    assert payload["traceability"]["approved_source"] == "#275"


def test_pr_receipt_schema_accepts_bounded_wait_outcome() -> None:
    result = pr_receipt.PullRequestReceiptResult(status="PASS", control_surface_changed=False, changed_paths=[])
    result.wait = {"state": "READY", "attempts": 1, "elapsed_seconds": 0.5, "reason": None}
    payload = pr_receipt.result_payload(result)

    Draft202012Validator(json.loads((ROOT / "evals/agent/pr-receipt.schema.json").read_text())).validate(payload)


def test_pr_receipt_cli_preserves_nul_delimited_unusual_paths(tmp_path: Path) -> None:
    body_path = tmp_path / "body.md"
    paths_path = tmp_path / "changed-paths.bin"
    output_path = tmp_path / "receipt.json"
    paths = [".agents/политика.yml", "docs/line\nbreak.md"]
    body_path.write_text(COMPLETE_BODY, encoding="utf-8")
    paths_path.write_bytes(b"\0".join(path.encode() for path in paths) + b"\0")

    code = pr_receipt.main(
        [
            "--body-file",
            str(body_path),
            "--changed-paths-file",
            str(paths_path),
            "--output",
            str(output_path),
            "--format",
            "json",
        ]
    )

    assert code == 0
    assert json.loads(output_path.read_text(encoding="utf-8"))["changed_paths"] == sorted(paths)
