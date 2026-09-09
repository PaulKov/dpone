from __future__ import annotations

import hashlib
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ACTION_REF = re.compile(r"uses:\s*([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*)@([^#\s]+)")
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
PINNED_UV_VERSION = 'version: "0.11.28"'


def test_all_github_actions_are_pinned_to_full_commit_shas() -> None:
    unpinned: list[str] = []
    for workflow in sorted((ROOT / ".github/workflows").glob("*.yml")):
        for line_number, line in enumerate(workflow.read_text(encoding="utf-8").splitlines(), start=1):
            match = ACTION_REF.search(line)
            if match and not FULL_SHA.fullmatch(match.group(2)):
                unpinned.append(f"{workflow.relative_to(ROOT)}:{line_number}: {match.group(0)}")

    assert unpinned == []


def test_setup_uv_steps_pin_uv_runtime_version() -> None:
    missing: list[str] = []
    for workflow in sorted((ROOT / ".github/workflows").glob("*.yml")):
        lines = workflow.read_text(encoding="utf-8").splitlines()
        for line_number, line in enumerate(lines, start=1):
            if "uses: astral-sh/setup-uv@" not in line:
                continue

            block: list[str] = []
            for follow in lines[line_number : line_number + 8]:
                if re.match(r"\s*-\s+(name|uses):", follow):
                    break
                block.append(follow)

            if PINNED_UV_VERSION not in "\n".join(block):
                missing.append(f"{workflow.relative_to(ROOT)}:{line_number}")

    assert missing == []


def test_dependency_review_workflow_blocks_high_severity_pr_dependencies() -> None:
    workflow_path = ROOT / ".github/workflows/dependency-review.yml"
    workflow = workflow_path.read_text(encoding="utf-8")
    parsed = yaml.safe_load(workflow)
    events = parsed.get("on", parsed.get(True))
    review_steps = [
        step
        for step in parsed["jobs"]["dependency-review"]["steps"]
        if str(step.get("uses", "")).startswith("actions/dependency-review-action@")
    ]

    assert parsed["name"] == "Dependency Review"
    assert set(events) == {"pull_request", "push"}
    assert len(review_steps) == 2
    assert all(step["with"]["fail-on-severity"] == "high" for step in review_steps)
    assert all(step["with"]["comment-summary-in-pr"] == "never" for step in review_steps)


def test_branch_protection_docs_require_dependency_review_check() -> None:
    docs = (ROOT / "docs/github-branch-protection.md").read_text(encoding="utf-8")

    assert "Dependency Review" in docs
    assert "dependency-review.yml" in docs
    assert "fail-on-severity: high" in docs


def test_ci_uploads_agent_governance_gate_receipt() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "tools/agent_policy/governance_gate.py" in workflow
    assert "test_artifacts/agent-policy/agent_governance_gate.json" in workflow
    assert "name: agent-governance-gate" in workflow


def test_ci_attests_agent_governance_gate_receipt() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    source = workflow["jobs"]["governance-source"]
    finalizer = workflow["jobs"]["governance-attestation"]

    assert source["needs"] == ["quality"]
    assert source["permissions"] == {"contents": "read"}
    assert finalizer.keys() == {"needs", "runs-on", "permissions", "steps"}
    assert finalizer["needs"] == ["governance-source"]
    assert finalizer["permissions"] == {
        "actions": "read",
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
    }
    assert len(finalizer["steps"]) == 2
    assert finalizer["steps"][1]["uses"].startswith("actions/attest-build-provenance@")
    assert finalizer["steps"][1]["with"]["subject-path"] == (
        "test_artifacts/agent-policy/attested-governance/agent_governance_gate.json"
    )


def test_agent_pr_receipt_requires_governance_artifact_attestation() -> None:
    workflow = (ROOT / ".github/workflows/agent-pr-receipt.yml").read_text(encoding="utf-8")

    assert "attestations: read" in workflow
    assert "--require-github-attestation" in workflow
    assert '--pull-request-number "${{ github.event.pull_request.number }}"' in workflow


def test_ci_governance_gate_uses_pull_request_changed_paths() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "fetch-depth: 0" in workflow
    assert "BASE_SHA: ${{ github.event.pull_request.base.sha }}" in workflow
    assert "HEAD_SHA: ${{ github.event.pull_request.head.sha || github.sha }}" in workflow
    assert (
        "mapfile -d '' -t changed_paths < <(git diff --no-renames --name-only "
        '--diff-filter=ACDMRT -z "$BASE_SHA...$HEAD_SHA")' in workflow
    )
    assert '"${changed_paths[@]}"' in workflow
    assert '--head-commit "$HEAD_SHA"' in workflow


def test_pr3b_workflow_topology_and_action_only_codeql_match_the_docs() -> None:
    workflows_doc = " ".join((ROOT / "docs/cicd/workflows.md").read_text(encoding="utf-8").split())
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    jobs = ci["jobs"]
    quality, source, finalizer = (jobs[name] for name in ("quality", "governance-source", "governance-attestation"))
    assert quality["permissions"] == source["permissions"] == {"contents": "read"}
    assert source["needs"] == ["quality"]
    assert source["outputs"] == {
        "artifact_id": "${{ steps.governance-upload.outputs.artifact-id }}",
        "artifact_digest": "${{ steps.governance-upload.outputs.artifact-digest }}",
    }
    upload = next(step for step in source["steps"] if step.get("id") == "governance-upload")
    assert upload["with"] == {
        "name": "agent-governance-gate",
        "path": "test_artifacts/agent-policy/agent_governance_gate.json",
        "if-no-files-found": "error",
        "retention-days": 90,
        "archive": True,
        "overwrite": False,
        "include-hidden-files": False,
    }
    assert finalizer["needs"] == ["governance-source"]
    assert finalizer["permissions"] == {
        "actions": "read",
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
    }
    assert finalizer["steps"] == [
        {
            "uses": "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
            "with": {
                "artifact-ids": "${{ needs.governance-source.outputs.artifact_id }}",
                "path": "test_artifacts/agent-policy/attested-governance",
                "digest-mismatch": "error",
            },
        },
        {
            "uses": "actions/attest-build-provenance@e8998f949152b193b063cb0ec769d69d929409be",
            "with": {"subject-path": "test_artifacts/agent-policy/attested-governance/agent_governance_gate.json"},
        },
    ]
    for expected in (
        "repository-controlled code read-only",
        "provider artifact ID and digest",
        "source-free",
        "exactly two pinned provider actions",
        "Attestation proves provenance of those bytes, not semantic `PASS`",
    ):
        assert expected in workflows_doc

    codeql = yaml.safe_load((ROOT / ".github/workflows/codeql.yml").read_text(encoding="utf-8"))
    analyze = codeql["jobs"]["analyze"]
    assert codeql["permissions"] == {"contents": "read", "security-events": "write"}
    assert set(codeql["jobs"]) == {"analyze"}
    assert analyze["runs-on"] == "ubuntu-latest"
    assert analyze["steps"] == [
        {
            "uses": "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
            "with": {"persist-credentials": False},
        },
        {
            "uses": "github/codeql-action/init@99df26d4f13ea111d4ec1a7dddef6063f76b97e9",
            "with": {"languages": "python"},
        },
        {"uses": "github/codeql-action/analyze@99df26d4f13ea111d4ec1a7dddef6063f76b97e9"},
    ]
    assert not (ROOT / ".github/codeql/codeql-config.yml").exists()
    assert all(
        expected in workflows_doc
        for expected in (
            "Closed, action-only Python static security analysis",
            "no repository command",
            "custom query/config/pack",
            "hosted CodeQL on the resulting exact head",
        )
    )


def test_ci_quality_checks_out_and_evaluates_the_exact_pull_request_head() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    exact_head = "${{ github.event.pull_request.head.sha || github.sha }}"
    assert f"ref: {exact_head}" in workflow
    assert f"HEAD_SHA: {exact_head}" in workflow
    assert "HEAD_SHA: ${{ github.sha }}" not in workflow
    assert 'if [ "$EVENT_NAME" = "pull_request" ]; then' in workflow
    assert 'BASE_SHA="$(git merge-base "$HEAD_SHA" "$EVENT_BASE_SHA")"' in workflow


def test_agent_pr_receipt_closes_merged_prs_without_manual_dispatch() -> None:
    parsed = yaml.safe_load((ROOT / ".github/workflows/agent-pr-receipt.yml").read_text(encoding="utf-8"))
    events = parsed.get("on", parsed.get(True))

    assert events["pull_request"]["types"] == ["opened", "reopened", "synchronize", "edited", "closed"]
    assert "workflow_dispatch" not in events
    assert "github.event.action == 'synchronize'" in str(parsed["jobs"]["receipt"]["if"])
    assert parsed["concurrency"]["cancel-in-progress"] is True
    closure = parsed["jobs"]["merge-closure"]
    assert "github.event.pull_request.merged == true" in str(closure["if"])
    assert closure["permissions"]["checks"] == "write"
    assert parsed["permissions"]["checks"] == "read"
    assert "--integration-sha" not in (ROOT / ".github/workflows/agent-pr-receipt.yml").read_text(encoding="utf-8")


def test_agent_pr_merge_closure_projects_required_check_to_integration_commit() -> None:
    workflow_path = ROOT / ".github/workflows/agent-pr-receipt.yml"
    workflow = workflow_path.read_text(encoding="utf-8")
    parsed = yaml.safe_load(workflow)
    closure = parsed["jobs"]["merge-closure"]
    steps = closure["steps"]
    step_names = [step["name"] for step in steps]

    assert "tools/agent_policy/pr_merge_check.py" in workflow
    assert "Publish exact integration check" in workflow
    assert '--event "${GITHUB_EVENT_PATH}"' in workflow
    assert "--producer-exit-code test_artifacts/agent-receipt-output/pr-receipt-exit-code.txt" in workflow
    assert "checks: write" in workflow
    assert set(closure["permissions"]) == {"actions", "checks", "contents"}
    assert step_names.index("Upload immutable merge-closure receipt") < step_names.index(
        "Publish exact integration check"
    )
    receipt_upload = steps[step_names.index("Upload immutable merge-closure receipt")]
    projection_upload = steps[step_names.index("Upload exact integration check projection")]
    assert receipt_upload["with"]["if-no-files-found"] == "error"
    assert projection_upload["with"]["name"] == "agent-pr-merge-check"


def test_agent_pr_receipt_changed_paths_disable_rename_detection() -> None:
    workflow = (ROOT / ".github/workflows/agent-pr-receipt.yml").read_text(encoding="utf-8")

    assert "git diff --no-renames --name-only --diff-filter=ACDMRT -z" in workflow


def test_agent_pr_receipt_delegates_reviewed_head_fetch_to_failure_receipt_cli() -> None:
    workflow = (ROOT / ".github/workflows/agent-pr-receipt.yml").read_text(encoding="utf-8")

    assert "refs/pull/${PR_NUMBER}/head" not in workflow
    assert "tools/agent_policy/pr_merge_receipt.py" in workflow


def test_ci_guards_agent_policy_tooling_and_tests_module_size() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "Agent policy module size" in workflow
    assert "--package tools/agent_policy" in workflow
    assert "Agent policy test module size" in workflow
    assert "--package tests/agent_policy" in workflow


def test_heavy_workflows_cancel_only_superseded_pull_request_runs() -> None:
    for workflow_name in ("ci.yml", "airflow-pack-compat.yml"):
        parsed = yaml.safe_load((ROOT / ".github/workflows" / workflow_name).read_text(encoding="utf-8"))
        concurrency = parsed["concurrency"]

        assert "github.event_name == 'pull_request'" in concurrency["group"]
        assert "github.event.pull_request.number || github.run_id" in concurrency["group"]
        assert concurrency["cancel-in-progress"] == "${{ github.event_name == 'pull_request' }}"


def test_dependabot_groups_weekly_minor_patch_updates_with_bounded_open_prs() -> None:
    payload = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text(encoding="utf-8"))
    updates = {item["package-ecosystem"]: item for item in payload["updates"]}

    for ecosystem, group_name, limit in (
        ("github-actions", "github-actions-minor-patch", 2),
        ("uv", "uv-minor-patch", 3),
    ):
        config = updates[ecosystem]
        group = config["groups"][group_name]
        assert config["schedule"]["interval"] == "weekly"
        assert config["open-pull-requests-limit"] == limit
        assert group["patterns"] == ["*"]
        assert group["update-types"] == ["minor", "patch"]


def test_release_sensitive_workflow_blobs_match_pr3a_implementation_base() -> None:
    expected_blobs = {
        "release.yml": "49ca44b0ebaa4d834bed510688acbf8f8a706209",
        "runtime-image.yml": "64e5cbfb588a8864267c10b62456495520c42c65",
        "certification-release-summary.yml": "a2454065619ceaa5ea4e75fb04f35c799cc9fb9c",
        "route-certification-release.yml": "c4eae6a78493e8bdbf180a802e94021b82c2f095",
        "route-release-finalize.yml": "7a2a6674ffd59c5fb494b7ab1583f85b90868db8",
    }

    actual_blobs = {
        path.name: hashlib.sha1(f"blob {path.stat().st_size}\0".encode() + path.read_bytes()).hexdigest()
        for path in (ROOT / ".github/workflows").glob("*.yml")
        if path.name in expected_blobs
    }

    assert actual_blobs == expected_blobs
