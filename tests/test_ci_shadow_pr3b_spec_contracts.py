from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import yaml
from tools.agent_policy.public_snapshot_history import HistoricalCheck, assert_public_snapshot_continuity

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"
PARENT = ROOT / "docs/feature-design-ci-pr-gate-exact-sha-evidence.md"
MKDOCS = ROOT / "mkdocs.yml"
ADR_0037 = ROOT / "docs/adr/0037-immutable-agent-pr-merge-closure.md"
DESIGN_BASE = "c5567128e6e9b847b4b2a023a74ea6d60e7ccd09"
ENVELOPE_SOURCE = "518537e616033ecf86bae96d831b8deb23eb8bca"
PR2_MERGE = "ec0323feba829d547e5f9c2d23acaac5b686f1a4"
SENSITIVE_SHA256 = "55b80fe3c773f0313e07288083403e32a8fa002813e96da8c61953c4fede0679"
CODEQL_PROFILE_SHA256 = "36a5ff903eab36601bd3cfe3636a53497a9d72cd7ca71ce39ef9c19a8aef3c88"
QUALITY_JOB_SHA256 = "734cd4a8eb1b6105bff06af68e616108ab03f0b0b7f203eab3e66d5a859c9b32"
GOVERNANCE_PRODUCER_SHA256 = "0bf8016384b224f58cd6be67fc892531cfdad21c114b48fb9f0d90a5d3c9f97f"
GOVERNANCE_FINALIZER_SHA256 = "c5832e92a84ddd4eaa44cbf009d45eb7de2c4a7c3bcfc4258da1702b5aa0b3af"
MERGE_JOB_SHA256 = "92c783c76af4f23ccbce06892fd297f73c830120a030b739a8b05055dec406f3"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _squash(value: str) -> str:
    return " ".join(value.split())


def _marked_yaml(text: str, marker: str) -> dict[str, Any]:
    begin = f"<!-- {marker}:begin -->"
    end = f"<!-- {marker}:end -->"
    assert text.count(begin) == 1
    assert text.count(end) == 1
    marked = text.split(begin, maxsplit=1)[1].split(end, maxsplit=1)[0]
    block = marked.split("```yaml\n", maxsplit=1)[1].split("\n```", maxsplit=1)[0]
    parsed = yaml.safe_load(block)
    assert isinstance(parsed, dict)
    return parsed


def _git_yaml(commit: str, path: str) -> dict[str, Any]:
    completed = subprocess.run(
        ["git", "show", f"{commit}:{path}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    parsed = yaml.safe_load(completed.stdout)
    assert isinstance(parsed, dict)
    return parsed


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _attestor_projection(workflow: dict[str, Any]) -> dict[str, Any]:
    job = workflow["jobs"]["quality"]
    steps = [step for step in job["steps"] if str(step.get("uses", "")).startswith("actions/attest-build-provenance@")]
    assert len(steps) == 1
    step = steps[0]
    return {
        "id": "ADR0037_GOVERNANCE_SOURCE_ATTESTOR",
        "workflow": ".github/workflows/ci.yml",
        "job": "quality",
        "permissions": job["permissions"],
        "action": {key: step[key] for key in ("uses", "if", "with")},
    }


def _execution_envelope(workflow: dict[str, Any], job_id: str) -> dict[str, Any]:
    return {
        "workflow_permissions": workflow.get("permissions", {}),
        "workflow_env": workflow.get("env", {}),
        "workflow_defaults": workflow.get("defaults", {}),
        "job": workflow["jobs"][job_id],
    }


def _valid_lifecycle(spec: str, adr: str) -> bool:
    metadata = spec.split("## Executive summary", maxsplit=1)[0]
    statuses = [line for line in metadata.splitlines() if line.startswith("- Status:")]
    pending = "- [ ] Maintainer changed status to `APPROVED` after reviewing the exact head."
    approved = "- [x] Maintainer changed status to `APPROVED` after reviewing the exact head."
    proposed_adr = all(
        value in adr
        for value in (
            "- Amendment status: PROPOSED",
            "- Approval identity: pending",
            "- Approval date: pending",
        )
    )
    accepted_adr = all(
        value in adr
        for value in (
            "- Amendment status: ACCEPTED",
            "- Approval identity: PaulKov",
            "- Approval date: 2026-08-11",
        )
    )
    return (
        statuses == ["- Status: RESEARCHED"]
        and spec.count(pending) == 1
        and approved not in spec
        and proposed_adr
        and not accepted_adr
    ) or (
        statuses == ["- Status: APPROVED"]
        and spec.count(approved) == 1
        and pending not in spec
        and accepted_adr
        and not proposed_adr
    )


def test_pr3b_lifecycle_and_parent_navigation_are_exact() -> None:
    raw = _read(SPEC)
    adr = _read(ADR_0037)
    parent = _read(PARENT)
    nav = _read(MKDOCS)

    assert _valid_lifecycle(raw, adr)
    researched = raw.replace("- Status: APPROVED", "- Status: RESEARCHED", 1).replace(
        "- [x] Maintainer changed status to `APPROVED` after reviewing the exact head.",
        "- [ ] Maintainer changed status to `APPROVED` after reviewing the exact head.",
        1,
    )
    proposed_adr = (
        adr.replace("- Amendment status: ACCEPTED", "- Amendment status: PROPOSED", 1)
        .replace("- Approval identity: PaulKov", "- Approval identity: pending", 1)
        .replace("- Approval date: 2026-08-11", "- Approval date: pending", 1)
    )
    approved = researched.replace("- Status: RESEARCHED", "- Status: APPROVED", 1).replace(
        "- [ ] Maintainer changed status to `APPROVED` after reviewing the exact head.",
        "- [x] Maintainer changed status to `APPROVED` after reviewing the exact head.",
        1,
    )
    accepted_adr = (
        proposed_adr.replace("- Amendment status: PROPOSED", "- Amendment status: ACCEPTED", 1)
        .replace("- Approval identity: pending", "- Approval identity: PaulKov", 1)
        .replace("- Approval date: pending", "- Approval date: 2026-08-11", 1)
    )
    assert _valid_lifecycle(researched, proposed_adr)
    assert _valid_lifecycle(approved, accepted_adr)
    assert not _valid_lifecycle(approved, proposed_adr)
    assert not _valid_lifecycle(researched, accepted_adr)
    assert DESIGN_BASE in raw
    link = "[PR 3B](feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md)"
    assert parent.count(link) == 2
    assert "linked child status and paired ADR 0037 amendment are the lifecycle authority" in parent
    assert "child specification remains `RESEARCHED`" not in parent
    assert nav.count("feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md") == 1
    assert nav.count("feature-design-ci-shadow-pr3a-ci-hygiene.md") == 1


def test_pr3b_cli_report_and_failure_contract_are_closed() -> None:
    spec = _squash(_read(SPEC))

    for expected in (
        "usage: workflow_security_privileged.py [-h] --root ROOT",
        "[--format {text,json}]",
        "`--format` defaults to `text`",
        "canonical compact JSON object followed by one newline",
        "Policy results, including `FAIL` and `UNVERIFIED`, are written only to stdout",
        "Exit `0` means report status `PASS`",
        "Exit `1` means `FAIL` or `UNVERIFIED`",
        "Argparse usage/type errors exit `2`",
        "never creates or modifies a file",
        '"schema_version": 1',
        '"status": "PASS"',
        '"route_authority": []',
        '"privileged_profiles": []',
        "`route_count >= root_count`",
        "`report.routes[].route_id` set byte-for-byte with the complete",
        "schema shape alone is never proof of graph completeness",
        "Aggregate precedence is `FAIL` over `UNVERIFIED` over `PASS`",
    ):
        assert expected in spec

    for code in (
        "PRIVILEGE_PULL_REQUEST_TARGET",
        "PRIVILEGE_UNAPPROVED_PR_WRITE",
        "PRIVILEGE_PR_SECRET_OR_ENVIRONMENT",
        "PRIVILEGE_PR_SELF_HOSTED",
        "PRIVILEGE_WRITE_ALL",
        "PRIVILEGE_READ_ALL",
        "PRIVILEGE_CODEQL_PROFILE_DRIFT",
        "PRIVILEGE_ADR0037_PROFILE_DRIFT",
        "PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE",
        "PRIVILEGE_UNKNOWN_EXPRESSION",
        "PRIVILEGE_UNKNOWN_PERMISSION",
        "PRIVILEGE_INVALID_WORKFLOW",
        "PRIVILEGE_RESOURCE_LIMIT",
        "PRIVILEGE_CONCURRENT_MUTATION",
    ):
        assert spec.count(f"`{code}`") >= 1

    raw = _read(SPEC)
    assert "return finalize_report(report, snapshot_reference, canonical_route_ids=[])" in raw
    assert raw.count("return finalize_report(report, snapshot_reference, canonical_route_ids=[])") == 2
    assert "return finalize_report(report, snapshot_reference, canonical_route_ids=routes.canonical_ids)" in raw
    assert raw.count("snapshot_reference = snapshot.reference(") == 2
    assert raw.count("build_minimal_nonpass_report(") == 2
    assert raw.count("snapshot_reference,") >= 5
    assert "report_snapshot_matches(report, snapshot_reference)" in raw


def test_pr3b_codeql_profile_is_exact_action_only_yaml() -> None:
    profile = _marked_yaml(_read(SPEC), "pr3b-codeql-profile")

    assert profile == {
        "workflow": ".github/workflows/codeql.yml",
        "events": {
            "pull_request": {"branches": ["master"]},
            "push": {"branches": ["master"]},
            "schedule": [{"cron": "21 3 * * 1"}],
        },
        "permissions": {"contents": "read", "security-events": "write"},
        "jobs": {
            "analyze": {
                "name": "Analyze Python",
                "runs-on": "ubuntu-latest",
                "steps": [
                    {
                        "uses": "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
                        "with": {"persist-credentials": False},
                    },
                    {
                        "uses": "github/codeql-action/init@99df26d4f13ea111d4ec1a7dddef6063f76b97e9",
                        "with": {"languages": "python"},
                    },
                    {"uses": "github/codeql-action/analyze@99df26d4f13ea111d4ec1a7dddef6063f76b97e9"},
                ],
            }
        },
    }
    assert _canonical_sha256(profile) == CODEQL_PROFILE_SHA256
    profile_section = _squash(_read(SPEC).split("### Exact CodeQL profile", maxsplit=1)[1])
    for forbidden in (
        "custom query/config/pack",
        "autobuild",
        "artifact download",
        "local/Docker action",
        "repository custom query filter",
    ):
        assert forbidden in profile_section


def test_pr3b_adr0037_imported_profile_fingerprints_are_reproducible() -> None:
    raw = _read(SPEC)
    replacement = _marked_yaml(raw, "pr3b-adr0037-attestor-profile")
    assert_public_snapshot_continuity(ROOT, (SPEC.relative_to(ROOT).as_posix(), ADR_0037.relative_to(ROOT).as_posix()))
    assert _canonical_sha256(replacement["producer"]) == GOVERNANCE_PRODUCER_SHA256
    assert _canonical_sha256(replacement["finalizer"]) == GOVERNANCE_FINALIZER_SHA256
    assert replacement["producer"]["permissions"] == {"contents": "read"}
    finalizer = replacement["finalizer"]
    assert set(finalizer) == {"workflow_permissions", "workflow_env", "workflow_defaults", "job"}
    assert finalizer["workflow_permissions"] == {"contents": "read"}
    assert finalizer["workflow_env"] == finalizer["workflow_defaults"] == {}
    assert set(finalizer["job"]) == {"needs", "runs-on", "permissions", "steps"}
    assert finalizer["job"]["needs"] == ["governance-source"]
    assert finalizer["job"]["permissions"] == {
        "actions": "read",
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
    }
    assert [step["uses"].split("@", 1)[0] for step in finalizer["job"]["steps"]] == [
        "actions/download-artifact",
        "actions/attest-build-provenance",
    ]
    assert all("run" not in step for step in finalizer["job"]["steps"])
    for expected in (
        SENSITIVE_SHA256,
        QUALITY_JOB_SHA256,
        GOVERNANCE_PRODUCER_SHA256,
        GOVERNANCE_FINALIZER_SHA256,
        MERGE_JOB_SHA256,
    ):
        assert expected in raw
    assert "no trailing newline" in raw
    assert 'separators=(",", ":")' in raw
    assert "must yield\n`PRIVILEGE_ADR0037_PROFILE_DRIFT`" in raw
    assert "Rolling CI back" in raw
    adr = _read(ADR_0037)
    assert "### PR 3B execution-envelope amendment" in adr
    assert "The `PROPOSED` state has no authority while the linked" in adr
    assert QUALITY_JOB_SHA256 in adr
    assert GOVERNANCE_FINALIZER_SHA256 in adr
    assert MERGE_JOB_SHA256 in adr


def test_pr3b_graph_permission_and_resource_boundaries_are_executable() -> None:
    spec = _squash(_read(SPEC))

    for expected in (
        "direct `pull_request` and forbidden `pull_request_target` roots",
        "`LOCAL_WORKFLOW_CALL`",
        "`WORKFLOW_RUN`",
        "`NEEDS`",
        "direct root caller is reusable-workflow level 1",
        "nine local call edges reach level 10",
        "tenth local call edge would create level 11",
        "Maximum `workflow_run` chaining depth is one consumer edge",
        "downstream `workflow_run` trigger from that depth-1 consumer",
        "`secrets: inherit` block the route proof",
        "`A && B`",
        "`A || B`",
        "`none < read < write`",
        "a job permission mapping replaces the workflow mapping",
        "256 workflow files",
        "1 MiB per workflow",
        "32 MiB total workflow bytes",
        "4,096 jobs",
        "8,192 graph edges",
        "expression length 8 KiB",
        "512 expression tokens",
        "YAML depth 32",
        "100,000 YAML nodes",
        "depth 10/11",
        "50/51 unique reusable targets",
    ):
        assert expected in spec


def test_pr3b_architecture_evidence_docs_and_platform_scope_are_complete() -> None:
    spec = _squash(_read(SPEC))

    for path in (
        "workflow_privilege_contracts.py",
        "workflow_privilege_snapshot.py",
        "workflow_privilege_parser.py",
        "workflow_privilege_graph.py",
        "workflow_privilege_expressions.py",
        "workflow_privilege_permissions.py",
        "workflow_privilege_profiles.py",
        "workflow_privilege_report.py",
        "workflow_privilege_service.py",
        "workflow_security_privileged.py",
        "workflow_security.py",
        "evals/agent/workflow-security-privileged-policy.schema.json",
        "evals/agent/workflow-security-privileged-report.schema.json",
        "docs/ci-cd.md",
        "docs/cicd/workflows.md",
        "docs/developer-ci-cd.md",
        "docs/cicd/runbooks.md",
        "docs/testing/overview.md",
        "docs/testing/index.md",
        "docs/agent-governance.md",
        "docs/agent-security-mapping.md",
        "docs/agent-risk-register.md",
        "docs/github-branch-protection.md",
    ):
        assert path in spec
    for expected in (
        "100% of the closed adversarial matrix blocks as FAIL/UNVERIFIED",
        "100 repeated scans produce one SHA-256",
        "Mocked/static tests are not hosted PASS",
        "No new ADR number is required",
        "GitHub Actions, hosted service",
        "GitHub reusable workflows",
        "GitHub `workflow_run`",
        "GitHub CodeQL v4",
        "checked 2026-08-11",
    ):
        assert expected in spec
    for product in ("dlt", "Informatica", "Airbyte", "Fivetran", "Pentaho", "Microsoft SSIS", "gusty"):
        assert product in spec


def historical_adr0037_fingerprints() -> None:
    pr2_projection = _attestor_projection(_git_yaml(PR2_MERGE, ".github/workflows/ci.yml"))
    base_ci = _git_yaml(DESIGN_BASE, ".github/workflows/ci.yml")
    source_ci = _git_yaml(ENVELOPE_SOURCE, ".github/workflows/ci.yml")
    base_projection = _attestor_projection(base_ci)
    receipt = _git_yaml(DESIGN_BASE, ".github/workflows/agent-pr-receipt.yml")
    source_receipt = _git_yaml(ENVELOPE_SOURCE, ".github/workflows/agent-pr-receipt.yml")

    assert pr2_projection == base_projection
    assert _execution_envelope(source_ci, "quality") == _execution_envelope(base_ci, "quality")
    assert _execution_envelope(source_receipt, "merge-closure") == _execution_envelope(receipt, "merge-closure")
    assert _canonical_sha256(pr2_projection) == SENSITIVE_SHA256
    assert _canonical_sha256(_execution_envelope(base_ci, "quality")) == QUALITY_JOB_SHA256
    assert _canonical_sha256(_execution_envelope(receipt, "merge-closure")) == MERGE_JOB_SHA256


HISTORICAL_CHECKS = (
    HistoricalCheck(
        "PR3B historical fingerprints", (PR2_MERGE, DESIGN_BASE, ENVELOPE_SOURCE), historical_adr0037_fingerprints
    ),
)
