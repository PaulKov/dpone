from __future__ import annotations

import json
import re
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, SnapshotFile
from tools.agent_policy.workflow_privilege_graph import build_graph, expand_routes
from tools.agent_policy.workflow_privilege_parser import parse_workflow, valid_workflow_call
from tools.agent_policy.workflow_privilege_permissions import resolve_authority

from tests.ci_shadow_pr3b_report_contract_support import policy

ROOT = Path(__file__).resolve().parents[1]
RUNBOOK_ANCHOR = "docs/cicd/runbooks.md#semantic-pr-privilege-boundary"
INTERNAL_DIAGNOSTIC = "PRIVILEGE_INTERNAL_REPORT_INVALID: report construction or schema validation failed"
CERTIFICATION = Path("test_artifacts/agent-policy/pr3b-semantic-privilege-certification.json")
REPORT_SCHEMA = Path("evals/agent/workflow-security-privileged-report.schema.json")


def _read(relative_path: str | Path) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def _yaml(relative_path: str | Path) -> dict[str, Any]:
    parsed = yaml.safe_load(_read(relative_path))
    assert isinstance(parsed, dict)
    return parsed


def _squash(value: str) -> str:
    return " ".join(value.split())


def _section(relative_path: str, heading: str) -> str:
    return _read(relative_path).split(heading, maxsplit=1)[1].split("\n## ", maxsplit=1)[0]


def _parsed_workflow(path: str, content: str) -> dict[str, Any]:
    raw = content.encode()
    source = SnapshotFile(path, "0644", len(raw), sha256(raw).hexdigest(), raw)
    return parse_workflow(source, limits=V1_LIMITS).to_mapping()


def _heading_slug(heading: str) -> str:
    normalized = re.sub(r"[^\w\s-]", "", heading.strip().lower())
    return re.sub(r"[\s-]+", "-", normalized).strip("-")


def test_pr3b_runbook_anchor_and_all_policy_recoveries_are_rendered() -> None:
    policy = _yaml(".agents/policy/workflow-security-privileged.yml")
    recovery = policy["recovery"]
    assert recovery["runbook_anchor"] == RUNBOOK_ANCHOR

    relative_path, separator, anchor = RUNBOOK_ANCHOR.partition("#")
    assert separator == "#"
    runbook = _read(relative_path)
    headings = re.findall(r"^#{1,6}\s+(.+?)\s*$", runbook, flags=re.MULTILINE)
    assert anchor in {_heading_slug(heading) for heading in headings}

    section = runbook.split("### Finding and recovery reference", maxsplit=1)[1]
    section = section.split("### Governance source/finalizer recovery", maxsplit=1)[0]
    rows = re.findall(
        r"^\| `(?P<code>PRIVILEGE_[A-Z0-9_]+)` "
        r"\| `(?P<status>FAIL|UNVERIFIED)` "
        r"\| `(?P<recovery>[A-Z0-9_]+)` "
        r"\| (?P<guidance>[^|\n]+) \|$",
        section,
        flags=re.MULTILINE,
    )
    rendered = {
        code: {"status": status, "recovery_command_id": command_id}
        for code, status, command_id, guidance in rows
        if guidance.strip()
    }
    expected = recovery["code_to_outcome"]

    assert len(expected) == 15
    assert len(rows) == len(rendered) == 15
    assert rendered == expected


def test_pr3b_docs_freeze_standalone_and_umbrella_stream_contracts() -> None:
    workflow_raw = _read("docs/cicd/workflows.md")
    testing_raw = _read("docs/testing/index.md")
    runbook_raw = _read("docs/cicd/runbooks.md")
    workflow_reference = _squash(workflow_raw)
    testing_reference = _squash(testing_raw)
    runbook = _squash(runbook_raw)

    assert "| `PASS` | stdout only, exit `0` |" in workflow_raw
    assert "| `FAIL` or `UNVERIFIED` | schema-valid report on stdout only, exit `1` |" in workflow_raw
    assert "| CLI usage error | no stdout, argparse diagnostics on stderr, exit `2` |" in workflow_raw
    assert "exit `3` |" in workflow_raw
    assert "| `PASS` | `0` | one complete text or canonical JSON report | empty |" in testing_raw
    assert "| `FAIL` / `UNVERIFIED` | `1` | one schema-valid text or canonical JSON report | empty |" in testing_raw
    assert "| invalid CLI arguments | `2` | empty | argparse usage/type diagnostic |" in testing_raw
    assert "| internal report failure | `3` | empty | exactly" in testing_raw
    assert "| `PASS` | `0` | report on stdout; empty stderr |" in runbook_raw
    assert "| `FAIL` | `1` | schema-valid report on stdout; empty stderr |" in runbook_raw
    assert "| `UNVERIFIED` | `1` | schema-valid report on stdout; empty stderr |" in runbook_raw
    assert "| CLI usage error | `2` | no stdout; argparse diagnostic on stderr |" in runbook_raw
    assert "| Internal report failure | `3` | no stdout; fixed diagnostic on stderr |" in runbook_raw

    assert "required `--root`" in workflow_reference
    assert "`--root` is required" in runbook
    assert "--root ." in testing_reference
    assert "--format text" in testing_reference
    assert "fixed `PRIVILEGE_INTERNAL_REPORT_INVALID` diagnostic" in workflow_reference
    assert INTERNAL_DIAGNOSTIC in testing_reference
    assert INTERNAL_DIAGNOSTIC in runbook
    assert "report on stdout; empty stderr" in runbook
    assert "schema-valid report on stdout; empty stderr" in runbook
    assert "fixed diagnostic on stderr" in runbook
    assert "one canonical compact object plus one newline" in workflow_reference
    assert "uv sync --locked --all-extras" in workflow_reference
    assert "uv sync --locked --all-extras" in testing_reference
    assert "uv sync --locked --all-extras" in runbook
    guarded = "uv run --locked --no-sync --offline --no-python-downloads python -B"
    assert all(guarded in page for page in (workflow_reference, testing_reference, runbook))
    assert "environment preparation, not part of the scanner contract" in workflow_reference
    assert "apply to the direct scanner process from Python process start" in workflow_reference
    assert "do not describe `uv sync` or other wrapper behavior" in workflow_reference
    assert "convenience command that assumes the environment is already prepared" in workflow_reference
    assert "scanner process: read-only, credential-free, no network and no file creation" in workflow_reference
    assert "exactly `status`, `errors`, and `warnings`" in testing_reference
    assert "semantic-pr-privilege-internal=PRIVILEGE_INTERNAL_REPORT_INVALID" in runbook
    assert "emits no partial semantic finding or traceback" in runbook
    assert "ordinary compatibility handling exits `1`" in testing_reference

    transcript = re.search(r"The expected text shape.*?```text\n(.*?)\n```", runbook_raw, re.DOTALL)
    assert transcript is not None
    completed = subprocess.run(
        [sys.executable, "-B", "tools/agent_policy/workflow_security_privileged.py", "--root", ".", "--format", "text"],
        cwd=ROOT,
        check=False,
        capture_output=True,
    )
    assert completed.returncode == 0 and completed.stderr == b""
    assert completed.stdout == f"{transcript.group(1)}\n".encode()


def test_pr3b_developer_guidance_is_least_privilege_and_rollback_safe() -> None:
    raw_guide = _read("docs/developer-ci-cd.md")
    guide = _squash(raw_guide)

    for expected in (
        "PR-reachable privilege checklist",
        "start with `permissions: {}` or the smallest explicit read map",
        "Do not use `read-all`, `write-all`, PR secrets, environments, or self-hosted runners",
        "repository commands, checkout, setup, install, cache, and scripts stay in read-only jobs",
        "one exact mandatory closed profile",
        "A write-scope allowlist entry cannot authorize a reachable route",
        "Never widen the fixture or policy to hide an unknown route",
        "separately reviewed closed-profile contract change",
        "prior approved ADR 0037 amendment",
        "Emergency containment may disable the finalizer",
        "never authorizes restoring OIDC or attestation permissions to the `quality` job",
    ):
        assert expected in guide

    example = re.search(r"\s+```yaml\n(\s+name: PR checks\n.*?)\n\s+```", raw_guide, flags=re.DOTALL)
    assert example is not None
    workflow = yaml.safe_load("\n".join(line[3:] for line in example.group(1).splitlines()))
    assert workflow["permissions"] == {"contents": "read"}
    check = workflow["jobs"]["check"]
    assert check["runs-on"] == "ubuntu-latest"
    assert check["steps"][0] == {
        "uses": "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "with": {"persist-credentials": False},
    }
    assert workflow.get("on", workflow.get(True)) == {"pull_request": {"branches": ["master"]}}
    assert check["steps"][1:3] == [
        {
            "uses": "astral-sh/setup-uv@37802adc94f370d6bfd71619e3f0bf239e1f3b78",
            "with": {"version": "0.11.28"},
        },
        {
            "uses": "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1",
            "with": {"python-version": "3.12"},
        },
    ]
    assert check["steps"][3:] == [
        {"run": "uv sync --locked --all-extras"},
        {"run": "uv run --no-sync pytest tests/test_runtime_optional_import_safety.py -q"},
    ]

    section = raw_guide.split("## Calling a local reusable workflow safely", maxsplit=1)[1]
    rendered_section = _squash(section)
    blocks = re.findall(
        r"```yaml\n(.*?)\n```", section.split("## Choosing concurrency behavior", maxsplit=1)[0], re.DOTALL
    )
    assert len(blocks) == 2
    paths = ".github/workflows/pr-report.yml .github/workflows/reusable-report.yml".split()
    workflows = {path: _parsed_workflow(path, block) for path, block in zip(paths, blocks, strict=True)}
    caller, callee = (workflows[path] for path in paths)
    caller_job = caller["jobs"]["report"]
    assert caller["permissions"] == callee["permissions"] == {}
    assert valid_workflow_call(caller_job, callee)
    assert [type(value) for value in caller_job["with"].values()] == [bool, int, str]
    assert not valid_workflow_call({**caller_job, "strategy": {}}, callee)
    assert not valid_workflow_call({**caller_job, "with": {**caller_job["with"], "attempts": True}}, callee)

    closed_policy = policy()
    graph = build_graph(workflows, closed_policy)
    assert [(root.workflow, root.event) for root in graph.roots] == [(paths[0], "pull_request")]
    assert len(graph.edges) == 1 and graph.edges[0].kind == "LOCAL_WORKFLOW_CALL"
    assert (graph.edges[0].source_workflow, graph.edges[0].target_workflow) == tuple(paths)
    assert graph.findings == ()
    expansion = expand_routes(graph, workflows, closed_policy)
    authorities = [resolve_authority(route.to_mapping(), workflows, closed_policy) for route in expansion.routes]
    assert authorities and all(not authority.findings and not authority.privileged for authority in authorities)
    assert all(authority.effective_permissions["contents"] == "read" for authority in authorities)
    for expected in (
        "**Execute:**",
        "status=PASS` with `finding_count=0",
        "pull-request 2",
        "no artifact upload or persisted output",
        "**Recover:**",
        "semantic privilege runbook",
        "do not widen policy or add an override",
    ):
        assert expected in rendered_section


def test_pr3b_docs_close_umbrella_inputs_and_codeql_upgrade() -> None:
    expected_by_page = {
        "docs/cicd/workflows.md": (
            "fixed semantic",
            "release/runtime",
            ".github/workflows/codeql.yml",
            ".agents/policy/workflow-security-privileged.yml",
            "evals/agent/workflow-security-privileged-policy.schema.json",
            "tools/agent_policy/workflow_privilege_profiles.py",
            "two byte-identical",
        ),
        "docs/cicd/runbooks.md": (
            "legacy general-linter overrides only",
            "hosted exact-head CodeQL",
            "literal-only",
            "`secrets: inherit`",
            "`strategy`",
            "invalid workflow_call inputs",
        ),
    }
    for page, expected in expected_by_page.items():
        rendered = _squash(_read(page))
        assert all(marker in rendered for marker in expected)


def test_pr3b_testing_pages_freeze_complete_focused_suite() -> None:
    pages = (
        ("docs/testing/index.md", "## Semantic PR privilege boundary gate"),
        ("docs/testing/overview.md", "## Semantic workflow privilege tests"),
    )
    for page, heading in pages:
        blocks = re.findall(r"```bash\n(.*?)\n```", _section(page, heading), re.DOTALL)
        command = next(block for block in blocks if "uv run pytest" in block)
        assert "test_workflow_security_fail_closed.py" in command
        assert "test_workflow_security_job_scope.py" in command


def test_pr3b_runbook_separates_boundary_input_and_internal_recovery() -> None:
    raw = _read("docs/cicd/runbooks.md")
    section = _squash(raw.split("| Umbrella diagnostic", maxsplit=1)[1].split("## CodeQL failures", maxsplit=1)[0])
    for expected in (
        "`privileged-boundary scan UNVERIFIED`",
        "boundary module cannot load",
        "raises while scanning one release or runtime workflow",
        "**Input branch:**",
        "**Internal branch:**",
        "tests/agent_policy/test_release_privileged_boundary.py",
        "tests/agent_policy/test_workflow_security_privileged_cli.py",
        "integration blocked",
        "exact commit, command, exit code, stdout, and stderr",
        "escalate the internal scanner-boundary defect",
        "Do not add an override",
    ):
        assert expected in section


def test_pr3b_governance_security_and_risk_pages_name_exact_evidence_assets() -> None:
    governance = _squash(_read("docs/agent-governance.md"))
    security = _squash(_read("docs/agent-security-mapping.md"))
    risk = _squash(_read("docs/agent-risk-register.md"))

    for expected in (
        "workflow_security_privileged.py",
        "governance-source",
        "governance-attestation",
        "agent_governance_gate.json",
        "Agent PR receipt",
        "hosted CodeQL",
    ):
        assert expected in governance

    for expected in (
        "Transitive semantic PR privilege boundary",
        "pinned closed profiles",
        "Canonical semantic report",
        "governance receipt/attestation",
        "hosted CodeQL",
        "every unknown route fail-closed",
    ):
        assert expected in security

    ag005 = next(line for line in _read("docs/agent-risk-register.md").splitlines() if "| AG-005 |" in line)
    for expected in (
        "PR-controlled code reaches write/OIDC",
        "exact CodeQL and ADR 0037 profiles",
        "`pr3b-semantic-privilege-certification.json`",
        "`.agents/policy/workflow-security-privileged.yml`",
        "`agent_governance_gate.json`",
        "Artifact Attestation",
        "hosted CodeQL",
        "Agent PR receipt",
    ):
        assert expected in ag005
    assert "local scanner `PASS` proves only the immutable checked-out workflow bytes" in risk
    assert "Missing provider evidence is `UNVERIFIED`" in risk


def test_pr3b_local_pass_is_not_hosted_or_branch_protection_authority() -> None:
    overview = _squash(_read("docs/ci-cd.md"))
    testing = _squash(_read("docs/testing/index.md"))
    protection = _squash(_read("docs/github-branch-protection.md"))

    assert "A local `PASS` is static repository evidence" in overview
    assert "not proof that hosted CodeQL" in overview
    assert "provider-authenticated evidence for the exact head" in testing
    assert "Report unavailable hosted evidence as `UNVERIFIED`, not `PASS`" in testing
    assert (
        "does not add a required context, GitHub App, ruleset entry, "
        "classic-protection setting, secret, environment, or publisher"
    ) in protection
    assert "new required context, App binding, or publisher was introduced" in protection
    assert "PR3B required-context/App/publisher delta: none" in protection
    assert "Do not change branch protection" in protection
    assert "replacement publisher" in protection

    policy = _yaml(".agents/policy/github-branch-protection.yml")
    contexts = policy["ruleset"]["required_status_checks"]["checks"]
    assert len(contexts) == 21
    assert not any(
        marker in context.casefold()
        for context in contexts
        for marker in ("pr3b", "semantic privilege", "governance-source", "governance-attestation")
    )
    assert policy["ruleset"]["bypass"] == {
        "allowed_repository_roles": ["admin"],
        "actors": [{"actor_id": 5, "actor_type": "RepositoryRole", "bypass_mode": "always"}],
        "disallowed_for": ["agent", "automation", "broad_team"],
    }


def test_pr3b_certification_is_valid_exact_canonical_producer_output() -> None:
    command = [
        sys.executable,
        str(ROOT / "tools/agent_policy/workflow_security_privileged.py"),
        "--root",
        ".",
        "--format",
        "json",
    ]
    first = subprocess.run(command, cwd=ROOT, check=False, capture_output=True)
    second = subprocess.run(command, cwd=ROOT, check=False, capture_output=True)
    tracked = (ROOT / CERTIFICATION).read_bytes()

    assert first.returncode == second.returncode == 0
    assert first.stderr == second.stderr == b""
    assert first.stdout == second.stdout == tracked
    assert tracked.endswith(b"\n") and not tracked.endswith(b"\n\n")

    report = json.loads(tracked)
    schema = json.loads(_read(REPORT_SCHEMA))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(report)
    assert report["status"] == "PASS"
    assert report["ok"] is True
    assert report["root"] == "."
