"""Synthetic Git histories prove conservative acceptance classification."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from tools.agent_policy.acceptance_plan import build_plan

DOMAIN = "examples/demo/gitops/domains/demo.yaml"
BEFORE = """domain: demo
workloads:
  demo:
    manifest: demo.yaml
dags:
  demo:
    schedule: "0 6 * * *"
    start_date: "2026-01-01"
    catchup: false
    workloads: [demo]
"""
AFTER = BEFORE.replace("0 6 * * *", "0 7 * * *")


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()


def commit(repo: Path, files: dict[str, str | None]) -> str:
    for name, content in files.items():
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if content is None:
            path.unlink()
        else:
            path.write_text(content, encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "synthetic acceptance fixture", "--allow-empty")
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repository(tmp_path: Path) -> tuple[Path, str]:
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.email", "synthetic@example.invalid")
    git(tmp_path, "config", "user.name", "Synthetic tests")
    return tmp_path, commit(tmp_path, {DOMAIN: BEFORE})


def test_schedule_only_binds_commits_and_preserves_gates(repository):
    repo, base = repository
    head = commit(repo, {DOMAIN: AFTER})
    payload = build_plan(repo, base, head)
    assert payload["classification"] == "schedule_only"
    assert (payload["base_sha"], payload["head_sha"]) == (base, head)
    assert payload["preserve_existing_required_checks"] is True
    assert payload["classifier_version"]
    checks = {item["id"]: item for item in payload["required_checks"]}
    assert "scheduling_contracts" in checks
    assert "synthetic_container_smoke" not in checks
    assert all(item["status"] == "UNVERIFIED" and item["artifacts"] == [] for item in checks.values())
    assert json.loads(json.dumps(payload)) == payload


@pytest.mark.parametrize(
    "update",
    [
        {DOMAIN: AFTER.replace("catchup: false", "catchup: true")},
        {DOMAIN: AFTER, "src/dpone/runtime/example.py": "raise RuntimeError('never execute')"},
        {DOMAIN: AFTER, "unknown.xyz": "unknown"},
        {DOMAIN: None},
        {DOMAIN: None, DOMAIN.replace("demo.yaml", "renamed.yaml"): AFTER},
        {DOMAIN: AFTER, "examples/new/gitops/domains/new.yaml": AFTER},
        {DOMAIN: "dags: [invalid"},
        {DOMAIN: AFTER + "domain: duplicate\n"},
        {DOMAIN: AFTER.replace("catchup: false", "catchup: 0")},
        {DOMAIN: AFTER.replace("0 7 * * *", "not cron")},
        {DOMAIN: AFTER.replace("0 7 * * *", "99 7 * * *")},
        {DOMAIN: AFTER.replace("start_date:", "unknown:")},
        {DOMAIN: AFTER.replace("domain: demo", "domain: !!python/object/apply:os.system [echo forbidden]")},
        {DOMAIN: AFTER.replace("workloads: [demo]", "workloads: &ref [demo]")},
        {DOMAIN: AFTER.replace("catchup: false", "catchup: .nan")},
    ],
)
def test_ambiguous_and_mixed_diffs_require_broader_acceptance(repository, update):
    repo, base = repository
    head = commit(repo, update)
    payload = build_plan(repo, base, head)
    assert payload["classification"] == "broad"
    assert payload["preserve_existing_required_checks"] is True
    assert "synthetic_container_smoke" in {check["id"] for check in payload["required_checks"]}


def test_content_equivalence_alone_is_not_schedule_change(repository):
    repo, base = repository
    head = commit(repo, {DOMAIN: "# comment only\n" + BEFORE})
    assert build_plan(repo, base, head)["classification"] == "broad"


def test_uncommitted_files_are_outside_exact_revision_plan(repository):
    repo, base = repository
    head = commit(repo, {DOMAIN: AFTER})
    (repo / DOMAIN).write_text("malformed [", encoding="utf-8")
    assert build_plan(repo, base, head)["classification"] == "schedule_only"
    assert build_plan(repo, base, base)["classification"] == "no_changes"


def test_invalid_revision_fails_without_success_plan(repository):
    repo, base = repository
    with pytest.raises(ValueError, match="revision"):
        build_plan(repo, base, "--help")


def test_mode_change_rejects_schedule_only(repository):
    repo, base = repository
    (repo / DOMAIN).chmod(0o755)
    head = commit(repo, {DOMAIN: AFTER})
    assert build_plan(repo, base, head)["classification"] == "broad"


def test_multiple_dags_require_all_other_fields_unchanged(repository):
    repo, _ = repository
    content = BEFORE + "  second:\n    schedule: '@daily'\n    start_date: '2026-01-01'\n    workloads: [demo]\n"
    base = commit(repo, {DOMAIN: content})
    head = commit(repo, {DOMAIN: content.replace("@daily", "@hourly")})
    assert build_plan(repo, base, head)["classification"] == "schedule_only"


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"\xff",
        b"---\ndomain: demo\n---\ndomain: extra\n",
        b"x: [" + b"[" * 1000,
        b"x: " + b"a" * 262_144,
        BEFORE.replace('"2026-01-01"', '"invalid date"').encode(),
        BEFORE.replace("catchup: false", "catchup: []").encode(),
        BEFORE.replace("catchup: false", "max_active_runs: true").encode(),
        BEFORE.replace("catchup: false", "max_active_runs: 0").encode(),
        BEFORE.replace("catchup: false", "tags: invalid").encode(),
        BEFORE.replace("catchup: false", "catchup: !!bool invalid").encode(),
        BEFORE.replace("catchup: false", "default_args: {retries: !!int invalid}").encode(),
        BEFORE.replace("catchup: false", "wiring: {mode: invalid}").encode(),
        BEFORE.replace("catchup: false", "wiring: {mode: []}").encode(),
        BEFORE.replace("catchup: false", "wiring: {max_parallel_workloads: false}").encode(),
        BEFORE.replace("catchup: false", "default_args: []").encode(),
        BEFORE.replace("catchup: false", "wiring: {unknown: true}").encode(),
        BEFORE.replace("workloads: [demo]", "workloads: [missing]").encode(),
        BEFORE.replace("domain: demo", "? [a, b]\n: invalid\ndomain: demo").encode(),
        BEFORE.replace("domain: demo", "domain: demo\nunknown: true").encode(),
    ],
)
def test_malformed_or_unsupported_unchanged_fields_cannot_earn_schedule_only(content):
    from tools.agent_policy.acceptance_impact import schedule_only

    after = content.replace(b"0 6 * * *", b"0 7 * * *")
    assert schedule_only(DOMAIN, content, after)[0] is False


def test_mapping_order_and_quotes_are_semantically_normalized(repository):
    repo, base = repository
    head = commit(
        repo, {DOMAIN: AFTER.replace('"0 7 * * *"', "'0 7 * * *'").replace("domain: demo\n", "") + "domain: demo\n"}
    )
    assert build_plan(repo, base, head)["classification"] == "schedule_only"


def test_filename_with_newline_is_not_split_into_additional_files(repository):
    repo, base = repository
    name = "unknown\nfile.py"
    head = commit(repo, {name: "raise RuntimeError('must not execute')"})
    result = build_plan(repo, base, head)
    assert result["classification"] == "broad"
    assert [item["path"] for item in result["files"]] == [name]


def test_cli_writes_exact_plan_and_invalid_revision_has_nonzero_exit(repository, tmp_path):
    repo, base = repository
    head = commit(repo, {DOMAIN: AFTER})
    script = Path(__file__).resolve().parents[1] / "tools/agent_policy/acceptance_plan.py"
    import sys

    output = tmp_path / "artifacts" / "acceptance.json"
    command = [sys.executable, str(script), "--repo", str(repo), "--base-ref", base, "--head-ref", head]
    completed = subprocess.run([*command, "--output", str(output)], capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert json.loads(output.read_text())["head_sha"] == head
    completed = subprocess.run([*command[:-1], "missing"], capture_output=True, text=True)
    assert completed.returncode == 2
    assert not completed.stdout
    assert "Acceptance planning failed" in completed.stderr


def test_schedule_contract_command_references_existing_tests():
    from tools.agent_policy.acceptance_plan import SCHEDULE_COMMAND

    root = Path(__file__).resolve().parents[1]
    assert all((root / path).is_file() for path in SCHEDULE_COMMAND.split() if path.startswith("tests/"))


def test_git_replace_cannot_change_the_trees_bound_to_evidence(repository):
    repo, base = repository
    head = commit(repo, {DOMAIN: AFTER})
    git(repo, "replace", base, head)
    result = build_plan(repo, base, head)
    assert result["classification"] == "schedule_only"
    assert (result["base_sha"], result["head_sha"]) == (base, head)


@pytest.mark.parametrize(
    "xml",
    [
        "<testsuites/>",
        '<testsuite tests="0" failures="0" errors="0" skipped="0"/>',
        '<testsuite tests="1" failures="0" errors="0" skipped="0"/>',
        '<testsuite tests="1" failures="0" errors="0" skipped="1"><testcase classname="tests.test_demo" name="test_x"><skipped/></testcase></testsuite>',
        '<testsuite tests="1" failures="0" errors="0" skipped="0"><testcase classname="tests.test_demo" name="test_x"><failure/></testcase></testsuite>',
        '<testsuite tests="1" failures="0" errors="1" skipped="0"><testcase classname="tests.test_demo" name="test_x"/></testsuite>',
        '<testsuite tests="bad" failures="0" errors="0" skipped="0"/>',
        '<testsuite tests="1" failures="0" errors="0" skipped="0"><testcase classname="tests.wrong_suite" name="test_x"/></testsuite>',
        '<!DOCTYPE foo [<!ENTITY a "text">]><testsuites/>',
        "<malformed",
    ],
)
def test_junit_receipt_refuses_empty_skipped_failed_or_missing_suites(tmp_path, xml):
    from tools.agent_policy.acceptance_plan import verify_junit

    path = tmp_path / "junit.xml"
    path.write_text(xml)
    with pytest.raises(ValueError):
        verify_junit(path, ["tests/test_demo.py"], "a" * 40)


def test_junit_receipt_requires_actual_tests_for_each_expected_file(tmp_path):
    from tools.agent_policy.acceptance_plan import verify_junit

    path = tmp_path / "junit.xml"
    path.write_text(
        '<testsuites><testsuite tests="2" failures="0" errors="0" skipped="0">'
        '<testcase classname="tests.test_demo" name="test_x"/>'
        '<testcase classname="tests.integration.test_route.TestClass" name="test_y"/>'
        "</testsuite></testsuites>"
    )
    result = verify_junit(path, ["tests/test_demo.py", "tests/integration/test_route.py"], "a" * 40)
    assert result["status"] == "PASS"
    assert result["tests"] == 2
    assert result["head_sha"] == "a" * 40
    assert result["files"] == {"tests/test_demo.py": 1, "tests/integration/test_route.py": 1}
    assert len(result["artifact_sha256"]) == 64


def test_receipt_rejects_uncommitted_or_different_candidate_source(repository):
    from tools.agent_policy.acceptance_plan import require_clean_candidate

    repo, base = repository
    assert require_clean_candidate(repo, base) == base
    artifact = repo / "test_artifacts" / "acceptance" / "junit.xml"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<testsuites/>")
    assert require_clean_candidate(repo, base) == base
    (repo / DOMAIN).write_text(AFTER)
    with pytest.raises(ValueError, match="uncommitted"):
        require_clean_candidate(repo, base)
    head = commit(repo, {DOMAIN: AFTER})
    with pytest.raises(ValueError, match="candidate"):
        require_clean_candidate(repo, base)
    (repo / "untracked.py").write_text("# uncommitted code")
    with pytest.raises(ValueError, match="uncommitted"):
        require_clean_candidate(repo, head)
