from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator

from tests.agent_policy._ci_shadow_history_fixtures import initialize_repository

ROOT = Path(__file__).resolve().parents[1]
TASK = ROOT / "test_artifacts/agent-policy/dpone-ci-shadow-pr3b-spec.yml"
AMENDMENT_TASK = ROOT / "test_artifacts/agent-policy/dpone-ci-shadow-pr3b-public-output-amendment.yml"
TASK_SCHEMA = ROOT / "evals/agent/agent-task-contract.schema.json"
DESIGN_BASE = "c5567128e6e9b847b4b2a023a74ea6d60e7ccd09"
DESIGN_INTEGRATION = "f18298c14af247225758f7fe8901bc8462994dd4"
AMENDMENT_SPEC_PATH = "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"
AMENDMENT_TASK_PATH = "test_artifacts/agent-policy/dpone-ci-shadow-pr3b-public-output-amendment.yml"
PENDING_STATUS = "- Public-output amendment status: RESEARCHED"
APPROVED_STATUS = "- Public-output amendment status: APPROVED"
PENDING_CHECKLIST = "- [ ] Maintainer changed public-output amendment status to `APPROVED` after"
APPROVED_CHECKLIST = "- [x] Maintainer changed public-output amendment status to `APPROVED` after"


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _git_raw(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def _committed_changed_paths(root: Path, base: str, head: str = "HEAD") -> set[str]:
    head_oid = _git(root, "rev-parse", head)
    _git(root, "merge-base", "--is-ancestor", base, head_oid)
    output = _git(
        root,
        "diff",
        "--no-renames",
        "--name-only",
        "--diff-filter=ACDMRT",
        f"{base}...{head_oid}",
        "--",
    )
    return {line for line in output.splitlines() if line}


def _assert_task_scope(task: dict[str, Any], changed_paths: set[str]) -> None:
    owned_paths = set(task["owned_paths"])
    integrator_paths = set(task["integrator_owned_paths"])
    assert changed_paths == owned_paths | integrator_paths

    def forbidden(path: str) -> bool:
        return any(path == prefix or path.startswith(f"{prefix}/") for prefix in task["forbidden_paths"])

    assert not {path for path in changed_paths - integrator_paths if forbidden(path)}


def _amendment_scope_head(root: Path, *, head: str, task_path: str) -> str:
    head_oid = _git(root, "rev-parse", head)
    scope_head = _git(root, "log", "--first-parent", "-1", "--format=%H", head_oid, "--", task_path)
    if not scope_head:
        raise ValueError(f"amendment task has no committed scope head: {task_path}")
    return scope_head


def _amendment_introduction_head(root: Path, *, head: str, task_path: str) -> str:
    head_oid = _git(root, "rev-parse", head)
    commits = _git(root, "rev-list", "--topo-order", "--reverse", head_oid).splitlines()
    introduction_head = next(
        (
            commit
            for commit in commits
            if _path_exists(root, commit=commit, path=task_path)
            and all(not _path_exists(root, commit=parent, path=task_path) for parent in _parents(root, commit))
        ),
        "",
    )
    if not introduction_head:
        raise ValueError(f"amendment task has no introduction commit: {task_path}")
    return introduction_head


def _parents(root: Path, commit: str) -> tuple[str, ...]:
    return tuple(_git(root, "rev-list", "--parents", "-n", "1", commit).split()[1:])


def _path_exists(root: Path, *, commit: str, path: str) -> bool:
    completed = subprocess.run(
        ["git", "cat-file", "-e", f"{commit}:{path}"],
        cwd=root,
        check=False,
        capture_output=True,
    )
    return completed.returncode == 0


def _assert_frozen_authority_history(
    root: Path,
    *,
    introduction_head: str,
    head_oid: str,
    authority_paths: tuple[str, ...],
) -> None:
    canonical = {path: _git(root, "rev-parse", f"{introduction_head}:{path}") for path in authority_paths}
    for commit in _git(root, "rev-list", f"{introduction_head}..{head_oid}").splitlines():
        completed = subprocess.run(
            ["git", "merge-base", "--is-ancestor", introduction_head, commit],
            cwd=root,
            check=False,
            capture_output=True,
        )
        descendant = completed.returncode == 0
        for path, expected_oid in canonical.items():
            if not _path_exists(root, commit=commit, path=path):
                assert not descendant
                continue
            assert _git(root, "rev-parse", f"{commit}:{path}") == expected_oid


def _assert_amendment_scope_lifecycle(
    root: Path,
    task: dict[str, Any],
    *,
    head: str,
    task_path: str,
    spec_path: str,
) -> None:
    head_oid = _git(root, "rev-parse", head)
    introduction_head = _amendment_introduction_head(root, head=head_oid, task_path=task_path)
    introduction_spec = _git_raw(root, "show", f"{introduction_head}:{spec_path}")
    if APPROVED_STATUS in introduction_spec:
        introduction_with_parents = _git(root, "rev-list", "--parents", "-n", "1", introduction_head).split()
        assert len(introduction_with_parents) == 2
        _assert_task_scope(task, _committed_changed_paths(root, task["base_commit"], introduction_head))
        assert _git(root, "rev-parse", f"{head_oid}:{spec_path}") == _git(
            root, "rev-parse", f"{introduction_head}:{spec_path}"
        )
        assert _git(root, "rev-parse", f"{head_oid}:{task_path}") == _git(
            root, "rev-parse", f"{introduction_head}:{task_path}"
        )
        _assert_frozen_authority_history(
            root,
            introduction_head=introduction_head,
            head_oid=head_oid,
            authority_paths=(spec_path, task_path),
        )
        return

    assert PENDING_STATUS in introduction_spec
    scope_head = _amendment_scope_head(root, head=head_oid, task_path=task_path)
    _assert_task_scope(task, _committed_changed_paths(root, task["base_commit"], scope_head))

    parent_oid = _git(root, "rev-parse", f"{head_oid}^")
    try:
        parent_spec = _git_raw(root, "show", f"{parent_oid}:{spec_path}")
    except subprocess.CalledProcessError:
        parent_spec = ""
    current_spec = _git_raw(root, "show", f"{head_oid}:{spec_path}")
    if PENDING_STATUS in parent_spec and APPROVED_STATUS in current_spec:
        expected_approved = parent_spec.replace(PENDING_STATUS, APPROVED_STATUS, 1).replace(
            PENDING_CHECKLIST,
            APPROVED_CHECKLIST,
            1,
        )
        assert current_spec == expected_approved
        assert _committed_changed_paths(root, parent_oid, head_oid) == {spec_path}

    if head_oid == scope_head:
        return

    scope_spec = _git_raw(root, "show", f"{scope_head}:{spec_path}")
    if PENDING_STATUS in scope_spec:
        expected_approved = scope_spec.replace(PENDING_STATUS, APPROVED_STATUS, 1).replace(
            PENDING_CHECKLIST,
            APPROVED_CHECKLIST,
            1,
        )
        assert current_spec == expected_approved
        assert _committed_changed_paths(root, scope_head, head_oid) == {spec_path}
        return

    assert APPROVED_STATUS in scope_spec
    assert APPROVED_CHECKLIST in scope_spec


def _commit(root: Path, message: str, files: dict[str, str]) -> str:
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    _git(root, "add", "--all")
    _git(root, "commit", "-m", message)
    return _git(root, "rev-parse", "HEAD")


def _amendment_repository(root: Path) -> tuple[str, dict[str, Any]]:
    root.mkdir()
    _git(root, "init", "--quiet")
    _git(root, "config", "user.name", "PR3B Contract Test")
    _git(root, "config", "user.email", "pr3b@example.invalid")
    base = _commit(root, "base", {"design.md": "Status: DRAFT\n"})
    task = {
        "base_commit": base,
        "owned_paths": ["design.md", "amendment.yml"],
        "integrator_owned_paths": [],
        "forbidden_paths": ["unexpected.md"],
    }
    return base, task


def _assert_amendment_rejected(root: Path, task: dict[str, Any], head: str) -> None:
    with pytest.raises((AssertionError, ValueError)):
        _assert_amendment_scope_lifecycle(root, task, head=head, task_path="amendment.yml", spec_path="design.md")


def _repository_with_declared_change(tmp_path: Path) -> tuple[Path, str, str]:
    root = tmp_path / "repository"
    root.mkdir()
    _git(root, "init", "--quiet")
    _git(root, "config", "user.name", "PR3B Contract Test")
    _git(root, "config", "user.email", "pr3b@example.invalid")
    base = _commit(
        root,
        "base",
        {
            "design.md": "Status: DRAFT\n",
            "test_artifacts/agent-policy/agent_governance_gate.json": '{"head":"base"}\n',
        },
    )
    head = _commit(root, "research design", {"design.md": "Status: RESEARCHED\n"})
    return root, base, head


def _minimal_task() -> dict[str, Any]:
    return {
        "owned_paths": ["design.md"],
        "integrator_owned_paths": [],
        "forbidden_paths": ["unexpected.md"],
    }


def test_pr3b_task_contract_uses_the_immutable_base_to_head_diff(tmp_path: Path) -> None:
    task = yaml.safe_load(TASK.read_text(encoding="utf-8"))
    assert isinstance(task, dict)

    assert task["base_commit"] == DESIGN_BASE
    assert task["specification"] == "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"
    assert task["integrator"] == task["shared_file_owner"] == "/root"
    assert task["public_contract_impact"]["migration_required"] is True
    for path in (
        "tests/test_ci_shadow_pr3b_spec_contracts.py",
        "tests/test_ci_shadow_pr3b_scope_contracts.py",
        "tests/test_ci_shadow_pr3b_schema_contracts.py",
        "tests/test_ci_shadow_pr3b_report_contracts.py",
        "tests/test_ci_shadow_pr3b_report_routes_contracts.py",
        "tests/ci_shadow_pr3b_report_route_model.py",
        "docs/adr/0037-immutable-agent-pr-merge-closure.md",
    ):
        assert path in task["owned_paths"]
    assert set(task["integrator_owned_paths"]) == {
        "mkdocs.yml",
        "docs/quality-metrics.md",
        "CHANGELOG.md",
        "evals/agent/agent-task-contract.schema.json",
        "tools/agent_policy/task_contract.py",
        "docs/agent-task-contracts.md",
        "docs/agent-templates/agent-task-contract.yml",
        "tests/agent_policy/test_task_contract.py",
    }
    assert set(task["owned_paths"]).isdisjoint(task["integrator_owned_paths"])
    assert ".github/workflows/ci.yml" in task["read_only_paths"]
    assert ".github/workflows/codeql.yml" in task["read_only_paths"]
    assert "mkdocs.yml" in task["forbidden_paths"]
    assert "docs/quality-metrics.md" in task["forbidden_paths"]
    assert ".github" in task["forbidden_paths"]
    assert "tools/agent_policy" in task["forbidden_paths"]
    assert any("remains RESEARCHED" in criterion for criterion in task["acceptance_criteria"])
    assert any(
        "immutable" in criterion and "DESIGN_BASE...HEAD" in criterion for criterion in task["acceptance_criteria"]
    )
    stop_condition = next(stop for stop in task["stop_conditions"] if "production workflow" in stop)
    assert "outside the explicitly listed root-owned task-contract" in stop_condition
    assert "PR3B runtime or production workflow/security bytes" in task["goal"]
    assert any("test_ci_shadow_pr3b_scope_contracts.py" in command for command in task["required_checks"]["focused"])

    schema = json.loads(TASK_SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(task)
    # Retained task metadata is not proof that historical objects exist in a fresh clone.
    # Exercise its declared scope against actual synthetic commits instead.
    root = tmp_path / "design-scope"
    base = initialize_repository(root)
    declared = set(task["owned_paths"]) | set(task["integrator_owned_paths"])
    integration = _commit(root, "synthetic declared design", {path: "fixture\n" for path in declared})
    _commit(root, "synthetic future work", {"future.txt": "outside the frozen scope\n"})
    _git(root, "merge-base", "--is-ancestor", integration, "HEAD")
    _assert_task_scope(task, _committed_changed_paths(root, base, integration))
    with pytest.raises(AssertionError):
        _assert_task_scope(task, _committed_changed_paths(root, base))


def test_public_output_amendment_scope_is_exact_from_base_to_head(tmp_path: Path) -> None:
    task = yaml.safe_load(AMENDMENT_TASK.read_text(encoding="utf-8"))
    assert isinstance(task, dict)
    assert task["base_commit"] == DESIGN_INTEGRATION

    root = tmp_path / "declared-amendment"
    base = initialize_repository(root)
    fixture_task = {**task, "base_commit": base}
    declared = set(task["owned_paths"]) | set(task["integrator_owned_paths"])
    files = {path: "synthetic amendment scope\n" for path in declared}
    files[AMENDMENT_SPEC_PATH] = (ROOT / AMENDMENT_SPEC_PATH).read_text(encoding="utf-8")
    files[AMENDMENT_TASK_PATH] = yaml.safe_dump(fixture_task)
    _commit(root, "synthetic approved amendment introduction", files)
    _commit(root, "synthetic unrelated successor", {"future.txt": "unrelated\n"})
    _assert_amendment_scope_lifecycle(
        root,
        fixture_task,
        head="HEAD",
        task_path=AMENDMENT_TASK_PATH,
        spec_path=AMENDMENT_SPEC_PATH,
    )


def test_dirty_governance_artifact_does_not_change_committed_scope(tmp_path: Path) -> None:
    root, base, head = _repository_with_declared_change(tmp_path)
    (root / "test_artifacts/agent-policy/agent_governance_gate.json").write_text(
        '{"head":"runtime-generated"}\n',
        encoding="utf-8",
    )

    assert _committed_changed_paths(root, base, head) == {"design.md"}


def test_untracked_file_does_not_change_committed_scope(tmp_path: Path) -> None:
    root, base, head = _repository_with_declared_change(tmp_path)
    (root / "untracked.txt").write_text("not committed\n", encoding="utf-8")

    assert _committed_changed_paths(root, base, head) == {"design.md"}


def test_committed_undeclared_file_fails_scope_contract(tmp_path: Path) -> None:
    root, base, _head = _repository_with_declared_change(tmp_path)
    unexpected_head = _commit(root, "unexpected", {"unexpected.md": "undeclared\n"})
    changed_paths = _committed_changed_paths(root, base, unexpected_head)

    assert changed_paths == {"design.md", "unexpected.md"}
    with pytest.raises(AssertionError):
        _assert_task_scope(_minimal_task(), changed_paths)


def test_non_ancestor_base_fails_closed(tmp_path: Path) -> None:
    root, base, head = _repository_with_declared_change(tmp_path)

    with pytest.raises(subprocess.CalledProcessError):
        _committed_changed_paths(root, head, base)


def test_status_only_successor_keeps_the_declared_committed_scope(tmp_path: Path) -> None:
    root, base, _head = _repository_with_declared_change(tmp_path)
    approved = _commit(root, "approve design", {"design.md": "Status: APPROVED\n"})
    changed_paths = _committed_changed_paths(root, base, approved)

    assert changed_paths == {"design.md"}
    _assert_task_scope(_minimal_task(), changed_paths)


def test_amendment_scope_allows_only_the_exact_lifecycle_successor(tmp_path: Path) -> None:
    root = tmp_path / "amendment"
    _base, task = _amendment_repository(root)
    researched = f"{PENDING_STATUS}\n{PENDING_CHECKLIST} fresh review.\n"
    researched_head = _commit(root, "research amendment", {"design.md": researched, "amendment.yml": "task: scope\n"})

    approved = researched.replace(PENDING_STATUS, APPROVED_STATUS).replace(PENDING_CHECKLIST, APPROVED_CHECKLIST)
    approved_head = _commit(root, "approve amendment", {"design.md": approved})
    _assert_amendment_scope_lifecycle(
        root,
        task,
        head=approved_head,
        task_path="amendment.yml",
        spec_path="design.md",
    )

    smuggled_head = _commit(root, "smuggle unrelated path", {"unexpected.md": "not lifecycle-only\n"})
    _assert_amendment_rejected(root, task, smuggled_head)

    _git(root, "checkout", "--quiet", "-b", "smuggled-successor", researched_head)
    task_changed_head = _commit(
        root,
        "smuggle task change into approval",
        {"design.md": approved, "amendment.yml": "task: widened\n"},
    )
    _assert_amendment_rejected(root, task, task_changed_head)


def test_approved_integration_freezes_scope_for_later_unrelated_work(tmp_path: Path) -> None:
    root = tmp_path / "integrated-amendment"
    _base, task = _amendment_repository(root)
    approved = f"{APPROVED_STATUS}\n{APPROVED_CHECKLIST} fresh review.\n"
    _commit(root, "squash approved amendment", {"design.md": approved, "amendment.yml": "task: scope\n"})
    later_head = _commit(root, "later unrelated work", {"later.md": "independent\n"})

    _assert_amendment_scope_lifecycle(
        root,
        task,
        head=later_head,
        task_path="amendment.yml",
        spec_path="design.md",
    )

    changed_spec_head = _commit(
        root,
        "mutate approved specification",
        {"design.md": f"{approved}semantic drift\n"},
    )
    _assert_amendment_rejected(root, task, changed_spec_head)

    _git(root, "checkout", "--quiet", "-b", "changed-approved-task", later_head)
    changed_task_head = _commit(root, "mutate approved task", {"amendment.yml": "task: drifted\n"})
    _assert_amendment_rejected(root, task, changed_task_head)

    _git(root, "checkout", "--quiet", "-b", "restored-approved-task", later_head)
    _commit(root, "temporarily mutate approved task", {"amendment.yml": "task: temporary drift\n"})
    restored_task_head = _commit(root, "restore approved task", {"amendment.yml": "task: scope\n"})
    _assert_amendment_rejected(root, task, restored_task_head)

    _git(root, "checkout", "--quiet", "-b", "readded-approved-spec", later_head)
    (root / "design.md").unlink()
    _git(root, "add", "--all")
    _git(root, "commit", "-m", "temporarily delete approved specification")
    readded_spec_head = _commit(root, "restore approved specification", {"design.md": approved})
    _assert_amendment_rejected(root, task, readded_spec_head)

    for branch, path, content in (
        ("spec-terminal-newline", "design.md", f"{approved}\n"),
        ("task-terminal-newline", "amendment.yml", "task: scope\n\n"),
    ):
        _git(root, "checkout", "--quiet", "-b", branch, later_head)
        whitespace_head = _commit(root, f"mutate {path} trailing bytes", {path: content})
        _assert_amendment_rejected(root, task, whitespace_head)


def test_approved_integration_rejects_noncanonical_parent_history(tmp_path: Path) -> None:
    root = tmp_path / "alternate-parent-amendment"
    base, task = _amendment_repository(root)
    approved = f"{APPROVED_STATUS}\n{APPROVED_CHECKLIST} fresh review.\n"

    _git(root, "checkout", "--quiet", "-b", "alternate", base)
    _commit(
        root,
        "alternate task introduction",
        {"design.md": f"{approved}alternate authority\n", "amendment.yml": "task: alternate\n"},
    )
    _git(root, "checkout", "--quiet", "-b", "canonical", base)
    canonical_head = _commit(
        root,
        "squash approved amendment",
        {"design.md": approved, "amendment.yml": "task: scope\n"},
    )
    _git(root, "merge", "--quiet", "--no-ff", "-s", "ours", "alternate", "-m", "merge alternate history")
    merged_head = _git(root, "rev-parse", "HEAD")

    assert _amendment_introduction_head(root, head=merged_head, task_path="amendment.yml") == canonical_head
    _assert_amendment_rejected(root, task, merged_head)


def test_approved_integration_rejects_two_parent_merge(tmp_path: Path) -> None:
    root = tmp_path / "merge-commit-amendment"
    base, task = _amendment_repository(root)
    approved = f"{APPROVED_STATUS}\n{APPROVED_CHECKLIST} fresh review.\n"

    _git(root, "checkout", "--quiet", "-b", "feature", base)
    _commit(root, "feature approved amendment", {"design.md": approved, "amendment.yml": "task: scope\n"})
    _git(root, "checkout", "--quiet", "-b", "mainline", base)
    _commit(root, "concurrent main work", {"main.md": "unrelated\n"})
    _git(root, "merge", "--quiet", "--no-ff", "feature", "-m", "merge approved amendment")
    merge_head = _git(root, "rev-parse", "HEAD")

    _assert_amendment_rejected(root, task, merge_head)
