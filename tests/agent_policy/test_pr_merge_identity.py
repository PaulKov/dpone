from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "PaulKov/dpone"
BASE_REF = "master"


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


merge_identity = _load(
    "dpone_agent_pr_merge_identity_test",
    "tools/agent_policy/pr_merge_identity.py",
)


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", *args),
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _commit(root: Path, relative: str, content: str, message: str) -> str:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    _git(root, "add", "--", relative)
    _git(root, "commit", "-m", message)
    return _git(root, "rev-parse", "HEAD")


def _repository(root: Path) -> tuple[str, str]:
    _git(root, "init", "-b", BASE_REF)
    _git(root, "config", "user.email", "agent-tests@example.invalid")
    _git(root, "config", "user.name", "Agent tests")
    base = _commit(root, "README.md", "base\n", "base")
    _git(root, "checkout", "-b", "feature")
    head = _commit(
        root,
        "tools/agent_policy/control.py",
        "CONTROL = True\n",
        "reviewed head",
    )
    return base, head


def _closed_event(
    *,
    head_sha: str,
    integration_sha: str,
    base_sha: str = "d" * 40,
) -> dict[str, Any]:
    return {
        "action": "closed",
        "repository": {"full_name": REPOSITORY},
        "pull_request": {
            "number": 455,
            "merged": True,
            "state": "closed",
            "merged_at": "2026-07-29T00:03:00Z",
            "merge_commit_sha": integration_sha,
            "body": "Immutable reviewed body\n",
            "head": {"sha": head_sha},
            "base": {
                "ref": BASE_REF,
                "sha": base_sha,
                "repo": {"full_name": REPOSITORY},
            },
        },
    }


def _validate_event(payload: Mapping[str, Any], integration_sha: str) -> Any:
    return merge_identity.validate_merge_event(
        dict(payload),
        repository=REPOSITORY,
        protected_base_refs=[BASE_REF],
        integration_commit_sha=integration_sha,
    )


def test_closed_event_is_the_only_authority_for_merge_identity() -> None:
    reviewed_head = "a" * 40
    integration_commit = "b" * 40

    identity = _validate_event(
        _closed_event(head_sha=reviewed_head, integration_sha=integration_commit),
        integration_commit,
    )

    assert identity.pr_number == 455
    assert identity.reviewed_head_sha == reviewed_head
    assert identity.integration_commit_sha == integration_commit
    assert identity.repository == REPOSITORY
    assert identity.protected_base_ref == BASE_REF
    assert identity.body == "Immutable reviewed body\n"
    assert identity.merged_at == "2026-07-29T00:03:00Z"


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"action": "edited"}, "closed"),
        ({"pull_request": {"merged": False}}, "merged"),
        (
            {"pull_request": {"base": {"ref": "develop", "repo": {"full_name": REPOSITORY}}}},
            "base",
        ),
        (
            {
                "pull_request": {
                    "base": {
                        "ref": BASE_REF,
                        "repo": {"full_name": "attacker/fork"},
                    }
                }
            },
            "repository",
        ),
        ({"pull_request": {"head": {"sha": "abc123"}}}, "Git SHA"),
    ],
)
def test_merge_event_rejects_noncanonical_or_incomplete_authority(
    change: dict[str, Any],
    message: str,
) -> None:
    integration_commit = "b" * 40
    event = _closed_event(head_sha="a" * 40, integration_sha=integration_commit)
    for key, value in change.items():
        if isinstance(value, dict) and isinstance(event.get(key), dict):
            event[key].update(value)
        else:
            event[key] = value

    with pytest.raises(ValueError, match=message):
        _validate_event(event, integration_commit)


def test_merge_event_requires_event_and_integration_sha_to_match() -> None:
    event = _closed_event(head_sha="a" * 40, integration_sha="b" * 40)

    with pytest.raises(ValueError, match="integration|merge"):
        merge_identity.validate_merge_event(
            event,
            repository=REPOSITORY,
            protected_base_refs=[BASE_REF],
            integration_commit_sha="c" * 40,
        )


def test_git_identity_independently_requires_checked_out_integration_sha(
    tmp_path: Path,
) -> None:
    base, reviewed_head = _repository(tmp_path)
    integration_commit = "c" * 40
    event = _validate_event(
        _closed_event(
            head_sha=reviewed_head,
            integration_sha=integration_commit,
            base_sha=base,
        ),
        integration_commit,
    )

    with pytest.raises(ValueError, match="checked-out commit"):
        merge_identity.verify_integration_identity(tmp_path, event)


def test_two_parent_merge_binds_second_parent_and_exact_tree(tmp_path: Path) -> None:
    base, reviewed_head = _repository(tmp_path)
    _git(tmp_path, "checkout", BASE_REF)
    _git(tmp_path, "merge", "--no-ff", "feature", "-m", "merge reviewed head")
    integration_commit = _git(tmp_path, "rev-parse", "HEAD")

    identity = merge_identity.verify_integration_identity(
        tmp_path,
        _validate_event(
            _closed_event(
                head_sha=reviewed_head,
                integration_sha=integration_commit,
                base_sha=base,
            ),
            integration_commit,
        ),
    )

    assert identity.method == "merge"
    assert identity.reviewed_head_sha == reviewed_head
    assert identity.base_parent_sha == base
    assert identity.integration_commit_sha == integration_commit
    assert identity.reviewed_head_tree == identity.integration_tree


def test_one_parent_squash_requires_reviewed_head_tree(tmp_path: Path) -> None:
    base, reviewed_head = _repository(tmp_path)
    _git(tmp_path, "checkout", BASE_REF)
    _git(tmp_path, "merge", "--squash", "feature")
    _git(tmp_path, "commit", "-m", "squash reviewed head")
    integration_commit = _git(tmp_path, "rev-parse", "HEAD")

    identity = merge_identity.verify_integration_identity(
        tmp_path,
        _validate_event(
            _closed_event(
                head_sha=reviewed_head,
                integration_sha=integration_commit,
                base_sha=base,
            ),
            integration_commit,
        ),
    )

    assert identity.method == "squash"
    assert identity.base_parent_sha == base
    assert identity.reviewed_head_tree == identity.integration_tree


def test_merge_rejects_a_tree_that_differs_from_reviewed_head(tmp_path: Path) -> None:
    _base, reviewed_head = _repository(tmp_path)
    _git(tmp_path, "checkout", BASE_REF)
    _git(tmp_path, "merge", "--no-ff", "feature", "-m", "merge reviewed head")
    (tmp_path / "unreviewed.txt").write_text("not reviewed\n", encoding="utf-8")
    _git(tmp_path, "add", "--", "unreviewed.txt")
    _git(tmp_path, "commit", "--amend", "--no-edit")
    integration_commit = _git(tmp_path, "rev-parse", "HEAD")

    with pytest.raises(ValueError, match="tree"):
        merge_identity.verify_integration_identity(
            tmp_path,
            _validate_event(
                _closed_event(
                    head_sha=reviewed_head,
                    integration_sha=integration_commit,
                ),
                integration_commit,
            ),
        )


def test_merge_rejects_a_different_second_parent(tmp_path: Path) -> None:
    base, reviewed_head = _repository(tmp_path)
    _git(tmp_path, "checkout", "-b", "different", base)
    _commit(tmp_path, "different.txt", "different\n", "different head")
    different_head = _git(tmp_path, "rev-parse", "HEAD")
    _git(tmp_path, "checkout", BASE_REF)
    reviewed_tree = _git(tmp_path, "rev-parse", f"{reviewed_head}^{{tree}}")
    integration_commit = _git(
        tmp_path,
        "commit-tree",
        reviewed_tree,
        "-p",
        base,
        "-p",
        different_head,
        "-m",
        "merge different head with reviewed tree",
    )
    _git(tmp_path, "checkout", "--detach", integration_commit)

    with pytest.raises(ValueError, match="second parent|reviewed head"):
        merge_identity.verify_integration_identity(
            tmp_path,
            _validate_event(
                _closed_event(
                    head_sha=reviewed_head,
                    integration_sha=integration_commit,
                    base_sha=base,
                ),
                integration_commit,
            ),
        )


def test_first_parent_paths_preserve_both_sides_of_rename_out(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init", "-b", BASE_REF)
    _git(tmp_path, "config", "user.email", "agent-tests@example.invalid")
    _git(tmp_path, "config", "user.name", "Agent tests")
    _commit(
        tmp_path,
        ".agents/policy/required.yml",
        "required: true\n",
        "base control policy",
    )
    _git(tmp_path, "checkout", "-b", "feature")
    destination = tmp_path / "docs/required.yml"
    destination.parent.mkdir(parents=True)
    _git(
        tmp_path,
        "mv",
        ".agents/policy/required.yml",
        "docs/required.yml",
    )
    _git(tmp_path, "commit", "-m", "rename out of control surface")
    _git(tmp_path, "checkout", BASE_REF)
    _git(tmp_path, "merge", "--squash", "feature")
    _git(tmp_path, "commit", "-m", "squash rename")
    integration_commit = _git(tmp_path, "rev-parse", "HEAD")

    paths = merge_identity.exact_first_parent_paths(
        tmp_path,
        base_parent_sha=_git(tmp_path, "rev-parse", "HEAD^"),
        integration_commit_sha=integration_commit,
    )

    assert list(paths) == [
        ".agents/policy/required.yml",
        "docs/required.yml",
    ]


def test_cli_writes_failure_receipt_without_leaking_token(
    tmp_path: Path,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    receipt_cli = _load(
        "dpone_agent_pr_merge_receipt_cli_test",
        "tools/agent_policy/pr_merge_receipt.py",
    )
    event = _closed_event(head_sha="a" * 40, integration_sha="c" * 40)
    event["action"] = "edited"
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps(event), encoding="utf-8")
    policy_path = tmp_path / ".agents/policy/github-branch-protection.yml"
    policy_path.parent.mkdir(parents=True)
    policy_path.write_text("ruleset:\n  branches: [master]\n", encoding="utf-8")
    output_dir = tmp_path / "receipt"
    token_env = "DPONE_TEST_MERGE_RECEIPT_TOKEN"
    secret = "must-not-appear-in-output"
    monkeypatch.setenv(token_env, secret)
    monkeypatch.setenv("GITHUB_SHA", "c" * 40)
    output_dir.mkdir()
    (output_dir / "source-agent-pr-receipt.zip").write_bytes(b"stale source")
    args = [
        "--event",
        str(event_path),
        "--root",
        str(tmp_path),
        "--repository",
        REPOSITORY,
        "--policy",
        str(policy_path),
        "--workflow",
        "Agent PR receipt",
        "--run-id",
        "900",
        "--run-attempt",
        "1",
        "--github-token-env",
        token_env,
        "--output-dir",
        str(output_dir),
    ]

    code = receipt_cli.main(args)

    payload = json.loads((output_dir / "agent_pr_merge_receipt.json").read_text(encoding="utf-8"))
    captured = capsys.readouterr()
    assert code == 1
    assert payload["status"] == "FAIL"
    assert payload["errors"]
    assert sorted(path.name for path in output_dir.iterdir()) == ["agent_pr_merge_receipt.json"]
    assert captured.out == ""
    assert "ERROR:" in captured.err
    assert secret not in captured.out + captured.err + json.dumps(payload)
