from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
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
    "dpone_agent_pr_merge_ownership_identity_test",
    "tools/agent_policy/pr_merge_identity.py",
)
merge_check = _load(
    "dpone_agent_pr_merge_ownership_check_test",
    "tools/agent_policy/pr_merge_check.py",
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


def _closed_event(*, head_sha: str, integration_sha: str, base_sha: str) -> dict[str, Any]:
    return {
        "action": "closed",
        "repository": {"full_name": REPOSITORY},
        "pull_request": {
            "number": 575,
            "merged": True,
            "state": "closed",
            "merged_at": "2026-08-20T00:00:00Z",
            "merge_commit_sha": integration_sha,
            "body": "Reviewed inner change\n",
            "head": {"sha": head_sha},
            "base": {
                "ref": BASE_REF,
                "sha": base_sha,
                "repo": {"full_name": REPOSITORY},
            },
        },
    }


def test_transitively_included_pr_does_not_own_shared_integration_check(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init", "-b", BASE_REF)
    _git(tmp_path, "config", "user.email", "agent-tests@example.invalid")
    _git(tmp_path, "config", "user.name", "Agent tests")
    base = _commit(tmp_path, "README.md", "base\n", "base")
    _git(tmp_path, "checkout", "-b", "feature")
    reviewed_head = _commit(tmp_path, "inner.txt", "inner\n", "inner reviewed head")
    direct_head = _commit(tmp_path, "outer.txt", "outer\n", "direct reviewed head")
    _git(tmp_path, "checkout", BASE_REF)
    _git(tmp_path, "merge", "--no-ff", "feature", "-m", "merge direct reviewed head")
    integration_commit = _git(tmp_path, "rev-parse", "HEAD")
    event = merge_identity.validate_merge_event(
        _closed_event(
            head_sha=reviewed_head,
            integration_sha=integration_commit,
            base_sha=base,
        ),
        repository=REPOSITORY,
        protected_base_refs=[BASE_REF],
        integration_commit_sha=integration_commit,
    )

    ownership = merge_identity.classify_integration_ownership(tmp_path, event)

    assert ownership.classification == "transitive"
    assert ownership.reviewed_head_sha == reviewed_head
    assert ownership.direct_reviewed_head_sha == direct_head
    with pytest.raises(ValueError, match="transitively included"):
        merge_identity.verify_integration_identity(tmp_path, event)


def test_transitive_closed_event_never_publishes_shared_integration_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    integration_sha = "d" * 40
    reviewed_head_sha = "a" * 40
    event_path = tmp_path / "event.json"
    event_path.write_text(
        json.dumps(
            _closed_event(
                head_sha=reviewed_head_sha,
                integration_sha=integration_sha,
                base_sha="c" * 40,
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        merge_check.merge_ownership,
        "classify_event_ownership",
        lambda *_args, **_kwargs: SimpleNamespace(
            classification="transitive",
            integration_commit_sha=integration_sha,
            reviewed_head_sha=reviewed_head_sha,
            direct_reviewed_head_sha="b" * 40,
        ),
    )
    monkeypatch.delenv("DPONE_TEST_GITHUB_TOKEN", raising=False)

    code = merge_check.main(
        [
            "--event",
            str(event_path),
            "--receipt",
            str(tmp_path / "missing-receipt.json"),
            "--producer-exit-code",
            str(tmp_path / "missing-exit-code.txt"),
            "--repository",
            REPOSITORY,
            "--run-id",
            "101",
            "--run-attempt",
            "1",
            "--github-token-env",
            "DPONE_TEST_GITHUB_TOKEN",
        ]
    )

    report = json.loads(capsys.readouterr().out)
    assert code == 0
    assert report["status"] == "N/A"
    assert report["integration_commit_sha"] == integration_sha
    assert report["message"].startswith("Transitively included PR")
