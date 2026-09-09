from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
REVIEWED_HEAD = "a" * 40


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


merge_identity = _load(
    "dpone_agent_pr_merge_identity_fetch_test",
    "tools/agent_policy/pr_merge_identity.py",
)


class FetchRunner:
    def __init__(self, *, fetched_sha: str = REVIEWED_HEAD) -> None:
        self.fetched_sha = fetched_sha
        self.commands: list[list[str]] = []

    def __call__(self, command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.commands.append(command)
        args = command[3:]
        if args == ["cat-file", "-e", f"{REVIEWED_HEAD}^{{commit}}"]:
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="missing")
        if args[:4] == ["fetch", "--no-tags", "--force", "origin"]:
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if args == ["rev-parse", "refs/remotes/dpone/reviewed-head"]:
            return subprocess.CompletedProcess(command, 0, stdout=f"{self.fetched_sha}\n", stderr="")
        raise AssertionError(command)


def _event() -> Any:
    return merge_identity.MergeEvent(
        repository="PaulKov/dpone",
        protected_base_ref="master",
        base_sha="b" * 40,
        pr_number=455,
        merged_at="2026-07-29T00:03:00Z",
        reviewed_head_sha=REVIEWED_HEAD,
        integration_commit_sha="c" * 40,
        body="Reviewed body\n",
    )


def test_missing_reviewed_head_is_fetched_from_exact_pull_request_ref(tmp_path: Path) -> None:
    runner = FetchRunner()

    merge_identity._ensure_reviewed_head(tmp_path, _event(), runner=runner)

    assert runner.commands[1][3:] == [
        "fetch",
        "--no-tags",
        "--force",
        "origin",
        "refs/pull/455/head:refs/remotes/dpone/reviewed-head",
    ]


def test_fetched_pull_request_ref_must_equal_closed_event_head(tmp_path: Path) -> None:
    runner = FetchRunner(fetched_sha="f" * 40)

    with pytest.raises(ValueError, match="does not equal"):
        merge_identity._ensure_reviewed_head(tmp_path, _event(), runner=runner)
