from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


path_evidence = _load("dpone_agent_path_evidence_test", "tools/agent_policy/path_evidence.py")
merge_identity = _load(
    "dpone_agent_pr_merge_identity_paths_test",
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


def test_nul_evidence_preserves_unicode_and_embedded_newline_paths() -> None:
    paths = [".agents/политика.yml", "docs/line\nbreak.md"]
    content = b"\0".join(path.encode() for path in reversed(paths)) + b"\0"

    assert path_evidence.parse_path_evidence(content, field="paths") == sorted(paths)


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (b"docs/file.md\0docs/truncated.md", "truncated"),
        (b'"docs/quoted\\tpath.md"\n', "C-quoted"),
        (b"\xff\0", "UTF-8"),
    ],
)
def test_ambiguous_or_invalid_path_evidence_fails_closed(content: bytes, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        path_evidence.parse_path_evidence(content, field="paths")


def test_exact_git_paths_round_trip_unicode_and_embedded_newline(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-b", "master")
    _git(tmp_path, "config", "user.email", "agent-tests@example.invalid")
    _git(tmp_path, "config", "user.name", "Agent tests")
    (tmp_path / "README.md").write_text("base\n", encoding="utf-8")
    _git(tmp_path, "add", "README.md")
    _git(tmp_path, "commit", "-m", "base")
    base = _git(tmp_path, "rev-parse", "HEAD")
    paths = [".agents/политика.yml", "docs/line\nbreak.md"]
    for relative in paths:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("changed\n", encoding="utf-8")
        _git(tmp_path, "add", "--", relative)
    _git(tmp_path, "commit", "-m", "unusual paths")
    integration = _git(tmp_path, "rev-parse", "HEAD")

    actual = merge_identity.exact_first_parent_paths(
        tmp_path,
        base_parent_sha=base,
        integration_commit_sha=integration,
    )

    assert actual == sorted(paths)
