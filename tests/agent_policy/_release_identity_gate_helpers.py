from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
COMMIT_SHA = "a" * 40


def load_release_identity_module(name: str = "dpone_agent_release_identity_gate_tests") -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools/agent_policy/release_identity_gate.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


release_identity = load_release_identity_module()


def write_project(root: Path, relative: str, *, name: str, version: str, body: str = "") -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "[project]",
                f'name = "{name}"',
                f'version = "{version}"',
                body,
                "",
            ]
        ),
        encoding="utf-8",
    )


def repository(tmp_path: Path, *, version: str = "1.2.3") -> Path:
    write_project(
        tmp_path,
        "pyproject.toml",
        name="dpone",
        version=version,
        body="\n".join(
            [
                'dependencies = ["dpone-airflow-pack==1.2.3"]',
                "[project.optional-dependencies]",
                'accel = ["dpone-native-accel==1.2.3"]',
            ]
        ),
    )
    write_project(
        tmp_path,
        "packages/dpone-native-accel/pyproject.toml",
        name="dpone-native-accel",
        version=version,
    )
    write_project(
        tmp_path,
        "packages/dpone-airflow-pack/pyproject.toml",
        name="dpone-airflow-pack",
        version=version,
    )
    write_project(
        tmp_path,
        "packages/apache-airflow-providers-dpone/pyproject.toml",
        name="apache-airflow-providers-dpone",
        version=version,
        body='dependencies = ["dpone-airflow-pack==1.2.3"]',
    )
    (tmp_path / "CHANGELOG.md").write_text(
        "# Changelog\n\n## 1.2.3 - 2026-07-19\n\nRelease notes.\n",
        encoding="utf-8",
    )
    policy = tmp_path / ".agents/policy/github-branch-protection.yml"
    policy.parent.mkdir(parents=True, exist_ok=True)
    policy.write_text(
        "\n".join(
            [
                "ruleset:",
                "  id: 18806829",
                "  branches:",
                "    - master",
                "  required_status_checks:",
                "    checks:",
                "      - Quality checks (3.12)",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return tmp_path


def git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", *args),
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def successful_git(command: tuple[str, ...], cwd: Path) -> Any:
    if command[1] == "show":
        _, relative = command[2].split(":", maxsplit=1)
        return release_identity.CommandResult(
            returncode=0,
            stdout=(cwd / relative).read_text(encoding="utf-8"),
        )
    if command[1:3] == ("cat-file", "-t"):
        return release_identity.CommandResult(returncode=0, stdout="tag\n")
    if command[1:3] == ("rev-list", "-n"):
        return release_identity.CommandResult(returncode=0, stdout=f"{COMMIT_SHA}\n")
    if command[1] == "rev-parse":
        return release_identity.CommandResult(returncode=0, stdout=f"{'c' * 40}\n")
    if command[1:3] == ("merge-base", "--is-ancestor"):
        return release_identity.CommandResult(returncode=0, stdout="")
    raise AssertionError(command)


def evaluate(tmp_path: Path, monkeypatch: Any, **overrides: Any) -> Any:
    root = overrides.pop("root", None) or repository(tmp_path)
    monkeypatch.setattr(release_identity, "_run_command", overrides.pop("runner", successful_git))
    return release_identity.evaluate_release_identity(
        root=root,
        tag=overrides.pop("tag", "v1.2.3"),
        commit_sha=overrides.pop("commit_sha", COMMIT_SHA),
        remote_ref=overrides.pop("remote_ref", None),
        **overrides,
    )


def publish_origin_master(root: Path, commit_sha: str) -> None:
    git(root, "branch", "-M", "master")
    (root / ".git/refs/remotes/origin").mkdir(parents=True, exist_ok=True)
    (root / ".git/refs/remotes/origin/master").write_text(f"{commit_sha}\n", encoding="utf-8")


def codes(report: Any) -> list[str]:
    return [blocker.code for blocker in report.blockers]
