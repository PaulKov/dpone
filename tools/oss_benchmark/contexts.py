"""Run and release context helpers for benchmark evidence."""

from __future__ import annotations

import subprocess
import tomllib
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path

GitRunner = Callable[..., str]


@dataclass(frozen=True)
class ReleaseContext:
    """Resolved dpone release metadata rendered in benchmark v2 evidence."""

    dpone_version: str
    release_tag: str
    release_sha: str
    branch: str
    dirty: bool
    resolved_by: str

    def to_jsonable(self) -> dict[str, str | bool]:
        return asdict(self)


def resolve_release_context(
    *,
    repo_root: Path,
    branch: str,
    git_sha: str,
    dirty: bool,
    explicit_version: str = "",
    explicit_tag: str = "",
    explicit_sha: str = "",
    git_runner: GitRunner | None = None,
) -> ReleaseContext:
    """Resolve release metadata from explicit CLI values, pyproject, then git."""

    runner = git_runner or _git_runner(repo_root)
    if explicit_version or explicit_tag or explicit_sha:
        version = explicit_version or _version_from_tag(explicit_tag)
        tag = explicit_tag or (f"v{version}" if version else "")
        return ReleaseContext(
            dpone_version=version,
            release_tag=tag,
            release_sha=explicit_sha or git_sha,
            branch=branch,
            dirty=dirty,
            resolved_by="cli",
        )

    version = _read_pyproject_version(repo_root)
    if version:
        tag = f"v{version}"
        tag_sha = runner("rev-list", "-n", "1", tag)
        if tag_sha:
            return ReleaseContext(
                dpone_version=version,
                release_tag=tag,
                release_sha=tag_sha,
                branch=branch,
                dirty=dirty,
                resolved_by="pyproject",
            )
        return ReleaseContext(
            dpone_version=version,
            release_tag=runner("describe", "--tags", "--abbrev=0"),
            release_sha=git_sha,
            branch=branch,
            dirty=dirty,
            resolved_by="git",
        )

    tag = runner("describe", "--tags", "--abbrev=0")
    return ReleaseContext(
        dpone_version=_version_from_tag(tag),
        release_tag=tag,
        release_sha=git_sha,
        branch=branch,
        dirty=dirty,
        resolved_by="git" if tag else "unresolved",
    )


def release_context_to_jsonable(context: ReleaseContext) -> dict[str, str | bool]:
    """Return JSON-safe release context metadata."""

    return context.to_jsonable()


def release_context_is_resolved(payload: dict[str, object]) -> bool:
    """Return whether payload has enough release metadata for release gating."""

    context = payload.get("release_context")
    if not isinstance(context, dict):
        return False
    return bool(context.get("dpone_version") and context.get("release_sha"))


def _read_pyproject_version(repo_root: Path) -> str:
    path = repo_root / "pyproject.toml"
    if not path.exists():
        return ""
    payload = tomllib.loads(path.read_text(encoding="utf-8"))
    project = payload.get("project")
    if isinstance(project, dict):
        return str(project.get("version") or "")
    return ""


def _version_from_tag(tag: str) -> str:
    return tag[1:] if tag.startswith("v") else tag


def _git_runner(repo_root: Path) -> GitRunner:
    def run(*args: str) -> str:
        result = subprocess.run(["git", *args], cwd=repo_root, capture_output=True, text=True, check=False)
        return result.stdout.strip() if result.returncode == 0 else ""

    return run
