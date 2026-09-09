from __future__ import annotations

import os
import re
import subprocess
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from .gitlab_mr import GitLabMergeRequestResult, GitLabMrClient
from .yaml_update import update_airflow_dev_snapshot_file

_BRANCH_SAFE_RE = re.compile(r"[^a-zA-Z0-9._-]+")
_ENV_LINE_RE = re.compile(r"^([A-Z0-9_]+)=(.*)$")


@dataclass(frozen=True)
class SnapshotValues:
    package_spec: str
    snapshot_version: str | None
    snapshot_sha: str | None


@dataclass(frozen=True)
class PromoteSnapshotSettings:
    package_spec: str
    snapshot_version: str | None
    snapshot_sha: str | None
    repo_url: str | None
    repo_dir: Path | None
    target_branch: str
    values_file: str
    branch_prefix: str
    push_user: str | None
    push_token: str | None
    git_author_name: str
    git_author_email: str
    commit_message: str | None
    mr_title: str | None
    mr_description: str | None
    mr_labels: tuple[str, ...]
    project_id: str | None
    api_url: str | None
    api_token: str | None
    open_mr: bool
    draft_mr: bool
    remove_source_branch: bool
    target_dir: str
    install_mode: str
    enabled: bool
    dry_run: bool


@dataclass(frozen=True)
class PromoteSnapshotResult:
    branch_name: str
    changed: bool
    created_file: bool
    repo_dir: Path
    values_file: Path
    commit_message: str
    mr_title: str
    package_spec: str
    mr_url: str | None
    dry_run: bool
    existed_mr: bool


def load_snapshot_values(snapshot_env_path: str | Path | None = None) -> SnapshotValues:
    values: dict[str, str] = {}
    if snapshot_env_path is not None:
        path = Path(snapshot_env_path)
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            match = _ENV_LINE_RE.match(line)
            if match:
                values[match.group(1)] = match.group(2)
    for key in ("DPONE_PACKAGE_SPEC", "DPONE_SNAPSHOT_VERSION", "DPONE_SNAPSHOT_SHA"):
        if key not in values and os.getenv(key):
            values[key] = str(os.getenv(key))
    package_spec = values.get("DPONE_PACKAGE_SPEC")
    if not package_spec:
        raise ValueError("DPONE_PACKAGE_SPEC must be provided via snapshot.env or environment")
    return SnapshotValues(
        package_spec=package_spec,
        snapshot_version=values.get("DPONE_SNAPSHOT_VERSION"),
        snapshot_sha=values.get("DPONE_SNAPSHOT_SHA"),
    )


def build_promote_branch_name(branch_prefix: str, package_spec: str) -> str:
    suffix = package_spec.split("==", 1)[-1]
    safe_suffix = _BRANCH_SAFE_RE.sub("-", suffix).strip(".-")
    return f"{branch_prefix.rstrip('/')}/{safe_suffix}"


def build_default_commit_message(package_spec: str) -> str:
    return f"chore(airflow-dev): promote dpone snapshot {package_spec}"


def build_default_mr_description(package_spec: str, snapshot_sha: str | None) -> str:
    lines = [
        "## Automated dpone dev snapshot promote",
        "",
        f"- package: `{package_spec}`",
    ]
    if snapshot_sha:
        lines.append(f"- source sha: `{snapshot_sha}`")
    pipeline_url = os.getenv("CI_PIPELINE_URL")
    if pipeline_url:
        lines.append(f"- source pipeline: {pipeline_url}")
    lines.extend(
        [
            "",
            "This MR updates the airflow-dev snapshot pin used by Argo CD.",
            "Merge when the desired dpone snapshot should be rolled out to dev.",
        ]
    )
    return "\n".join(lines)


def _run_git(args: Sequence[str], *, cwd: Path, env: dict[str, str] | None = None) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        env=env,
        check=True,
        text=True,
        capture_output=True,
    )
    return completed.stdout.strip()


def _authenticated_repo_url(repo_url: str, *, user: str, token: str) -> str:
    parts = urlsplit(repo_url)
    if parts.scheme not in {"http", "https"}:
        return repo_url
    netloc = parts.netloc
    if "@" in netloc:
        netloc = netloc.split("@", 1)[1]
    auth = f"{user}:{token}@{netloc}"
    return urlunsplit((parts.scheme, auth, parts.path, parts.query, parts.fragment))


def _clone_repo(settings: PromoteSnapshotSettings) -> Path:
    if settings.repo_dir is not None:
        return settings.repo_dir
    if not settings.repo_url:
        raise ValueError("repo_url is required when repo_dir is not provided")
    temp_dir = Path(tempfile.mkdtemp(prefix="dpone-argocd-promote-"))
    repo_dir = temp_dir / "argocd-main"
    repo_url = settings.repo_url
    if settings.push_user and settings.push_token:
        repo_url = _authenticated_repo_url(repo_url, user=settings.push_user, token=settings.push_token)
    _run_git(["clone", "--branch", settings.target_branch, "--single-branch", repo_url, str(repo_dir)], cwd=temp_dir)
    return repo_dir


def _prepare_branch(repo_dir: Path, settings: PromoteSnapshotSettings, branch_name: str) -> None:
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": settings.git_author_name,
            "GIT_AUTHOR_EMAIL": settings.git_author_email,
            "GIT_COMMITTER_NAME": settings.git_author_name,
            "GIT_COMMITTER_EMAIL": settings.git_author_email,
        }
    )
    _run_git(["fetch", "origin", settings.target_branch], cwd=repo_dir, env=env)
    _run_git(["checkout", settings.target_branch], cwd=repo_dir, env=env)
    _run_git(["reset", "--hard", f"origin/{settings.target_branch}"], cwd=repo_dir, env=env)
    _run_git(["checkout", "-B", branch_name], cwd=repo_dir, env=env)


def promote_snapshot_to_argocd(settings: PromoteSnapshotSettings) -> PromoteSnapshotResult:
    branch_name = build_promote_branch_name(settings.branch_prefix, settings.package_spec)
    commit_message = settings.commit_message or build_default_commit_message(settings.package_spec)
    mr_title = settings.mr_title or commit_message
    mr_description = settings.mr_description or build_default_mr_description(
        settings.package_spec,
        settings.snapshot_sha,
    )

    if settings.dry_run:
        repo_dir = settings.repo_dir or Path("<dry-run>")
        values_file = (repo_dir / settings.values_file) if settings.repo_dir else Path(settings.values_file)
        return PromoteSnapshotResult(
            branch_name=branch_name,
            changed=True,
            created_file=not values_file.exists() if settings.repo_dir else False,
            repo_dir=repo_dir,
            values_file=values_file,
            commit_message=commit_message,
            mr_title=mr_title,
            package_spec=settings.package_spec,
            mr_url=None,
            dry_run=True,
            existed_mr=False,
        )

    repo_dir = _clone_repo(settings)
    _prepare_branch(repo_dir, settings, branch_name)
    update_result = update_airflow_dev_snapshot_file(
        repo_dir / settings.values_file,
        settings.package_spec,
        enabled=settings.enabled,
        install_mode=settings.install_mode,
        target_dir=settings.target_dir,
    )

    if not update_result.changed:
        return PromoteSnapshotResult(
            branch_name=branch_name,
            changed=False,
            created_file=update_result.created,
            repo_dir=repo_dir,
            values_file=update_result.path,
            commit_message=commit_message,
            mr_title=mr_title,
            package_spec=settings.package_spec,
            mr_url=None,
            dry_run=False,
            existed_mr=False,
        )

    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": settings.git_author_name,
            "GIT_AUTHOR_EMAIL": settings.git_author_email,
            "GIT_COMMITTER_NAME": settings.git_author_name,
            "GIT_COMMITTER_EMAIL": settings.git_author_email,
        }
    )
    _run_git(["add", settings.values_file], cwd=repo_dir, env=env)
    _run_git(["commit", "-m", commit_message], cwd=repo_dir, env=env)
    _run_git(["push", "--force-with-lease", "origin", branch_name], cwd=repo_dir, env=env)

    mr_result: GitLabMergeRequestResult | None = None
    if settings.open_mr:
        if not settings.project_id:
            raise ValueError("project_id is required when open_mr=True")
        api_url = settings.api_url or os.getenv("CI_API_V4_URL")
        api_token = settings.api_token or settings.push_token
        if not api_url:
            raise ValueError("api_url (or CI_API_V4_URL) is required when open_mr=True")
        if not api_token:
            raise ValueError("api_token (or push_token) is required when open_mr=True")
        client = GitLabMrClient(api_url=api_url, private_token=api_token)
        mr_result = client.ensure_merge_request(
            project_id=settings.project_id,
            source_branch=branch_name,
            target_branch=settings.target_branch,
            title=mr_title,
            description=mr_description,
            labels=settings.mr_labels,
            remove_source_branch=settings.remove_source_branch,
            draft=settings.draft_mr,
        )

    return PromoteSnapshotResult(
        branch_name=branch_name,
        changed=True,
        created_file=update_result.created,
        repo_dir=repo_dir,
        values_file=update_result.path,
        commit_message=commit_message,
        mr_title=mr_title,
        package_spec=settings.package_spec,
        mr_url=mr_result.web_url if mr_result else None,
        dry_run=False,
        existed_mr=mr_result.existed if mr_result else False,
    )
