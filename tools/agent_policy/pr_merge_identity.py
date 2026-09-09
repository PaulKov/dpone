"""Validate immutable pull-request merge events and Git object relationships."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

Runner = Callable[..., Any]


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


merge_paths = _load_sibling("dpone_agent_pr_merge_paths", "pr_merge_paths.py")
exact_first_parent_paths = merge_paths.exact_first_parent_paths


@dataclass(frozen=True)
class MergeEvent:
    """Canonical authority extracted from a merged pull-request event."""

    repository: str
    protected_base_ref: str
    base_sha: str
    pr_number: int
    merged_at: str
    reviewed_head_sha: str
    integration_commit_sha: str
    body: str


@dataclass(frozen=True)
class IntegrationIdentity:
    """Verified immutable Git relationship between reviewed and integrated code."""

    method: str
    base_parent_sha: str
    reviewed_head_sha: str
    reviewed_head_tree: str
    integration_commit_sha: str
    integration_tree: str
    changed_paths: list[str]


@dataclass(frozen=True)
class IntegrationOwnership:
    """Classify whether one closed PR owns the shared integration commit."""

    classification: str
    integration_commit_sha: str
    reviewed_head_sha: str
    direct_reviewed_head_sha: str


def validate_merge_event(
    event: dict[str, Any],
    *,
    repository: str,
    protected_base_refs: Sequence[str],
    integration_commit_sha: str,
) -> MergeEvent:
    """Validate and normalize the immutable merged pull-request event."""

    if event.get("action") != "closed":
        raise ValueError("merge closure requires pull_request action closed")
    event_repository = _mapping(event.get("repository"))
    if event_repository.get("full_name") != repository:
        raise ValueError(f"event repository must be {repository}")
    pull_request = _mapping(event.get("pull_request"))
    if pull_request.get("merged") is not True or pull_request.get("state") != "closed":
        raise ValueError("pull request must be closed and merged")

    base = _mapping(pull_request.get("base"))
    base_repo = _mapping(base.get("repo"))
    if base_repo.get("full_name") != repository:
        raise ValueError(f"pull request base repository must be {repository}")
    base_ref = _required_string(base.get("ref"), field="pull_request.base.ref")
    if base_ref not in set(protected_base_refs):
        raise ValueError(f"pull request base ref {base_ref!r} is not a protected release branch")

    exact_commit = _required_sha(integration_commit_sha, field="integration_commit_sha")
    merge_commit = _required_sha(pull_request.get("merge_commit_sha"), field="pull_request.merge_commit_sha")
    if merge_commit != exact_commit:
        raise ValueError(
            f"pull_request.merge_commit_sha {merge_commit} does not equal integration commit {exact_commit}"
        )
    head = _mapping(pull_request.get("head"))
    reviewed_head = _required_sha(head.get("sha"), field="pull_request.head.sha")
    base_sha = _required_sha(base.get("sha"), field="pull_request.base.sha")
    number = pull_request.get("number")
    if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
        raise ValueError("pull_request.number must be a positive integer")
    merged_at = _required_timestamp(pull_request.get("merged_at"), field="pull_request.merged_at")
    body = pull_request.get("body")
    if body is None:
        body = ""
    if not isinstance(body, str):
        raise ValueError("pull_request.body must be a string or null")

    return MergeEvent(
        repository=repository,
        protected_base_ref=base_ref,
        base_sha=base_sha,
        pr_number=number,
        merged_at=merged_at,
        reviewed_head_sha=reviewed_head,
        integration_commit_sha=exact_commit,
        body=body,
    )


def verify_integration_identity(
    root: Path,
    event: MergeEvent,
    *,
    runner: Runner = subprocess.run,
) -> IntegrationIdentity:
    """Prove the merge/squash relationship and exact first-parent delta."""

    ownership = classify_integration_ownership(root, event, runner=runner)
    if ownership.classification != "direct":
        raise ValueError("transitively included pull request does not own the exact integration receipt")
    parents = _git(root, ["show", "-s", "--format=%P", event.integration_commit_sha], runner=runner).split()
    if len(parents) == 2:
        method = "merge"
    elif len(parents) == 1:
        method = "squash"
    else:
        raise ValueError("integration commit must have one squash parent or two merge parents")

    base_parent = _required_sha(parents[0], field="integration first parent")
    _require_ancestor(
        root,
        ancestor=base_parent,
        descendant=event.reviewed_head_sha,
        message="integration first parent must be an ancestor of the reviewed head",
        runner=runner,
    )

    integration_tree = _required_sha(
        _git(root, ["rev-parse", f"{event.integration_commit_sha}^{{tree}}"], runner=runner),
        field="integration tree",
    )
    reviewed_tree = _required_sha(
        _git(root, ["rev-parse", f"{event.reviewed_head_sha}^{{tree}}"], runner=runner),
        field="reviewed head tree",
    )
    if integration_tree != reviewed_tree:
        raise ValueError("integration tree must equal reviewed head tree")

    return IntegrationIdentity(
        method=method,
        base_parent_sha=base_parent,
        reviewed_head_sha=event.reviewed_head_sha,
        reviewed_head_tree=reviewed_tree,
        integration_commit_sha=event.integration_commit_sha,
        integration_tree=integration_tree,
        changed_paths=exact_first_parent_paths(
            root,
            base_parent_sha=base_parent,
            integration_commit_sha=event.integration_commit_sha,
            runner=runner,
        ),
    )


def classify_integration_ownership(
    root: Path,
    event: MergeEvent,
    *,
    runner: Runner = subprocess.run,
) -> IntegrationOwnership:
    """Distinguish the direct merged PR from PRs included through its head."""

    _ensure_reviewed_head(root, event, runner=runner)
    checked_out = checked_out_commit(root, runner=runner)
    if checked_out != event.integration_commit_sha:
        raise ValueError(
            f"checked-out commit {checked_out} does not equal integration commit {event.integration_commit_sha}"
        )
    parents = _git(root, ["show", "-s", "--format=%P", event.integration_commit_sha], runner=runner).split()
    integration_tree = _required_sha(
        _git(root, ["rev-parse", f"{event.integration_commit_sha}^{{tree}}"], runner=runner),
        field="integration tree",
    )
    reviewed_tree = _required_sha(
        _git(root, ["rev-parse", f"{event.reviewed_head_sha}^{{tree}}"], runner=runner),
        field="reviewed head tree",
    )
    if len(parents) == 1:
        if integration_tree != reviewed_tree:
            raise ValueError(
                "one-parent integration tree does not identify this reviewed head as the direct squash owner"
            )
        direct_head = event.reviewed_head_sha
        classification = "direct"
    elif len(parents) == 2:
        direct_head = _required_sha(parents[1], field="integration second parent")
        if direct_head == event.reviewed_head_sha:
            classification = "direct"
        else:
            _require_ancestor(
                root,
                ancestor=event.reviewed_head_sha,
                descendant=direct_head,
                message="integration second parent neither equals nor contains the reviewed head",
                runner=runner,
            )
            direct_tree = _required_sha(
                _git(root, ["rev-parse", f"{direct_head}^{{tree}}"], runner=runner),
                field="integration second-parent tree",
            )
            if integration_tree != direct_tree:
                raise ValueError("transitive integration tree must equal the direct reviewed head tree")
            classification = "transitive"
    else:
        raise ValueError("integration commit must have one squash parent or two merge parents")

    _require_ancestor(
        root,
        ancestor=_required_sha(parents[0], field="integration first parent"),
        descendant=event.reviewed_head_sha,
        message="integration first parent must be an ancestor of the reviewed head",
        runner=runner,
    )
    return IntegrationOwnership(
        classification=classification,
        integration_commit_sha=event.integration_commit_sha,
        reviewed_head_sha=event.reviewed_head_sha,
        direct_reviewed_head_sha=direct_head,
    )


def checked_out_commit(root: Path, *, runner: Runner = subprocess.run) -> str:
    """Return the canonical full SHA of the checked-out commit."""

    return _required_sha(_git(root, ["rev-parse", "HEAD"], runner=runner), field="checked-out commit")


def _require_ancestor(
    root: Path,
    *,
    ancestor: str,
    descendant: str,
    message: str,
    runner: Runner,
) -> None:
    result = runner(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", ancestor, descendant],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise ValueError(message)


def _ensure_reviewed_head(root: Path, event: MergeEvent, *, runner: Runner) -> None:
    present = runner(
        ["git", "-C", str(root), "cat-file", "-e", f"{event.reviewed_head_sha}^{{commit}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if present.returncode == 0:
        return
    fetched_ref = "refs/remotes/dpone/reviewed-head"
    _git(
        root,
        [
            "fetch",
            "--no-tags",
            "--force",
            "origin",
            f"refs/pull/{event.pr_number}/head:{fetched_ref}",
        ],
        runner=runner,
    )
    fetched = _git(root, ["rev-parse", fetched_ref], runner=runner)
    if fetched != event.reviewed_head_sha:
        raise ValueError("fetched pull-request head does not equal the closed-event reviewed head")


def _git(root: Path, args: list[str], *, runner: Runner) -> str:
    result = runner(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        diagnostic = " ".join((result.stderr or result.stdout or "git command failed").split())
        raise ValueError(f"git {' '.join(args)} failed: {diagnostic}")
    return result.stdout.strip()


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _required_string(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _required_sha(value: Any, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field} must be a full lowercase Git SHA")
    return value


def _required_timestamp(value: Any, *, field: str) -> str:
    timestamp = _required_string(value, field=field)
    try:
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    return timestamp
