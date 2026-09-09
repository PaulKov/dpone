from __future__ import annotations

import re
from dataclasses import dataclass

from dpone.gitops.models import GitOpsIssue

ALLOWED_GIT_SYNC_FILTERS = frozenset({"blob:none", "tree:0"})
MIN_FILTER_VERSION = (4, 7, 0)
FILTER_SUPPORTED = "supported"
FILTER_UNSUPPORTED = "unsupported"
FILTER_UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowGitSyncFilterCapability:
    image: str
    support: str
    version: tuple[int, int, int] | None

    @property
    def can_render_filter(self) -> bool:
        return self.support != FILTER_UNSUPPORTED


def git_sync_filter_capability(image: str) -> GitOpsAirflowGitSyncFilterCapability:
    version = _parse_git_sync_version(image)
    if version is None:
        return GitOpsAirflowGitSyncFilterCapability(image=image, support=FILTER_UNKNOWN, version=None)
    support = FILTER_SUPPORTED if version >= MIN_FILTER_VERSION else FILTER_UNSUPPORTED
    return GitOpsAirflowGitSyncFilterCapability(image=image, support=support, version=version)


def should_render_git_sync_filter(*, image: str, filter_value: str | None) -> bool:
    if not filter_value or filter_value not in ALLOWED_GIT_SYNC_FILTERS:
        return False
    return git_sync_filter_capability(image).can_render_filter


def evaluate_git_sync_filter_request(
    *,
    image: str,
    filter_value: str | None,
    runner_policy: str,
    path: str,
    source: str,
) -> tuple[tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
    if not filter_value:
        return (), ()
    if filter_value not in ALLOWED_GIT_SYNC_FILTERS:
        return (), (_issue("git_sync_filter_invalid", "Unsupported git-sync partial clone filter", path, source),)

    capability = git_sync_filter_capability(image)
    if capability.support == FILTER_SUPPORTED:
        return (), ()
    if capability.support == FILTER_UNSUPPORTED:
        return (), (
            _issue(
                "git_sync_filter_unsupported",
                "The selected git-sync image does not support --filter; use git-sync v4.7.0+ or remove the filter",
                path,
                source,
            ),
        )

    issue = _issue(
        "git_sync_filter_support_unverified",
        "dpone cannot verify that this git-sync image supports --filter; release mode requires git-sync v4.7.0+",
        path,
        source,
    )
    if runner_policy == "release":
        return (), (issue,)
    return (issue,), ()


def _parse_git_sync_version(image: str) -> tuple[int, int, int] | None:
    leaf = image.rsplit("/", 1)[-1]
    tag = leaf.rsplit(":", 1)[-1] if ":" in leaf else ""
    tag = tag.split("@", 1)[0]
    match = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", tag)
    if match is None:
        return None
    major, minor, patch = (int(part) for part in match.groups())
    return major, minor, patch


def _issue(code: str, message: str, path: str, source: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=source)


__all__ = [
    "ALLOWED_GIT_SYNC_FILTERS",
    "FILTER_SUPPORTED",
    "FILTER_UNKNOWN",
    "FILTER_UNSUPPORTED",
    "GitOpsAirflowGitSyncFilterCapability",
    "evaluate_git_sync_filter_request",
    "git_sync_filter_capability",
    "should_render_git_sync_filter",
]
