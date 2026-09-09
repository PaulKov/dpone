"""Bounded current pull-request identity observation for the shadow auditor."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from dpone.contracts.ci_shadow_audit import MergeTuple, eligible_set_digest
from dpone.ports.ci_shadow_audit import (
    CiShadowAuditDeadline,
    CiShadowAuditDeadlineProvider,
    CiShadowAuditProvider,
)


class ShadowAuditIdentityError(ValueError):
    """A current pull-request identity cannot be proven safely."""


_SHA_LENGTH = 40
_MAX_OBSERVATIONS = 5
_TIMEOUT_SECONDS = 30.0
_POLL_INTERVAL_SECONDS = 1.0
_SHARED_IDENTITY_FIELDS = (
    "number",
    "repository_id",
    "base_repository_id",
    "head_repository_id",
    "base_ref",
    "head_ref",
    "base_sha",
    "head_sha",
    "state",
)


def stable_merge_snapshot(
    claims: Mapping[str, object],
    workflow_run: Mapping[str, object],
    provider: CiShadowAuditProvider,
    clock: Callable[[], float],
    wait: Callable[[float], None],
) -> MergeTuple:
    """Return two consecutive equal observations within one strict deadline."""

    deadline = CiShadowAuditDeadline(clock=clock, expires_at=clock() + _TIMEOUT_SECONDS)
    previous: MergeTuple | None = None
    for index in range(_MAX_OBSERVATIONS):
        deadline.remaining_seconds()
        observation = _observe_once(claims, workflow_run, provider, deadline)
        deadline.remaining_seconds()
        if observation is None:
            previous = None
            if index + 1 < _MAX_OBSERVATIONS:
                wait(min(_POLL_INTERVAL_SECONDS, deadline.remaining_seconds()))
                deadline.remaining_seconds()
            continue
        if observation == previous:
            return observation
        previous = observation
    raise ShadowAuditIdentityError("stable merge tuple was not observed")


def _observe_once(
    claims: Mapping[str, object],
    workflow_run: Mapping[str, object],
    provider: CiShadowAuditProvider,
    deadline: CiShadowAuditDeadline,
) -> MergeTuple | None:
    open_prs = _list_open_pull_requests(provider, deadline)
    if not isinstance(open_prs, Sequence):
        raise ShadowAuditIdentityError("open PR pagination is incomplete")
    repository_id = positive_int(claims.get("repository_id"), "repository_id")
    pr_number = positive_int(claims.get("pr_number"), "pr_number")
    head_repository_id = positive_int(workflow_run.get("head_repository_id"), "workflow_run.head_repository_id")
    head_ref = nonempty_string(workflow_run.get("head_branch"), "workflow_run.head_branch")
    head_sha = sha(workflow_run.get("head_sha"), "workflow_run.head_sha")
    candidates = [
        candidate
        for raw in open_prs
        if isinstance(raw, Mapping)
        for candidate in [
            _eligible_list_candidate(
                raw,
                repository_id=repository_id,
                head_repository_id=head_repository_id,
                head_ref=head_ref,
                head_sha=head_sha,
            )
        ]
        if candidate is not None
    ]
    if len(candidates) != 1 or candidates[0]["number"] != pr_number:
        raise ShadowAuditIdentityError("open PR eligibility is ambiguous")
    exact = _get_pull_request(provider, pr_number, deadline)
    if exact.get("mergeable") is None:
        return None
    record = _eligible_pr(
        exact,
        repository_id=repository_id,
        head_repository_id=head_repository_id,
        head_ref=head_ref,
        head_sha=head_sha,
    )
    if record is None or record["number"] != pr_number:
        raise ShadowAuditIdentityError("claimed PR is not currently eligible")
    if any(record.get(field) != candidates[0].get(field) for field in _SHARED_IDENTITY_FIELDS):
        raise ShadowAuditIdentityError("exact PR identity disagrees with open PR enumeration")
    merge_sha = sha(_resolve_pull_merge_ref(provider, pr_number, deadline), "merge ref")
    if sha(record["merge_commit_sha"], "merge_commit_sha") != merge_sha:
        raise ShadowAuditIdentityError("merge ref disagrees with exact PR")
    parents = _get_commit_parents(provider, merge_sha, deadline)
    if len(parents) != 2:
        raise ShadowAuditIdentityError("merge commit has incorrect parent count")
    base_sha, observed_head_sha = (sha(value, "merge parent") for value in parents)
    if observed_head_sha != head_sha or base_sha != sha(record["base_sha"], "base_sha"):
        raise ShadowAuditIdentityError("merge parents disagree with current PR")
    digest = eligible_set_digest([record])
    return MergeTuple(repository_id, pr_number, base_sha, head_sha, merge_sha, digest)


def _list_open_pull_requests(
    provider: CiShadowAuditProvider, deadline: CiShadowAuditDeadline
) -> Sequence[Mapping[str, object]]:
    if isinstance(provider, CiShadowAuditDeadlineProvider):
        return provider.list_open_pull_requests_bounded(deadline=deadline)
    deadline.remaining_seconds()
    records = provider.list_open_pull_requests()
    deadline.remaining_seconds()
    return records


def _get_pull_request(
    provider: CiShadowAuditProvider, number: int, deadline: CiShadowAuditDeadline
) -> Mapping[str, object]:
    if isinstance(provider, CiShadowAuditDeadlineProvider):
        return provider.get_pull_request_bounded(number, deadline=deadline)
    deadline.remaining_seconds()
    record = provider.get_pull_request(number)
    deadline.remaining_seconds()
    return record


def _resolve_pull_merge_ref(provider: CiShadowAuditProvider, number: int, deadline: CiShadowAuditDeadline) -> str:
    if isinstance(provider, CiShadowAuditDeadlineProvider):
        return provider.resolve_pull_merge_ref_bounded(number, deadline=deadline)
    deadline.remaining_seconds()
    merge_sha = provider.resolve_pull_merge_ref(number)
    deadline.remaining_seconds()
    return merge_sha


def _get_commit_parents(
    provider: CiShadowAuditProvider, merge_sha: str, deadline: CiShadowAuditDeadline
) -> Sequence[str]:
    if isinstance(provider, CiShadowAuditDeadlineProvider):
        return provider.get_commit_parents_bounded(merge_sha, deadline=deadline)
    deadline.remaining_seconds()
    parents = provider.get_commit_parents(merge_sha)
    deadline.remaining_seconds()
    return parents


def _eligible_list_candidate(
    raw: Mapping[str, object],
    *,
    repository_id: int,
    head_repository_id: int,
    head_ref: str,
    head_sha: str,
) -> dict[str, object] | None:
    """Select ambiguity candidates from fields available on GitHub list rows."""

    if not set(_SHARED_IDENTITY_FIELDS) <= set(raw) or raw.get("state") != "open":
        return None
    if raw.get("repository_id") != repository_id or raw.get("base_repository_id") != repository_id:
        return None
    if (
        raw.get("head_repository_id") != head_repository_id
        or raw.get("head_ref") != head_ref
        or raw.get("head_sha") != head_sha
        or raw.get("base_ref") != "master"
    ):
        return None
    return {
        "number": positive_int(raw.get("number"), "PR number"),
        "repository_id": repository_id,
        "base_repository_id": repository_id,
        "head_repository_id": head_repository_id,
        "base_ref": "master",
        "head_ref": head_ref,
        "base_sha": sha(raw.get("base_sha"), "base_sha"),
        "head_sha": sha(raw.get("head_sha"), "head_sha"),
        "state": "open",
    }


def _eligible_pr(
    raw: Mapping[str, object],
    *,
    repository_id: int,
    head_repository_id: int,
    head_ref: str,
    head_sha: str,
) -> dict[str, object] | None:
    required = {*_SHARED_IDENTITY_FIELDS, "mergeable", "merge_commit_sha"}
    if not required <= set(raw) or raw.get("state") != "open" or raw.get("mergeable") is not True:
        return None
    if raw.get("repository_id") != repository_id or raw.get("base_repository_id") != repository_id:
        return None
    if (
        raw.get("head_repository_id") != head_repository_id
        or raw.get("head_ref") != head_ref
        or raw.get("head_sha") != head_sha
        or raw.get("base_ref") != "master"
    ):
        return None
    positive_int(raw.get("number"), "PR number")
    sha(raw.get("base_sha"), "base_sha")
    sha(raw.get("head_sha"), "head_sha")
    sha(raw.get("merge_commit_sha"), "merge_commit_sha")
    return dict(raw)


def positive_int(value: object, name: str) -> int:
    """Return a non-boolean positive integer or reject the observation."""

    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ShadowAuditIdentityError(f"{name} must be a positive integer")
    return value


def nonempty_string(value: object, name: str) -> str:
    """Return one non-empty string or reject the observation."""

    if not isinstance(value, str) or not value:
        raise ShadowAuditIdentityError(f"{name} must be a non-empty string")
    return value


def sha(value: object, name: str) -> str:
    """Return one canonical lowercase SHA-1 object identifier."""

    if not isinstance(value, str) or len(value) != _SHA_LENGTH or any(char not in "0123456789abcdef" for char in value):
        raise ShadowAuditIdentityError(f"{name} must be a lowercase SHA")
    return value
