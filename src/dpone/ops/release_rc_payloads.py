"""Reusable release-candidate merge-train payload normalizers."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.ops.release_rc_models import ReleaseRcCheckRun, ReleaseRcMergeTrain, ReleaseRcPullRequest

MERGE_TRAIN_SCHEMA_VERSION = "dpone.release_rc_merge_train.v1"


def read_merge_train(path: str | Path) -> ReleaseRcMergeTrain:
    """Read a stable release-candidate merge train JSON file."""

    payload = _read_mapping(path)
    return merge_train_from_payload(payload)


def read_pull_requests(paths: Sequence[str | Path]) -> tuple[ReleaseRcPullRequest, ...]:
    """Read ordered GitHub CLI PR JSON exports."""

    return tuple(pull_request_from_payload(_read_mapping(path)) for path in paths)


def merge_train_from_pull_requests(
    *,
    base_branch: str,
    head_branch: str,
    pull_requests: Sequence[ReleaseRcPullRequest],
) -> ReleaseRcMergeTrain:
    """Build a merge train from already-normalized pull requests."""

    return ReleaseRcMergeTrain(
        base_branch=base_branch,
        head_branch=head_branch,
        pull_requests=tuple(pull_requests),
    )


def merge_train_from_payload(payload: Mapping[str, object]) -> ReleaseRcMergeTrain:
    """Normalize a merge-train payload into the public model."""

    pull_requests = tuple(pull_request_from_payload(item) for item in _mapping_items(payload.get("pull_requests")))
    return ReleaseRcMergeTrain(
        base_branch=str(payload.get("base_branch", "")),
        head_branch=str(payload.get("head_branch", "")),
        pull_requests=pull_requests,
    )


def pull_request_from_payload(payload: Mapping[str, object]) -> ReleaseRcPullRequest:
    """Normalize one GitHub-style PR payload into a release RC PR record."""

    return ReleaseRcPullRequest(
        number=_int_value(payload.get("number")),
        title=str(payload.get("title", "")),
        base_ref=str(payload.get("base_ref", payload.get("baseRefName", ""))),
        head_ref=str(payload.get("head_ref", payload.get("headRefName", ""))),
        state=str(payload.get("state", "")).upper(),
        merge_state=str(payload.get("merge_state", payload.get("mergeStateStatus", ""))).upper(),
        is_draft=bool(payload.get("is_draft", payload.get("isDraft", False))),
        checks=tuple(check_from_payload(item) for item in _check_items(payload)),
        url=str(payload.get("url", "")),
    )


def check_from_payload(payload: Mapping[str, object]) -> ReleaseRcCheckRun:
    """Normalize one check run/status payload from GitHub CLI JSON."""

    return ReleaseRcCheckRun(
        name=str(payload.get("name", payload.get("context", ""))),
        status=str(payload.get("status", payload.get("state", ""))).upper(),
        conclusion=str(payload.get("conclusion", "")).upper(),
        details_url=str(payload.get("details_url", payload.get("detailsUrl", payload.get("targetUrl", "")))),
    )


def merge_train_to_payload(merge_train: ReleaseRcMergeTrain) -> dict[str, object]:
    """Render a merge train as stable JSON payload."""

    return {"schema_version": MERGE_TRAIN_SCHEMA_VERSION, **merge_train.to_dict()}


def _read_mapping(path: str | Path) -> Mapping[str, object]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _check_items(payload: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    direct = _mapping_items(payload.get("checks"))
    if direct:
        return direct
    return _mapping_items(payload.get("statusCheckRollup"))


def _mapping_items(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list | tuple):
        return tuple()
    return tuple(item for item in value if isinstance(item, Mapping))


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


__all__ = [
    "MERGE_TRAIN_SCHEMA_VERSION",
    "check_from_payload",
    "merge_train_from_payload",
    "merge_train_from_pull_requests",
    "merge_train_to_payload",
    "pull_request_from_payload",
    "read_merge_train",
    "read_pull_requests",
]
