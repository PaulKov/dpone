"""Authenticate the trusted default-branch capacity workflow before any scan."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol

from dpone.ports.ci_shadow_reconciliation import CiShadowReconciliationProvider

_WORKFLOW_PATH = ".github/workflows/pr-gate-shadow-capacity.yml"
_DEFAULT_BRANCH = "master"
_RequestClass = Literal[
    "producer_list_page_requests",
    "auditor_list_page_requests",
    "exact_producer_run_requests",
    "attempt_run_requests",
    "jobs_page_requests",
    "artifact_metadata_requests",
    "artifact_download_requests",
    "redirect_requests",
    "git_object_requests",
    "pull_request_identity_requests",
    "polling_requests",
]


class RequestBudget(Protocol):
    """Narrow dispatch budget required to authenticate one workflow attempt."""

    @property
    def remaining_response_bytes(self) -> int: ...

    def dispatch(self, request_class: _RequestClass, operation: Callable[[float], bytes]) -> bytes: ...


class CapacitySourceAuthenticationError(ValueError):
    """The runner event, environment, and provider run identity do not agree."""


@dataclass(frozen=True)
class CapacitySource:
    """Provider-authenticated identity produced only by the source authenticator."""

    repository_id: int
    workflow_id: int
    workflow_sha: str
    run_id: int
    run_attempt: int

    def __post_init__(self) -> None:
        if any(value < 1 for value in (self.repository_id, self.workflow_id, self.run_id, self.run_attempt)):
            raise ValueError("capacity source identifiers must be positive")
        if len(self.workflow_sha) != 40 or any(char not in "0123456789abcdef" for char in self.workflow_sha):
            raise ValueError("capacity source workflow SHA must be lowercase")


def authenticate_capacity_source(
    event: Mapping[str, object],
    environment: Mapping[str, str],
    *,
    provider: CiShadowReconciliationProvider,
    budget: RequestBudget,
) -> CapacitySource:
    """Bind the running workflow to GitHub's exact immutable attempt record."""

    repository = _mapping(event.get("repository"), "event repository")
    repository_id = _positive(repository.get("id"), "repository id")
    repository_name = _nonempty(repository.get("full_name"), "repository name")
    if environment.get("GITHUB_REPOSITORY") != repository_name:
        raise CapacitySourceAuthenticationError("runner repository does not match the event")
    run_id = _environment_positive(environment.get("GITHUB_RUN_ID"), "GITHUB_RUN_ID")
    attempt = _environment_positive(environment.get("GITHUB_RUN_ATTEMPT"), "GITHUB_RUN_ATTEMPT")
    workflow_sha = _sha(environment.get("GITHUB_SHA"), "GITHUB_SHA")
    expected_ref = f"{repository_name}/{_WORKFLOW_PATH}@refs/heads/{_DEFAULT_BRANCH}"
    if environment.get("GITHUB_WORKFLOW_REF") != expected_ref:
        raise CapacitySourceAuthenticationError("runner workflow ref is not the approved default-branch workflow")
    payload = budget.dispatch(
        "attempt_run_requests",
        lambda timeout: provider.get_workflow_run_attempt(
            run_id=run_id,
            attempt=attempt,
            timeout_seconds=timeout,
            max_response_bytes=budget.remaining_response_bytes,
        ),
    )
    run = _object(payload)
    if (
        _positive(run.get("id"), "provider run id") != run_id
        or _positive(run.get("run_attempt"), "provider run attempt") != attempt
        or _positive(_mapping(run.get("repository"), "provider repository").get("id"), "provider repository id")
        != repository_id
        or run.get("event") != "workflow_dispatch"
        or run.get("head_branch") != _DEFAULT_BRANCH
        or run.get("head_sha") != workflow_sha
        or run.get("path") != _WORKFLOW_PATH
    ):
        raise CapacitySourceAuthenticationError("provider workflow run does not match the trusted capacity source")
    return CapacitySource(
        repository_id=repository_id,
        workflow_id=_positive(run.get("workflow_id"), "provider workflow id"),
        workflow_sha=workflow_sha,
        run_id=run_id,
        run_attempt=attempt,
    )


def _object(payload: bytes) -> Mapping[str, object]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapacitySourceAuthenticationError("provider workflow run is not JSON") from exc
    return _mapping(value, "provider workflow run")


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise CapacitySourceAuthenticationError(f"{label} is malformed")
    return value


def _nonempty(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise CapacitySourceAuthenticationError(f"{label} is malformed")
    return value


def _positive(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise CapacitySourceAuthenticationError(f"{label} is malformed")
    return value


def _environment_positive(value: object, label: str) -> int:
    if not isinstance(value, str) or not value.isdecimal():
        raise CapacitySourceAuthenticationError(f"{label} is malformed")
    return _positive(int(value), label)


def _sha(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise CapacitySourceAuthenticationError(f"{label} is malformed")
    return value


__all__ = ["CapacitySource", "CapacitySourceAuthenticationError", "authenticate_capacity_source"]
