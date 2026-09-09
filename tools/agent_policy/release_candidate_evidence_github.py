"""GitHub provider boundary for release-candidate evidence publication."""

from __future__ import annotations

import importlib.util
import sys
import time
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, TypeVar


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


codec = _load_sibling("dpone_release_candidate_codec_github", "release_candidate_evidence_codec.py")
contract = _load_sibling("dpone_release_candidate_contract_github", "release_candidate_evidence_contract.py")
policy = _load_sibling("dpone_release_candidate_policy_github", "release_candidate_evidence_policy.py")
github_api = _load_sibling("dpone_release_candidate_api_github", "pr_receipt_github_api.py")
T = TypeVar("T")
_UTC = timezone.utc  # noqa: UP017 -- the repository's public type target is Python 3.10
PUBLICATION_WORKFLOW_PATHS = frozenset({".github/workflows/release.yml", ".github/workflows/runtime-image.yml"})
PUBLICATION_DISCOVERY_ATTEMPTS = 31
PUBLICATION_DISCOVERY_POLL_SECONDS = 10.0


class WorkflowRunReader(Protocol):
    """Small provider interface needed to authenticate publication runs."""

    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]: ...
    def list_publication_workflow_runs(
        self,
        repository: str,
        *,
        workflow_path: str,
        commit_sha: str,
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class PublicationRunIdentity:
    """Provider-authenticated identity of one publication workflow run."""

    workflow_path: str
    run_id: int
    run_attempt: int
    created_at: datetime
    updated_at: datetime

    @property
    def canonical_created_at(self) -> str:
        return self.created_at.astimezone(_UTC).isoformat().replace("+00:00", "Z")

    def to_receipt(self) -> dict[str, Any]:
        """Project only the closed fields retained by verification receipts."""

        payload = {
            "workflow_path": self.workflow_path,
            "run_id": self.run_id,
            "run_attempt": self.run_attempt,
            "created_at": self.canonical_created_at,
        }
        codec.require_exact_keys(
            payload,
            contract.PAIRED_PUBLICATION_RUN_KEYS,
            field="paired_publication_run",
        )
        return payload


@dataclass(frozen=True, slots=True)
class PublicationBoundary:
    """Authenticated caller, complete run pair, and immutable cutoff."""

    caller: PublicationRunIdentity
    runs: tuple[PublicationRunIdentity, ...]
    cutoff: datetime

    def __post_init__(self) -> None:
        expected_paths = tuple(sorted(PUBLICATION_WORKFLOW_PATHS))
        observed_paths = tuple(run.workflow_path for run in self.runs)
        if observed_paths != expected_paths:
            raise ValueError("paired publication runs must contain the two approved workflows in path order")
        matching_callers = [run for run in self.runs if run.workflow_path == self.caller.workflow_path]
        if len(matching_callers) != 1 or matching_callers[0] != self.caller:
            raise ValueError("current publication caller does not match its paired run identity")
        if self.cutoff != min(run.created_at for run in self.runs):
            raise ValueError("publication cutoff is not the exact minimum paired creation time")

    @property
    def workflow_path(self) -> str:
        return self.caller.workflow_path

    @property
    def run_id(self) -> int:
        return self.caller.run_id

    @property
    def run_attempt(self) -> int:
        return self.caller.run_attempt

    @property
    def paired_publication_runs(self) -> list[dict[str, Any]]:
        return [run.to_receipt() for run in self.runs]

    @property
    def canonical_cutoff(self) -> str:
        return self.cutoff.astimezone(_UTC).isoformat().replace("+00:00", "Z")

    @property
    def publication_pair_sha256(self) -> str:
        return codec.canonical_json_sha256(self.paired_publication_runs)


class GitHubReleaseCandidateEvidenceAdapter:
    """Bounded GitHub REST adapter whose token never enters evidence."""

    def __init__(
        self,
        token: str,
        *,
        attempts: int = 3,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not token:
            raise ValueError("GitHub token is required")
        if attempts < 1:
            raise ValueError("GitHub retry attempts must be positive")
        self._token = token
        self._attempts = attempts
        self._sleeper = sleeper

    def list_check_runs(self, repository: str, commit_sha: str) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"check_name": policy.JOB_NAME, "filter": "all", "per_page": 100})
        return self._items(
            f"repos/{repository}/commits/{commit_sha}/check-runs?{query}",
            array_key="check_runs",
        )

    def list_workflow_runs(self, repository: str, *, commit_sha: str) -> list[dict[str, Any]]:
        workflow = urllib.parse.quote(policy.WORKFLOW_PATH, safe="")
        query = urllib.parse.urlencode({"event": "workflow_dispatch", "head_sha": commit_sha, "per_page": 100})
        return self._items(
            f"repos/{repository}/actions/workflows/{workflow}/runs?{query}",
            array_key="workflow_runs",
        )

    def list_publication_workflow_runs(
        self,
        repository: str,
        *,
        workflow_path: str,
        commit_sha: str,
    ) -> list[dict[str, Any]]:
        workflow = urllib.parse.quote(workflow_path, safe="")
        query = urllib.parse.urlencode({"event": "push", "head_sha": commit_sha, "per_page": 100})
        return self._items(
            f"repos/{repository}/actions/workflows/{workflow}/runs?{query}",
            array_key="workflow_runs",
        )

    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]:
        return self._retry(
            lambda: github_api.github_json(f"repos/{repository}/actions/runs/{run_id}", token=self._token)
        )

    def list_attempt_jobs(self, repository: str, *, run_id: int, run_attempt: int) -> list[dict[str, Any]]:
        return self._items(
            f"repos/{repository}/actions/runs/{run_id}/attempts/{run_attempt}/jobs?per_page=100",
            array_key="jobs",
        )

    def list_run_artifacts(self, repository: str, run_id: int) -> list[dict[str, Any]]:
        return self._items(
            f"repos/{repository}/actions/runs/{run_id}/artifacts?per_page=100",
            array_key="artifacts",
        )

    def download_artifact(self, url: str) -> bytes:
        return self._retry(lambda: github_api.github_bytes(url, token=self._token))

    def _items(self, path: str, *, array_key: str) -> list[dict[str, Any]]:
        return self._retry(lambda: github_api.github_paginated_items(path, token=self._token, array_key=array_key))

    def _retry(self, operation: Callable[[], T]) -> T:
        errors: list[str] = []
        for attempt in range(1, self._attempts + 1):
            try:
                return operation()
            except (OSError, RuntimeError) as exc:
                errors.append(str(exc))
                if attempt < self._attempts:
                    self._sleeper(0.25 * attempt)
        raise RuntimeError(f"GitHub release-evidence request failed after {self._attempts} attempts: {errors[-1]}")


def fetch_publication_boundary(
    *,
    adapter: WorkflowRunReader,
    repository: str,
    commit_sha: str,
    release: str,
    workflow_path: str,
    run_id: int,
    run_attempt: int,
    discovery_attempts: int = PUBLICATION_DISCOVERY_ATTEMPTS,
    discovery_poll_seconds: float = PUBLICATION_DISCOVERY_POLL_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
) -> PublicationBoundary:
    """Fetch and authenticate the current tag-push publication workflow run."""

    if workflow_path not in PUBLICATION_WORKFLOW_PATHS:
        raise ValueError("publication workflow path is not approved")
    positive_run_id = positive_int(run_id, "publication_run_id")
    positive_attempt = positive_int(run_attempt, "publication_run_attempt")
    _validate_discovery_policy(attempts=discovery_attempts, poll_seconds=discovery_poll_seconds)
    unresolved: list[str] = []
    for observation in range(discovery_attempts):
        current_created, current_updated = _validate_current_publication_run(
            run=adapter.get_workflow_run(repository, positive_run_id),
            repository=repository,
            commit_sha=commit_sha,
            release=release,
            workflow_path=workflow_path,
            run_id=positive_run_id,
            run_attempt=positive_attempt,
        )
        boundaries: dict[str, PublicationRunIdentity] = {}
        unresolved = []
        for approved_path in sorted(PUBLICATION_WORKFLOW_PATHS):
            boundary = _select_publication_trigger(
                listed=adapter.list_publication_workflow_runs(
                    repository,
                    workflow_path=approved_path,
                    commit_sha=commit_sha,
                ),
                repository=repository,
                commit_sha=commit_sha,
                release=release,
                workflow_path=approved_path,
            )
            if boundary is None:
                unresolved.append(approved_path)
            else:
                boundaries[approved_path] = boundary
        current = boundaries.get(workflow_path)
        if current is not None:
            if current.run_id != positive_run_id or current.created_at != current_created:
                raise ValueError("current publication workflow immutable detail/list identity is inconsistent")
            if current.run_attempt != positive_attempt or current.updated_at != current_updated:
                unresolved.append(f"{workflow_path} mutable detail/list projection")
        if len(boundaries) == len(PUBLICATION_WORKFLOW_PATHS) and not unresolved:
            ordered_runs = tuple(boundaries[path] for path in sorted(PUBLICATION_WORKFLOW_PATHS))
            if current is None:
                raise ValueError("current publication caller is missing from the paired runs")
            return PublicationBoundary(
                caller=current,
                runs=ordered_runs,
                cutoff=min(boundary.created_at for boundary in ordered_runs),
            )
        if observation + 1 < discovery_attempts:
            sleeper(discovery_poll_seconds)
    raise ValueError(f"publication workflow is unavailable after bounded poll: {', '.join(unresolved)}")


def _select_publication_trigger(
    *,
    listed: list[dict[str, Any]],
    repository: str,
    commit_sha: str,
    release: str,
    workflow_path: str,
) -> PublicationRunIdentity | None:
    exact: list[dict[str, Any]] = []
    for run in listed:
        if run.get("head_branch") != release:
            continue
        expected = {"event": "push", "head_sha": commit_sha, "head_branch": release}
        if any(run.get(key) != value for key, value in expected.items()):
            raise ValueError(f"publication workflow {workflow_path} identity is invalid")
        if workflow_run_path(run, field="publication_workflow_run.path") != workflow_path:
            raise ValueError(f"publication workflow {workflow_path} identity is invalid")
        run_repository = require_mapping(run.get("repository"), "publication_workflow_run.repository")
        if run_repository.get("full_name") != repository:
            raise ValueError(f"publication workflow {workflow_path} identity is invalid")
        exact.append(run)
    if not exact:
        return None
    if len(exact) != 1:
        raise ValueError(f"publication workflow {workflow_path} must resolve to exactly one exact tag-push run")
    run = exact[0]
    created_at, updated_at = publication_timestamps(run)
    return PublicationRunIdentity(
        workflow_path=workflow_path,
        run_id=positive_int(run.get("id"), "publication_workflow_run.id"),
        run_attempt=positive_int(run.get("run_attempt"), "publication_workflow_run.run_attempt"),
        created_at=created_at,
        updated_at=updated_at,
    )


def _validate_current_publication_run(
    *,
    run: dict[str, Any],
    repository: str,
    commit_sha: str,
    release: str,
    workflow_path: str,
    run_id: int,
    run_attempt: int,
) -> tuple[datetime, datetime]:
    expected = {
        "id": run_id,
        "event": "push",
        "head_sha": commit_sha,
        "head_branch": release,
        "run_attempt": run_attempt,
        "conclusion": None,
    }
    if any(run.get(key) != value for key, value in expected.items()):
        raise ValueError("publication workflow run identity is invalid")
    if run.get("status") != "in_progress":
        raise ValueError("publication workflow run status is not current in_progress")
    repository_value = require_mapping(run.get("repository"), "publication_workflow_run.repository")
    if repository_value.get("full_name") != repository:
        raise ValueError("publication workflow repository is invalid")
    if workflow_run_path(run, field="publication_workflow_run.path") != workflow_path:
        raise ValueError("publication workflow run path is invalid")
    return publication_timestamps(run)


def _validate_discovery_policy(*, attempts: int, poll_seconds: float) -> None:
    if isinstance(attempts, bool) or not isinstance(attempts, int) or not 1 <= attempts <= 31:
        raise ValueError("publication discovery attempts are outside the approved bound")
    if isinstance(poll_seconds, bool) or not isinstance(poll_seconds, int | float):
        raise ValueError("publication discovery poll interval must be a finite number")
    if not 1 <= poll_seconds <= 10:
        raise ValueError("publication discovery poll interval is outside the approved bound")


def workflow_run_path(run: dict[str, Any], *, field: str) -> str:
    return codec.require_string(run.get("path"), field=field).split("@", 1)[0]


def publication_timestamps(run: dict[str, Any]) -> tuple[datetime, datetime]:
    created_at = parse_timestamp(run.get("created_at"), field="publication_workflow_run.created_at")
    updated_at = parse_timestamp(run.get("updated_at"), field="publication_workflow_run.updated_at")
    if created_at > updated_at:
        raise ValueError("publication workflow run timestamps are inconsistent")
    return created_at, updated_at


def require_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a mapping")
    return value


def positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def parse_timestamp(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(_UTC)
