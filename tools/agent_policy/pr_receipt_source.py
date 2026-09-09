"""Select and verify an immutable pre-merge Agent PR receipt artifact."""

from __future__ import annotations

import hashlib
import importlib.util
import re
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, TypeVar

SELF_CHECK_NAME = "Agent PR receipt"
SOURCE_ARTIFACT_NAME = "agent-pr-receipt"
EXPECTED_WORKFLOW_PATH = ".github/workflows/agent-pr-receipt.yml"
SHA256_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
T = TypeVar("T")


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


github_api = _load_sibling("dpone_agent_pr_receipt_source_github_api", "pr_receipt_github_api.py")
github_metadata = _load_sibling("dpone_agent_pr_receipt_source_metadata", "pr_receipt_github_metadata.py")
resource_limits = _load_sibling(
    "dpone_agent_pr_receipt_source_resource_limits",
    "artifact_resource_limits.py",
)
source_archive = _load_sibling(
    "dpone_agent_pr_receipt_source_archive",
    "pr_receipt_source_archive.py",
)
SourceReceipt = source_archive.SourceReceipt
validate_source_archive = source_archive.validate_source_archive


class SourceReceiptAdapter(Protocol):
    """Read-only provider operations required for source receipt selection."""

    def list_check_runs(self, repository: str, reviewed_head_sha: str) -> list[dict[str, Any]]: ...

    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]: ...

    def list_run_artifacts(self, repository: str, run_id: int) -> list[dict[str, Any]]: ...

    def download_artifact(self, url: str) -> bytes: ...


@dataclass(frozen=True)
class SourceArtifact:
    """Exact successful pre-merge workflow artifact and its downloaded bytes."""

    check_run_id: int
    workflow_run_id: int
    workflow_run_attempt: int
    artifact_id: int
    artifact_digest: str
    artifact_size_bytes: int
    archive_sha256: str
    archive_size_bytes: int
    created_at: str
    completed_at: str
    archive_bytes: bytes


class GitHubSourceReceiptAdapter:
    """GitHub REST adapter that never serializes its token."""

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

    def list_check_runs(self, repository: str, reviewed_head_sha: str) -> list[dict[str, Any]]:
        return self._retry(
            lambda: github_api.github_paginated_items(
                f"repos/{repository}/commits/{reviewed_head_sha}/check-runs",
                token=self._token,
                array_key="check_runs",
            )
        )

    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]:
        return self._retry(
            lambda: github_api.github_json(f"repos/{repository}/actions/runs/{run_id}", token=self._token)
        )

    def list_run_artifacts(self, repository: str, run_id: int) -> list[dict[str, Any]]:
        return self._retry(
            lambda: github_api.github_paginated_items(
                f"repos/{repository}/actions/runs/{run_id}/artifacts",
                token=self._token,
                array_key="artifacts",
            )
        )

    def download_artifact(self, url: str) -> bytes:
        return self._retry(lambda: github_api.github_bytes(url, token=self._token))

    def _retry(self, operation: Callable[[], T]) -> T:
        errors: list[str] = []
        for attempt in range(1, self._attempts + 1):
            try:
                return operation()
            except RuntimeError as exc:
                errors.append(str(exc))
                if attempt < self._attempts:
                    self._sleeper(0.25 * attempt)
        raise RuntimeError(f"GitHub evidence request failed after {self._attempts} attempts: {errors[-1]}")


def select_source_receipt(
    *,
    repository: str,
    reviewed_head_sha: str,
    merged_at: str,
    adapter: SourceReceiptAdapter,
) -> SourceArtifact:
    """Select the latest authoritative successful pre-merge receipt artifact."""

    merge_time = _timestamp(merged_at, field="merged_at")
    candidates: list[tuple[datetime, int, int, dict[str, Any], dict[str, Any]]] = []
    for check in adapter.list_check_runs(repository, reviewed_head_sha):
        if (
            check.get("name") != SELF_CHECK_NAME
            or check.get("status") != "completed"
            or check.get("conclusion") != "success"
            or check.get("head_sha") != reviewed_head_sha
        ):
            continue
        app = check.get("app")
        if not isinstance(app, dict) or app.get("slug") != "github-actions":
            continue
        try:
            check_id = _positive_int(check.get("id"), field="check_run.id")
            completed_at = _required_string(check.get("completed_at"), field="check_run.completed_at")
            completed_time = _timestamp(completed_at, field="check_run.completed_at")
        except ValueError:
            continue
        if completed_time > merge_time:
            continue
        run_id = github_metadata.workflow_run_id_from_payload(check) or github_metadata.optional_int(
            check.get("workflow_run_id")
        )
        if run_id is None or run_id <= 0:
            continue
        run = adapter.get_workflow_run(repository, run_id)
        try:
            _validate_workflow_run(
                run,
                expected_run_id=run_id,
                repository=repository,
                reviewed_head_sha=reviewed_head_sha,
                merge_time=merge_time,
            )
        except ValueError:
            continue
        candidates.append((completed_time, check_id, run_id, check, run))

    if not candidates:
        raise ValueError("no successful immutable pre-merge Agent PR receipt check run exists")
    _completed_time, check_id, run_id, check, run = max(candidates, key=lambda item: (item[0], item[1], item[2]))

    artifacts = [
        artifact
        for artifact in adapter.list_run_artifacts(repository, run_id)
        if artifact.get("name") == SOURCE_ARTIFACT_NAME
    ]
    if len(artifacts) != 1:
        raise ValueError(f"source workflow run {run_id} must contain exactly one {SOURCE_ARTIFACT_NAME} artifact")
    artifact = artifacts[0]
    artifact_workflow_run = artifact.get("workflow_run")
    if not isinstance(artifact_workflow_run, dict):
        raise ValueError("source Agent PR receipt artifact workflow run identity is missing")
    artifact_run_id = github_metadata.optional_int(artifact_workflow_run.get("id"))
    if artifact_run_id != run_id:
        raise ValueError("source Agent PR receipt artifact workflow run does not match selected run")
    if artifact_workflow_run.get("head_sha") != reviewed_head_sha:
        raise ValueError("source Agent PR receipt artifact head SHA does not match reviewed head")
    if artifact.get("expired") is not False:
        raise ValueError("source Agent PR receipt artifact is expired or has unknown expiry state")
    artifact_id = _positive_int(artifact.get("id"), field="artifact.id")
    digest = _required_digest(artifact.get("digest"), field="artifact.digest")
    metadata_size = _positive_int(artifact.get("size_in_bytes"), field="artifact.size_in_bytes")
    resource_limits.validate_provider_size(
        metadata_size,
        resource="source Agent PR receipt artifact",
    )
    created_at = _required_string(artifact.get("created_at"), field="artifact.created_at")
    if _timestamp(created_at, field="artifact.created_at") > merge_time:
        raise ValueError("source Agent PR receipt artifact was created after pull-request merge")
    url = _required_string(artifact.get("archive_download_url"), field="artifact.archive_download_url")
    archive_bytes, archive_digest, archive_size = _download_verified_archive(
        adapter,
        url=url,
        expected_digest=digest,
        expected_size=metadata_size,
    )

    return SourceArtifact(
        check_run_id=check_id,
        workflow_run_id=run_id,
        workflow_run_attempt=_positive_int(run.get("run_attempt"), field="workflow_run.run_attempt"),
        artifact_id=artifact_id,
        artifact_digest=digest,
        artifact_size_bytes=metadata_size,
        archive_sha256=archive_digest,
        archive_size_bytes=archive_size,
        created_at=created_at,
        completed_at=_required_string(check.get("completed_at"), field="check_run.completed_at"),
        archive_bytes=archive_bytes,
    )


def _validate_workflow_run(
    run: dict[str, Any],
    *,
    expected_run_id: int,
    repository: str,
    reviewed_head_sha: str,
    merge_time: datetime,
) -> None:
    if run.get("event") != "pull_request" or run.get("status") != "completed" or run.get("conclusion") != "success":
        raise ValueError("source workflow run must be a successful pull_request run")
    if _positive_int(run.get("id"), field="workflow_run.id") != expected_run_id:
        raise ValueError("source workflow run ID does not match selected check run")
    if run.get("name") != SELF_CHECK_NAME:
        raise ValueError(f"source workflow run name must be {SELF_CHECK_NAME}")
    if run.get("head_sha") != reviewed_head_sha:
        raise ValueError("source workflow run head SHA does not match reviewed head")
    repository_payload = run.get("repository")
    if not isinstance(repository_payload, dict) or repository_payload.get("full_name") != repository:
        raise ValueError("source workflow run repository does not match canonical repository")
    path = _required_string(run.get("path"), field="workflow_run.path").split("@", 1)[0]
    if path != EXPECTED_WORKFLOW_PATH:
        raise ValueError(f"source workflow run path must be {EXPECTED_WORKFLOW_PATH}")
    updated_at = _required_string(run.get("updated_at"), field="workflow_run.updated_at")
    if _timestamp(updated_at, field="workflow_run.updated_at") > merge_time:
        raise ValueError("source workflow run completed after pull-request merge")


def _download_verified_archive(
    adapter: SourceReceiptAdapter,
    *,
    url: str,
    expected_digest: str,
    expected_size: int,
) -> tuple[bytes, str, int]:
    archive_bytes = adapter.download_artifact(url)
    resource_limits.validate_downloaded_size(
        archive_bytes,
        resource="source Agent PR receipt artifact",
    )
    archive_digest = f"sha256:{hashlib.sha256(archive_bytes).hexdigest()}"
    archive_size = len(archive_bytes)
    if archive_digest != expected_digest:
        raise ValueError(f"source artifact digest mismatch: metadata {expected_digest}, downloaded {archive_digest}")
    if archive_size != expected_size:
        raise ValueError(f"source artifact size mismatch: metadata {expected_size}, downloaded {archive_size}")
    return archive_bytes, archive_digest, archive_size


def _timestamp(value: str, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed


def _required_string(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _required_digest(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or SHA256_DIGEST.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _positive_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value
