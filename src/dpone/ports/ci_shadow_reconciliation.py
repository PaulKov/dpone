"""Read-only provider contracts for bounded CI-shadow reconciliation acquisition."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class ArtifactHttpResponse:
    """One non-followed artifact response for the metered transport service."""

    status: int
    headers: Mapping[str, str]
    body: bytes


class CiShadowReconciliationProvider(Protocol):
    """No-write provider reads required by the canonical observation service."""

    def list_workflow_runs(
        self,
        *,
        workflow_id: int,
        event: str,
        created_from: datetime,
        created_to: datetime,
        page: int,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> bytes:
        """Return raw closed UTC-second workflow-run JSON bytes or raise on uncertainty."""

    def get_workflow_run_attempt(
        self, *, run_id: int, attempt: int, timeout_seconds: float, max_response_bytes: int
    ) -> bytes:
        """Return one exact historical workflow-run attempt record."""

    def list_attempt_jobs(
        self, *, run_id: int, attempt: int, page: int, timeout_seconds: float, max_response_bytes: int
    ) -> bytes:
        """Return one exact-attempt Jobs page; ``total_count`` is required."""

    def list_run_artifacts(self, *, run_id: int, page: int, timeout_seconds: float, max_response_bytes: int) -> bytes:
        """Return one exact-run artifact metadata page; ``total_count`` is required."""

    def download_artifact_archive(self, *, artifact_id: int, timeout_seconds: float, max_response_bytes: int) -> bytes:
        """Return exactly one provider artifact ZIP archive through a metered read."""


class CiShadowGitObjectProvider(Protocol):
    """Read immutable Git objects used to bind the acquisition implementation."""

    def get_git_commit_tree(self, *, commit_sha: str, timeout_seconds: float, max_response_bytes: int) -> bytes:
        """Return raw GitHub commit JSON containing its root tree identity."""

    def get_git_tree(self, *, tree_sha: str, timeout_seconds: float, max_response_bytes: int) -> bytes:
        """Return raw recursive Git tree JSON for one immutable root tree."""

    def get_git_blob(self, *, blob_sha: str, timeout_seconds: float, max_response_bytes: int) -> bytes:
        """Return raw immutable Git blob JSON."""


__all__ = ["ArtifactHttpResponse", "CiShadowGitObjectProvider", "CiShadowReconciliationProvider"]
