from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from tests.agent_policy._release_candidate_evidence_helpers import COMMIT_SHA, REPOSITORY


@dataclass
class FakeReleaseEvidenceAdapter:
    """Deterministic in-memory provider adapter with request tracing."""

    check_runs: list[dict[str, Any]]
    workflow_runs: list[dict[str, Any]] = field(default_factory=list)
    publication_workflow_runs: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    runs: dict[int, dict[str, Any]] = field(default_factory=dict)
    jobs: dict[tuple[int, int], list[dict[str, Any]]] = field(default_factory=dict)
    artifacts: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    downloads: dict[str, bytes] = field(default_factory=dict)
    requested_runs: list[int] = field(default_factory=list)
    requested_publication_workflows: list[str] = field(default_factory=list)
    downloaded_urls: list[str] = field(default_factory=list)

    def list_check_runs(self, repository: str, commit_sha: str) -> list[dict[str, Any]]:
        assert (repository, commit_sha) == (REPOSITORY, COMMIT_SHA)
        return self.check_runs

    def list_workflow_runs(
        self,
        repository: str,
        *,
        commit_sha: str,
    ) -> list[dict[str, Any]]:
        assert repository == REPOSITORY
        assert commit_sha == COMMIT_SHA
        return self.workflow_runs

    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]:
        assert repository == REPOSITORY
        self.requested_runs.append(run_id)
        return self.runs[run_id]

    def list_publication_workflow_runs(
        self,
        repository: str,
        *,
        workflow_path: str,
        commit_sha: str,
    ) -> list[dict[str, Any]]:
        assert repository == REPOSITORY
        assert commit_sha == COMMIT_SHA
        self.requested_publication_workflows.append(workflow_path)
        return self.publication_workflow_runs.get(workflow_path, [])

    def list_attempt_jobs(
        self,
        repository: str,
        *,
        run_id: int,
        run_attempt: int,
    ) -> list[dict[str, Any]]:
        assert repository == REPOSITORY
        return self.jobs.get((run_id, run_attempt), [])

    def list_run_artifacts(self, repository: str, run_id: int) -> list[dict[str, Any]]:
        assert repository == REPOSITORY
        return self.artifacts.get(run_id, [])

    def download_artifact(self, url: str) -> bytes:
        self.downloaded_urls.append(url)
        return self.downloads[url]
