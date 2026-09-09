"""Select the latest exact-SHA provider-bound release-candidate artifact."""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


codec = _load_sibling("dpone_release_candidate_codec_source", "release_candidate_evidence_codec.py")
policy = _load_sibling("dpone_release_candidate_policy_source", "release_candidate_evidence_policy.py")
github = _load_sibling("dpone_release_candidate_github_source", "release_candidate_evidence_github.py")
resource_limits = _load_sibling("dpone_release_candidate_resource_limits_source", "artifact_resource_limits.py")
GitHubReleaseCandidateEvidenceAdapter = github.GitHubReleaseCandidateEvidenceAdapter
PUBLICATION_WORKFLOW_PATHS = github.PUBLICATION_WORKFLOW_PATHS
_mapping = github.require_mapping
_positive_int = github.positive_int
_timestamp = github.parse_timestamp


class ReleaseCandidateEvidenceAdapter(Protocol):
    """Read-only provider operations required for exact-attempt selection."""

    def list_check_runs(self, repository: str, commit_sha: str) -> list[dict[str, Any]]: ...
    def list_workflow_runs(self, repository: str, *, commit_sha: str) -> list[dict[str, Any]]: ...
    def list_publication_workflow_runs(
        self,
        repository: str,
        *,
        workflow_path: str,
        commit_sha: str,
    ) -> list[dict[str, Any]]: ...
    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]: ...
    def list_attempt_jobs(self, repository: str, *, run_id: int, run_attempt: int) -> list[dict[str, Any]]: ...
    def list_run_artifacts(self, repository: str, run_id: int) -> list[dict[str, Any]]: ...
    def download_artifact(self, url: str) -> bytes: ...


@dataclass(frozen=True, slots=True)
class SelectedReleaseCandidateArtifact:
    """Provider identity and bytes for the latest exact-SHA attempt."""

    check_run_id: int
    check_suite_id: int
    workflow_run_id: int
    workflow_run_attempt: int
    job_id: int
    artifact_id: int
    artifact_name: str
    artifact_digest: str
    artifact_size_bytes: int
    archive_sha256: str
    archive_size_bytes: int
    archive_bytes: bytes
    publication_workflow_path: str
    publication_run_id: int
    publication_run_attempt: int
    paired_publication_runs: tuple[dict[str, Any], ...]
    publication_cutoff: str
    publication_pair_sha256: str


def select_release_candidate_artifact(
    *,
    repository: str,
    commit_sha: str,
    release: str,
    publication_workflow_path: str,
    publication_run_id: int,
    publication_run_attempt: int,
    adapter: ReleaseCandidateEvidenceAdapter,
) -> SelectedReleaseCandidateArtifact:
    """Select the provider-latest exact-SHA run without fallback."""

    _validate_subject(repository=repository, commit_sha=commit_sha, release=release)
    publication = github.fetch_publication_boundary(
        adapter=adapter,
        repository=repository,
        commit_sha=commit_sha,
        release=release,
        workflow_path=publication_workflow_path,
        run_id=publication_run_id,
        run_attempt=publication_run_attempt,
    )
    publication_created = publication.cutoff
    listed_runs = adapter.list_workflow_runs(repository, commit_sha=commit_sha)
    candidates = [
        run
        for run in listed_runs
        if run.get("head_sha") == commit_sha
        and run.get("event") == "workflow_dispatch"
        and _workflow_path(run, field="workflow_run.path") == policy.WORKFLOW_PATH
    ]
    if not candidates:
        raise ValueError("release-candidate evidence workflow run is unavailable for the exact commit")
    ordered = [
        (created_at, run) for run in candidates if (created_at := _workflow_run_created_at(run)) <= publication_created
    ]
    if not ordered:
        raise ValueError("release-candidate evidence workflow has no pre-publication dispatch")
    latest_order = max(order for order, _run in ordered)
    latest = [run for order, run in ordered if order == latest_order]
    if len(latest) != 1:
        raise ValueError("latest release-candidate workflow run ordering is ambiguous")
    listed_run = latest[0]
    run_id = _positive_int(listed_run.get("id"), "workflow_run.id")
    listed_created = _timestamp(listed_run.get("created_at"), field="workflow_run.created_at")
    listed_updated = _timestamp(listed_run.get("updated_at"), field="workflow_run.updated_at")
    listed_attempt = _positive_int(listed_run.get("run_attempt"), "workflow_run.run_attempt")
    run = adapter.get_workflow_run(repository, run_id)
    run_attempt, suite_id = _validate_run(
        run,
        repository=repository,
        commit_sha=commit_sha,
        run_id=run_id,
        release=release,
        listed_created=listed_created,
        listed_updated=listed_updated,
        listed_attempt=listed_attempt,
        publication_created=publication_created,
    )
    jobs = adapter.list_attempt_jobs(
        repository,
        run_id=run_id,
        run_attempt=run_attempt,
    )
    matching_jobs = [job for job in jobs if job.get("name") == policy.JOB_NAME]
    if len(matching_jobs) != 1:
        raise ValueError("latest workflow attempt must contain exactly one terminal evidence job")
    job = matching_jobs[0]
    check_url = codec.require_string(job.get("check_run_url"), field="job.check_run_url")
    check_id = _check_run_id(check_url, repository=repository)
    job_id = _validate_job(
        job,
        repository=repository,
        commit_sha=commit_sha,
        run_id=run_id,
        run_attempt=run_attempt,
        check_id=check_id,
    )
    matching_checks = [
        check for check in adapter.list_check_runs(repository, commit_sha) if check.get("id") == check_id
    ]
    if len(matching_checks) != 1:
        raise ValueError("terminal evidence job must resolve to exactly one exact-SHA check")
    check = matching_checks[0]
    _validate_check(
        check,
        commit_sha=commit_sha,
        check_id=check_id,
        check_suite_id=suite_id,
        publication_created=publication_created,
    )

    artifact_name = policy.artifact_name(commit_sha, run_id, run_attempt)
    artifacts = [
        artifact for artifact in adapter.list_run_artifacts(repository, run_id) if artifact.get("name") == artifact_name
    ]
    if len(artifacts) != 1:
        raise ValueError("selected attempt must retain exactly one current-attempt authority artifact")
    artifact = artifacts[0]
    if artifact.get("expired") is not False:
        raise ValueError("release-candidate authority artifact is expired or expiry is unknown")
    if _timestamp(artifact.get("created_at"), field="artifact.created_at") >= publication_created:
        raise ValueError("release-candidate authority artifact was created at or after publication started")
    artifact_run = _mapping(artifact.get("workflow_run"), "artifact.workflow_run")
    if artifact_run.get("id") != run_id or artifact_run.get("head_sha") != commit_sha:
        raise ValueError("release-candidate authority artifact workflow identity is invalid")
    artifact_id = _positive_int(artifact.get("id"), "artifact.id")
    artifact_digest = codec.require_digest(artifact.get("digest"), field="artifact.digest")
    artifact_size = _positive_int(artifact.get("size_in_bytes"), "artifact.size_in_bytes")
    resource_limits.validate_provider_size(
        artifact_size,
        resource="release-candidate evidence artifact",
    )
    url = codec.require_string(artifact.get("archive_download_url"), field="artifact.archive_download_url")
    archive = adapter.download_artifact(url)
    resource_limits.validate_downloaded_size(
        archive,
        resource="release-candidate evidence artifact",
    )
    archive_digest = codec.sha256_bytes(archive)
    if archive_digest != artifact_digest or len(archive) != artifact_size:
        raise ValueError("downloaded release-candidate artifact does not match provider metadata")
    return SelectedReleaseCandidateArtifact(
        check_run_id=check_id,
        check_suite_id=suite_id,
        workflow_run_id=run_id,
        workflow_run_attempt=run_attempt,
        job_id=job_id,
        artifact_id=artifact_id,
        artifact_name=artifact_name,
        artifact_digest=artifact_digest,
        artifact_size_bytes=artifact_size,
        archive_sha256=archive_digest,
        archive_size_bytes=len(archive),
        archive_bytes=archive,
        publication_workflow_path=publication.workflow_path,
        publication_run_id=publication.run_id,
        publication_run_attempt=publication.run_attempt,
        paired_publication_runs=tuple(publication.paired_publication_runs),
        publication_cutoff=publication.canonical_cutoff,
        publication_pair_sha256=publication.publication_pair_sha256,
    )


def _workflow_path(run: dict[str, Any], *, field: str) -> str:
    return codec.require_string(run.get("path"), field=field).split("@", 1)[0]


def _workflow_run_created_at(run: dict[str, Any]) -> datetime:
    """Return the provider creation time that orders immutable dispatches."""

    created = _timestamp(run.get("created_at"), field="workflow_run.created_at")
    updated = _timestamp(run.get("updated_at"), field="workflow_run.updated_at")
    if created > updated:
        raise ValueError("release-candidate workflow timestamps are inconsistent")
    return created


def _validate_run(
    run: dict[str, Any],
    *,
    repository: str,
    commit_sha: str,
    run_id: int,
    release: str,
    listed_created: datetime,
    listed_updated: datetime,
    listed_attempt: int,
    publication_created: datetime,
) -> tuple[int, int]:
    expected = {
        "id": run_id,
        "name": policy.workflow_run_name(release=release, commit_sha=commit_sha),
        "event": "workflow_dispatch",
        "head_sha": commit_sha,
        "head_branch": "master",
        "status": "completed",
        "conclusion": "success",
    }
    if any(run.get(key) != value for key, value in expected.items()):
        raise ValueError("latest release-candidate workflow run identity or result is invalid")
    repo = _mapping(run.get("repository"), "workflow_run.repository")
    if repo.get("full_name") != repository:
        raise ValueError("release-candidate workflow repository is invalid")
    if _workflow_path(run, field="workflow_run.path") != policy.WORKFLOW_PATH:
        raise ValueError("release-candidate workflow path is invalid")
    created = _timestamp(run.get("created_at"), field="workflow_run.created_at")
    updated = _timestamp(run.get("updated_at"), field="workflow_run.updated_at")
    run_attempt = _positive_int(run.get("run_attempt"), "workflow_run.run_attempt")
    if (created, updated, run_attempt) != (listed_created, listed_updated, listed_attempt):
        raise ValueError("latest release-candidate workflow list/detail identity is inconsistent")
    if created > updated:
        raise ValueError("release-candidate workflow timestamps are inconsistent")
    if created > publication_created:
        raise ValueError("latest release-candidate workflow started after publication")
    if updated >= publication_created:
        raise ValueError("release-candidate workflow completed at or after publication started")
    return run_attempt, _positive_int(run.get("check_suite_id"), "workflow_run.check_suite_id")


def _check_run_id(check_run_url: str, *, repository: str) -> int:
    prefix = f"https://api.github.com/repos/{repository}/check-runs/"
    if not check_run_url.startswith(prefix):
        raise ValueError("terminal evidence job check URL is invalid")
    raw_id = check_run_url.removeprefix(prefix)
    if not raw_id.isascii() or not raw_id.isdecimal():
        raise ValueError("terminal evidence job check URL is invalid")
    return _positive_int(int(raw_id), "check_run.id")


def _validate_check(
    check: dict[str, Any],
    *,
    commit_sha: str,
    check_id: int,
    check_suite_id: int,
    publication_created: datetime,
) -> None:
    expected = {
        "id": check_id,
        "name": policy.JOB_NAME,
        "head_sha": commit_sha,
        "status": "completed",
        "conclusion": "success",
    }
    if any(check.get(key) != value for key, value in expected.items()):
        raise ValueError("terminal evidence check identity or result is invalid")
    if _timestamp(check.get("completed_at"), field="check_run.completed_at") >= publication_created:
        raise ValueError("release-candidate evidence check completed at or after publication started")
    app = _mapping(check.get("app"), "check_run.app")
    if app.get("id") != policy.APP_ID or app.get("slug") != policy.APP_SLUG:
        raise ValueError("terminal evidence check has an untrusted producer")
    suite = _mapping(check.get("check_suite"), "check_run.check_suite")
    if _positive_int(suite.get("id"), "check_run.check_suite.id") != check_suite_id:
        raise ValueError("terminal evidence check suite does not match the latest workflow run")


def _validate_job(
    job: dict[str, Any],
    *,
    repository: str,
    commit_sha: str,
    run_id: int,
    run_attempt: int,
    check_id: int,
) -> int:
    job_id = _positive_int(job.get("id"), "job.id")
    expected = {
        "name": policy.JOB_NAME,
        "head_sha": commit_sha,
        "run_id": run_id,
        "status": "completed",
        "conclusion": "success",
        "check_run_url": f"https://api.github.com/repos/{repository}/check-runs/{check_id}",
    }
    if any(job.get(key) != value for key, value in expected.items()):
        raise ValueError("release-candidate job identity or result is invalid")
    return job_id


def _validate_subject(*, repository: str, commit_sha: str, release: str) -> None:
    if codec.REPOSITORY.fullmatch(repository) is None:
        raise ValueError("repository must use owner/name form")
    if codec.FULL_SHA.fullmatch(commit_sha) is None:
        raise ValueError("commit SHA must be full lowercase hexadecimal")
    if codec.RELEASE.fullmatch(release) is None:
        raise ValueError("release must use canonical stable form vX.Y.Z")
