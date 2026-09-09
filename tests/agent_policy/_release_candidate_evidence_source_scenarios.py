from __future__ import annotations

from typing import Any

from tests.agent_policy._release_candidate_evidence_helpers import (
    ARTIFACT_ID,
    CHECK_RUN_ID,
    CHECK_SUITE_ID,
    COMMIT_SHA,
    JOB_ID,
    RELEASE,
    REPOSITORY,
    RUN_ATTEMPT,
    RUN_ID,
    digest,
    load_module,
    policy,
)
from tests.agent_policy._release_candidate_evidence_provider_helpers import (
    FakeReleaseEvidenceAdapter,
)

source = load_module(
    "dpone_release_candidate_evidence_source_test",
    "tools/agent_policy/release_candidate_evidence_source.py",
)
RAW_ARCHIVE = b"provider-bound archive"
CANDIDATE_CREATED_AT = "2026-08-13T09:59:00Z"
COMPLETED_AT = "2026-08-13T10:00:00Z"
PUBLICATION_CREATED_AT = "2026-08-13T10:01:00Z"
PUBLICATION_UPDATED_AT = "2026-08-13T10:01:30Z"
PUBLICATION_RUN_ID = 901
PUBLICATION_RUN_ATTEMPT = 3
PUBLICATION_WORKFLOW_PATH = ".github/workflows/release.yml"
PARTNER_PUBLICATION_RUN_ID = 902
PARTNER_PUBLICATION_WORKFLOW_PATH = ".github/workflows/runtime-image.yml"


def download_url(artifact_id: int) -> str:
    return f"https://api.github.test/repos/{REPOSITORY}/actions/artifacts/{artifact_id}/zip"


def check_payload(
    check_id: int = CHECK_RUN_ID,
    *,
    conclusion: str | None = "success",
    status: str = "completed",
    suite_id: int = CHECK_SUITE_ID,
) -> dict[str, Any]:
    return {
        "id": check_id,
        "name": policy.JOB_NAME,
        "head_sha": COMMIT_SHA,
        "status": status,
        "conclusion": conclusion,
        "completed_at": COMPLETED_AT,
        "app": {"id": policy.APP_ID, "slug": policy.APP_SLUG},
        "check_suite": {"id": suite_id},
    }


def candidate_run(
    run_id: int = RUN_ID,
    *,
    run_attempt: int = RUN_ATTEMPT,
    suite_id: int = CHECK_SUITE_ID,
    created_at: str = CANDIDATE_CREATED_AT,
    updated_at: str = COMPLETED_AT,
    status: str = "completed",
    conclusion: str | None = "success",
) -> dict[str, Any]:
    return {
        "id": run_id,
        "name": policy.workflow_run_name(release=RELEASE, commit_sha=COMMIT_SHA),
        "event": "workflow_dispatch",
        "head_sha": COMMIT_SHA,
        "head_branch": "master",
        "status": status,
        "conclusion": conclusion,
        "check_suite_id": suite_id,
        "run_attempt": run_attempt,
        "repository": {"full_name": REPOSITORY},
        "path": f"{policy.WORKFLOW_PATH}@refs/heads/master",
        "created_at": created_at,
        "updated_at": updated_at,
    }


def publication_run(
    *,
    run_id: int = PUBLICATION_RUN_ID,
    workflow_path: str = PUBLICATION_WORKFLOW_PATH,
    created_at: str = PUBLICATION_CREATED_AT,
    updated_at: str = PUBLICATION_UPDATED_AT,
) -> dict[str, Any]:
    return {
        "id": run_id,
        "event": "push",
        "head_sha": COMMIT_SHA,
        "head_branch": RELEASE,
        "status": "in_progress",
        "conclusion": None,
        "run_attempt": PUBLICATION_RUN_ATTEMPT,
        "repository": {"full_name": REPOSITORY},
        "path": f"{workflow_path}@refs/tags/{RELEASE}",
        "created_at": created_at,
        "updated_at": updated_at,
    }


def job_payload(
    job_id: object = JOB_ID,
    *,
    check_id: int = CHECK_RUN_ID,
    run_id: int = RUN_ID,
    run_attempt: int = RUN_ATTEMPT,
) -> dict[str, Any]:
    return {
        "id": job_id,
        "name": policy.JOB_NAME,
        "head_sha": COMMIT_SHA,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "status": "completed",
        "conclusion": "success",
        "check_run_url": f"https://api.github.com/repos/{REPOSITORY}/check-runs/{check_id}",
    }


def artifact_payload(
    artifact_id: int = ARTIFACT_ID,
    *,
    raw: bytes = RAW_ARCHIVE,
    run_id: int = RUN_ID,
    run_attempt: int = RUN_ATTEMPT,
    expired: bool = False,
) -> dict[str, Any]:
    return {
        "id": artifact_id,
        "name": policy.artifact_name(COMMIT_SHA, run_id, run_attempt),
        "expired": expired,
        "created_at": COMPLETED_AT,
        "workflow_run": {"id": run_id, "head_sha": COMMIT_SHA},
        "digest": digest(raw),
        "size_in_bytes": len(raw),
        "archive_download_url": download_url(artifact_id),
    }


def adapter(*, raw: bytes = RAW_ARCHIVE) -> FakeReleaseEvidenceAdapter:
    candidate = candidate_run()
    publication = publication_run()
    partner_publication = publication_run(
        run_id=PARTNER_PUBLICATION_RUN_ID,
        workflow_path=PARTNER_PUBLICATION_WORKFLOW_PATH,
    )
    return FakeReleaseEvidenceAdapter(
        check_runs=[check_payload()],
        workflow_runs=[dict(candidate)],
        publication_workflow_runs={
            PUBLICATION_WORKFLOW_PATH: [dict(publication)],
            PARTNER_PUBLICATION_WORKFLOW_PATH: [dict(partner_publication)],
        },
        runs={PUBLICATION_RUN_ID: publication, RUN_ID: candidate},
        jobs={(RUN_ID, RUN_ATTEMPT): [job_payload()]},
        artifacts={RUN_ID: [artifact_payload(raw=raw)]},
        downloads={download_url(ARTIFACT_ID): raw},
    )


def add_older_pass(
    provider: FakeReleaseEvidenceAdapter,
    *,
    run_id: int = RUN_ID - 1,
    created_at: str = "2026-08-13T09:58:00Z",
) -> None:
    attempt = 1
    suite_id = CHECK_SUITE_ID - 1
    check_id = CHECK_RUN_ID - 1
    job_id = JOB_ID - 1
    artifact_id = ARTIFACT_ID - 1
    run = candidate_run(
        run_id,
        run_attempt=attempt,
        suite_id=suite_id,
        created_at=created_at,
    )
    provider.workflow_runs.append(run)
    provider.runs[run_id] = run
    provider.check_runs.append(check_payload(check_id, suite_id=suite_id))
    provider.jobs[(run_id, attempt)] = [job_payload(job_id, check_id=check_id, run_id=run_id, run_attempt=attempt)]
    provider.artifacts[run_id] = [artifact_payload(artifact_id, run_id=run_id, run_attempt=attempt)]
    provider.downloads[download_url(artifact_id)] = RAW_ARCHIVE


def select(provider: FakeReleaseEvidenceAdapter) -> Any:
    return source.select_release_candidate_artifact(
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
        release=RELEASE,
        publication_workflow_path=PUBLICATION_WORKFLOW_PATH,
        publication_run_id=PUBLICATION_RUN_ID,
        publication_run_attempt=PUBLICATION_RUN_ATTEMPT,
        adapter=provider,
    )
