from __future__ import annotations

from typing import Any

import pytest
from tests.agent_policy._release_candidate_evidence_helpers import (
    ARTIFACT_ID,
    CHECK_RUN_ID,
    CHECK_SUITE_ID,
    COMMIT_SHA,
    JOB_ID,
    RUN_ATTEMPT,
    RUN_ID,
    digest,
    policy,
)
from tests.agent_policy._release_candidate_evidence_provider_helpers import (
    FakeReleaseEvidenceAdapter,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    CANDIDATE_CREATED_AT,
    PARTNER_PUBLICATION_RUN_ID,
    PARTNER_PUBLICATION_WORKFLOW_PATH,
    PUBLICATION_CREATED_AT,
    PUBLICATION_RUN_ATTEMPT,
    PUBLICATION_RUN_ID,
    PUBLICATION_WORKFLOW_PATH,
    RAW_ARCHIVE,
    source,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    adapter as _adapter,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    add_older_pass as _add_older_pass,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    artifact_payload as _artifact,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    candidate_run as _candidate_run,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    job_payload as _job,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    select as _select,
)


def test_selects_exact_successful_provider_attempt_and_verifies_downloaded_bytes() -> None:
    selected = _select(_adapter())

    assert selected.check_run_id == CHECK_RUN_ID
    assert selected.check_suite_id == CHECK_SUITE_ID
    assert selected.workflow_run_id == RUN_ID
    assert selected.workflow_run_attempt == RUN_ATTEMPT
    assert selected.job_id == JOB_ID
    assert selected.job_id != selected.check_run_id
    assert selected.artifact_id == ARTIFACT_ID
    assert selected.artifact_name == policy.artifact_name(COMMIT_SHA, RUN_ID, RUN_ATTEMPT)
    assert selected.artifact_digest == digest(RAW_ARCHIVE)
    assert selected.archive_sha256 == digest(RAW_ARCHIVE)
    assert selected.archive_bytes == RAW_ARCHIVE
    assert selected.publication_workflow_path == PUBLICATION_WORKFLOW_PATH
    assert selected.publication_run_id == PUBLICATION_RUN_ID
    assert selected.publication_run_attempt == PUBLICATION_RUN_ATTEMPT
    assert selected.paired_publication_runs == (
        {
            "workflow_path": PUBLICATION_WORKFLOW_PATH,
            "run_id": PUBLICATION_RUN_ID,
            "run_attempt": PUBLICATION_RUN_ATTEMPT,
            "created_at": PUBLICATION_CREATED_AT,
        },
        {
            "workflow_path": PARTNER_PUBLICATION_WORKFLOW_PATH,
            "run_id": PARTNER_PUBLICATION_RUN_ID,
            "run_attempt": PUBLICATION_RUN_ATTEMPT,
            "created_at": PUBLICATION_CREATED_AT,
        },
    )
    assert selected.publication_cutoff == PUBLICATION_CREATED_AT
    assert selected.publication_pair_sha256 == source.codec.canonical_json_sha256(
        list(selected.paired_publication_runs)
    )


@pytest.mark.parametrize(
    ("latest_status", "latest_conclusion"),
    [("queued", None), ("completed", "failure"), ("completed", "cancelled")],
)
def test_newest_workflow_run_wins_and_never_falls_back_to_older_pass(
    latest_status: str,
    latest_conclusion: str | None,
) -> None:
    adapter = _adapter()
    _add_older_pass(adapter, run_id=RUN_ID + 100)
    adapter.runs[RUN_ID]["status"] = latest_status
    adapter.runs[RUN_ID]["conclusion"] = latest_conclusion
    adapter.workflow_runs.reverse()

    with pytest.raises(ValueError, match="latest.*run identity or result"):
        _select(adapter)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID, RUN_ID]
    assert adapter.downloaded_urls == []


def test_equal_provider_activity_is_ambiguous_and_never_uses_run_id_as_authority() -> None:
    adapter = _adapter()
    _add_older_pass(adapter, run_id=RUN_ID - 1, created_at=CANDIDATE_CREATED_AT)
    adapter.runs[RUN_ID]["conclusion"] = "failure"

    with pytest.raises(ValueError, match="ordering is ambiguous"):
        _select(adapter)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID]


def test_later_dispatch_wins_even_when_older_run_was_rerun_more_recently() -> None:
    adapter = _adapter()
    older_rerun_id = RUN_ID - 1
    older_rerun = _candidate_run(
        older_rerun_id,
        run_attempt=RUN_ATTEMPT + 1,
        created_at="2026-08-13T09:58:00Z",
        updated_at="2026-08-13T10:00:30Z",
    )
    adapter.workflow_runs.insert(0, dict(older_rerun))
    adapter.runs[older_rerun_id] = older_rerun
    adapter.runs[RUN_ID]["status"] = "queued"
    adapter.runs[RUN_ID]["conclusion"] = None

    with pytest.raises(ValueError, match="latest.*run identity or result"):
        _select(adapter)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID, RUN_ID]
    assert adapter.downloaded_urls == []


def test_post_cutoff_dispatch_is_ignored_before_latest_selection() -> None:
    adapter = _adapter()
    post_cutoff_id = RUN_ID + 1
    post_cutoff = _candidate_run(
        post_cutoff_id,
        created_at="2026-08-13T10:01:01Z",
        updated_at="2026-08-13T10:01:01Z",
        status="queued",
        conclusion=None,
    )
    adapter.workflow_runs.insert(0, dict(post_cutoff))
    adapter.runs[post_cutoff_id] = post_cutoff

    assert _select(adapter).workflow_run_id == RUN_ID
    assert adapter.requested_runs == [PUBLICATION_RUN_ID, RUN_ID]


@pytest.mark.parametrize(
    "created_at",
    ["2026-08-13T10:00:30Z", PUBLICATION_CREATED_AT],
)
def test_newer_queued_dispatch_at_or_before_cutoff_blocks_older_pass(
    created_at: str,
) -> None:
    adapter = _adapter()
    queued_id = RUN_ID + 1
    queued = _candidate_run(
        queued_id,
        created_at=created_at,
        updated_at=created_at,
        status="queued",
        conclusion=None,
    )
    adapter.workflow_runs.insert(0, dict(queued))
    adapter.runs[queued_id] = queued

    with pytest.raises(ValueError, match="latest.*run identity or result"):
        _select(adapter)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID, queued_id]
    assert adapter.downloaded_urls == []


def test_selected_pre_cutoff_dispatch_completed_after_cutoff_never_falls_back() -> None:
    adapter = _adapter()
    _add_older_pass(adapter)
    adapter.workflow_runs[0]["updated_at"] = "2026-08-13T10:01:01Z"
    adapter.runs[RUN_ID]["updated_at"] = "2026-08-13T10:01:01Z"

    with pytest.raises(ValueError, match="completed at or after publication"):
        _select(adapter)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID, RUN_ID]
    assert adapter.downloaded_urls == []


@pytest.mark.parametrize("missing_surface", ["job", "check"])
def test_newest_run_missing_terminal_binding_blocks_older_pass(missing_surface: str) -> None:
    adapter = _adapter()
    _add_older_pass(adapter)
    if missing_surface == "job":
        adapter.jobs[(RUN_ID, RUN_ATTEMPT)] = []
        message = "exactly one terminal evidence job"
    else:
        adapter.check_runs = [check for check in adapter.check_runs if check["id"] != CHECK_RUN_ID]
        message = "exactly one exact-SHA check"

    with pytest.raises(ValueError, match=message):
        _select(adapter)

    assert adapter.downloaded_urls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("created_at", "2026-08-13T09:58:59Z"),
        ("updated_at", "2026-08-13T09:59:59Z"),
        ("run_attempt", RUN_ATTEMPT + 1),
    ],
)
def test_rejects_candidate_workflow_list_detail_attempt_identity_drift(
    field: str,
    value: object,
) -> None:
    adapter = _adapter()
    adapter.runs[RUN_ID][field] = value

    with pytest.raises(ValueError, match="list/detail identity is inconsistent"):
        _select(adapter)

    assert adapter.downloaded_urls == []


def test_non_candidate_list_entries_do_not_participate_in_latest_selection() -> None:
    adapter = _adapter()
    wrong_sha = _candidate_run(RUN_ID + 1, created_at="2026-08-13T10:00:30Z")
    wrong_sha["head_sha"] = "b" * 40
    wrong_event = _candidate_run(RUN_ID + 2, created_at="2026-08-13T10:00:30Z")
    wrong_event["event"] = "push"
    adapter.workflow_runs = [wrong_sha, wrong_event, adapter.workflow_runs[0]]

    assert _select(adapter).workflow_run_id == RUN_ID


@pytest.mark.parametrize(
    ("field", "value"),
    [("id", 1), ("slug", "third-party")],
)
def test_rejects_non_github_actions_check_producer(field: str, value: object) -> None:
    adapter = _adapter()
    adapter.check_runs[0]["app"][field] = value

    with pytest.raises(ValueError, match="untrusted producer"):
        _select(adapter)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event", "push"),
        ("head_sha", "b" * 40),
        ("head_branch", "feature"),
        ("status", "in_progress"),
        ("conclusion", "failure"),
        ("path", ".github/workflows/attacker.yml@refs/heads/master"),
    ],
)
def test_rejects_latest_candidate_run_identity_or_result_drift(field: str, value: object) -> None:
    adapter = _adapter()
    adapter.runs[RUN_ID][field] = value

    with pytest.raises(ValueError, match="run identity|workflow path"):
        _select(adapter)


def test_rejects_candidate_repository_drift() -> None:
    adapter = _adapter()
    adapter.runs[RUN_ID]["repository"] = {"full_name": "attacker/fork"}

    with pytest.raises(ValueError, match="workflow repository"):
        _select(adapter)


def test_job_check_url_must_bind_the_terminal_check_in_latest_attempt() -> None:
    adapter = _adapter()
    adapter.jobs[(RUN_ID, RUN_ATTEMPT)] = [_job(check_id=CHECK_RUN_ID + 1)]

    with pytest.raises(ValueError, match="exactly one exact-SHA check"):
        _select(adapter)


def test_job_id_collision_cannot_replace_exact_check_run_url_binding() -> None:
    adapter = _adapter()
    adapter.jobs[(RUN_ID, RUN_ATTEMPT)] = [_job(job_id=CHECK_RUN_ID, check_id=CHECK_RUN_ID + 1)]

    with pytest.raises(ValueError, match="exactly one exact-SHA check"):
        _select(adapter)


def test_rejects_multiple_terminal_jobs_in_latest_attempt() -> None:
    adapter = _adapter()
    adapter.jobs[(RUN_ID, RUN_ATTEMPT)] = [_job(), _job(job_id=JOB_ID + 1)]

    with pytest.raises(ValueError, match="exactly one terminal evidence job"):
        _select(adapter)


@pytest.mark.parametrize("job_id", [True, 0, -1, "501"])
def test_actions_job_id_must_be_an_independent_positive_integer(job_id: object) -> None:
    adapter = _adapter()
    adapter.jobs[(RUN_ID, RUN_ATTEMPT)] = [_job(job_id=job_id)]

    with pytest.raises(ValueError, match="job.id.*positive integer"):
        _select(adapter)


@pytest.mark.parametrize("conclusion", ["failure", "cancelled", "skipped"])
def test_rejects_non_successful_terminal_job(conclusion: str) -> None:
    adapter = _adapter()
    adapter.jobs[(RUN_ID, RUN_ATTEMPT)][0]["conclusion"] = conclusion

    with pytest.raises(ValueError, match="job identity or result"):
        _select(adapter)


def test_only_current_attempt_artifact_name_is_authoritative() -> None:
    adapter = _adapter()
    adapter.artifacts[RUN_ID] = [_artifact(run_attempt=RUN_ATTEMPT - 1)]

    with pytest.raises(ValueError, match="current-attempt authority artifact"):
        _select(adapter)


@pytest.mark.parametrize("expired", [True, None])
def test_expired_or_unknown_expiry_is_unverified(expired: object) -> None:
    adapter = _adapter()
    adapter.artifacts[RUN_ID][0]["expired"] = expired

    with pytest.raises(ValueError, match="expired or expiry is unknown"):
        _select(adapter)


def test_rejects_multiple_current_attempt_artifacts() -> None:
    adapter = _adapter()
    adapter.artifacts[RUN_ID].append(_artifact(ARTIFACT_ID + 1))

    with pytest.raises(ValueError, match="exactly one current-attempt"):
        _select(adapter)


def test_rejects_artifact_workflow_identity_drift() -> None:
    adapter = _adapter()
    adapter.artifacts[RUN_ID][0]["workflow_run"]["head_sha"] = "b" * 40

    with pytest.raises(ValueError, match="artifact workflow identity"):
        _select(adapter)


@pytest.mark.parametrize("mismatch", ["digest", "size"])
def test_rejects_provider_digest_or_size_mismatch(mismatch: str) -> None:
    adapter = _adapter()
    if mismatch == "digest":
        adapter.artifacts[RUN_ID][0]["digest"] = "sha256:" + "0" * 64
    else:
        adapter.artifacts[RUN_ID][0]["size_in_bytes"] += 1

    with pytest.raises(ValueError, match="does not match provider metadata"):
        _select(adapter)


def test_provider_failure_is_fail_closed() -> None:
    class FailingAdapter(FakeReleaseEvidenceAdapter):
        def list_workflow_runs(
            self,
            repository: str,
            *,
            commit_sha: str,
        ) -> list[dict[str, Any]]:
            raise RuntimeError("provider unavailable")

    adapter = _adapter()
    failing = FailingAdapter(
        check_runs=[],
        publication_workflow_runs=adapter.publication_workflow_runs,
        runs=adapter.runs,
    )

    with pytest.raises(RuntimeError, match="provider unavailable"):
        _select(failing)
