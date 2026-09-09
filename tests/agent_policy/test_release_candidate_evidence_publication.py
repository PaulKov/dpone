from __future__ import annotations

import pytest
from tests.agent_policy._release_candidate_evidence_helpers import (
    COMMIT_SHA,
    RELEASE,
    REPOSITORY,
    RUN_ID,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    PARTNER_PUBLICATION_WORKFLOW_PATH,
    PUBLICATION_CREATED_AT,
    PUBLICATION_RUN_ATTEMPT,
    PUBLICATION_RUN_ID,
    PUBLICATION_WORKFLOW_PATH,
    source,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    adapter as _adapter,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    publication_run as _publication_run,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    select as _select,
)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("id", PUBLICATION_RUN_ID + 1, "identity"),
        ("event", "workflow_dispatch", "identity"),
        ("head_sha", "b" * 40, "identity"),
        ("head_branch", "v0.73.0", "identity"),
        ("run_attempt", PUBLICATION_RUN_ATTEMPT + 1, "identity"),
        ("status", "queued", "status"),
        ("status", "completed", "status"),
        ("path", ".github/workflows/attacker.yml@refs/tags/v0.74.0", "path"),
    ],
)
def test_rejects_publication_run_identity_drift(
    field: str,
    value: object,
    message: str,
) -> None:
    adapter = _adapter()
    adapter.runs[PUBLICATION_RUN_ID][field] = value

    with pytest.raises(ValueError, match=f"publication workflow.*{message}"):
        _select(adapter)


def test_rejects_publication_repository_drift() -> None:
    adapter = _adapter()
    adapter.runs[PUBLICATION_RUN_ID]["repository"] = {"full_name": "attacker/fork"}

    with pytest.raises(ValueError, match="publication workflow repository"):
        _select(adapter)


def test_rejects_unapproved_publication_workflow_before_candidate_selection() -> None:
    adapter = _adapter()

    with pytest.raises(ValueError, match="workflow path is not approved"):
        source.select_release_candidate_artifact(
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            publication_workflow_path=".github/workflows/attacker.yml",
            publication_run_id=PUBLICATION_RUN_ID,
            publication_run_attempt=PUBLICATION_RUN_ATTEMPT,
            adapter=adapter,
        )

    assert adapter.requested_runs == []


@pytest.mark.parametrize("created_at", ["not-a-timestamp", "2026-08-13T10:01:00"])
def test_publication_created_at_must_be_timezone_aware_iso8601(created_at: str) -> None:
    adapter = _adapter()
    adapter.runs[PUBLICATION_RUN_ID]["created_at"] = created_at

    with pytest.raises(ValueError, match="publication_workflow_run.created_at.*timestamp|timezone"):
        _select(adapter)


@pytest.mark.parametrize("updated_at", ["not-a-timestamp", "2026-08-13T10:01:30"])
def test_publication_updated_at_must_be_timezone_aware_iso8601(updated_at: str) -> None:
    adapter = _adapter()
    adapter.runs[PUBLICATION_RUN_ID]["updated_at"] = updated_at

    with pytest.raises(ValueError, match="publication_workflow_run.updated_at.*timestamp|timezone"):
        _select(adapter)


def test_publication_created_at_cannot_follow_provider_updated_at() -> None:
    adapter = _adapter()
    adapter.runs[PUBLICATION_RUN_ID]["updated_at"] = "2026-08-13T10:00:59Z"

    with pytest.raises(ValueError, match="publication workflow run timestamps are inconsistent"):
        _select(adapter)


def test_cross_workflow_earliest_publication_cutoff_blocks_later_evidence() -> None:
    adapter = _adapter()
    release_trigger = adapter.publication_workflow_runs[PUBLICATION_WORKFLOW_PATH][0]
    release_trigger["created_at"] = "2026-08-13T10:00:00Z"
    release_trigger["updated_at"] = "2026-08-13T10:00:30Z"
    current_runtime = _publication_run(
        run_id=PUBLICATION_RUN_ID,
        workflow_path=PARTNER_PUBLICATION_WORKFLOW_PATH,
        created_at="2026-08-13T10:02:00Z",
        updated_at="2026-08-13T10:02:30Z",
    )
    adapter.publication_workflow_runs[PARTNER_PUBLICATION_WORKFLOW_PATH] = [dict(current_runtime)]
    adapter.runs[PUBLICATION_RUN_ID] = current_runtime
    adapter.runs[RUN_ID]["updated_at"] = "2026-08-13T10:01:00Z"
    adapter.workflow_runs[0]["updated_at"] = "2026-08-13T10:01:00Z"

    with pytest.raises(ValueError, match="workflow completed at or after publication"):
        source.select_release_candidate_artifact(
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            publication_workflow_path=PARTNER_PUBLICATION_WORKFLOW_PATH,
            publication_run_id=PUBLICATION_RUN_ID,
            publication_run_attempt=PUBLICATION_RUN_ATTEMPT,
            adapter=adapter,
        )

    assert adapter.downloaded_urls == []


def test_ambiguous_partner_publication_workflow_blocks() -> None:
    adapter = _adapter()
    partner = adapter.publication_workflow_runs[PARTNER_PUBLICATION_WORKFLOW_PATH][0]
    adapter.publication_workflow_runs[PARTNER_PUBLICATION_WORKFLOW_PATH] = [dict(partner), dict(partner)]

    with pytest.raises(ValueError, match="must resolve to exactly one exact tag-push run"):
        _select(adapter)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event", "workflow_dispatch"),
        ("head_sha", "b" * 40),
        ("path", ".github/workflows/attacker.yml@refs/tags/v0.74.0"),
        ("repository", {"full_name": "attacker/fork"}),
    ],
)
def test_partner_publication_workflow_identity_drift_blocks(
    field: str,
    value: object,
) -> None:
    adapter = _adapter()
    adapter.publication_workflow_runs[PARTNER_PUBLICATION_WORKFLOW_PATH][0][field] = value

    with pytest.raises(ValueError, match="publication workflow.*identity is invalid"):
        _select(adapter)


@pytest.mark.parametrize(
    ("surface", "message"),
    [
        ("started", "no pre-publication dispatch"),
        ("completed", "workflow completed at or after publication"),
        ("check", "check completed at or after publication"),
        ("artifact", "artifact was created at or after publication"),
    ],
)
def test_evidence_after_provider_publication_start_is_never_authoritative(
    surface: str,
    message: str,
) -> None:
    adapter = _adapter()
    after_publication = "2026-08-13T10:02:00Z"
    if surface == "started":
        adapter.runs[RUN_ID]["created_at"] = after_publication
        adapter.runs[RUN_ID]["updated_at"] = after_publication
        adapter.workflow_runs[0]["created_at"] = after_publication
        adapter.workflow_runs[0]["updated_at"] = after_publication
    elif surface == "completed":
        adapter.runs[RUN_ID]["updated_at"] = after_publication
        adapter.workflow_runs[0]["updated_at"] = after_publication
    elif surface == "check":
        adapter.check_runs[0]["completed_at"] = after_publication
    else:
        adapter.artifacts[RUN_ID][0]["created_at"] = after_publication

    with pytest.raises(ValueError, match=message):
        _select(adapter)


@pytest.mark.parametrize(
    ("surface", "message"),
    [
        ("completed", "workflow completed at or after publication"),
        ("check", "check completed at or after publication"),
        ("artifact", "artifact was created at or after publication"),
    ],
)
def test_timestamp_equal_to_provider_publication_boundary_fails_closed(
    surface: str,
    message: str,
) -> None:
    adapter = _adapter()
    if surface == "completed":
        adapter.runs[RUN_ID]["updated_at"] = PUBLICATION_CREATED_AT
        adapter.workflow_runs[0]["updated_at"] = PUBLICATION_CREATED_AT
    elif surface == "check":
        adapter.check_runs[0]["completed_at"] = PUBLICATION_CREATED_AT
    else:
        adapter.artifacts[RUN_ID][0]["created_at"] = PUBLICATION_CREATED_AT

    with pytest.raises(ValueError, match=message):
        _select(adapter)
