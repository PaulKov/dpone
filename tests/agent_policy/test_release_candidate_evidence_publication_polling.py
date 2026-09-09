from __future__ import annotations

import copy
import inspect
from collections.abc import Callable
from typing import Any

import pytest
from tests.agent_policy._release_candidate_evidence_helpers import (
    COMMIT_SHA,
    RELEASE,
    REPOSITORY,
)
from tests.agent_policy._release_candidate_evidence_provider_helpers import (
    FakeReleaseEvidenceAdapter,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    PARTNER_PUBLICATION_RUN_ID,
    PARTNER_PUBLICATION_WORKFLOW_PATH,
    PUBLICATION_RUN_ATTEMPT,
    PUBLICATION_RUN_ID,
    PUBLICATION_WORKFLOW_PATH,
    source,
)
from tests.agent_policy._release_candidate_evidence_source_scenarios import (
    adapter as _adapter,
)

github = source.github
POLL_SECONDS = 1.0


class SequencedPublicationAdapter(FakeReleaseEvidenceAdapter):
    """Return one scripted publication-list observation per global sweep."""

    def __init__(
        self,
        base: FakeReleaseEvidenceAdapter,
        *,
        responses: dict[str, list[list[dict[str, Any]]]],
    ) -> None:
        super().__init__(
            check_runs=base.check_runs,
            workflow_runs=base.workflow_runs,
            publication_workflow_runs=base.publication_workflow_runs,
            runs=base.runs,
            jobs=base.jobs,
            artifacts=base.artifacts,
            downloads=base.downloads,
        )
        self._responses = copy.deepcopy(responses)
        self._response_indexes = {path: 0 for path in responses}

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
        observations = self._responses[workflow_path]
        index = self._response_indexes[workflow_path]
        self._response_indexes[workflow_path] = index + 1
        return copy.deepcopy(observations[min(index, len(observations) - 1)])


def _responses(
    base: FakeReleaseEvidenceAdapter,
) -> dict[str, list[list[dict[str, Any]]]]:
    return {
        path: [[copy.deepcopy(base.publication_workflow_runs[path][0])]]
        for path in (PUBLICATION_WORKFLOW_PATH, PARTNER_PUBLICATION_WORKFLOW_PATH)
    }


def _fetch(
    adapter: FakeReleaseEvidenceAdapter,
    *,
    discovery_attempts: int | None = 3,
    sleeper: Callable[[float], None],
) -> Any:
    discovery = (
        {}
        if discovery_attempts is None
        else {
            "discovery_attempts": discovery_attempts,
            "discovery_poll_seconds": POLL_SECONDS,
        }
    )
    return github.fetch_publication_boundary(
        adapter=adapter,
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
        release=RELEASE,
        workflow_path=PUBLICATION_WORKFLOW_PATH,
        run_id=PUBLICATION_RUN_ID,
        run_attempt=PUBLICATION_RUN_ATTEMPT,
        sleeper=sleeper,
        **discovery,
    )


def test_publication_discovery_defaults_are_bounded_and_explicit() -> None:
    parameters = inspect.signature(github.fetch_publication_boundary).parameters

    assert parameters["discovery_attempts"].default == 31
    assert parameters["discovery_poll_seconds"].default == 10.0


def test_empty_lists_are_polled_in_global_sweeps_until_both_runs_appear() -> None:
    base = _adapter()
    responses = _responses(base)
    release_run = responses[PUBLICATION_WORKFLOW_PATH][0]
    partner_run = responses[PARTNER_PUBLICATION_WORKFLOW_PATH][0]
    partner_run[0]["created_at"] = "2026-08-13T10:00:00Z"
    partner_run[0]["updated_at"] = "2026-08-13T10:00:30Z"
    responses[PUBLICATION_WORKFLOW_PATH] = [[], release_run]
    responses[PARTNER_PUBLICATION_WORKFLOW_PATH] = [[], [], partner_run]
    adapter = SequencedPublicationAdapter(base, responses=responses)
    sleeps: list[float] = []

    boundary = _fetch(adapter, sleeper=sleeps.append)

    assert boundary.canonical_cutoff == "2026-08-13T10:00:00Z"
    assert boundary.paired_publication_runs == [
        {
            "workflow_path": PUBLICATION_WORKFLOW_PATH,
            "run_id": PUBLICATION_RUN_ID,
            "run_attempt": PUBLICATION_RUN_ATTEMPT,
            "created_at": "2026-08-13T10:01:00Z",
        },
        {
            "workflow_path": PARTNER_PUBLICATION_WORKFLOW_PATH,
            "run_id": PARTNER_PUBLICATION_RUN_ID,
            "run_attempt": PUBLICATION_RUN_ATTEMPT,
            "created_at": "2026-08-13T10:00:00Z",
        },
    ]
    assert boundary.publication_pair_sha256 == github.codec.canonical_json_sha256(boundary.paired_publication_runs)
    assert adapter.requested_runs == [PUBLICATION_RUN_ID] * 3
    assert adapter.requested_publication_workflows == [
        path for _sweep in range(3) for path in (PUBLICATION_WORKFLOW_PATH, PARTNER_PUBLICATION_WORKFLOW_PATH)
    ]
    assert sleeps == [POLL_SECONDS, POLL_SECONDS]


def test_empty_partner_exhausts_31_observations_over_the_300_second_bound() -> None:
    base = _adapter()
    responses = _responses(base)
    responses[PARTNER_PUBLICATION_WORKFLOW_PATH] = [[]]
    adapter = SequencedPublicationAdapter(base, responses=responses)
    sleeps: list[float] = []

    with pytest.raises(ValueError, match="publication workflow.*unavailable|bounded.*poll"):
        _fetch(adapter, discovery_attempts=None, sleeper=sleeps.append)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID] * 31
    assert adapter.requested_publication_workflows.count(PUBLICATION_WORKFLOW_PATH) == 31
    assert adapter.requested_publication_workflows.count(PARTNER_PUBLICATION_WORKFLOW_PATH) == 31
    assert sleeps == [10.0] * 30
    assert adapter.downloaded_urls == []


def test_multiple_listed_runs_fail_on_first_observation_without_sleep() -> None:
    base = _adapter()
    responses = _responses(base)
    partner = responses[PARTNER_PUBLICATION_WORKFLOW_PATH][0][0]
    duplicate = copy.deepcopy(partner)
    duplicate["id"] += 1
    responses[PARTNER_PUBLICATION_WORKFLOW_PATH] = [[partner, duplicate]]
    adapter = SequencedPublicationAdapter(base, responses=responses)
    sleeps: list[float] = []

    with pytest.raises(ValueError, match="exactly one exact tag-push run"):
        _fetch(adapter, sleeper=sleeps.append)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID]
    assert adapter.requested_publication_workflows == [
        PUBLICATION_WORKFLOW_PATH,
        PARTNER_PUBLICATION_WORKFLOW_PATH,
    ]
    assert sleeps == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event", "workflow_dispatch"),
        ("head_sha", "b" * 40),
        ("path", ".github/workflows/attacker.yml@refs/tags/v0.74.0"),
        ("repository", {"full_name": "attacker/fork"}),
    ],
)
def test_nonempty_identity_mismatch_never_waits_for_later_valid_response(
    field: str,
    value: object,
) -> None:
    base = _adapter()
    responses = _responses(base)
    valid = responses[PARTNER_PUBLICATION_WORKFLOW_PATH][0]
    invalid = copy.deepcopy(valid)
    invalid[0][field] = value
    responses[PARTNER_PUBLICATION_WORKFLOW_PATH] = [invalid, valid]
    adapter = SequencedPublicationAdapter(base, responses=responses)
    sleeps: list[float] = []

    with pytest.raises(ValueError, match="exactly one exact tag-push run|identity"):
        _fetch(adapter, sleeper=sleeps.append)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID]
    assert adapter.requested_publication_workflows.count(PARTNER_PUBLICATION_WORKFLOW_PATH) == 1
    assert sleeps == []


def test_other_tag_run_is_ignored_until_requested_tag_run_appears() -> None:
    base = _adapter()
    responses = _responses(base)
    valid = responses[PARTNER_PUBLICATION_WORKFLOW_PATH][0]
    other_tag = copy.deepcopy(valid)
    other_tag[0]["head_branch"] = "v0.73.0"
    responses[PARTNER_PUBLICATION_WORKFLOW_PATH] = [other_tag, valid]
    adapter = SequencedPublicationAdapter(base, responses=responses)
    sleeps: list[float] = []

    boundary = _fetch(adapter, sleeper=sleeps.append)

    assert boundary.run_id == PUBLICATION_RUN_ID
    assert adapter.requested_runs == [PUBLICATION_RUN_ID, PUBLICATION_RUN_ID]
    assert adapter.requested_publication_workflows == [
        path for _sweep in range(2) for path in (PUBLICATION_WORKFLOW_PATH, PARTNER_PUBLICATION_WORKFLOW_PATH)
    ]
    assert sleeps == [POLL_SECONDS]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("id", 0, "positive integer"),
        ("created_at", "not-a-timestamp", "timestamp"),
        ("updated_at", "2026-08-13T09:59:00Z", "timestamps are inconsistent"),
    ],
)
def test_nonempty_malformed_run_fails_immediately(
    field: str,
    value: object,
    message: str,
) -> None:
    base = _adapter()
    responses = _responses(base)
    valid = responses[PARTNER_PUBLICATION_WORKFLOW_PATH][0]
    invalid = copy.deepcopy(valid)
    invalid[0][field] = value
    responses[PARTNER_PUBLICATION_WORKFLOW_PATH] = [invalid, valid]
    adapter = SequencedPublicationAdapter(base, responses=responses)
    sleeps: list[float] = []

    with pytest.raises(ValueError, match=message):
        _fetch(adapter, sleeper=sleeps.append)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID]
    assert adapter.requested_publication_workflows.count(PARTNER_PUBLICATION_WORKFLOW_PATH) == 1
    assert sleeps == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_attempt", PUBLICATION_RUN_ATTEMPT + 1),
        ("updated_at", "2026-08-13T10:01:45Z"),
    ],
)
def test_current_run_mutable_list_projection_drift_is_polled_until_consistent(
    field: str,
    value: object,
) -> None:
    base = _adapter()
    responses = _responses(base)
    current = responses[PUBLICATION_WORKFLOW_PATH][0]
    stale = copy.deepcopy(current)
    stale[0][field] = value
    responses[PUBLICATION_WORKFLOW_PATH] = [stale, current]
    adapter = SequencedPublicationAdapter(base, responses=responses)
    sleeps: list[float] = []

    boundary = _fetch(adapter, sleeper=sleeps.append)

    assert boundary.run_attempt == PUBLICATION_RUN_ATTEMPT
    assert adapter.requested_runs == [PUBLICATION_RUN_ID, PUBLICATION_RUN_ID]
    assert sleeps == [POLL_SECONDS]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_attempt", PUBLICATION_RUN_ATTEMPT + 1),
        ("updated_at", "2026-08-13T10:01:45Z"),
    ],
)
def test_persistent_current_run_mutable_list_projection_drift_fails_after_bound(
    field: str,
    value: object,
) -> None:
    base = _adapter()
    responses = _responses(base)
    responses[PUBLICATION_WORKFLOW_PATH][0][0][field] = value
    adapter = SequencedPublicationAdapter(base, responses=responses)
    sleeps: list[float] = []

    with pytest.raises(ValueError, match="bounded poll.*mutable detail/list projection"):
        _fetch(adapter, discovery_attempts=3, sleeper=sleeps.append)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID] * 3
    assert adapter.requested_publication_workflows == [
        path for _sweep in range(3) for path in (PUBLICATION_WORKFLOW_PATH, PARTNER_PUBLICATION_WORKFLOW_PATH)
    ]
    assert sleeps == [POLL_SECONDS, POLL_SECONDS]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", PUBLICATION_RUN_ID + 1),
        ("created_at", "2026-08-13T10:00:59Z"),
    ],
)
def test_current_run_immutable_list_detail_drift_fails_immediately(
    field: str,
    value: object,
) -> None:
    base = _adapter()
    responses = _responses(base)
    responses[PUBLICATION_WORKFLOW_PATH][0][0][field] = value
    adapter = SequencedPublicationAdapter(base, responses=responses)
    sleeps: list[float] = []

    with pytest.raises(ValueError, match="immutable detail/list identity is inconsistent"):
        _fetch(adapter, sleeper=sleeps.append)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID]
    assert sleeps == []


def test_current_run_is_reauthenticated_on_every_global_sweep() -> None:
    base = _adapter()
    responses = _responses(base)
    responses[PARTNER_PUBLICATION_WORKFLOW_PATH] = [
        [],
        responses[PARTNER_PUBLICATION_WORKFLOW_PATH][0],
    ]
    adapter = SequencedPublicationAdapter(base, responses=responses)
    sleeps: list[float] = []

    def complete_current_run(seconds: float) -> None:
        sleeps.append(seconds)
        adapter.runs[PUBLICATION_RUN_ID]["status"] = "completed"

    with pytest.raises(ValueError, match="status is not current in_progress"):
        _fetch(adapter, sleeper=complete_current_run)

    assert adapter.requested_runs == [PUBLICATION_RUN_ID, PUBLICATION_RUN_ID]
    assert adapter.requested_publication_workflows.count(PARTNER_PUBLICATION_WORKFLOW_PATH) == 1
    assert sleeps == [POLL_SECONDS]
