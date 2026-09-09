from __future__ import annotations

from typing import Any
from unittest.mock import Mock, call

import pytest
from tests.agent_policy._release_commit_gate_helpers import (
    COMMIT_SHA,
    CONTEXT,
    GITHUB_ACTIONS_APP_ID,
    REPO,
    RULESET_ID,
    assert_codes,
    check,
    check_payload,
    codes,
    evaluate,
    observation,
    release_gate,
    requirement,
    ruleset,
    snapshot,
)


def test_fetches_both_sources_but_serializes_only_provider_check_runs(monkeypatch: Any) -> None:
    ruleset_payload = ruleset("Build")
    paginated = Mock(
        side_effect=[
            [check_payload(name="Build", evidence_id=11)],
            [{"id": 12, "context": "Build", "sha": COMMIT_SHA, "state": "failure"}],
        ]
    )
    current = Mock(return_value=ruleset_payload)
    monkeypatch.setattr(release_gate.github_api, "github_json", current)
    monkeypatch.setattr(release_gate.github_api, "github_paginated_items", paginated)
    live_snapshot = release_gate.fetch_live_snapshot(
        repo=REPO,
        commit_sha=COMMIT_SHA,
        ruleset_id=RULESET_ID,
        token="approved-token",
    )
    current.assert_called_once_with(
        f"repos/{REPO}/rulesets/{RULESET_ID}",
        token="approved-token",
        fresh=True,
    )
    assert paginated.call_args_list == [
        call(
            f"repos/PaulKov/dpone/commits/{COMMIT_SHA}/check-runs?filter=latest",
            token="approved-token",
            array_key="check_runs",
        ),
        call(f"repos/PaulKov/dpone/commits/{COMMIT_SHA}/statuses", token="approved-token"),
    ]
    assert live_snapshot.required_contexts == (requirement("Build", GITHUB_ACTIONS_APP_ID),)
    assert live_snapshot.observations[0].integration_id == GITHUB_ACTIONS_APP_ID
    report = evaluate(live_snapshot)
    assert report.status == "PASS"
    assert {item["source"] for item in report.contexts[0]["current_observations"]} == {"check_run"}
    assert report.contexts[0]["observed_count"] == 1


@pytest.mark.parametrize("state", ["in_progress", "failure", "cancelled", "skipped", "neutral", "stale"])
def test_non_success_states_fail_closed(state: str) -> None:
    report = evaluate(snapshot(observations=(observation(state=state),)))
    suffix = {"in_progress": "PENDING", "failure": "FAILED"}.get(state, state.upper())
    assert report.status == "FAIL"
    assert report.to_payload()["decision"] == "NO-GO"
    assert codes(report) == [f"REQUIRED_CONTEXT_{suffix}"]


@pytest.mark.parametrize(
    ("live_snapshot", "code"),
    [
        (
            snapshot(required_contexts=(requirement("Missing"),), observations=(observation(context="Other"),)),
            "REQUIRED_CONTEXT_MISSING",
        ),
        (snapshot(required_contexts=()), "REQUIRED_CONTEXTS_UNAVAILABLE"),
    ],
)
def test_invalid_snapshot_shapes_fail_closed(live_snapshot: Any, code: str) -> None:
    assert_codes(live_snapshot, code)


@pytest.mark.parametrize(("older_state", "newer_state"), [("success", "failure"), ("failure", "success")])
def test_commit_statuses_are_diagnostic_only(older_state: str, newer_state: str) -> None:
    report = evaluate(
        snapshot(
            observations=(
                observation(source="status", state=older_state, evidence_id=10),
                observation(source="status", state=newer_state, evidence_id=11),
                observation(evidence_id=12),
            )
        )
    )
    assert codes(report) == []
    assert report.contexts[0]["observed_count"] == 1
    assert report.contexts[0]["current_observations"] == [dict(observation(evidence_id=12)._asdict())]


@pytest.mark.parametrize("newer_suite_id", [100, 200])
def test_same_app_success_cannot_hide_unsuccessful_check_run(newer_suite_id: int) -> None:
    assert_codes(
        snapshot(
            observations=(
                check(evidence_id=10, suite_id=100, conclusion="failure"),
                check(evidence_id=11, suite_id=newer_suite_id, conclusion="success"),
            ),
        ),
        "REQUIRED_CONTEXT_FAILED",
    )


@pytest.mark.parametrize(
    ("normalizer", "payload"),
    [
        (release_gate._check_run, check_payload(sha="b" * 40)),
        (
            release_gate._commit_status,
            {"id": 2, "context": "Quality checks (3.12)", "sha": "b" * 40, "state": "success"},
        ),
    ],
)
def test_evidence_payload_must_match_the_requested_commit(normalizer: Any, payload: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="commit"):
        normalizer(payload, COMMIT_SHA)


@pytest.mark.parametrize(
    ("app_id", "spoofed", "code"),
    [
        (GITHUB_ACTIONS_APP_ID, observation(evidence_id=11, integration_id=99999), "PRODUCER_MISMATCH"),
        (GITHUB_ACTIONS_APP_ID, observation(source="status", evidence_id=12), "PRODUCER_MISMATCH"),
        (None, observation(source="status", evidence_id=13), "PRODUCER_UNBOUND"),
        (None, observation(evidence_id=14, integration_id=99999), "PRODUCER_UNBOUND"),
    ],
)
def test_synthetic_evidence_cannot_satisfy_required_producer(
    app_id: int | None,
    spoofed: Any,
    code: str,
) -> None:
    assert_codes(
        snapshot(required_contexts=(requirement(app_id=app_id),), observations=(spoofed,)),
        f"REQUIRED_CONTEXT_{code}",
    )


def test_unexpected_live_context_is_ruleset_policy_drift() -> None:
    live_snapshot = snapshot(
        required_contexts=(requirement(), requirement("Build")),
        observations=(observation(), observation(context="Build", evidence_id=2)),
    )
    report = evaluate(live_snapshot, policy_contexts=(CONTEXT,))
    assert codes(report) == ["RULESET_POLICY_DRIFT"]
    assert "unexpected: Build" in report.blockers[0].message
