from __future__ import annotations

import json
from unittest.mock import Mock

import pytest
from tests.agent_policy._release_commit_gate_helpers import (
    codes,
    observation,
    poll,
    release_gate,
    requirement,
    snapshot,
)


def test_unbound_required_context_is_a_terminal_poll_result() -> None:
    fetcher = Mock(
        return_value=snapshot(
            required_contexts=(requirement(app_id=None),),
        )
    )
    sleeper = Mock()
    report = poll(fetcher, sleeper, lambda: 0.0, timeout=60.0)
    assert codes(report) == ["REQUIRED_CONTEXT_PRODUCER_UNBOUND"]
    assert report.attempts == 1
    fetcher.assert_called_once()
    sleeper.assert_not_called()


def test_bounded_polling_retries_pending_evidence_until_success() -> None:
    fetcher = Mock(
        side_effect=[
            snapshot(observations=(observation(state="in_progress"),)),
            snapshot(),
        ]
    )
    sleeps: list[float] = []
    now = [0.0]

    def sleeper(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    report = poll(fetcher, sleeper, lambda: now[0], timeout=5.0)
    assert report.status == "PASS"
    assert report.attempts == 2
    assert sleeps == [2.0]
    assert release_gate._poll_attempt_count(1e308, 5e-324) == release_gate.MAX_POLL_ATTEMPTS


@pytest.mark.parametrize(
    ("failure", "expected_code"),
    [
        (RuntimeError("request failed with approved-token"), "GITHUB_API_UNAVAILABLE"),
        (AttributeError("malformed approved-token payload"), "LIVE_EVIDENCE_INVALID"),
    ],
)
def test_fetch_failure_is_bounded_and_does_not_leak_credentials(
    failure: Exception,
    expected_code: str,
) -> None:
    fetcher = Mock(side_effect=failure)
    now = [0.0]

    def sleeper(seconds: float) -> None:
        now[0] += seconds

    report = poll(fetcher, sleeper, lambda: now[0], timeout=4.0)
    rendered = json.dumps(report.to_payload(), sort_keys=True)
    assert fetcher.call_count == 3
    assert report.attempts == 3
    assert codes(report) == [expected_code]
    assert "approved-token" not in rendered
