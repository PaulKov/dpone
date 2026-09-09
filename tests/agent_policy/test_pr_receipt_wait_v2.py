"""Focused state matrix for #607 bounded exact-head receipt polling."""

from tools.agent_policy.pr_receipt_wait import Observation, wait_for_exact_head


def test_pending_then_ready_uses_bounded_backoff() -> None:
    observations = iter((Observation.pending("checks pending"), Observation.ready()))
    sleeps: list[float] = []

    outcome = wait_for_exact_head(
        expected_head="a" * 40,
        fetch_head=lambda: "a" * 40,
        observe=lambda: next(observations),
        now=iter((0.0, 0.0, 0.0, 5.0, 5.0)).__next__,
        sleep=sleeps.append,
        timeout_seconds=30,
        initial_backoff_seconds=5,
        max_backoff_seconds=10,
    )

    assert (outcome.state, outcome.attempts, sleeps) == ("READY", 2, [5])


def test_head_change_and_timeout_are_not_passes() -> None:
    stale = wait_for_exact_head(
        expected_head="a" * 40,
        fetch_head=lambda: "b" * 40,
        observe=Observation.ready,
        now=lambda: 0.0,
        sleep=lambda _: None,
        timeout_seconds=30,
        initial_backoff_seconds=5,
        max_backoff_seconds=10,
    )
    clock = iter((0.0, 0.0, 0.0, 5.0, 5.0, 10.0)).__next__
    timeout = wait_for_exact_head(
        expected_head="a" * 40,
        fetch_head=lambda: "a" * 40,
        observe=lambda: Observation.pending("artifact pending"),
        now=clock,
        sleep=lambda _: None,
        timeout_seconds=10,
        initial_backoff_seconds=5,
        max_backoff_seconds=10,
    )

    assert (stale.state, stale.attempts) == ("STALE_HEAD", 0)
    assert (timeout.state, timeout.attempts) == ("TIMEOUT", 2)
