"""Exact observation values and actual POSIX supervision regressions."""

import sys
import time

import pytest

from dpone.adapters.target_acceptance.reader import supervise
from dpone.contracts.quality_replay import TargetAcceptanceError


@pytest.mark.parametrize("mode", ["hang", "trickle", "frame_without_eos"])
def test_real_uncooperative_worker_is_bounded_and_revoked(mode):
    started = time.monotonic()
    with pytest.raises(TargetAcceptanceError) as caught:
        supervise(
            [sys.executable, "tests/target_acceptance_fixtures/worker.py", mode],
            b"{}",
            deadline=started + 0.15,
        )
    assert time.monotonic() - started < 3
    assert caught.value.quiescent
    assert caught.value.output_revoked
    assert caught.value.remote_termination == "UNVERIFIED"


@pytest.mark.parametrize("mode", ["duplicate", "oversize", "bad_exit"])
def test_real_worker_invalid_delivery_never_accepted(mode):
    with pytest.raises(TargetAcceptanceError):
        supervise(
            [sys.executable, "tests/target_acceptance_fixtures/worker.py", mode], b"{}", deadline=time.monotonic() + 2
        )


def test_real_worker_clean_frame():
    assert supervise(
        [sys.executable, "tests/target_acceptance_fixtures/worker.py", "clean"], b"{}", deadline=time.monotonic() + 2
    ) == {"ok": True}


def test_uncertain_reap_is_explicit_and_output_is_revoked(monkeypatch):
    from dpone.adapters.target_acceptance import reader as supervisor

    real_stop = supervisor._stop

    def uncertain(process):
        assert real_stop(process)
        return False

    monkeypatch.setattr(supervisor, "_stop", uncertain)
    with pytest.raises(TargetAcceptanceError) as caught:
        supervise(
            [sys.executable, "tests/target_acceptance_fixtures/worker.py", "hang"],
            b"{}",
            deadline=time.monotonic() + 0.1,
        )
    assert not caught.value.quiescent
    assert caught.value.output_revoked


def test_cancellation_propagates_after_real_child_reaped(monkeypatch):
    from dpone.adapters.target_acceptance import reader as supervisor

    def cancel(*args):
        raise KeyboardInterrupt()

    monkeypatch.setattr(supervisor, "_exchange", cancel)
    with pytest.raises(KeyboardInterrupt) as caught:
        supervise(
            [sys.executable, "tests/target_acceptance_fixtures/worker.py", "hang"], b"{}", deadline=time.monotonic() + 2
        )
    assert caught.value.quiescent
    assert caught.value.output_revoked


def test_delayed_spawn_never_accepts_late_result(monkeypatch):
    from dpone.adapters.target_acceptance import reader as supervisor

    real_spawn = supervisor.subprocess.Popen

    def delayed(*args, **kwargs):
        child = real_spawn(*args, **kwargs)
        time.sleep(0.08)
        return child

    monkeypatch.setattr(supervisor.subprocess, "Popen", delayed)
    with pytest.raises(TargetAcceptanceError) as caught:
        supervise(
            [sys.executable, "tests/target_acceptance_fixtures/worker.py", "clean"],
            b"{}",
            deadline=time.monotonic() + 0.03,
        )
    assert caught.value.quiescent
