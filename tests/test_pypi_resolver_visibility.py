from __future__ import annotations

import subprocess

import pytest
from tools.pypi_resolver_visibility import resolver_command, wait_for_resolver


def _result(returncode: int, output: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], returncode, stdout=output, stderr="")


def test_resolver_command_matches_runtime_pip_surface() -> None:
    command = resolver_command("0.87.3")

    assert command[-1] == "dpone[full,accel]==0.87.3"
    assert "--dry-run" in command
    assert "--ignore-installed" in command
    assert "--no-cache-dir" in command
    assert "https://pypi.org/simple" in command


def test_wait_for_resolver_passes_on_first_attempt() -> None:
    calls: list[tuple[str, ...]] = []

    receipt = wait_for_resolver(
        "0.87.3",
        attempts=3,
        interval_seconds=0,
        runner=lambda command, _timeout: calls.append(tuple(command)) or _result(0),
        sleeper=lambda _seconds: pytest.fail("sleep must not run after success"),
    )

    assert receipt["status"] == "PASS"
    assert receipt["attempt"] == 1
    assert len(calls) == 1


def test_wait_for_resolver_retries_then_passes() -> None:
    results = iter((_result(1, "not visible"), _result(0)))
    sleeps: list[float] = []

    receipt = wait_for_resolver(
        "0.87.3",
        attempts=3,
        interval_seconds=7,
        runner=lambda _command, _timeout: next(results),
        sleeper=sleeps.append,
    )

    assert receipt["status"] == "PASS"
    assert receipt["attempt"] == 2
    assert sleeps == [7]


def test_wait_for_resolver_fails_closed_after_bound() -> None:
    receipt = wait_for_resolver(
        "0.87.3",
        attempts=3,
        interval_seconds=0,
        runner=lambda _command, _timeout: _result(1, "missing native accel"),
        sleeper=lambda _seconds: None,
    )

    assert receipt["status"] == "FAIL"
    assert receipt["attempt"] == 3
    assert receipt["last_output"] == "missing native accel"


@pytest.mark.parametrize("attempts, interval", [(0, 0), (61, 0), (1, -1), (1, 301)])
def test_wait_for_resolver_rejects_unbounded_inputs(attempts: int, interval: int) -> None:
    with pytest.raises(ValueError):
        wait_for_resolver("0.87.3", attempts=attempts, interval_seconds=interval)
