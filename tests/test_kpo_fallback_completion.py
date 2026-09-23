"""Synthetic pod-state regressions for degraded live-log completion polling."""

from __future__ import annotations

from types import SimpleNamespace as NS
from typing import Any

import pytest
from dpone_airflow_pack.pod_completion import await_base_container_completion


def _container(name: str, exit_code: int | None = None) -> Any:
    terminated = None if exit_code is None else NS(exit_code=exit_code, message="private-response")
    return NS(name=name, state=NS(terminated=terminated))


def _pod(*, phase: str = "Pending", init_exit: int | None = None, base_exit: int | None = None) -> Any:
    return NS(
        spec=NS(restart_policy="Never", init_containers=[]),
        status=NS(
            phase=phase,
            message="private-response",
            init_container_statuses=[_container("private-init-name", init_exit)],
            container_statuses=[_container("main", base_exit)],
        ),
    )


def _poll(pods: list[Any], *, sleep: Any = None) -> tuple[Any, list[float], list[Any]]:
    operator = NS(base_container_name="main", base_container_status_polling_interval=2, remote_pod=None)
    reads: list[Any] = []
    sleeps: list[float] = []
    iterator = iter(pods)

    def read_pod(*, pod: Any) -> Any:
        reads.append(pod)
        return next(iterator)

    await_base_container_completion(operator, pod=pods[0], read_pod=read_pod, sleep=sleep or sleeps.append)
    return operator, sleeps, reads


@pytest.mark.parametrize("phase", ["Pending", "Failed"])
@pytest.mark.parametrize("after_running", [False, True])
def test_failed_init_stops_at_entry_or_during_poll(phase: str, after_running: bool) -> None:
    failed = _pod(phase=phase, init_exit=7)
    snapshots = ([_pod(phase="Running")] if after_running else []) + [failed]
    operator = NS(base_container_name="main", base_container_status_polling_interval=2, remote_pod=None)
    sleeps: list[float] = []
    iterator = iter(snapshots)

    with pytest.raises(RuntimeError, match="DPONE_KPO_INIT_CONTAINER_FAILED") as raised:
        await_base_container_completion(
            operator, pod=object(), read_pod=lambda **kwargs: next(iterator), sleep=sleeps.append
        )

    assert operator.remote_pod is failed
    assert sleeps == ([2] if after_running else [])
    assert "exit_code=7" in str(raised.value)
    assert "private" not in str(raised.value)
    assert len(str(raised.value)) < 200


@pytest.mark.parametrize("phase", ["Failed", "Succeeded"])
def test_terminal_pod_with_waiting_base_does_not_loop(phase: str) -> None:
    with pytest.raises(RuntimeError, match="DPONE_KPO_POD_TERMINATED"):
        _poll([_pod(phase=phase)])


@pytest.mark.parametrize("base_exit", [0, 9])
def test_running_pod_and_sidecar_preserve_base_completion(base_exit: int) -> None:
    snapshots = [_pod(), _pod(phase="Running"), _pod(phase="Running", base_exit=base_exit)]
    operator, sleeps, reads = _poll(snapshots)

    assert operator.remote_pod is snapshots[-1]
    assert sleeps == [2, 2]
    assert reads == [snapshots[0]] * 3


def test_successful_init_and_base_complete_without_sleep() -> None:
    operator, sleeps, _ = _poll([_pod(phase="Succeeded", init_exit=0, base_exit=0)])
    assert operator.remote_pod.status.phase == "Succeeded"
    assert sleeps == []


def test_missing_container_statuses_continue_polling() -> None:
    incomplete = NS(status=NS(phase="Pending", container_statuses=None, init_container_statuses=None))
    _, sleeps, _ = _poll([incomplete, _pod(phase="Running", base_exit=0)])
    assert sleeps == [2]


def test_status_read_error_propagates_unchanged() -> None:
    error = RuntimeError("status read unavailable")

    def read_pod(**kwargs: Any) -> Any:
        raise error

    with pytest.raises(RuntimeError) as raised:
        await_base_container_completion(NS(), pod=object(), read_pod=read_pod, sleep=lambda _: None)
    assert raised.value is error


def test_cancellation_propagates_without_another_read() -> None:
    error = KeyboardInterrupt()

    def cancel(_: float) -> None:
        raise error

    with pytest.raises(KeyboardInterrupt) as raised:
        _poll([_pod()], sleep=cancel)
    assert raised.value is error


@pytest.mark.parametrize("restart_policy", ["OnFailure", "Always"])
def test_retryable_init_termination_does_not_end_running_pod(restart_policy: str) -> None:
    retrying = _pod(init_exit=7)
    retrying.spec.restart_policy = restart_policy
    _, sleeps, _ = _poll([retrying, _pod(phase="Running", init_exit=0, base_exit=0)])
    assert sleeps == [2]


def test_restartable_init_sidecar_failure_does_not_end_poll() -> None:
    retrying = _pod(init_exit=7)
    retrying.spec.init_containers = [NS(name="private-init-name", restart_policy="Always")]
    _, sleeps, _ = _poll([retrying, _pod(phase="Running", base_exit=0)])
    assert sleeps == [2]


def test_historical_init_failure_does_not_end_poll() -> None:
    ready = _pod(phase="Running", init_exit=0)
    ready.status.init_container_statuses[0].last_state = NS(terminated=NS(exit_code=7))
    _, sleeps, _ = _poll([ready, _pod(phase="Succeeded", base_exit=0)])
    assert sleeps == [2]


def test_failed_base_exit_still_delegates_final_outcome_to_provider() -> None:
    _, sleeps, _ = _poll([_pod(phase="Failed", base_exit=9)])
    assert sleeps == []


@pytest.mark.parametrize("exit_code", ["private-response", 10**100, True, None])
def test_init_diagnostic_rejects_unbounded_or_untyped_exit_codes(exit_code: Any) -> None:
    failed = _pod(phase="Failed", init_exit=7)
    failed.status.init_container_statuses[0].state.terminated.exit_code = exit_code
    with pytest.raises(RuntimeError, match="exit_code=unknown"):
        _poll([failed])
