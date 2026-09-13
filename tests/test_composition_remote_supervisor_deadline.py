"""Host phase bounds use deterministic RPC doubles, not live host certification."""

from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest

from dpone.adapters import composition_clickhouse_supervisor as supervisor
from dpone.app import composition_dispatcher_capture_custody as custody_module
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.runtime.composition_execution_budget import CompositionExecutionBudget, ExecutionBudgetExpired
from tests.test_composition_dispatcher_capture_custody import configured


def remote(monkeypatch, *, phase):
    now = [100.0]
    calls = []
    monkeypatch.setattr(supervisor.time, "monotonic", lambda: now[0])
    client = SimpleNamespace(capture=lambda ref, *, deadline: calls.append(deadline) or {"facts": "original"})
    monkeypatch.setattr(supervisor, "CaptureSupervisorFactsClient", lambda *a, **k: client)
    instance = supervisor.RemoteClickHouseLocalSupervisor(
        enrollment_sha256="sha256:" + "a" * 64,
        socket_path=Path("/host.sock"),
        dispatcher_gid=101,
        absolute_deadline=phase,
    )
    return instance, now, calls, client


@pytest.mark.parametrize("phase,expected", [(105.0, 105.0), (120.0, 110.0)])
def test_remote_caps_outer_deadline(monkeypatch, phase, expected):
    instance, _, calls, _ = remote(monkeypatch, phase=lambda: phase)
    assert instance._capture(SimpleNamespace(enrollment_sha256="original"), 110.0) == {"facts": "original"}
    assert calls == [expected]


@pytest.mark.parametrize("phase", [100.0, float("inf"), float("nan"), True])
def test_remote_invalid_or_expired_phase_rejects_before_rpc(monkeypatch, phase):
    instance, _, calls, _ = remote(monkeypatch, phase=lambda: phase)
    with pytest.raises(CompositionAdmissionError):
        instance._capture(SimpleNamespace(enrollment_sha256="original"), 110.0)
    assert calls == []


def test_remote_rechecks_phase_after_rpc(monkeypatch):
    phase = [105.0]
    instance, now, calls, client = remote(monkeypatch, phase=lambda: phase[0])

    def capture(ref, *, deadline):
        calls.append(deadline)
        now[0] = 104.0
        phase[0] = 103.0
        return {}

    client.capture = capture
    with pytest.raises(CompositionAdmissionError):
        instance._capture(SimpleNamespace(enrollment_sha256="original"), 110.0)
    assert calls == [105.0]


def test_invalid_remote_callback_rejects_before_client(monkeypatch):
    monkeypatch.setattr(supervisor, "CaptureSupervisorFactsClient", lambda *a, **k: pytest.fail("client created"))
    with pytest.raises(CompositionAdmissionError):
        supervisor.RemoteClickHouseLocalSupervisor(
            enrollment_sha256="sha256:" + "a" * 64,
            socket_path=Path("/host.sock"),
            dispatcher_gid=101,
            absolute_deadline=True,
        )


def custody(monkeypatch, callback):
    now = [100.0]
    monkeypatch.setattr(custody_module.time, "monotonic", lambda: now[0])
    _, enrollment, calls, client = configured(monkeypatch)
    instance = custody_module.DispatcherCaptureCustody(
        enrollment=enrollment,
        socket_path="/host.sock",
        deadline=110.0,
        absolute_deadline=callback,
    )
    return instance, now, calls, client


def test_custody_expiry_requires_explicit_cleanup_and_keeps_one_pair_bound(monkeypatch):
    now = [100.0]
    budget = CompositionExecutionBudget(10, stop_event=Event(), clock=lambda: now[0])
    instance, host_now, calls, _ = custody(monkeypatch, budget.io_deadline)
    now[0] = host_now[0] = 111.0
    with pytest.raises(ExecutionBudgetExpired):
        instance.require_host()
    assert calls == []
    assert budget.begin_cleanup() == 170.0
    instance.require_host()
    assert [deadline for _, deadline in calls] == [121.0, 121.0]
    with pytest.raises(ExecutionBudgetExpired):
        budget.require_effect()
    now[0] = host_now[0] = 170.0
    with pytest.raises(ExecutionBudgetExpired):
        instance.require_host()
    assert len(calls) == 2


def test_custody_callback_cannot_extend_execution_plus_sixty(monkeypatch):
    instance, now, calls, _ = custody(monkeypatch, lambda: 1000.0)
    now[0] = 169.0
    instance.require_host()
    assert [deadline for _, deadline in calls] == [170.0, 170.0]
    now[0] = 170.0
    with pytest.raises(CompositionAdmissionError):
        instance.require_host()
    assert len(calls) == 2


@pytest.mark.parametrize("phase", [100.0, float("inf"), float("nan"), True])
def test_custody_invalid_phase_rejects_before_rpc(monkeypatch, phase):
    instance, _, calls, _ = custody(monkeypatch, lambda: phase)
    with pytest.raises(CompositionAdmissionError):
        instance.require_host()
    assert calls == []


def test_custody_rechecks_shortened_phase_after_rpc(monkeypatch):
    phase = [105.0]
    instance, now, calls, client = custody(monkeypatch, lambda: phase[0])
    original = client.capture

    def capture(ref, *, deadline):
        facts = original(ref, deadline=deadline)
        now[0], phase[0] = 104.0, 103.0
        return facts

    client.capture = capture
    with pytest.raises(CompositionAdmissionError):
        instance.require_host()
    assert len(calls) == 1


def test_invalid_custody_callback_rejects_before_client(monkeypatch):
    _, enrollment, _, _ = configured(monkeypatch)
    monkeypatch.setattr(custody_module, "CaptureSupervisorFactsClient", lambda *a, **k: pytest.fail("client created"))
    with pytest.raises(CompositionAdmissionError):
        custody_module.DispatcherCaptureCustody(
            enrollment=enrollment,
            socket_path="/host.sock",
            deadline=custody_module.time.monotonic() + 10,
            absolute_deadline=True,
        )
