"""The P10a composition root samples one clock value and adds no effects."""

import pytest

from dpone.app import mssql_sqlclient_writer_admission_composition as composition


def test_composition_samples_clock_once_and_delegates_exact_values(monkeypatch):
    terminal = object()
    result = object()
    calls = []

    def admit(value, **values):
        calls.append((value, values))
        return result

    clock_calls = []

    def clock():
        clock_calls.append(1)
        return 10.0

    monkeypatch.setattr(composition, "admit_sqlclient_writer", admit)

    observed = composition.admit_mssql_sqlclient_writer(
        terminal,
        startup_deadline=11.0,
        operation_deadline=12.0,
        termination_timeout_seconds=3,
        max_worker_address_space_bytes=8 << 30,
        clock=clock,
    )

    assert observed is result
    assert clock_calls == [1]
    assert calls == [
        (
            terminal,
            {
                "now": 10.0,
                "startup_deadline": 11.0,
                "operation_deadline": 12.0,
                "termination_timeout_seconds": 3,
                "max_worker_address_space_bytes": 8 << 30,
            },
        )
    ]


def test_clock_failure_never_calls_admission(monkeypatch):
    calls = []
    monkeypatch.setattr(composition, "admit_sqlclient_writer", lambda *args, **kwargs: calls.append(1))

    def clock():
        raise RuntimeError("clock unavailable")

    with pytest.raises(RuntimeError, match="clock unavailable"):
        composition.admit_mssql_sqlclient_writer(
            object(),
            startup_deadline=11.0,
            operation_deadline=12.0,
            termination_timeout_seconds=3,
            max_worker_address_space_bytes=8 << 30,
            clock=clock,
        )

    assert calls == []
