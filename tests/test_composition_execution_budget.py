"""Deadlines close admission, never cancel work or renew cleanup authority."""

from threading import Event

import pytest

from dpone.runtime.composition_execution_budget import CompositionExecutionBudget, ExecutionBudgetExpired


@pytest.mark.parametrize("timeout", [0, 901, True, 1.0, None])
def test_execution_timeout_is_integer_1_to_900(timeout):
    with pytest.raises(ValueError):
        CompositionExecutionBudget(timeout, stop_event=Event())


def test_one_absolute_execution_budget_and_nonrenewable_cleanup():
    now = [10.0]
    budget = CompositionExecutionBudget(2, stop_event=Event(), clock=lambda: now[0])
    assert budget.execution_deadline == 12
    assert budget.remaining_execution() == 2
    now[0] = 12
    assert budget.execution_expired
    with pytest.raises(ExecutionBudgetExpired):
        budget.require_effect()
    assert budget.begin_cleanup() == 72
    now[0] = 20
    assert budget.begin_cleanup() == 72 and budget.remaining_cleanup() == 52
    now[0] = 72
    with pytest.raises(ExecutionBudgetExpired):
        budget.remaining_cleanup()


def test_stop_and_explicit_cleanup_block_all_new_effects():
    stop = Event()
    budget = CompositionExecutionBudget(900, stop_event=stop, clock=lambda: 10)
    stop.set()
    with pytest.raises(ExecutionBudgetExpired):
        budget.remaining_execution()
    assert budget.begin_cleanup() == 70
    stop.clear()
    with pytest.raises(ExecutionBudgetExpired):
        budget.require_effect()


def test_cleanup_does_not_start_implicitly_from_remaining_query():
    budget = CompositionExecutionBudget(1, stop_event=Event())
    with pytest.raises(ExecutionBudgetExpired):
        budget.remaining_cleanup()


def test_late_unwind_cannot_extend_cleanup_past_execution_plus_sixty():
    now = [10.0]
    budget = CompositionExecutionBudget(2, stop_event=Event(), clock=lambda: now[0])
    now[0] = 100
    assert budget.begin_cleanup() == 72
    with pytest.raises(ExecutionBudgetExpired):
        budget.remaining_cleanup()


def test_explicit_stop_records_earlier_cleanup_bound():
    now = [10.0]
    budget = CompositionExecutionBudget(900, stop_event=Event(), clock=lambda: now[0])
    now[0] = 15
    budget.stop()
    now[0] = 40
    assert budget.begin_cleanup() == 75


def test_shared_signal_timestamp_is_first_publication_and_bounds_late_observation():
    from dpone.runtime.composition_execution_budget import ExecutionStopSignal

    now = [10.0]
    signal = ExecutionStopSignal(clock=lambda: now[0])
    budget = CompositionExecutionBudget(900, stop_event=signal, clock=lambda: now[0])
    now[0] = 15
    signal.notify()
    now[0] = 30
    signal.notify()
    now[0] = 80
    with pytest.raises(ExecutionBudgetExpired):
        budget.require_effect()
    assert signal.stopped_at == 15 and budget.begin_cleanup() == 75
    with pytest.raises(ExecutionBudgetExpired):
        budget.remaining_cleanup()


def test_io_deadline_tracks_explicit_phase_without_renewal_or_effect_authority():
    now = [10.0]
    budget = CompositionExecutionBudget(2, stop_event=Event(), clock=lambda: now[0])
    assert budget.io_deadline() == 12
    now[0] = 12
    with pytest.raises(ExecutionBudgetExpired):
        budget.io_deadline()
    budget.begin_cleanup()
    assert budget.io_deadline() == 72
    with pytest.raises(ExecutionBudgetExpired):
        budget.require_effect()
    now[0] = 72
    with pytest.raises(ExecutionBudgetExpired):
        budget.io_deadline()


def test_io_after_signal_uses_first_stop_deadline_without_opening_effects():
    from dpone.runtime.composition_execution_budget import ExecutionStopSignal

    now = [10.0]
    signal = ExecutionStopSignal(clock=lambda: now[0])
    budget = CompositionExecutionBudget(900, stop_event=signal, clock=lambda: now[0])
    now[0] = 15
    signal.notify()
    now[0] = 25
    assert budget.io_deadline() == 75
    with pytest.raises(ExecutionBudgetExpired):
        budget.require_effect()
