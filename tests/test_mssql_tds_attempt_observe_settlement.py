"""Ordinary admission blocks pending settlement; journal teardown remains available.

These are denial-only guard tests on real journal actors. Injected pending state
is not a producer fixture and never establishes successful settlement authority.
"""

from time import monotonic
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from tests import test_mssql_tds_attempt as attempt_fixtures

setup = attempt_fixtures.setup
create = attempt_fixtures.create


@pytest.mark.parametrize("complete", [False, None, 0, 1])
def test_pending_or_invalid_completion_blocks_before_journal_call(setup, monkeypatch, complete):
    attempt = create(setup)
    attempt._observe_settlement = SimpleNamespace(complete=complete)
    calls = []
    execute = attempt._directory.execute

    def counted(*args, **kwargs):
        calls.append(args[0])
        return execute(*args, **kwargs)

    monkeypatch.setattr(attempt._directory, "execute", counted)
    try:
        with pytest.raises((WindowContractError, WindowOutcomeUnknown)):
            attempt.reserve_operation(UUID(int=55), TdsCoordinatorCommand.CREATE, "a" * 64, deadline=monotonic() + 2)
        assert calls == []
        assert attempt._poisoned
    finally:
        attempt.close(deadline=monotonic() + 2)
    assert setup[2].live_count == 0


def test_failed_pending_settlement_allows_explicit_original_journal_teardown(setup):
    attempt = create(setup)
    attempt._observe_settlement = SimpleNamespace(complete=False)
    attempt._poisoned = True
    attempt.close(deadline=monotonic() + 2)
    assert attempt._closed
    assert attempt._observe_settlement.complete is False
    assert setup[2].live_count == 0


def test_absent_original_owner_rejects_factory_before_actor_effect(setup, monkeypatch):
    attempt = create(setup)

    def forbidden(*args, **kwargs):
        raise AssertionError("factory must not consult actors before owner capture")

    monkeypatch.setattr(attempt._lifecycle, "assert_authority", forbidden)
    monkeypatch.setattr(attempt._directory, "execute", forbidden)
    try:
        with pytest.raises(WindowContractError, match="prepared_origin_missing"):
            attempt._prepared_observe_settlement(UUID(int=99), deadline=monotonic() + 2)
        assert attempt._observe_settlement is None
        assert not attempt._busy
    finally:
        attempt.close(deadline=monotonic() + 2)
