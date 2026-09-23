"""Caught same-owner callback reentry cannot authorize later journal effects."""

from time import monotonic
from uuid import UUID

import pytest

from dpone.contracts.bounded_window import WindowContractError
from dpone.ports.mssql_tds_directory import (
    AssertDirectoryAuthority,
    AuthorizeDirectoryRetirement,
    ReserveDirectoryOperation,
    SealDirectoryWork,
)
from dpone.services.mssql_tds_attempt import TdsAttemptUnknown
from tests.test_mssql_tds_attempt import create, recover, saved_contained
from tests.test_mssql_tds_attempt import setup as setup


def swallowed_reentry(attempt, deadline):
    with pytest.raises(WindowContractError, match="reentrant"):
        attempt.close(deadline=deadline)


@pytest.mark.parametrize("boundary", ["parent", "directory", "mutation"])
def test_caught_reentry_blocks_success_and_future_effects(setup, monkeypatch, boundary):
    attempt = create(setup)
    commands = []
    execute = attempt._directory.execute
    authority = attempt._lifecycle.assert_authority

    def asserted(*, deadline):
        authority(deadline=deadline)
        if boundary == "parent":
            swallowed_reentry(attempt, deadline)

    def executed(command, *, deadline):
        commands.append(type(command))
        result = execute(command, deadline=deadline)
        if (boundary == "directory" and type(command) is AssertDirectoryAuthority) or (
            boundary == "mutation" and type(command) is SealDirectoryWork
        ):
            swallowed_reentry(attempt, deadline)
        return result

    monkeypatch.setattr(attempt._lifecycle, "assert_authority", asserted)
    monkeypatch.setattr(attempt._directory, "execute", executed)
    with pytest.raises(TdsAttemptUnknown):
        attempt.seal_work(deadline=monotonic() + 2)
    expected = [] if boundary == "parent" else [AssertDirectoryAuthority]
    if boundary == "mutation":
        expected.append(SealDirectoryWork)
    assert commands == expected
    with pytest.raises(TdsAttemptUnknown):
        attempt.close_admission(deadline=monotonic() + 2)
    assert commands == expected
    attempt.close(deadline=monotonic() + 2)
    assert setup[2].live_count == 0


@pytest.mark.parametrize("boundary", ["seal", "advance", "reauthority", "authorize", "reserve"])
def test_retirement_stops_after_caught_reentry_at_each_mutation(setup, monkeypatch, boundary):
    attempt = recover(setup, saved_contained(setup))
    events = []
    execute = attempt._directory.execute
    advance = attempt._lifecycle.advance
    authority = attempt._lifecycle.assert_authority
    authority_count = 0

    def asserted(*, deadline):
        nonlocal authority_count
        authority_count += 1
        authority(deadline=deadline)
        if authority_count == 2:
            events.append("reauthority")
            if boundary == "reauthority":
                swallowed_reentry(attempt, deadline)

    def advanced(*args, deadline, **kwargs):
        result = advance(*args, deadline=deadline, **kwargs)
        events.append("advance")
        if boundary == "advance":
            swallowed_reentry(attempt, deadline)
        return result

    def executed(command, *, deadline):
        result = execute(command, deadline=deadline)
        label = {
            SealDirectoryWork: "seal",
            AuthorizeDirectoryRetirement: "authorize",
            ReserveDirectoryOperation: "reserve",
        }.get(type(command))
        if label is not None:
            events.append(label)
            if boundary == label:
                swallowed_reentry(attempt, deadline)
        return result

    monkeypatch.setattr(attempt._directory, "execute", executed)
    monkeypatch.setattr(attempt._lifecycle, "advance", advanced)
    monkeypatch.setattr(attempt._lifecycle, "assert_authority", asserted)
    with pytest.raises(TdsAttemptUnknown):
        attempt.reserve_retirement(UUID(int=91), "b" * 64, deadline=monotonic() + 2)
    sequence = ["seal", "advance", "reauthority", "authorize", "reserve"]
    assert events == sequence[: sequence.index(boundary) + 1]
    with pytest.raises(TdsAttemptUnknown):
        attempt.reserve_retirement(UUID(int=92), "b" * 64, deadline=monotonic() + 2)
    assert events == sequence[: sequence.index(boundary) + 1]
    attempt.close(deadline=monotonic() + 2)
    assert setup[2].live_count == 0
