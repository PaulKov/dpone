"""Fresh CREATE reservation owns exactly one private departure sequence."""

from dataclasses import replace
from time import monotonic
from uuid import UUID

import pytest

from dpone.app.mssql_tds_attempt_composition import create_tds_attempt
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.ports.mssql_tds_directory import AssertDirectoryAuthority
from dpone.services.mssql_tds_attempt import TdsAttempt, TdsAttemptUnknown
from tests.test_mssql_tds_attempt import LIMITS, OWNER, PARENT
from tests.test_mssql_tds_attempt import setup as setup


def reserved(setup, backend="mssql_sqlclient"):
    _, lease, pool, factory = setup
    attempt = create_tds_attempt(
        pool,
        factory,
        PARENT,
        LIMITS,
        lease,
        supervisor_token=OWNER.supervisor_id,
        backend=backend,
        deadline=monotonic() + 2,
    )
    snapshot = attempt.reserve_operation(
        UUID(int=87),
        TdsCoordinatorCommand.CREATE,
        "a" * 64,
        deadline=monotonic() + 2,
    )
    slot = snapshot.state.slots[-1]
    identity = TdsCoordinatorIdentity(
        PARENT,
        slot.index,
        slot.operation_id,
        slot.command,
        slot.command_sha256,
        slot.owner_fence,
        "b" * 64,
    )
    return attempt, identity


def test_fresh_sequence_preserves_originals_and_cannot_repeat(setup):
    attempt, identity = reserved(setup)
    before = attempt.lifecycle, attempt.directory
    helper = UUID(int=99)
    deadline = monotonic() + 2
    with attempt._create_departure_sequence(helper, identity, deadline=deadline) as retained:
        assert retained[:2] == before
        assert isinstance(retained[2], TdsAttemptUnknown)
        assert attempt._assert_create_departure(helper, deadline=deadline) == before
    assert (attempt.lifecycle, attempt.directory) == before
    with pytest.raises(WindowContractError):
        with attempt._create_departure_sequence(UUID(int=100), identity, deadline=deadline):
            pytest.fail("repeated execution")
    attempt.close(deadline=deadline)


@pytest.mark.parametrize("origin", ["legacy", "schema1"])
def test_no_implicit_freshness_from_empty_state(setup, origin):
    attempt, identity = reserved(setup, "mssql_python" if origin == "schema1" else "mssql_sqlclient")
    if origin == "legacy":
        attempt = TdsAttempt(attempt._lifecycle, attempt._directory)
    with pytest.raises((ValueError, WindowContractError)):
        with attempt._create_departure_sequence(UUID(int=99), identity, deadline=monotonic() + 2):
            pytest.fail("unadmitted origin")
    attempt.close(deadline=monotonic() + 2)


@pytest.mark.parametrize("field,value", [("slot_index", 1), ("command_sha256", "c" * 64), ("original_fence", 2)])
def test_original_reservation_binding_before_effects(setup, monkeypatch, field, value):
    attempt, identity = reserved(setup)
    monkeypatch.setattr(attempt._lifecycle, "assert_authority", lambda **kw: pytest.fail("premature effect"))
    with pytest.raises((ValueError, WindowContractError)):
        with attempt._create_departure_sequence(
            UUID(int=99), replace(identity, **{field: value}), deadline=monotonic() + 2
        ):
            pytest.fail("changed reservation")
    attempt.close(deadline=monotonic() + 2)


def test_failed_assertion_consumes_sequence_and_retains_shutdown(setup, monkeypatch):
    attempt, identity = reserved(setup)
    calls = []

    def fail(**kw):
        calls.append(1)
        raise OSError("lost authority observation")

    monkeypatch.setattr(attempt._lifecycle, "assert_authority", fail)
    with pytest.raises(OSError):
        with attempt._create_departure_sequence(UUID(int=99), identity, deadline=monotonic() + 2):
            pytest.fail("failed authority")
    with pytest.raises((WindowContractError, TdsAttemptUnknown)):
        with attempt._create_departure_sequence(UUID(int=100), identity, deadline=monotonic() + 2):
            pytest.fail("retried authority")
    assert len(calls) == 1
    attempt.close(deadline=monotonic() + 2)


def test_caught_in_sequence_reentry_prevents_success(setup):
    attempt, identity = reserved(setup)
    with pytest.raises(TdsAttemptUnknown):
        with attempt._create_departure_sequence(UUID(int=99), identity, deadline=monotonic() + 2):
            with pytest.raises(WindowContractError, match="reentrant"):
                _ = attempt.directory
    attempt.close(deadline=monotonic() + 2)


def test_changed_ack_rejects(setup, monkeypatch):
    attempt, identity = reserved(setup)
    deadline = monotonic() + 2
    original = attempt._directory.execute

    def execute(command, *, deadline):
        result = original(command, deadline=deadline)
        return replace(result, revision=result.revision + 1) if type(command) is AssertDirectoryAuthority else result

    with pytest.raises((ValueError, WindowContractError)):
        with attempt._create_departure_sequence(UUID(int=99), identity, deadline=deadline):
            monkeypatch.setattr(attempt._directory, "execute", execute)
            attempt._assert_create_departure(UUID(int=99), deadline=deadline)
    attempt.close(deadline=deadline)


@pytest.mark.parametrize("deadline_delta", [1.0, float("inf"), float("nan")])
def test_private_assertion_cannot_extend_deadline(setup, deadline_delta):
    attempt, identity = reserved(setup)
    deadline = monotonic() + 2
    with pytest.raises(WindowContractError):
        with attempt._create_departure_sequence(UUID(int=99), identity, deadline=deadline):
            attempt._assert_create_departure(UUID(int=99), deadline=deadline + deadline_delta)
    attempt.close(deadline=deadline)


def test_caught_recursive_private_assertion_poisons_outer_sequence(setup, monkeypatch):
    attempt, identity = reserved(setup)
    original = attempt._lifecycle.assert_authority

    def authority(*, deadline):
        with pytest.raises(WindowContractError, match="reentrant"):
            attempt._assert_create_departure(UUID(int=99), deadline=deadline)
        original(deadline=deadline)

    monkeypatch.setattr(attempt._lifecycle, "assert_authority", authority)
    with pytest.raises(TdsAttemptUnknown):
        with attempt._create_departure_sequence(UUID(int=99), identity, deadline=monotonic() + 2):
            pytest.fail("caught assertion reentry")
    attempt.close(deadline=monotonic() + 2)


def test_invalid_snapshot_revision_rejects_before_directory_hash(setup, monkeypatch):
    from dpone.contracts import mssql_tds_directory
    from dpone.services.mssql_tds_attempt import _departure_binding

    attempt, identity = reserved(setup)
    parent, directory = attempt.lifecycle, replace(attempt.directory)
    object.__setattr__(directory, "revision", True)

    def forbidden_hash(*args):
        pytest.fail("hashing before snapshot validation")

    monkeypatch.setattr(mssql_tds_directory, "parent_digest", forbidden_hash)
    with pytest.raises(ValueError):
        _departure_binding(parent, directory, identity)
    attempt.close(deadline=monotonic() + 2)
