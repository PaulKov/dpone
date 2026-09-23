"""Pure bindings reject malformed originals; valid copies are not authority."""

from copy import deepcopy
from dataclasses import dataclass, replace
from uuid import UUID

import pytest

from dpone.contracts import mssql_tds_attempt_reservation as binding
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity
from dpone.contracts.mssql_tds_directory import (
    TdsCoordinatorCommand as Command,
)
from dpone.contracts.mssql_tds_directory import TdsDirectorySnapshot, initial_directory, reserve_operation
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase, TdsAttemptSnapshot, TdsAttemptState, TdsObjectIdentity
from tests.test_mssql_sqlclient_observe_departure_composition import composed_preparation as composed_preparation
from tests.test_mssql_sqlclient_observe_departure_composition import prepared as prepared
from tests.test_mssql_tds_directory import LIMITS, PARENT
from tests.test_mssql_tds_directory_journal import OWNER


def records(*, prepared=True, command=Command.GRANT):
    state = TdsAttemptState(
        PARENT,
        OWNER,
        TdsAttemptPhase.PREPARED if prepared else TdsAttemptPhase.CREATION_INTENT,
        int(prepared),
        object_identity=TdsObjectIdentity(1, "a" * 64) if prepared else None,
        observation_sha256="a" * 64 if prepared else None,
        schema_version=2,
        backend="mssql_sqlclient",
    )
    directory = reserve_operation(
        initial_directory(PARENT, LIMITS, schema_version=2),
        operation_id=UUID(int=8),
        command=command,
        command_sha256="a" * 64,
        owner_fence=OWNER.fence,
    )
    identity = TdsCoordinatorIdentity(PARENT, 0, UUID(int=8), command, "a" * 64, OWNER.fence, "a" * 64)
    return deepcopy((TdsAttemptSnapshot(state, 2), TdsDirectorySnapshot(directory, OWNER, 2), identity))


def validate(values):
    return binding.validate_prepared_grant_binding(*values)


def test_prepared_grant_and_detached_copy_are_structures_only():
    values = records()
    assert validate(values) is None
    assert validate(deepcopy(values)) is None
    with pytest.raises(ValueError):
        binding.validate_reservation_binding(*values, command=Command.GRANT)


@pytest.mark.parametrize("command", list(Command))
def test_command_and_legacy_phase_are_fixed(command):
    values = records()
    object.__setattr__(values[1].state.slots[0], "command", command)
    object.__setattr__(values[2], "command", command)
    if command is Command.GRANT:
        validate(values)
    else:
        with pytest.raises(ValueError):
            validate(values)


def test_legacy_create_and_legacy_uuid_alias_acceptance_are_preserved():
    values = records(prepared=False, command=Command.CREATE)
    binding.validate_reservation_binding(*values)
    with pytest.raises(ValueError):
        validate(values)
    for value in (values[1].state.slots[0].operation_id, values[2].operation_id):
        object.__setattr__(value, "int", True)
    binding.validate_reservation_binding(*values)


@pytest.mark.parametrize("location", ["object_id", "identity_uuid", "slot_uuid", "revision", "limit", "fence"])
def test_original_numeric_alias_rejected(location, monkeypatch):
    values = records()
    targets = {
        "object_id": (values[0].state.object_identity, "object_id", 1.0),
        "identity_uuid": (values[2].operation_id, "int", True),
        "slot_uuid": (values[1].state.slots[0].operation_id, "int", True),
        "revision": (values[1], "revision", 1.0),
        "limit": (values[1].state.limits, "max_entries", True),
        "fence": (values[1].ownership, "fence", 2.0),
    }
    object.__setattr__(*targets[location])
    hashes = []
    monkeypatch.setattr("dpone.contracts.mssql_tds_directory.parent_digest", lambda _: hashes.append(True))
    with pytest.raises(ValueError):
        validate(values)
    assert hashes == []


@pytest.mark.parametrize("location", ["object", "limit", "identity"])
def test_foreign_constructor_never_called(location):
    calls = []

    @dataclass
    class Foreign:
        def __post_init__(self):
            calls.append(True)

    foreign = object.__new__(Foreign)
    values = list(records())
    if location == "object":
        object.__setattr__(values[0].state, "object_identity", foreign)
    elif location == "limit":
        object.__setattr__(values[1].state.limits, "max_entries", foreign)
    else:
        values[2] = foreign
    with pytest.raises(ValueError):
        validate(values)
    assert calls == []


@pytest.mark.parametrize("field", ["work_sealed", "admission_closed"])
def test_closed_directory_rejected(field):
    values = records()
    object.__setattr__(values[1].state, field, True)
    with pytest.raises(ValueError):
        validate(values)


@pytest.mark.parametrize("location", ["backend", "schema", "owner", "operation", "digest", "retirement"])
def test_mismatched_original_binding_rejected(location):
    values = records()
    changes = {
        "backend": (values[0].state, "backend", "mssql_python"),
        "schema": (values[0].state, "schema_version", 1),
        "owner": (values[1], "ownership", replace(OWNER, owner="another")),
        "operation": (values[2], "operation_id", UUID(int=99)),
        "digest": (values[2], "command_sha256", "b" * 64),
        "retirement": (values[1].state, "retirement_authority", values[0].state),
    }
    object.__setattr__(*changes[location])
    with pytest.raises(ValueError):
        validate(values)


def test_actual_completed_observe_next_grant_reservation(prepared, monkeypatch):
    from tests.test_mssql_sqlclient_observe_departure_composition import (
        test_actual_prepared_entry_settles_only_after_six_acks_and_cleanup,
    )

    h = prepared
    test_actual_prepared_entry_settles_only_after_six_acks_and_cleanup(h, monkeypatch, None)
    assert h.attempt._observe_settlement.complete is True
    parent, directory = h.attempt.lifecycle, h.attempt.directory
    slot = directory.state.slots[-1]
    identity = TdsCoordinatorIdentity(
        parent.state.identity,
        slot.index,
        slot.operation_id,
        slot.command,
        slot.command_sha256,
        slot.owner_fence,
        h.attempt._prepared_origin.identity.implementation_sha256,
    )
    assert slot.command is Command.GRANT
    assert validate((parent, directory, identity)) is None
