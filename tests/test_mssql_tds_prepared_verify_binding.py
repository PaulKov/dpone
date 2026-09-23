"""VERIFY structure requires one exact fully settled GRANT predecessor."""

from copy import deepcopy
from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_tds_attempt_reservation import validate_prepared_verify_binding
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity
from dpone.contracts.mssql_tds_directory import (
    TdsCoordinatorCommand,
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    initial_directory,
    parent_digest,
    record_local_containment,
    record_remote_settlement,
    reserve_operation,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase, TdsAttemptSnapshot, TdsAttemptState, TdsObjectIdentity
from tests.test_mssql_tds_directory import LIMITS, PARENT
from tests.test_mssql_tds_directory_journal import OWNER


def records():
    parent = TdsAttemptSnapshot(
        TdsAttemptState(
            PARENT,
            OWNER,
            TdsAttemptPhase.PREPARED,
            1,
            object_identity=TdsObjectIdentity(1, "a" * 64),
            observation_sha256="a" * 64,
            schema_version=2,
            backend="mssql_sqlclient",
        ),
        2,
    )
    grant_id, verify_id = UUID(int=8), UUID(int=9)
    state = reserve_operation(
        initial_directory(PARENT, LIMITS, schema_version=2),
        operation_id=grant_id,
        command=TdsCoordinatorCommand.GRANT,
        command_sha256="a" * 64,
        owner_fence=OWNER.fence,
    )
    digest = parent_digest(PARENT)
    state = record_local_containment(state, 0, TdsLocalContainment(digest, grant_id, "b" * 64, "c" * 64))
    state = record_remote_settlement(state, 0, TdsRemoteSettlement(digest, grant_id, "d" * 64, "e" * 64))
    state = reserve_operation(
        state,
        operation_id=verify_id,
        command=TdsCoordinatorCommand.VERIFY,
        command_sha256="f" * 64,
        owner_fence=OWNER.fence,
    )
    directory = TdsDirectorySnapshot(state, OWNER, 5)
    identity = TdsCoordinatorIdentity(
        PARENT, 1, verify_id, TdsCoordinatorCommand.VERIFY, "f" * 64, OWNER.fence, "a" * 64
    )
    return deepcopy((parent, directory, identity))


def test_exact_settled_grant_predecessor_allows_verify_structure():
    values = records()
    assert validate_prepared_verify_binding(*values) is None
    assert validate_prepared_verify_binding(*deepcopy(values)) is None


@pytest.mark.parametrize("fault", ["command", "local", "remote", "tail", "phase"])
def test_missing_or_changed_predecessor_rejects(fault):
    parent, directory, identity = records()
    predecessor = directory.state.slots[-2]
    if fault == "command":
        object.__setattr__(predecessor, "command", TdsCoordinatorCommand.OBSERVE)
    elif fault == "local":
        object.__setattr__(predecessor, "local_containment", None)
    elif fault == "remote":
        object.__setattr__(predecessor, "remote_settlement", None)
    elif fault == "tail":
        object.__setattr__(directory.state.slots[-1], "command", TdsCoordinatorCommand.GRANT)
        object.__setattr__(identity, "command", TdsCoordinatorCommand.GRANT)
    else:
        object.__setattr__(parent.state, "phase", TdsAttemptPhase.VERIFIED)
    with pytest.raises(ValueError):
        validate_prepared_verify_binding(parent, directory, identity)


def test_equal_detached_structure_is_not_execution_authority():
    parent, directory, identity = records()
    detached = replace(directory)
    assert detached == directory and detached is not directory
    assert validate_prepared_verify_binding(parent, detached, identity) is None
