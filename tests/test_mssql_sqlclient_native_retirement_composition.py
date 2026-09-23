"""Composition contracts for restart-safe production retirement effects."""

from contextlib import contextmanager
from typing import Any, cast
from uuid import UUID

import pytest

from dpone.adapters.mssql_sqlclient_native_retirement_effects import CallbackSqlClientNativeRetirementEffects
from dpone.adapters.mssql_sqlclient_native_retirement_operator import SqlClientNativeRetirementOperator
from dpone.adapters.mssql_sqlclient_native_retirement_window_store import WindowStoreSqlClientNativeRetirementState
from dpone.app.mssql_sqlclient_native_retirement_composition import (
    SqlClientNativeRetirementDeployment,
    compose_sqlclient_native_retirement_effects,
)
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits


@contextmanager
def _session():
    yield cast(Any, object())


def _deployment() -> SqlClientNativeRetirementDeployment:
    server = SqlClientServerAuthority("server", "machine", "instance", "physical")
    database = SqlClientDatabaseAuthority(7, "database", str(UUID(int=1)), "aa")
    admission = SqlClientObserverAdmission(
        server,
        database,
        SqlClientLoginAuthority(5, "login", "aa", "login", "aa", 1, True),
        SqlClientTransportAuthority("TCP", "TSQL", "SQL", "TRUE"),
    )
    return SqlClientNativeRetirementDeployment(
        open_management_session=_session,
        admission=admission,
        expected_server=server,
        expected_database=database,
        lifecycle_observer=cast(Any, object()),
        directory_observer=cast(Any, object()),
        directory_limits=TdsDirectoryLimits(2, 1, 2, 1),
    )


def test_each_composition_uses_new_operator_over_exact_durable_state() -> None:
    deployment = _deployment()
    state = object.__new__(WindowStoreSqlClientNativeRetirementState)

    first = compose_sqlclient_native_retirement_effects(deployment, state)
    second = compose_sqlclient_native_retirement_effects(deployment, state)

    assert type(first) is type(second) is CallbackSqlClientNativeRetirementEffects
    first_operator = first._containment.__self__
    second_operator = second._containment.__self__
    assert type(first_operator) is type(second_operator) is SqlClientNativeRetirementOperator
    assert first_operator is not second_operator
    assert first_operator._progress is second_operator._progress is state


def test_composition_rejects_drifted_admitted_database_before_session() -> None:
    deployment = _deployment()
    drifted = SqlClientDatabaseAuthority(8, "other", str(UUID(int=2)), "bb")
    state = object.__new__(WindowStoreSqlClientNativeRetirementState)

    with pytest.raises(ValueError, match="retirement_composition_invalid"):
        compose_sqlclient_native_retirement_effects(
            SqlClientNativeRetirementDeployment(
                open_management_session=deployment.open_management_session,
                admission=deployment.admission,
                expected_server=deployment.expected_server,
                expected_database=drifted,
                lifecycle_observer=deployment.lifecycle_observer,
                directory_observer=deployment.directory_observer,
                directory_limits=deployment.directory_limits,
            ),
            state,
        )
