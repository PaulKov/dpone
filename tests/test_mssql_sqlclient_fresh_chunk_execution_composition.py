"""Composition contracts for one frozen fresh SqlClient execution."""

from __future__ import annotations

from uuid import UUID

import pytest

from dpone.adapters.mssql_sqlclient_writer_observer_process import PythonSqlClientWriterObserverLauncher
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.app.mssql_sqlclient_fresh_chunk_execution_composition import (
    SqlClientFreshChunkDeployment,
    compose_sqlclient_fresh_chunk_execution,
)
from dpone.app.mssql_sqlclient_fresh_chunk_executor import FreshSqlClientChunkExecution
from dpone.app.mssql_sqlclient_prepared_attempt_factory import SqlClientPreparedAttemptFactory
from dpone.contracts.mssql_sqlclient_credential_admission import SqlClientCredentialProfile
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody


def _deployment(events: list[str], **changes: object) -> SqlClientFreshChunkDeployment:
    values = dict(
        authorization=lambda _attempt: object(),
        pool=object.__new__(TdsActorPool),
        evidence_writer_factory=lambda: (_ for _ in ()).throw(AssertionError("evidence opened during composition")),
        startup_deadline=2.0,
        operation_deadline=3.0,
        termination_timeout_seconds=4,
        max_worker_address_space_bytes=1024,
        writer_profile=object.__new__(SqlClientCredentialProfile),
        writer_credentials=lambda: events.append("writer-credentials"),
        session_nonce=lambda: events.append("session-nonce"),
        observer_admission=object.__new__(SqlClientObserverAdmission),
        observer_profile=object.__new__(SqlClientCredentialProfile),
        observer_credentials=lambda: events.append("observer-credentials"),
        observer_launcher=object.__new__(PythonSqlClientWriterObserverLauncher),
        observer_startup_timeout=1.0,
        observer_termination_timeout=1.0,
        grant_id=lambda: UUID("11111111-1111-4111-8111-111111111111"),
        settlement_verifier=lambda: object(),
        clock_ns=lambda: 1,
    )
    values.update(changes)
    return SqlClientFreshChunkDeployment(**values)  # type: ignore[arg-type]


def test_composition_freezes_exact_fields_without_opening_credentials() -> None:
    events: list[str] = []
    deployment = _deployment(events)
    prepared = object.__new__(SqlClientPreparedAttemptFactory)

    execution = compose_sqlclient_fresh_chunk_execution(
        deployment, prepared, SqlClientAttemptRetirementCustody(lambda *args: None)
    )

    assert type(execution) is FreshSqlClientChunkExecution
    assert execution.prepared_attempt.__self__ is prepared
    assert execution.prepared_attempt.__func__ is SqlClientPreparedAttemptFactory.prepare
    for name in deployment.__dataclass_fields__:
        assert getattr(execution, name) is getattr(deployment, name)
    assert events == []


@pytest.mark.parametrize(
    "changes",
    (
        {"writer_credentials": None},
        {"startup_deadline": 4.0},
        {"observer_startup_timeout": 0.0},
        {"termination_timeout_seconds": True},
    ),
)
def test_invalid_deployment_fails_before_credentials(changes: dict[str, object]) -> None:
    events: list[str] = []
    deployment = _deployment(events, **changes)

    with pytest.raises(ValueError, match="fresh_chunk_composition_invalid"):
        compose_sqlclient_fresh_chunk_execution(
            deployment,
            object.__new__(SqlClientPreparedAttemptFactory),
            SqlClientAttemptRetirementCustody(lambda *args: None),
        )

    assert events == []


def test_composition_rejects_noncanonical_prepared_factory_without_effects() -> None:
    events: list[str] = []
    deployment = _deployment(events)

    with pytest.raises(ValueError, match="fresh_chunk_composition_invalid"):
        compose_sqlclient_fresh_chunk_execution(
            deployment,
            object(),  # type: ignore[arg-type]
            SqlClientAttemptRetirementCustody(lambda *args: None),
        )

    assert events == []
