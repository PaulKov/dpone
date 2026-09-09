from __future__ import annotations

import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.state.mssql_database_authority import MssqlDatabaseAuthorityVerifier
from dpone.runtime.state.mssql_database_authority_retry import (
    MSSQL_TRANSIENT_SESSION_SQLSTATES,
    MssqlStagingSessionRetryPolicy,
    MssqlStagingSessionRetryRunner,
    MssqlStagingVerificationStage,
    extract_sanitized_sqlstate,
)
from dpone.runtime.state.mssql_database_authority_support import (
    MssqlDatabaseAuthorityVerificationError,
    database_connector,
    master_connector,
)


@pytest.mark.parametrize("sqlstate", sorted(MSSQL_TRANSIENT_SESSION_SQLSTATES))
def test_retry_policy_accepts_only_closed_transient_sqlstate_allowlist(sqlstate: str) -> None:
    policy = MssqlStagingSessionRetryPolicy()
    error = MssqlDatabaseAuthorityVerificationError(
        "mssql_transaction.staging_database_session_unavailable",
        stage=MssqlStagingVerificationStage.STAGING_IDENTITY,
        sqlstate=sqlstate,
    )

    assert policy.is_retryable(error) is True


@pytest.mark.parametrize("sqlstate", (None, "28000", "42000", "42S02", "HY008", "08004"))
def test_retry_policy_rejects_auth_config_cancel_unknown_and_ambiguous_states(sqlstate: str | None) -> None:
    policy = MssqlStagingSessionRetryPolicy()
    error = MssqlDatabaseAuthorityVerificationError(
        "mssql_transaction.staging_database_session_unavailable",
        stage=MssqlStagingVerificationStage.STAGING_IDENTITY,
        sqlstate=sqlstate,
    )

    assert policy.is_retryable(error) is False


@pytest.mark.parametrize(
    "stage",
    (
        MssqlStagingVerificationStage.TARGET_IDENTITY,
        MssqlStagingVerificationStage.TARGET_CURRENT_DATABASE,
        MssqlStagingVerificationStage.MASTER_TARGET_TOPOLOGY,
        MssqlStagingVerificationStage.TARGET_STAGING_TOPOLOGY,
        MssqlStagingVerificationStage.STAGING_TIMEOUT_SCOPE,
        MssqlStagingVerificationStage.MASTER_TIMEOUT_SCOPE,
    ),
)
def test_retry_policy_never_retries_non_fresh_or_deterministic_stages(
    stage: MssqlStagingVerificationStage,
) -> None:
    policy = MssqlStagingSessionRetryPolicy()
    error = MssqlDatabaseAuthorityVerificationError(
        "mssql_transaction.staging_database_session_unavailable",
        stage=stage,
        sqlstate="08S01",
    )

    assert policy.is_retryable(error) is False


def test_sqlstate_extractor_uses_exact_tokens_and_causal_chain_only() -> None:
    root = _SqlStateError("08s01", "password=should-never-be-public")
    wrapped = RuntimeError("normalized")
    wrapped.__cause__ = root

    assert extract_sanitized_sqlstate(wrapped) == "08S01"
    assert extract_sanitized_sqlstate(RuntimeError("driver returned [08S01] with secret")) is None


@pytest.mark.parametrize(
    ("fault_owner", "fault_query", "expected_stage"),
    (
        ("staging_factory", None, MssqlStagingVerificationStage.STAGING_CONNECTOR),
        ("master_factory", None, MssqlStagingVerificationStage.MASTER_CONNECTOR),
        ("master", "identity", MssqlStagingVerificationStage.MASTER_IDENTITY),
        ("staging", "identity", MssqlStagingVerificationStage.STAGING_IDENTITY),
        ("staging", "current", MssqlStagingVerificationStage.STAGING_CURRENT_DATABASE),
        ("master", "catalog", MssqlStagingVerificationStage.STAGING_PIN),
    ),
)
def test_verifier_attributes_each_fresh_session_failure_stage(
    fault_owner: str,
    fault_query: str | None,
    expected_stage: MssqlStagingVerificationStage,
) -> None:
    masters: list[_Connector] = []
    staging_sessions: list[_Connector] = []
    failure = _SqlStateError("08S01", "server=secret.example;password=hidden")

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        if fault_owner == "master_factory":
            raise failure
        connector = _Connector(fault_query=fault_query if fault_owner == "master" else None, failure=failure)
        masters.append(connector)
        return connector

    def staging_factory(_connection: ResolvedBindingConnection, _database: str) -> _Connector:
        if fault_owner == "staging_factory":
            raise failure
        connector = _Connector(fault_query=fault_query if fault_owner == "staging" else None, failure=failure)
        staging_sessions.append(connector)
        return connector

    verifier = _verifier(
        master_factory=master_factory,
        staging_factory=staging_factory,
        retry_runner=_runner(max_retries=0),
    )

    with pytest.raises(MssqlDatabaseAuthorityVerificationError) as captured:
        verifier.acquire_staging_lease(target_connector=_Connector())

    assert captured.value.stage == expected_stage.value
    assert captured.value.sqlstate == "08S01"
    assert captured.value.attempts == 1
    assert captured.value.retry_exhausted is True
    assert captured.value.__cause__ is None
    rendered = "".join(traceback.format_exception(captured.value))
    assert "secret.example" not in rendered
    assert "password=hidden" not in rendered
    assert all(connector.closed for connector in masters)
    assert all(connector.closed for connector in staging_sessions)


def test_verifier_retries_with_entirely_fresh_sessions_then_holds_only_successful_lease(caplog) -> None:
    masters: list[_Connector] = []
    staging_sessions: list[_Connector] = []
    delays: list[float] = []

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        connector = _Connector()
        masters.append(connector)
        return connector

    def staging_factory(_connection: ResolvedBindingConnection, _database: str) -> _Connector:
        connector = _Connector(
            fault_query="identity" if not staging_sessions else None,
            failure=_SqlStateError("08S01", "password=hidden"),
        )
        staging_sessions.append(connector)
        return connector

    verifier = _verifier(
        master_factory=master_factory,
        staging_factory=staging_factory,
        retry_runner=_runner(sleeper=delays.append),
    )

    with caplog.at_level("WARNING"):
        lease = verifier.acquire_staging_lease(target_connector=_Connector())

    assert delays == [5.0]
    assert len(masters) == 2
    assert len(staging_sessions) == 2
    assert all(connector.closed for connector in masters)
    assert staging_sessions[0].closed is True
    assert staging_sessions[1].closed is False
    assert all(connector.timeout_scopes == [5] for connector in masters + staging_sessions)
    assert "stage=staging_identity" in caplog.text
    assert "sqlstate=08S01" in caplog.text
    assert "password=hidden" not in caplog.text

    lease.close()
    assert staging_sessions[1].closed is True


def test_verifier_exhausts_three_fresh_attempts_with_one_stable_sanitized_error() -> None:
    masters: list[_Connector] = []
    staging_sessions: list[_Connector] = []
    delays: list[float] = []

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        connector = _Connector()
        masters.append(connector)
        return connector

    def staging_factory(_connection: ResolvedBindingConnection, _database: str) -> _Connector:
        connector = _Connector(
            fault_query="identity",
            failure=_SqlStateError("HYT00", "dsn=private;uid=private"),
        )
        staging_sessions.append(connector)
        return connector

    verifier = _verifier(
        master_factory=master_factory,
        staging_factory=staging_factory,
        retry_runner=_runner(sleeper=delays.append),
    )

    with pytest.raises(MssqlDatabaseAuthorityVerificationError) as captured:
        verifier.acquire_staging_lease(target_connector=_Connector())

    error = captured.value
    assert str(error) == "mssql_transaction.staging_database_session_unavailable"
    assert error.stage == MssqlStagingVerificationStage.STAGING_IDENTITY.value
    assert error.sqlstate == "HYT00"
    assert error.attempts == 3
    assert error.retry_exhausted is True
    assert error.deadline_exhausted is False
    assert delays == [5.0, 10.0]
    assert len(masters) == len(staging_sessions) == 3
    assert all(connector.closed for connector in masters + staging_sessions)
    assert "dsn=private" not in "".join(traceback.format_exception(error))


@pytest.mark.parametrize("sqlstate", (None, "28000", "42000", "42S02", "HY008", "08004"))
def test_verifier_fails_fast_without_sleep_for_non_retryable_sqlstate(sqlstate: str | None) -> None:
    staging_sessions: list[_Connector] = []
    delays: list[float] = []

    def staging_factory(_connection: ResolvedBindingConnection, _database: str) -> _Connector:
        connector = _Connector(
            fault_query="identity",
            failure=_SqlStateError(sqlstate, "private driver diagnostic"),
        )
        staging_sessions.append(connector)
        return connector

    verifier = _verifier(
        master_factory=lambda _connection: _Connector(),
        staging_factory=staging_factory,
        retry_runner=_runner(sleeper=delays.append),
    )

    with pytest.raises(MssqlDatabaseAuthorityVerificationError) as captured:
        verifier.acquire_staging_lease(target_connector=_Connector())

    assert captured.value.attempts == 1
    assert captured.value.retry_exhausted is False
    assert delays == []
    assert len(staging_sessions) == 1
    assert staging_sessions[0].closed is True


@pytest.mark.parametrize(
    ("target_fault", "expected_stage"),
    (
        ("identity", MssqlStagingVerificationStage.TARGET_IDENTITY),
        ("current", MssqlStagingVerificationStage.TARGET_CURRENT_DATABASE),
    ),
)
def test_stale_preopened_target_session_is_attributed_and_never_retried(
    target_fault: str,
    expected_stage: MssqlStagingVerificationStage,
) -> None:
    masters: list[_Connector] = []
    staging_sessions: list[_Connector] = []
    delays: list[float] = []

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        return _append(masters, _Connector())

    def staging_factory(_connection: ResolvedBindingConnection, _database: str) -> _Connector:
        return _append(staging_sessions, _Connector())

    verifier = _verifier(
        master_factory=master_factory,
        staging_factory=staging_factory,
        retry_runner=_runner(sleeper=delays.append),
    )
    stale_target = _Connector(
        fault_query=target_fault,
        failure=_SqlStateError("08S01", "stale target connection"),
    )

    with pytest.raises(MssqlDatabaseAuthorityVerificationError) as captured:
        verifier.acquire_staging_lease(target_connector=stale_target)

    assert captured.value.code == "mssql_transaction.target_database_session_unavailable"
    assert captured.value.stage == expected_stage.value
    assert captured.value.sqlstate == "08S01"
    assert captured.value.attempts == 1
    assert captured.value.retry_exhausted is False
    assert delays == []
    assert len(masters) == len(staging_sessions) == 1
    assert masters[0].closed is True
    assert staging_sessions[0].closed is True


def test_verifier_never_retries_pin_mismatch_or_permission_denial() -> None:
    for master in (
        _Connector(view_any_database=False),
        _Connector(database_guid="ffffffff-ffff-ffff-ffff-ffffffffffff"),
    ):
        delays: list[float] = []
        staging_sessions: list[_Connector] = []
        verifier = _verifier(
            master_factory=lambda _connection, connector=master: connector,
            staging_factory=lambda _connection, _database: _append(staging_sessions, _Connector()),
            retry_runner=_runner(sleeper=delays.append),
        )

        with pytest.raises(MssqlDatabaseAuthorityVerificationError) as captured:
            verifier.acquire_staging_lease(target_connector=_Connector())

        assert captured.value.attempts == 1
        assert captured.value.retry_exhausted is False
        assert delays == []
        assert master.closed is True
        assert staging_sessions[0].closed is True


def test_verifier_propagates_cancellation_after_closing_fresh_sessions() -> None:
    masters: list[_Connector] = []
    staging_sessions: list[_Connector] = []

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        connector = _Connector()
        masters.append(connector)
        return connector

    def staging_factory(_connection: ResolvedBindingConnection, _database: str) -> _Connector:
        connector = _Connector(fault_query="identity", failure=KeyboardInterrupt())
        staging_sessions.append(connector)
        return connector

    verifier = _verifier(
        master_factory=master_factory,
        staging_factory=staging_factory,
        retry_runner=_runner(sleeper=lambda _delay: pytest.fail("cancellation must not sleep")),
    )

    with pytest.raises(KeyboardInterrupt):
        verifier.acquire_staging_lease(target_connector=_Connector())

    assert len(masters) == len(staging_sessions) == 1
    assert masters[0].closed is True
    assert staging_sessions[0].closed is True


@pytest.mark.parametrize(
    ("failing_owner", "expected_stage"),
    (
        ("master", MssqlStagingVerificationStage.MASTER_CLEANUP),
        ("staging", MssqlStagingVerificationStage.STAGING_CLEANUP),
    ),
)
def test_retry_stops_when_a_fresh_session_cannot_be_closed(
    failing_owner: str,
    expected_stage: MssqlStagingVerificationStage,
) -> None:
    masters: list[_Connector] = []
    staging_sessions: list[_Connector] = []
    sleeps: list[float] = []
    cleanup_failure = _SqlStateError("08S01", "password=cleanup-secret")

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        return _append(
            masters,
            _Connector(close_failure=cleanup_failure if failing_owner == "master" else None),
        )

    def staging_factory(_connection: ResolvedBindingConnection, _database: str) -> _Connector:
        return _append(
            staging_sessions,
            _Connector(
                fault_query="identity" if failing_owner == "staging" else None,
                failure=_SqlStateError("08S01", "password=query-secret"),
                close_failure=cleanup_failure if failing_owner == "staging" else None,
            ),
        )

    verifier = _verifier(
        master_factory=master_factory,
        staging_factory=staging_factory,
        retry_runner=_runner(sleeper=sleeps.append),
    )

    with pytest.raises(MssqlDatabaseAuthorityVerificationError) as captured:
        verifier.acquire_staging_lease(target_connector=_Connector())

    error = captured.value
    assert error.code == "mssql_transaction.staging_database_session_cleanup_failed"
    assert error.stage == expected_stage.value
    assert error.attempts == 1
    assert error.retry_exhausted is False
    assert sleeps == []
    assert len(masters) == len(staging_sessions) == 1
    assert masters[0].close_attempts == 1
    assert staging_sessions[0].close_attempts == 1
    _assert_secret_absent_from_exception_graph(error, "cleanup-secret", "query-secret")


def test_retry_error_detaches_raw_driver_exception_graph() -> None:
    secret = "password=driver-secret"
    raw = _SqlStateError("08S01", secret)
    runner = _runner(max_retries=0)

    def fail(_deadline: object) -> None:
        try:
            raise raw
        except Exception as exc:
            raise MssqlDatabaseAuthorityVerificationError(
                "mssql_transaction.staging_database_session_unavailable",
                stage=MssqlStagingVerificationStage.STAGING_CONNECTOR,
                sqlstate=extract_sanitized_sqlstate(exc),
            ) from None

    with pytest.raises(MssqlDatabaseAuthorityVerificationError) as captured:
        runner.run(fail)

    _assert_secret_absent_from_exception_graph(captured.value, secret)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_remaining_deadline_caps_master_connect_and_target_query_without_reconnect() -> None:
    now = [0.0]
    masters: list[_Connector] = []
    staging_sessions: list[_Connector] = []
    master_connection_caps: list[int] = []
    target = _Connector()

    def master_factory(connection: ResolvedBindingConnection) -> _Connector:
        master_connection_caps.append(connection.credentials.connect_timeout)
        return _append(masters, _Connector(query_durations={"identity": 2.0}, monotonic_time=now))

    def staging_factory(_connection: ResolvedBindingConnection, _database: str) -> _Connector:
        now[0] = 39.0
        return _append(staging_sessions, _Connector(monotonic_time=now))

    verifier = _verifier(
        master_factory=master_factory,
        staging_factory=staging_factory,
        retry_runner=MssqlStagingSessionRetryRunner(
            sleeper=lambda _delay: None,
            monotonic=lambda: now[0],
        ),
    )

    lease = verifier.acquire_staging_lease(target_connector=target)

    assert master_connection_caps == [6]
    assert target.timeout_scopes[0] == 4
    assert target.connection_open_attempts == 0
    lease.close()


def test_deadline_stops_after_an_over_budget_blocking_factory_before_any_query() -> None:
    now = [0.0]
    masters: list[_Connector] = []
    staging_sessions: list[_Connector] = []
    master_connection_caps: list[int] = []

    def master_factory(connection: ResolvedBindingConnection) -> _Connector:
        master_connection_caps.append(connection.credentials.connect_timeout)
        now[0] = 46.0
        return _append(masters, _Connector())

    def staging_factory(_connection: ResolvedBindingConnection, _database: str) -> _Connector:
        now[0] = 42.0
        return _append(staging_sessions, _Connector())

    verifier = _verifier(
        master_factory=master_factory,
        staging_factory=staging_factory,
        retry_runner=MssqlStagingSessionRetryRunner(
            sleeper=lambda _delay: None,
            monotonic=lambda: now[0],
        ),
    )

    with pytest.raises(MssqlDatabaseAuthorityVerificationError) as captured:
        verifier.acquire_staging_lease(target_connector=_Connector())

    assert master_connection_caps == [3]
    assert captured.value.deadline_exhausted is True
    assert captured.value.attempts == 1
    assert masters[0].queries == []
    assert staging_sessions[0].queries == []
    assert masters[0].closed is True
    assert staging_sessions[0].closed is True


def test_retry_deadline_blocks_the_next_attempt_before_sleep() -> None:
    attempts = 0
    sleeps: list[float] = []
    runner = MssqlStagingSessionRetryRunner(
        policy=MssqlStagingSessionRetryPolicy(deadline_seconds=5.0),
        sleeper=sleeps.append,
        monotonic=lambda: 0.0,
    )

    def fail(_deadline: object) -> None:
        nonlocal attempts
        attempts += 1
        raise MssqlDatabaseAuthorityVerificationError(
            "mssql_transaction.staging_database_session_unavailable",
            stage=MssqlStagingVerificationStage.STAGING_CONNECTOR,
            sqlstate="08001",
        )

    with pytest.raises(MssqlDatabaseAuthorityVerificationError) as captured:
        runner.run(fail)

    assert attempts == 1
    assert sleeps == []
    assert captured.value.attempts == 1
    assert captured.value.retry_exhausted is True
    assert captured.value.deadline_exhausted is True


def test_retry_deadline_closes_a_success_that_completed_out_of_budget() -> None:
    now = [0.0]
    result = _Closable()
    runner = MssqlStagingSessionRetryRunner(monotonic=lambda: now[0])

    def late_success(_deadline: object) -> _Closable:
        now[0] = 45.0
        return result

    with pytest.raises(MssqlDatabaseAuthorityVerificationError) as captured:
        runner.run(late_success)

    assert result.closed is True
    assert captured.value.stage == MssqlStagingVerificationStage.RETRY_DEADLINE.value
    assert captured.value.attempts == 1
    assert captured.value.deadline_exhausted is True


def test_authority_factories_cap_login_timeout_without_widening_lower_value(monkeypatch) -> None:
    captured: list[ResolvedBindingConnection] = []

    def create(connection: ResolvedBindingConnection, *, autocommit: bool) -> object:
        assert autocommit is True
        captured.append(connection)
        return object()

    monkeypatch.setattr(ResolvedConnectorFactory, "create", create)
    high = _connection(connect_timeout=90, additional_params={"LoginTimeout": "120"})
    low = _connection(connect_timeout=4, additional_params={"Connect Timeout": "3"})

    master_connector(high)
    database_connector(low, "DWH")

    assert captured[0].credentials.database == "master"
    assert captured[0].credentials.connect_timeout == 10
    assert captured[0].credentials.additional_params == {"connect_timeout": 10}
    assert captured[1].credentials.database == "DWH"
    assert captured[1].credentials.connect_timeout == 3
    assert captured[1].credentials.additional_params == {"connect_timeout": 3}


def _runner(
    *,
    max_retries: int = 2,
    sleeper: Any = None,
) -> MssqlStagingSessionRetryRunner:
    return MssqlStagingSessionRetryRunner(
        policy=MssqlStagingSessionRetryPolicy(max_retries=max_retries),
        sleeper=sleeper if sleeper is not None else lambda _delay: None,
        monotonic=lambda: 0.0,
    )


def _verifier(
    *,
    master_factory: Any,
    staging_factory: Any,
    retry_runner: MssqlStagingSessionRetryRunner,
) -> MssqlDatabaseAuthorityVerifier:
    connection = _connection()
    return MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=connection,
        state_connection=_connection(database="Example_System", database_id=9, token="2"),
        target_database="DWH",
        staging_database="DWH",
        state_database="Example_System",
        master_connector_factory=master_factory,
        staging_connector_factory=staging_factory,
        staging_retry_runner=retry_runner,
    )


def _connection(
    *,
    database: str = "DWH",
    database_id: int = 7,
    token: str = "1",
    connect_timeout: int = 10,
    additional_params: dict[str, Any] | None = None,
) -> ResolvedBindingConnection:
    pin = {
        "database_id": database_id,
        "create_token": f"2026-08-24T00:00:0{token}.0000000",
        "database_guid": f"{token * 8}-{token * 4}-{token * 4}-{token * 4}-{token * 12}",
    }
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(
            host="sql.example",
            database=database,
            connect_timeout=connect_timeout,
            additional_params=additional_params,
        ),
        safe_metadata={},
        descriptor=ResolvedConnectionDescriptor(
            connection_type="mssql",
            properties={"database": database, "database_authorities": {database: pin}},
        ),
    )


class _SqlStateError(RuntimeError):
    def __init__(self, sqlstate: str | None, diagnostic: str) -> None:
        arguments = (sqlstate, diagnostic) if sqlstate is not None else (diagnostic,)
        super().__init__(*arguments)
        self.sqlstate = sqlstate


class _Connector:
    def __init__(
        self,
        *,
        fault_query: str | None = None,
        failure: BaseException | None = None,
        view_any_database: bool = True,
        database_guid: str = "11111111-1111-1111-1111-111111111111",
        query_timeout: int = 0,
        close_failure: BaseException | None = None,
        query_durations: dict[str, float] | None = None,
        monotonic_time: list[float] | None = None,
    ) -> None:
        self.fault_query = fault_query
        self.failure = failure
        self.view_any_database = view_any_database
        self.database_guid = database_guid
        self.query_timeout = query_timeout
        self.close_failure = close_failure
        self.query_durations = dict(query_durations or {})
        self.monotonic_time = monotonic_time
        self.timeout_scopes: list[int] = []
        self.closed = False
        self.close_attempts = 0
        self.connection_open_attempts = 0
        self.queries: list[str] = []

    @contextmanager
    def bounded_query_timeout(self, seconds: int) -> Iterator[None]:
        previous = self.query_timeout
        self.query_timeout = min(previous, seconds) if previous > 0 else seconds
        self.timeout_scopes.append(self.query_timeout)
        try:
            yield
        finally:
            self.query_timeout = previous

    def get_records(
        self,
        query: str,
        params: tuple[Any, ...] = (),
        *,
        as_dict: bool,
    ) -> list[dict[str, Any]]:
        assert as_dict
        query_kind = _query_kind(query)
        self.queries.append(query_kind)
        if self.monotonic_time is not None:
            self.monotonic_time[0] += self.query_durations.get(query_kind, 0.0)
        if query_kind == self.fault_query:
            assert self.failure is not None
            raise self.failure
        if query_kind == "permission":
            return [{"permitted": int(self.view_any_database), "is_sysadmin": 0}]
        if query_kind == "identity":
            return [
                {
                    "server_name": "SQLNODE",
                    "machine_name": "SQLNODE",
                    "instance_name": "MSSQLSERVER",
                    "replica_name": "SQLNODE",
                    "effective_principal": "svc_dpone",
                    "original_login": "svc_dpone",
                }
            ]
        if query_kind == "catalog":
            assert params == (7, "DWH")
            return [
                {
                    "database_id": 7,
                    "database_name": "DWH",
                    "state_desc": "ONLINE",
                    "user_access_desc": "MULTI_USER",
                    "has_dbaccess": 1,
                    "create_token": "2026-08-24T00:00:01.0000000",
                    "database_guid": self.database_guid,
                }
            ]
        if query_kind == "current":
            return [{"database_id": 7, "database_name": "DWH"}]
        raise AssertionError(f"unexpected SQL: {query}")

    def close(self) -> None:
        self.close_attempts += 1
        if self.close_failure is not None:
            raise self.close_failure
        self.closed = True


class _Closable:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _query_kind(query: str) -> str:
    if "HAS_PERMS_BY_NAME" in query:
        return "permission"
    if "SERVERPROPERTY" in query:
        return "identity"
    if "database_recovery_status" in query:
        return "catalog"
    if "DB_ID() AS database_id" in query:
        return "current"
    raise AssertionError(f"unexpected SQL: {query}")


def _append(items: list[_Connector], connector: _Connector) -> _Connector:
    items.append(connector)
    return connector


def _assert_secret_absent_from_exception_graph(error: BaseException, *secrets: str) -> None:
    pending = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        rendered = " ".join(
            (
                repr(current),
                repr(current.args),
                repr(getattr(current, "__notes__", ())),
            )
        )
        assert all(secret not in rendered for secret in secrets)
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
