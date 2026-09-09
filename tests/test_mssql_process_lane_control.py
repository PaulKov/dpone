"""Parent-only MSSQL process-lane timeout scope contracts."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dpone.backfill.sql_state_mssql import MSSQLBackfillStateStore
from dpone.backfill.worker_runtime import parent_control_scope
from dpone.runtime.connectors import mssql as mssql_connector_module
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.etl.backfill_process_runtime import _parent_control_connectors
from dpone.runtime.etl.mssql_process_lane_control import (
    PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS,
    MssqlProcessLaneControlScope,
)
from dpone.runtime.state.mssql_fresh_session import (
    MssqlFreshSessionError,
    MssqlFreshSessionFactory,
)


def _connector(*, query_timeout: int) -> MSSQLConnector:
    return MSSQLConnector(
        host="mssql.example.test",
        port=1433,
        database="state",
        query_timeout=query_timeout,
    )


def test_connector_scope_caps_live_and_fresh_sessions_then_restores_exact_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _connector(query_timeout=60)

    class FakePyodbc:
        @staticmethod
        def connect(*_args: Any, **_kwargs: Any) -> Any:
            return SimpleNamespace(timeout=0)

    monkeypatch.setattr(mssql_connector_module, "_require_pyodbc", lambda: FakePyodbc())
    live_connection = connector.connection
    live_connection.timeout = 47

    with connector.bounded_query_timeout(PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS):
        with MssqlFreshSessionFactory().open(connector) as fresh:
            assert connector.query_timeout == PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS
            assert live_connection.timeout == PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS
            assert fresh.query_timeout == PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS

    assert connector.query_timeout == 60
    assert live_connection.timeout == 47


def test_connector_scope_never_widens_stricter_timeout_and_restores_nested_scope() -> None:
    connector = _connector(query_timeout=2)

    with connector.bounded_query_timeout(PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS):
        assert connector.query_timeout == 2
        with connector.bounded_query_timeout(1):
            assert connector.query_timeout == 1
        assert connector.query_timeout == 2

        with pytest.raises(RuntimeError, match="nested failure"):
            with connector.bounded_query_timeout(1):
                assert connector.query_timeout == 1
                raise RuntimeError("nested failure")

        assert connector.query_timeout == 2

    assert connector.query_timeout == 2


def test_parent_scope_bounds_each_distinct_authority_and_store_session(tmp_path: Any) -> None:
    sink = _connector(query_timeout=90)
    state = _connector(query_timeout=0)
    campaign = _connector(query_timeout=2)
    store = MSSQLBackfillStateStore(
        sink,
        campaign_lock_connector=campaign,
        cache_dir=tmp_path,
    )
    scope = MssqlProcessLaneControlScope((sink, state))

    with scope(store):
        assert sink.query_timeout == PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS
        assert state.query_timeout == PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS
        assert campaign.query_timeout == 2

    assert (sink.query_timeout, state.query_timeout, campaign.query_timeout) == (90, 0, 2)


def test_injected_open_session_applies_explicit_timeout_to_live_odbc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Connection:
        def __init__(self) -> None:
            self.timeout = 0
            self.closed = False

        def close(self) -> None:
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(
        mssql_connector_module,
        "_require_pyodbc",
        lambda: SimpleNamespace(connect=lambda *_args, **_kwargs: connection),
    )
    template = _connector(query_timeout=0)
    fresh = _connector(query_timeout=0)
    assert fresh.connection is connection
    factory = MssqlFreshSessionFactory(lambda _template: fresh)

    with factory.bounded_query_timeout(PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS):
        with factory.open(template) as session:
            assert session is fresh
            assert session.query_timeout == PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS
            assert connection.timeout == PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS

    assert fresh.query_timeout == 0
    assert connection.timeout == 0
    assert connection.closed is True


def test_fresh_session_scope_never_widens_nested_or_template_timeout() -> None:
    template = _connector(query_timeout=4)
    factory = MssqlFreshSessionFactory(lambda _template: _connector(query_timeout=60))

    with factory.bounded_query_timeout(2):
        with factory.bounded_query_timeout(5):
            with factory.open(template) as session:
                assert session.query_timeout == 2
        with factory.open(template) as session:
            assert session.query_timeout == 2

    with factory.open(template) as session:
        assert session.query_timeout == 4


def test_injected_fresh_session_without_live_timeout_port_fails_closed() -> None:
    template = _connector(query_timeout=0)
    unbounded = SimpleNamespace(
        query_timeout=0,
        get_records=lambda *_args, **_kwargs: [],
        close=lambda: None,
    )
    factory = MssqlFreshSessionFactory(lambda _template: unbounded)

    with pytest.raises(
        MssqlFreshSessionError,
        match="mssql_transaction.fresh_session_query_timeout_unbounded",
    ):
        with factory.bounded_query_timeout(PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS):
            with factory.open(template):
                pytest.fail("unbounded fresh session must never be exposed")


def test_connector_scope_entry_restores_config_when_odbc_rejects_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RejectingConnection:
        def __init__(self) -> None:
            self._timeout = 0

        @property
        def timeout(self) -> int:
            return self._timeout

        @timeout.setter
        def timeout(self, value: int) -> None:
            if value == PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS:
                raise RuntimeError("driver rejected timeout")
            self._timeout = value

    connection = RejectingConnection()
    pyodbc = SimpleNamespace(connect=lambda *_args, **_kwargs: connection)
    monkeypatch.setattr(mssql_connector_module, "_require_pyodbc", lambda: pyodbc)
    connector = _connector(query_timeout=60)
    live_connection = connector.connection
    live_connection.timeout = 47

    with pytest.raises(RuntimeError, match="driver rejected timeout"):
        with connector.bounded_query_timeout(PROCESS_LANE_CONTROL_QUERY_TIMEOUT_SECONDS):
            pytest.fail("rejected timeout must never enter the scope")

    assert connector.query_timeout == 60
    assert live_connection.timeout == 47


def test_parent_scope_fails_closed_without_bounded_connector_contract() -> None:
    scope = MssqlProcessLaneControlScope((SimpleNamespace(),))

    with pytest.raises(
        RuntimeError,
        match="mssql_transaction.process_lane_control_timeout_scope_required",
    ):
        with scope(SimpleNamespace()):
            pytest.fail("invalid connector must never enter the process-lane scope")


def test_mssql_process_execution_cannot_fall_back_to_an_unbounded_scope() -> None:
    runtime = SimpleNamespace(parent_control_scope=None)

    with pytest.raises(
        RuntimeError,
        match="mssql_transaction.process_lane_control_timeout_scope_required",
    ):
        parent_control_scope(runtime, SimpleNamespace(dialect="mssql"))


def test_parent_scope_includes_mssql_source_but_not_postgres_source() -> None:
    sink = SimpleNamespace(dialect="mssql")
    state = SimpleNamespace(dialect="mssql")
    mssql_source = SimpleNamespace(dialect="mssql")
    postgres_source = SimpleNamespace(dialect="postgres")

    def config(source: Any) -> Any:
        return SimpleNamespace(
            source_obj=SimpleNamespace(connector=source),
            sink_obj=SimpleNamespace(connector=sink),
        )

    assert _parent_control_connectors(config(mssql_source), SimpleNamespace(connector=state)) == (
        sink,
        state,
        mssql_source,
    )
    assert _parent_control_connectors(config(postgres_source), SimpleNamespace(connector=state)) == (
        sink,
        state,
    )
