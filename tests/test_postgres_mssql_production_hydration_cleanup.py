"""Fail-closed cleanup contracts for disposable PostgreSQL→MSSQL proofs."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from tests.integration.postgres import postgres_mssql_production_hydration_cleanup as cleanup


class _AdminConnector:
    def __init__(
        self,
        *,
        execute_error: Exception | None = None,
        close_error: Exception | None = None,
        absent: bool = True,
    ) -> None:
        self.execute_error = execute_error
        self.close_error = close_error
        self.absent = absent
        self.closed = 0

    def execute_query(self, _query: str) -> None:
        if self.execute_error is not None:
            raise self.execute_error

    def get_records(self, _query: str, _params: tuple[str, ...]) -> list[tuple[int]]:
        return [(1 if self.absent else 0,)]

    def close(self) -> None:
        self.closed += 1
        if self.close_error is not None:
            raise self.close_error


def test_mssql_database_cleanup_retries_with_fresh_bounded_admin() -> None:
    admins = [
        _AdminConnector(execute_error=TimeoutError("blocked")),
        _AdminConnector(absent=True),
    ]
    calls: list[dict[str, Any]] = []

    def factory(**kwargs: Any) -> _AdminConnector:
        calls.append(kwargs)
        return admins[len(calls) - 1]

    cleanup.drop_mssql_database("dpone_disposable", connector_factory=factory)

    assert calls == [
        {"database": "master", "connect_timeout": 5, "query_timeout": 5},
        {"database": "master", "connect_timeout": 5, "query_timeout": 5},
    ]
    assert [admin.closed for admin in admins] == [1, 1]


def test_mssql_database_cleanup_fails_when_catalog_readback_remains() -> None:
    admins = [_AdminConnector(absent=False), _AdminConnector(absent=False)]

    def factory(**_kwargs: Any) -> _AdminConnector:
        return admins.pop(0)

    with pytest.raises(RuntimeError, match="cleanup not proven"):
        cleanup.drop_mssql_database("dpone_disposable", connector_factory=factory)


def test_mssql_database_cleanup_fails_closed_when_close_fails_after_absence() -> None:
    admins = [
        _AdminConnector(close_error=TimeoutError("close blocked")),
        _AdminConnector(),
    ]

    def factory(**_kwargs: Any) -> _AdminConnector:
        return admins.pop(0)

    with pytest.raises(RuntimeError, match="close:TimeoutError"):
        cleanup.drop_mssql_database("dpone_disposable", connector_factory=factory)

    assert admins == []


def test_mssql_database_cleanup_retries_connector_creation() -> None:
    admin = _AdminConnector()
    attempts = 0

    def factory(**_kwargs: Any) -> _AdminConnector:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TimeoutError("login timeout")
        return admin

    cleanup.drop_mssql_database("dpone_disposable", connector_factory=factory)

    assert attempts == 2
    assert admin.closed == 1


def test_runtime_binding_cleanup_closes_unique_connectors_and_reports_all_errors() -> None:
    class Connector:
        def __init__(self, *, failing: bool = False) -> None:
            self.failing = failing
            self.closed = 0

        def close(self) -> None:
            self.closed += 1
            if self.failing:
                raise RuntimeError("driver detail must not enter the cleanup receipt")

    shared = Connector(failing=True)
    state = Connector()
    bindings = SimpleNamespace(
        source_obj=SimpleNamespace(connector=shared),
        sink_obj=SimpleNamespace(
            connector=shared,
            state_storage=SimpleNamespace(connector=state),
        ),
    )

    with pytest.raises(ExceptionGroup, match="runtime binding cleanup failed") as caught:
        cleanup.close_runtime_bindings(bindings)

    assert shared.closed == 1
    assert state.closed == 1
    assert "driver detail" not in str(caught.value)
    assert [str(error) for error in caught.value.exceptions] == ["runtime_binding_close failed with RuntimeError"]
