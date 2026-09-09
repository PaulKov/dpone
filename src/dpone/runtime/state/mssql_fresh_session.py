"""Independent-session factory used only for ambiguous commit probes."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from typing import Any


class MssqlFreshSessionError(RuntimeError):
    """A genuinely independent SQL Server session cannot be constructed."""


class MssqlFreshSessionFactory:
    """Create a connector without reusing the possibly ambiguous ODBC handle."""

    def __init__(self, factory: Callable[[Any], Any] | None = None) -> None:
        self._factory = factory
        self._query_timeout: ContextVar[int | None] = ContextVar(
            "dpone_mssql_fresh_session_query_timeout",
            default=None,
        )

    def _create(self, template: Any) -> Any:
        """Construct one raw session for the managed ``open`` boundary."""

        if self._factory is not None:
            session = self._factory(template)
        else:
            session = self._from_public_connector_contract(template)
        if session is template:
            raise MssqlFreshSessionError("mssql_transaction.fresh_session_reused")
        if not callable(getattr(session, "get_records", None)) or not callable(getattr(session, "close", None)):
            raise MssqlFreshSessionError("mssql_transaction.fresh_session_invalid")
        return session

    @contextmanager
    def bounded_query_timeout(self, seconds: int) -> Iterator[None]:
        """Bind an explicit maximum for every fresh session in this context."""

        if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds < 1:
            raise ValueError("MSSQL fresh-session timeout must be a positive integer")
        current = self._query_timeout.get()
        bounded = min(current, seconds) if current is not None else seconds
        token = self._query_timeout.set(bounded)
        try:
            yield
        finally:
            self._query_timeout.reset(token)

    @contextmanager
    def open(self, template: Any) -> Iterator[Any]:
        """Yield one fresh session under the active explicit/template timeout cap."""

        session = self._create(template)
        template_timeout = _minimum_positive_timeout(
            self._query_timeout.get(),
            _positive_query_timeout(template),
        )
        try:
            if template_timeout is None:
                yield session
                return
            scope = getattr(session, "bounded_query_timeout", None)
            if not callable(scope):
                raise MssqlFreshSessionError("mssql_transaction.fresh_session_query_timeout_unbounded")
            with scope(template_timeout):
                yield session
        finally:
            closer = getattr(session, "close", None)
            if callable(closer):
                with suppress(Exception):
                    closer()

    def _from_public_connector_contract(self, template: Any) -> Any:
        required = ("host", "port", "database", "driver", "encrypt", "trust_server_certificate")
        if any(not hasattr(template, field) for field in required):
            raise MssqlFreshSessionError("mssql_transaction.fresh_session_factory_required")
        connector_type = type(template)
        kwargs = {
            "host": template.host,
            "port": template.port,
            "database": template.database,
            "user": getattr(template, "user", None),
            "password": getattr(template, "password", None),
            "driver": template.driver,
            "encrypt": template.encrypt,
            "trust_server_certificate": template.trust_server_certificate,
            "connect_timeout": getattr(template, "connect_timeout", 10),
            "query_timeout": getattr(template, "query_timeout", 0),
            "autocommit": True,
            "application_name": f"{getattr(template, 'application_name', 'dpone-mssql')}-receipt-probe",
            "bcp_path": getattr(template, "bcp_path", "bcp"),
            "odbc_options": getattr(template, "odbc_options", None),
        }
        try:
            return connector_type(**kwargs)
        except (TypeError, ValueError) as exc:
            raise MssqlFreshSessionError("mssql_transaction.fresh_session_factory_required") from exc


def _positive_query_timeout(connector: Any) -> int | None:
    raw = getattr(connector, "query_timeout", 0)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        return None
    return raw


def _minimum_positive_timeout(*values: int | None) -> int | None:
    positive = tuple(value for value in values if value is not None)
    return min(positive) if positive else None


__all__ = ["MssqlFreshSessionError", "MssqlFreshSessionFactory"]
