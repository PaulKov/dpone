"""Scoped ODBC query-timeout capability for MSSQL connectors."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any


class MssqlBoundedQueryTimeoutMixin:
    """Temporarily cap configured and already-open ODBC statement timeouts."""

    if TYPE_CHECKING:
        query_timeout: int
        _connection: Any | None

    @contextmanager
    def bounded_query_timeout(self, seconds: int) -> Iterator[None]:
        """Apply a transactional, nestable cap and restore exact prior state."""

        if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds < 1:
            raise ValueError("MSSQL bounded query timeout must be a positive integer")
        previous_config = self.query_timeout
        if isinstance(previous_config, bool) or not isinstance(previous_config, int) or previous_config < 0:
            raise ValueError("MSSQL query timeout must be a non-negative integer")
        bounded = min(previous_config, seconds) if previous_config > 0 else seconds
        initial_connection = self._connection
        initial_live_timeout = getattr(initial_connection, "timeout") if initial_connection is not None else None
        try:
            self.query_timeout = bounded
            if self.query_timeout != bounded:
                raise RuntimeError("MSSQL bounded query timeout was not acknowledged")
            if initial_connection is not None:
                initial_connection.timeout = bounded
                if getattr(initial_connection, "timeout") != bounded:
                    raise RuntimeError("ODBC bounded query timeout was not acknowledged")
        except BaseException:
            self.query_timeout = previous_config
            if initial_connection is not None:
                initial_connection.timeout = initial_live_timeout
            raise
        try:
            yield
        finally:
            self.query_timeout = previous_config
            current_connection = self._connection
            if current_connection is initial_connection and current_connection is not None:
                current_connection.timeout = initial_live_timeout
            elif current_connection is not None:
                current_connection.timeout = previous_config


__all__ = ["MssqlBoundedQueryTimeoutMixin"]
