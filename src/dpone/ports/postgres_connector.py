"""PostgreSQL connector contracts consumed by runtime sinks."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol


class PostgresSinkConnectorPort(Protocol):
    """Thin PostgreSQL port required by the sink orchestration boundary."""

    def begin(self) -> None:
        """Start an explicit transaction."""

    def commit_transaction(self) -> None:
        """Commit the current transaction."""

    def rollback(self) -> None:
        """Rollback the current transaction."""

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        """Execute a PostgreSQL statement and return the affected-row count."""

    def get_records(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        as_dict: bool = False,
    ) -> list[Any]:
        """Return PostgreSQL query results."""
