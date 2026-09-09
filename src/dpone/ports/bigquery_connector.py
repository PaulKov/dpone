"""BigQuery connector contracts consumed by runtime sinks."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol


class BigQuerySinkConnectorPort(Protocol):
    """Thin BigQuery port required by the sink orchestration boundary."""

    project_id: str

    @property
    def connection(self) -> Any:
        """Return the underlying BigQuery client."""

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        """Execute a BigQuery statement and return the affected-row count."""
