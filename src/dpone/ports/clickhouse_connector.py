"""ClickHouse connector contracts consumed by runtime services."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol


class ClickHouseConnectorPort(Protocol):
    """Thin ClickHouse port used by sinks without importing connector implementations."""

    host: str
    port: int
    database: str
    user: str
    password: str
    application_name: str
    secure: bool
    compression: bool
    connect_timeout: int
    send_receive_timeout: int
    settings: dict[str, Any]

    @property
    def connection(self) -> Any:
        """Return the underlying ClickHouse client connection."""

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        """Execute a ClickHouse query and return the affected-row count."""

    def get_records(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        as_dict: bool = False,
    ) -> list[Any]:
        """Return ClickHouse query results."""

    def clone_for_partition(self, partition_index: int) -> ClickHouseConnectorPort:
        """Return an equivalent connector for parallel partition work."""
