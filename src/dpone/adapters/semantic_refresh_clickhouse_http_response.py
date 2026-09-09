"""Bounded tabular response validation for the ClickHouse HTTP gateway."""

from __future__ import annotations

from dpone.adapters.semantic_refresh_clickhouse_http_queries import ClickHouseHttpGatewayError
from dpone.ports.semantic_refresh_clickhouse_connection import SemanticRefreshClickHouseHttpClientPort


class ClickHouseHttpResponseReader:
    """Validate bounded scalar and tabular responses for one protected client."""

    def __init__(self, client: SemanticRefreshClickHouseHttpClientPort) -> None:
        self._client = client

    def scalar(self, statement: str) -> int:
        """Return one non-negative integer cell."""

        value = self.one(statement)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ClickHouseHttpGatewayError("ClickHouse HTTP scalar response is invalid")
        return value

    def text(self, statement: str) -> str:
        """Return one non-empty text cell."""

        value = self.one(statement)
        if not isinstance(value, str) or not value:
            raise ClickHouseHttpGatewayError("ClickHouse HTTP text response is invalid")
        return value

    def one(self, statement: str) -> object:
        """Return the only cell from the only response row."""

        try:
            rows = self._client.execute(statement)
        except Exception as exc:
            raise ClickHouseHttpGatewayError("ClickHouse HTTP query outcome is unavailable") from exc
        if not isinstance(rows, list) or len(rows) != 1:
            raise ClickHouseHttpGatewayError("ClickHouse HTTP query did not return exactly one row")
        row = rows[0]
        if not isinstance(row, tuple) or len(row) != 1:
            raise ClickHouseHttpGatewayError("ClickHouse HTTP query row is not scalar")
        return row[0]

    def optional_text(self, statement: str) -> str | None:
        """Return zero or one non-empty text cell without accepting ambiguity."""

        try:
            rows = self._client.execute(statement)
        except Exception as exc:
            raise ClickHouseHttpGatewayError("ClickHouse HTTP query outcome is unavailable") from exc
        if rows == []:
            return None
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], tuple) or len(rows[0]) != 1:
            raise ClickHouseHttpGatewayError("ClickHouse optional text response is ambiguous")
        value = rows[0][0]
        if not isinstance(value, str) or not value:
            raise ClickHouseHttpGatewayError("ClickHouse optional text response is invalid")
        return value

    def rows(self, statement: str, *, columns: int) -> tuple[tuple[object, ...], ...]:
        """Return one non-empty fixed-width tabular response."""

        return tabular_rows(self._client, statement, columns=columns)


def tabular_rows(
    client: SemanticRefreshClickHouseHttpClientPort,
    statement: str,
    *,
    columns: int,
) -> tuple[tuple[object, ...], ...]:
    """Execute and validate one non-empty fixed-width tabular response."""

    try:
        rows = client.execute(statement)
    except Exception as exc:
        raise ClickHouseHttpGatewayError("ClickHouse HTTP query outcome is unavailable") from exc
    if not isinstance(rows, list) or not rows or any(not isinstance(row, tuple) or len(row) != columns for row in rows):
        raise ClickHouseHttpGatewayError("ClickHouse HTTP tabular response is invalid")
    return tuple(rows)


__all__ = ["ClickHouseHttpResponseReader", "tabular_rows"]
