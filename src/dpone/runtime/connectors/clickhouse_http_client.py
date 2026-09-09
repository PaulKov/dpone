"""HTTP client adapter for ClickHouse.

The runtime ``ClickHouseConnector`` historically exposes the small subset of
``clickhouse-driver.Client`` used by dpone.  ClickHouse Cloud and managed
installations are often reachable only through HTTPS, so this adapter keeps the
same local protocol while delegating transport to ``clickhouse-connect``.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

_QUERY_PREFIXES = ("SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "EXISTS")


class ClickHouseHttpClientAdapter:
    """Expose a clickhouse-driver-like API on top of clickhouse-connect."""

    def __init__(self, client: Any, *, settings: dict[str, Any] | None = None) -> None:
        self._client = client
        self._settings = settings or None

    def execute(
        self,
        query: Any,
        params: Iterable[Any] | dict[str, Any] | None = None,
        *,
        with_column_types: bool = False,
        settings: dict[str, Any] | None = None,
    ) -> Any:
        statement = str(query)
        merged = {**(self._settings or {}), **(settings or {})} or None
        if _is_query(statement):
            result = self._client.query(statement, parameters=params, settings=merged)
            rows = list(getattr(result, "result_rows", ()) or ())
            if with_column_types:
                return rows, _column_types(result)
            return rows
        self._client.command(statement, parameters=params, settings=merged)
        return []

    def execute_iter(
        self,
        query: Any,
        params: Iterable[Any] | dict[str, Any] | None = None,
    ) -> Iterator[Any]:
        stream = self._client.query_rows_stream(str(query), parameters=params, settings=self._settings)
        if hasattr(stream, "__enter__"):
            with stream as rows:
                yield from rows
            return
        yield from stream

    def disconnect(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            close()


def create_clickhouse_http_client(
    *,
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
    secure: bool,
    connect_timeout: int,
    send_receive_timeout: int,
    ca_cert: str | None,
    settings: dict[str, Any] | None,
) -> ClickHouseHttpClientAdapter:
    """Create a clickhouse-connect client behind the runtime adapter."""

    from clickhouse_connect import get_client

    kwargs: dict[str, Any] = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "database": database,
        "secure": secure,
        "connect_timeout": connect_timeout,
        "send_receive_timeout": send_receive_timeout,
    }
    if ca_cert:
        kwargs["ca_cert"] = ca_cert
    return ClickHouseHttpClientAdapter(get_client(**kwargs), settings=settings)


def _is_query(statement: str) -> bool:
    return statement.lstrip().upper().startswith(_QUERY_PREFIXES)


def _column_types(result: Any) -> list[tuple[str, str]]:
    names = list(getattr(result, "column_names", ()) or ())
    raw_types = list(getattr(result, "column_types", ()) or ())
    return [(name, _type_name(raw_types[index]) if index < len(raw_types) else "") for index, name in enumerate(names)]


def _type_name(value: Any) -> str:
    type_name = getattr(value, "name", None) or getattr(value, "base_type", None)
    return str(type_name or value or "")


__all__ = ["ClickHouseHttpClientAdapter", "create_clickhouse_http_client"]
