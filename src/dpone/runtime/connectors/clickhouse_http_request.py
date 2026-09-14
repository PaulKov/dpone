"""Pure ClickHouse HTTP credentials, wire options and URL construction.

Transport execution and connection ownership belong to the calling adapter.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from types import FunctionType
from typing import Any
from urllib.parse import urlencode


@dataclass(frozen=True)
class ClickHouseHttpCredentials:
    """Connection settings for ClickHouse HTTP inserts."""

    host: str
    port: int
    database: str
    user: str
    password: str = ""
    secure: bool = False


@dataclass(frozen=True)
class ClickHouseHttpOptions:
    """Runtime options for ClickHouse HTTP file streaming."""

    input_format: str = "TabSeparated"
    timeout_seconds: int = 3600
    chunk_size: int = 1024 * 1024
    settings: dict[str, Any] = field(default_factory=dict)
    query_id: str | None = None
    insert_deduplication_token: str | None = None

    @classmethod
    def from_bulk_wire_contract(
        cls,
        contract: Any,
        *,
        base: ClickHouseHttpOptions | None = None,
    ) -> ClickHouseHttpOptions:
        """Return options with input format/settings required by a typed wire contract."""

        base_options = base or cls()
        return cls(
            input_format=str(getattr(contract, "input_format", base_options.input_format)),
            timeout_seconds=base_options.timeout_seconds,
            chunk_size=base_options.chunk_size,
            settings={**base_options.settings, **dict(contract.delimiter_profile.clickhouse_settings)},
            query_id=base_options.query_id,
            insert_deduplication_token=base_options.insert_deduplication_token,
        )


def build_path(
    credentials: ClickHouseHttpCredentials, options: ClickHouseHttpOptions, query: str, *, include_password: bool
) -> str:
    params = {
        "database": credentials.database,
        "user": credentials.user,
        "password": credentials.password if include_password else "***",
        "query": query,
    }
    if options.query_id:
        params["query_id"] = options.query_id
    if options.insert_deduplication_token:
        params["insert_deduplication_token"] = options.insert_deduplication_token
    params.update(options.settings)
    return "/?" + urlencode(params)


def build_insert_query(table: str, columns: Sequence[str], input_format: str) -> str:
    column_sql = ", ".join(f"`{column}`" for column in columns)
    return f"INSERT INTO {table} ({column_sql}) FORMAT {input_format}"


def build_insert_url(
    credentials: ClickHouseHttpCredentials,
    options: ClickHouseHttpOptions,
    table: str,
    columns: Sequence[str],
    *,
    include_password: bool = True,
    query: str | None = None,
) -> str:
    return build_path(
        credentials,
        options,
        query or build_insert_query(table, columns, options.input_format),
        include_password=include_password,
    )


# Historical globals and generated dataclass methods remain pickle/introspection compatible.
for _model in (
    ClickHouseHttpCredentials,
    ClickHouseHttpOptions,
):
    _model.__module__ = "dpone.runtime.connectors.clickhouse_http_bulk"
    for _member in vars(_model).values():
        if isinstance(_member, classmethod):
            _member = _member.__func__
        if isinstance(_member, FunctionType) and _member.__module__ == __name__:
            _member.__module__ = "dpone.runtime.connectors.clickhouse_http_bulk"
