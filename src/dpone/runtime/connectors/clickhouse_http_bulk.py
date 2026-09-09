"""HTTP streaming bulk inserts for ClickHouse."""

from __future__ import annotations

import http.client
import os
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
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


@dataclass(frozen=True)
class ClickHouseHttpResult:
    """Structured result returned by ``ClickHouseHttpBulkRunner``."""

    url: str
    redacted_url: str
    status: int
    response: str


class ClickHouseHttpBulkRunner:
    """Streams a local TSV file directly into ClickHouse over HTTP."""

    def __init__(
        self,
        credentials: ClickHouseHttpCredentials,
        options: ClickHouseHttpOptions | None = None,
        connection_factory: Callable[..., http.client.HTTPConnection] | None = None,
    ) -> None:
        self.credentials = credentials
        self.options = options or ClickHouseHttpOptions()
        if connection_factory is not None:
            self._connection_factory = connection_factory
        else:
            self._connection_factory = http.client.HTTPSConnection if credentials.secure else http.client.HTTPConnection

    def insert_file(self, table: str, columns: Sequence[str], input_path: str) -> ClickHouseHttpResult:
        column_sql = ", ".join(f"`{column}`" for column in columns)
        query = f"INSERT INTO {table} ({column_sql}) FORMAT {self.options.input_format}"
        url = self.build_insert_url(table, columns, include_password=True, query=query)
        redacted_url = self.build_insert_url(table, columns, include_password=False, query=query)
        connection = self._connection_factory(
            self.credentials.host,
            self.credentials.port,
            timeout=self.options.timeout_seconds,
        )
        try:
            connection.putrequest("POST", url)
            connection.putheader("Content-Length", str(os.path.getsize(input_path)))
            connection.endheaders()
            with open(input_path, "rb") as handle:
                while chunk := handle.read(self.options.chunk_size):
                    connection.send(chunk)
            response = connection.getresponse()
            body = response.read().decode("utf-8", errors="replace")
        finally:
            connection.close()

        result = ClickHouseHttpResult(
            url=url,
            redacted_url=redacted_url,
            status=int(response.status),
            response=body,
        )
        if result.status >= 300:
            raise RuntimeError(f"ClickHouse HTTP insert failed with status {result.status}: {redacted_url}\n{body}")
        return result

    def insert_stream(
        self,
        table: str,
        columns: Sequence[str],
        chunks: Iterable[bytes],
    ) -> ClickHouseHttpResult:
        column_sql = ", ".join(f"`{column}`" for column in columns)
        query = f"INSERT INTO {table} ({column_sql}) FORMAT {self.options.input_format}"
        url = self.build_insert_url(table, columns, include_password=True, query=query)
        redacted_url = self.build_insert_url(table, columns, include_password=False, query=query)
        connection = self._connection_factory(
            self.credentials.host,
            self.credentials.port,
            timeout=self.options.timeout_seconds,
        )
        try:
            connection.putrequest("POST", url)
            connection.putheader("Transfer-Encoding", "chunked")
            connection.endheaders()
            for chunk in chunks:
                if not chunk:
                    continue
                connection.send(f"{len(chunk):X}\r\n".encode("ascii"))
                connection.send(chunk)
                connection.send(b"\r\n")
            connection.send(b"0\r\n\r\n")
            response = connection.getresponse()
            body = response.read().decode("utf-8", errors="replace")
        finally:
            connection.close()

        result = ClickHouseHttpResult(
            url=url,
            redacted_url=redacted_url,
            status=int(response.status),
            response=body,
        )
        if result.status >= 300:
            raise RuntimeError(f"ClickHouse HTTP insert failed with status {result.status}: {redacted_url}\n{body}")
        return result

    def build_insert_url(
        self,
        table: str,
        columns: Sequence[str],
        *,
        include_password: bool = True,
        query: str | None = None,
    ) -> str:
        column_sql = ", ".join(f"`{column}`" for column in columns)
        query = query or f"INSERT INTO {table} ({column_sql}) FORMAT {self.options.input_format}"
        return self._path(query, include_password=include_password)

    def _path(self, query: str, *, include_password: bool) -> str:
        params = {
            "database": self.credentials.database,
            "user": self.credentials.user,
            "password": self.credentials.password if include_password else "***",
            "query": query,
        }
        if self.options.query_id:
            params["query_id"] = self.options.query_id
        if self.options.insert_deduplication_token:
            params["insert_deduplication_token"] = self.options.insert_deduplication_token
        params.update(self.options.settings)
        return "/?" + urlencode(params)
