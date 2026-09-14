"""HTTP streaming bulk inserts for ClickHouse."""

from __future__ import annotations

import http.client
import os
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from typing import Any as Any

from dpone.contracts.bounded_window import WindowContractError
from dpone.runtime.connectors.clickhouse_http_request import (
    ClickHouseHttpCredentials as ClickHouseHttpCredentials,
)
from dpone.runtime.connectors.clickhouse_http_request import (
    ClickHouseHttpOptions as ClickHouseHttpOptions,
)
from dpone.runtime.connectors.clickhouse_http_request import (
    build_insert_query,
    build_path,
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
        query = build_insert_query(table, columns, self.options.input_format)
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

    def insert_window_stream(
        self, database: str, table: str, columns: Sequence[str], chunks: Iterable[bytes], query_id: str
    ) -> ClickHouseHttpResult:
        """Admit one identified synchronous RowBinary attempt before network I/O.

        Window composition creates an independent runner per attempt. Keep endpoint
        and transport validation in this adapter, behind WindowBinaryIngest.
        """
        if self.options.input_format != "RowBinary" or self.options.settings.get("async_insert", 0):
            raise WindowContractError("Window inserts require synchronous RowBinary HTTP")
        if self.credentials.database != database:
            raise WindowContractError("HTTP runner database identity mismatch")
        self.options = replace(self.options, query_id=query_id)
        return self.insert_stream(table, columns, chunks)

    def insert_stream(
        self,
        table: str,
        columns: Sequence[str],
        chunks: Iterable[bytes],
    ) -> ClickHouseHttpResult:
        query = build_insert_query(table, columns, self.options.input_format)
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
        query = query or build_insert_query(table, columns, self.options.input_format)
        return self._path(query, include_password=include_password)

    def _path(self, query: str, *, include_password: bool) -> str:
        return build_path(self.credentials, self.options, query, include_password=include_password)
