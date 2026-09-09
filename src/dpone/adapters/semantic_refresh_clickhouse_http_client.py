"""Concrete bounded HTTP transport for semantic-refresh ClickHouse publication."""

from __future__ import annotations

import base64
import ssl
from collections.abc import Callable
from typing import Any
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from dpone.ports.semantic_refresh_clickhouse_connection import (
    normalize_clickhouse_endpoint_authority,
)


class ClickHousePublicationHttpTransportError(RuntimeError):
    """Raised when an exact ClickHouse HTTP request is not acknowledged."""


class ClickHousePublicationHttpClient:
    """Execute text queries and exact Parquet inserts over protected HTTP(S)."""

    def __init__(
        self,
        *,
        endpoint: str,
        username: str,
        password: str,
        timeout_seconds: float,
        transport_profile: str = "HTTPS_VERIFIED",
        ca_file: str | None = None,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        parsed = urlsplit(endpoint)
        if parsed.query or parsed.fragment or parsed.username or parsed.password or not parsed.hostname:
            raise ValueError("ClickHouse HTTP endpoint authority is invalid")
        if transport_profile == "HTTPS_VERIFIED":
            if parsed.scheme != "https":
                raise ValueError("production ClickHouse HTTP transport requires HTTPS")
            context: ssl.SSLContext | None = ssl.create_default_context(cafile=ca_file)
        elif transport_profile == "LOCAL_HTTP_UNVERIFIED":
            if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
                raise ValueError("unverified ClickHouse HTTP is confined to a local endpoint")
            if ca_file is not None:
                raise ValueError("local HTTP transport cannot accept a TLS CA file")
            context = None
        else:
            raise ValueError("ClickHouse HTTP transport profile is unsupported")
        if not isinstance(username, str) or not username:
            raise ValueError("ClickHouse HTTP username must be non-empty")
        if not isinstance(password, str):
            raise TypeError("ClickHouse HTTP password must be text")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int | float) or timeout_seconds <= 0:
            raise ValueError("ClickHouse HTTP timeout must be positive")
        self._endpoint = normalize_clickhouse_endpoint_authority(endpoint)
        self._authorization = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
        self._timeout_seconds = float(timeout_seconds)
        self._context = context
        self._opener = opener

    @property
    def endpoint_authority_id(self) -> str:
        """Return the normalized non-secret endpoint bound by deployment authority."""

        return self._endpoint

    def execute(self, statement: str) -> list[tuple[object, ...]]:
        """Execute one bounded statement and decode TabSeparated result rows."""

        body = self._post(
            statement=statement,
            content=b"",
            content_type="text/plain; charset=utf-8",
            query_id=None,
        )
        return tuple_rows(body)

    def execute_operation(self, statement: str, *, query_id: str) -> list[tuple[object, ...]]:
        """Execute one operation-bound statement with an observable query ID."""

        body = self._post(
            statement=statement,
            content=b"",
            content_type="text/plain; charset=utf-8",
            query_id=_query_id(query_id),
        )
        return tuple_rows(body)

    def insert_parquet(self, statement: str, content: bytes) -> list[tuple[object, ...]]:
        """Insert exact Parquet bytes under the supplied closed INSERT statement."""

        normalized = statement.strip().upper()
        if not normalized.startswith("INSERT INTO ") or " FORMAT PARQUET" not in normalized:
            raise ValueError("ClickHouse Parquet request must be a closed INSERT ... FORMAT Parquet statement")
        if not isinstance(content, bytes) or not content:
            raise ValueError("ClickHouse Parquet request content must be non-empty bytes")
        body = self._post(
            statement=statement,
            content=content,
            content_type="application/octet-stream",
            query_id=None,
        )
        if body.strip():
            raise ClickHousePublicationHttpTransportError("ClickHouse Parquet insert returned unexpected content")
        return []

    def insert_parquet_operation(
        self,
        statement: str,
        content: bytes,
        *,
        query_id: str,
    ) -> list[tuple[object, ...]]:
        """Insert Parquet under one observable operation query ID."""

        normalized = statement.strip().upper()
        if not normalized.startswith("INSERT INTO ") or " FORMAT PARQUET" not in normalized:
            raise ValueError("ClickHouse Parquet request must be a closed INSERT ... FORMAT Parquet statement")
        if not isinstance(content, bytes) or not content:
            raise ValueError("ClickHouse Parquet request content must be non-empty bytes")
        body = self._post(
            statement=statement,
            content=content,
            content_type="application/octet-stream",
            query_id=_query_id(query_id),
        )
        if body.strip():
            raise ClickHousePublicationHttpTransportError("ClickHouse Parquet insert returned unexpected content")
        return []

    def _post(
        self,
        *,
        statement: str,
        content: bytes,
        content_type: str,
        query_id: str | None,
    ) -> str:
        if not isinstance(statement, str) or not statement.strip():
            raise ValueError("ClickHouse HTTP statement must be non-empty")
        url = (
            self._endpoint
            + "?"
            + urlencode(
                {
                    "default_format": "TabSeparated",
                    "query": statement,
                    **({"query_id": query_id} if query_id is not None else {}),
                }
            )
        )
        request = Request(url, data=content, method="POST")
        request.add_header("Authorization", self._authorization)
        request.add_header("Content-Type", content_type)
        request.add_header("Accept", "text/tab-separated-values")
        try:
            kwargs: dict[str, object] = {"timeout": self._timeout_seconds}
            if self._context is not None:
                kwargs["context"] = self._context
            with self._opener(request, **kwargs) as response:  # noqa: S310 - endpoint is profile validated
                if getattr(response, "status", None) != 200:
                    raise ClickHousePublicationHttpTransportError("ClickHouse HTTP response was not successful")
                return bytes(response.read()).decode("utf-8").rstrip("\r\n")
        except ClickHousePublicationHttpTransportError:
            raise
        except Exception as exc:
            raise ClickHousePublicationHttpTransportError("ClickHouse HTTP acknowledgement is unavailable") from exc


def _query_id(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 128
        or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for character in value)
    ):
        raise ValueError("ClickHouse operation query ID is invalid")
    return value


def tuple_rows(body: str) -> list[tuple[object, ...]]:
    """Decode ClickHouse TabSeparated primitives used by the publication gateway."""

    if not body:
        return []
    rows: list[tuple[object, ...]] = []
    for line in body.splitlines():
        cells: list[object] = []
        for value in line.split("\t"):
            try:
                cells.append(int(value))
            except ValueError:
                cells.append(value)
        rows.append(tuple(cells))
    return rows


__all__ = [
    "ClickHousePublicationHttpClient",
    "ClickHousePublicationHttpTransportError",
    "tuple_rows",
]
