"""Concrete semantic-refresh ClickHouse HTTP transport tests."""

from __future__ import annotations

import ssl
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

from dpone.adapters.semantic_refresh_clickhouse_http_client import (
    ClickHousePublicationHttpClient,
)


class _Response:
    status = 200

    def __init__(self, body: bytes = b"") -> None:
        self._body = body

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


class _Opener:
    def __init__(self, body: bytes = b"") -> None:
        self.body = body
        self.calls: list[tuple[Any, dict[str, object]]] = []

    def __call__(self, request: Any, **kwargs: object) -> _Response:
        self.calls.append((request, kwargs))
        return _Response(self.body)


def test_verified_https_transport_binds_timeout_auth_and_query() -> None:
    opener = _Opener(b"1\tMergeTree\n")
    client = ClickHousePublicationHttpClient(
        endpoint="https://clickhouse.example:8443/",
        username="semantic-writer",
        password="secret",
        timeout_seconds=7,
        opener=opener,
    )

    assert client.execute("SELECT 1, engine FROM system.tables") == [(1, "MergeTree")]
    request, kwargs = opener.calls[0]
    query = parse_qs(urlsplit(request.full_url).query)
    assert query["query"] == ["SELECT 1, engine FROM system.tables"]
    assert request.get_header("Authorization").startswith("Basic ")
    assert request.data == b""
    assert kwargs["timeout"] == 7.0
    assert isinstance(kwargs["context"], ssl.SSLContext)


def test_parquet_transport_sends_exact_binary_body() -> None:
    opener = _Opener()
    client = ClickHousePublicationHttpClient(
        endpoint="http://127.0.0.1:58124/",
        username="default",
        password="local",
        timeout_seconds=20,
        transport_profile="LOCAL_HTTP_UNVERIFIED",
        opener=opener,
    )

    assert client.insert_parquet("INSERT INTO mart.stage FORMAT Parquet", b"PAR1bytesPAR1") == []
    request, kwargs = opener.calls[0]
    assert request.data == b"PAR1bytesPAR1"
    assert request.get_header("Content-type") == "application/octet-stream"
    assert "context" not in kwargs


def test_operation_transport_binds_exact_query_id() -> None:
    opener = _Opener(b"1\n")
    client = ClickHousePublicationHttpClient(
        endpoint="http://127.0.0.1:58124/",
        username="default",
        password="local",
        timeout_seconds=20,
        transport_profile="LOCAL_HTTP_UNVERIFIED",
        opener=opener,
    )

    assert client.execute_operation("SELECT 1", query_id="dpone-semref-run-operation-phase") == [(1,)]
    request, _kwargs = opener.calls[0]
    query = parse_qs(urlsplit(request.full_url).query)
    assert query["query_id"] == ["dpone-semref-run-operation-phase"]


def test_operation_transport_rejects_unbounded_query_id() -> None:
    client = ClickHousePublicationHttpClient(
        endpoint="http://127.0.0.1:58124/",
        username="default",
        password="local",
        timeout_seconds=20,
        transport_profile="LOCAL_HTTP_UNVERIFIED",
    )

    with pytest.raises(ValueError, match="operation query ID"):
        client.execute_operation("SELECT 1", query_id="unsafe/query-id")


@pytest.mark.parametrize(
    "endpoint",
    ["http://clickhouse.example:8123/", "https://user:secret@clickhouse.example/"],
)
def test_insecure_or_credential_bearing_endpoint_is_rejected(endpoint: str) -> None:
    with pytest.raises(ValueError):
        ClickHousePublicationHttpClient(
            endpoint=endpoint,
            username="semantic-writer",
            password="secret",
            timeout_seconds=7,
            transport_profile=("LOCAL_HTTP_UNVERIFIED" if endpoint.startswith("http:") else "HTTPS_VERIFIED"),
        )
