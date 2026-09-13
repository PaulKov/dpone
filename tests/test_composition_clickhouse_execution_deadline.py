"""Deadline/effect boundary faults; mocked authority is not live certification."""

import time
from threading import Event
from types import SimpleNamespace

import pytest

from dpone.adapters import composition_clickhouse_http as http
from dpone.adapters.composition_clickhouse_admin import ClickHousePrincipalHttpClient
from dpone.adapters.composition_clickhouse_transport import (
    ClickHouseDispatchTransport,
    ClickHouseDispatchTransportError,
)
from dpone.contracts.composition_identity import CompositionAdmissionError
from tests.test_composition_clickhouse_dispatch import create
from tests.test_composition_clickhouse_transport import Journal
from tests.test_composition_clickhouse_transport import server as server

CREDENTIALS = http.ClickHouseTransportCredentials("user", "private-password")


def test_actual_http_incomplete_body_obeys_external_cap_without_retry(server):
    endpoint, requests, behavior = server
    release = Event()
    behavior.update(chunked=b"0\r\n\r\n", response_entered=Event(), response_release=release)
    journal = Journal()
    started = time.monotonic()
    transport = ClickHouseDispatchTransport(
        endpoint=endpoint,
        credentials=CREDENTIALS,
        journal=journal,
        timeout_seconds=5,
        absolute_deadline=lambda: started + 0.2,
    )
    try:
        with pytest.raises(ClickHouseDispatchTransportError):
            transport.execute(create())
    finally:
        release.set()
    assert time.monotonic() - started < 1
    assert len(journal.claims) == len(requests) == 1


@pytest.mark.parametrize("limit", [True, float("nan"), float("inf"), -1])
def test_invalid_or_expired_deadline_opens_no_socket(monkeypatch, limit):
    client = http.BoundedClickHouseHttp(
        endpoint="http://127.0.0.1", credentials=CREDENTIALS, timeout_seconds=1, absolute_deadline=lambda: limit
    )
    monkeypatch.setattr(client, "_connection", lambda: pytest.fail("opened connection"))
    with pytest.raises(http.ClickHouseHttpError) as error:
        client.request(path="/?query_id=id", payload=b"", query_id="id")
    assert error.value.request_body_bytes == 0 and "private-password" not in str(error.value)


@pytest.mark.parametrize("outer,expected", [(102.0, 102.0), (999.0, 105.0)])
def test_request_deadline_caps_existing_timeout(monkeypatch, outer, expected):
    monkeypatch.setattr(http.time, "monotonic", lambda: 100.0)
    client = http.BoundedClickHouseHttp(
        endpoint="http://127.0.0.1", credentials=CREDENTIALS, timeout_seconds=5, absolute_deadline=lambda: outer
    )
    assert client.require_deadline() == expected


@pytest.mark.parametrize("boundary", ["expired", "before_claim", "after_claim"])
def test_transport_never_claims_before_budget_or_sends_after_effect_closure(monkeypatch, boundary):
    events = []

    def guard():
        events.append("guard")
        if boundary == "before_claim" or (boundary == "after_claim" and "claim" in events):
            raise RuntimeError("private callback detail")

    transport = ClickHouseDispatchTransport(
        endpoint="http://127.0.0.1",
        credentials=CREDENTIALS,
        journal=SimpleNamespace(claim_once=lambda _: events.append("claim")),
        timeout_seconds=1,
        absolute_deadline=lambda: -1 if boundary == "expired" else time.monotonic() + 10,
        require_effect=guard,
    )
    monkeypatch.setattr(transport._http, "request", lambda **_: pytest.fail("sent request"))
    with pytest.raises((CompositionAdmissionError, ClickHouseDispatchTransportError)) as error:
        transport.execute(create())
    assert "private" not in str(error.value)
    assert events.count("claim") == (1 if boundary == "after_claim" else 0)


@pytest.mark.parametrize(
    "statement,effect",
    [
        (
            "CREATE USER `{user}` IDENTIFIED WITH sha256_hash BY '{hash}' HOST NONE DEFAULT ROLE NONE GRANTEES NONE",
            True,
        ),
        ("GRANT SELECT ON `db`.`table` TO `{user}`", True),
        ("ALTER USER `{user}` HOST LOCAL", True),
        ("ALTER USER `{user}` HOST IP '127.0.0.1'", True),
        ("ALTER USER `{user}` HOST NONE", False),
        ("REVOKE ALL ON *.* FROM `{user}`", False),
    ],
)
def test_admin_effect_guard_preserves_exact_cleanup_commands(monkeypatch, statement, effect):
    events = []

    def guard():
        raise RuntimeError("closed execution budget")

    client = ClickHousePrincipalHttpClient(
        endpoint="http://127.0.0.1",
        credentials=CREDENTIALS,
        timeout_seconds=1,
        require_effect=guard,
        absolute_deadline=lambda: time.monotonic() + 5,
    )
    monkeypatch.setattr(client._http, "request", lambda **_: events.append("http") or SimpleNamespace(body=b""))
    statement = statement.format(user="dpone_ch_" + "a" * 64, hash="b" * 64)
    if effect:
        with pytest.raises(CompositionAdmissionError, match="command_unknown"):
            client.command(statement, parameters={}, redactions=("b" * 64,))
    else:
        client.command(statement, parameters={}, redactions=())
    assert events == ([] if effect else ["http"])


def test_admin_read_remains_available_under_cleanup_budget(monkeypatch):
    client = ClickHousePrincipalHttpClient(
        endpoint="http://127.0.0.1",
        credentials=CREDENTIALS,
        timeout_seconds=1,
        require_effect=lambda: pytest.fail("read invoked effect guard"),
    )
    body = b'{"meta":[{"name":"count()","type":"UInt64"}],"data":[["0"]],"rows":1}'
    monkeypatch.setattr(client._http, "request", lambda **_: SimpleNamespace(body=body))
    assert client.query("SELECT count() FROM system.transactions", parameters={}, max_rows=1) == ((0,),)
