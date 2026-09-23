"""Restricted P9 identity/departure never reinterprets the ODBC client GUID."""

from datetime import datetime
from uuid import UUID

import pytest

from dpone.adapters import mssql_sqlclient_restricted_session_departure as adapter
from dpone.adapters.mssql_sqlclient_restricted_session_departure_sql import (
    CONNECTIONS_SQL,
    REQUESTS_SQL,
    SESSIONS_SQL,
    TRANSACTIONS_SQL,
)
from dpone.contracts.mssql_sqlclient_restricted_session_departure_codec import (
    decode_restricted_session_departure,
    encode_restricted_session_departure,
)
from dpone.contracts.mssql_tds_session import (
    TdsRestrictedRemoteSessionIdentity,
    decode_restricted_session_identity,
    encode_restricted_session_identity,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_sqlclient_restricted_writer_settlement_codec import settlement_fixture


def test_restricted_identity_has_its_own_canonical_schema_and_rejects_aliases():
    identity = TdsRestrictedRemoteSessionIdentity(
        UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        51,
        datetime(2026, 1, 2),
        b"n" * 32,
        b"a" * 32,
    )
    payload = encode_restricted_session_identity(identity)

    assert decode_restricted_session_identity(payload) == identity
    body = strict_json_object(payload)
    assert body["schema"] == "dpone.tds.restricted-remote-session.v1"
    assert "connection_id" not in body and "connect_time" not in body
    body["client_connection_id"] = str(identity.client_connection_id).upper()
    with pytest.raises(ValueError, match="restricted_session_record_invalid"):
        decode_restricted_session_identity(canonical_json_bytes(body))


def test_restricted_departure_codec_round_trips_without_server_uuid_claim():
    _, _, result = settlement_fixture()
    payload = encode_restricted_session_departure(result.absence)

    assert decode_restricted_session_departure(payload) == result.absence
    assert result.absence.original.client_connection_id.bytes not in payload
    assert b'"client_connection_id"' in payload


def test_departure_queries_never_receive_client_connection_id(monkeypatch):
    _, request, expected = settlement_fixture()
    absence = expected.absence
    calls: list[tuple[str, tuple[object, ...]]] = []

    class Delegate:
        _admission = absence.admission

        def _begin(self):
            return None

        def _finish(self):
            return None

        def _fail(self):
            return None

        def _current(self):
            return None

        def _creator(self, principal):
            assert principal == absence.principal

        def _query(self, sql, *parameters):
            calls.append((sql, parameters))
            if sql == CONNECTIONS_SQL:
                return [(0, 0)]
            if sql == SESSIONS_SQL:
                return [(0, 0, 0)]
            if sql == REQUESTS_SQL:
                return [(0, 0, None, None, None, None)]
            if sql == TRANSACTIONS_SQL:
                return [(0,)]
            raise AssertionError("unexpected SQL")

    observer = adapter.SqlClientRestrictedSessionDepartureObserver(
        object(),
        admission=absence.admission,
        observer_admission=request.plan.management_admission,
        operation_deadline_ns=10**18,
        monotonic_ns=lambda: 1,
    )
    observer._delegate = Delegate()
    monkeypatch.setattr(adapter, "capture_observer_incarnation", lambda *args, **kwargs: absence.observer)

    assert (
        observer.observe(
            original=absence.original,
            database=absence.database,
            principal=absence.principal,
        )
        == absence
    )
    client_id = str(absence.original.client_connection_id)
    assert all(client_id not in tuple(str(value) for value in parameters) for _, parameters in calls)
    broad = [
        parameters
        for sql, parameters in calls
        if sql in {CONNECTIONS_SQL, SESSIONS_SQL, REQUESTS_SQL, TRANSACTIONS_SQL}
    ]
    assert broad and all(parameters[0] == absence.original.session_id for parameters in broad)
