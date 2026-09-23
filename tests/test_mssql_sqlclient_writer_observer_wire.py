"""Closed canonical frames for the contained one-shot observer."""

import json
from dataclasses import replace
from datetime import datetime
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_credentials import SqlClientCredentials
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientObserverAdmission,
    SqlClientWriterObservation,
    session_authority_digest,
)
from dpone.contracts.mssql_sqlclient_writer_observer_wire import (
    WriterObserverCommand,
    WriterObserverRequest,
    decode_command,
    decode_credentials,
    decode_observation,
    decode_ready,
    decode_request,
    encode_command,
    encode_credentials,
    encode_observation,
    encode_ready,
    encode_request,
    request_digest,
)
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
from tests.mssql_sqlclient_departure_v2_fixtures import sample

NONCE = b"n" * 32


def _request():
    own = sample().observer
    observer = SqlClientObserverAdmission(
        own.authority.server, own.authority.database, own.authority.login, own.authority.transport
    )
    target = replace(
        observer,
        login=replace(
            observer.login, principal_id=8, name="writer", sid="08", original_name="writer", original_sid="08"
        ),
    )
    return WriterObserverRequest(
        attempt_sha256="a" * 64,
        launch_sha256="b" * 64,
        observer_admission=observer,
        target_admission=target,
        operation_deadline_ns=3_000_000_000,
    )


def test_all_nonsecret_frames_roundtrip_and_exclude_password():
    request = _request()
    assert decode_request(encode_request(request)) == request
    incarnation = sample().observer
    assert decode_ready(encode_ready(request, NONCE, incarnation), request, NONCE) == incarnation
    command = WriterObserverCommand(request_sha256=request_digest(request), session_id=52, nonce=NONCE.hex())
    assert decode_command(encode_command(command), request) == command
    remote = TdsRemoteSessionIdentity(
        UUID(int=9),
        52,
        datetime(2026, 1, 1),
        datetime(2026, 1, 1, 0, 0, 1),
        NONCE,
        session_authority_digest(incarnation.authority),
    )
    observation = SqlClientWriterObservation(remote, incarnation.authority)
    assert decode_observation(encode_observation(request, command, observation), request, command) == observation
    joined = b"".join(
        (
            encode_request(request),
            encode_ready(request, NONCE, incarnation),
            encode_command(command),
            encode_observation(request, command, observation),
        )
    )
    assert b"secret-canary" not in joined


def test_credentials_are_exactly_bound_and_only_secret_frame_contains_password():
    request = _request()
    credentials = SqlClientCredentials(
        "localhost",
        1433,
        request.observer_admission.database.database_name,
        request.observer_admission.login.name,
        "secret-canary",
        "disposable_test",
    )
    payload = encode_credentials(credentials, request, NONCE)
    assert b"secret-canary" in payload
    assert decode_credentials(payload, request, NONCE) == credentials
    with pytest.raises(ValueError):
        decode_credentials(payload, replace(request, launch_sha256="c" * 64), NONCE)


@pytest.mark.parametrize("mutation", ["extra", "float", "whitespace", "duplicate"])
def test_request_wire_rejects_alternate_or_open_shapes(mutation):
    payload = encode_request(_request())
    data = json.loads(payload)
    if mutation == "extra":
        data["extra"] = True
        payload = json.dumps(data, separators=(",", ":"), sort_keys=True).encode()
    elif mutation == "float":
        data["operation_deadline_ns"] = 3_000_000_000.0
        payload = json.dumps(data, separators=(",", ":"), sort_keys=True).encode()
    elif mutation == "whitespace":
        payload = b" " + payload
    else:
        payload = payload.replace(b"{", b'{"attempt_sha256":"' + b"a" * 64 + b'",', 1)
    with pytest.raises(ValueError):
        decode_request(payload)


@pytest.mark.parametrize("nanoseconds", [2**53 + 1, 4_611_686_018_427_400_249, 2**63 - 1 - 1024])
def test_operation_deadline_projection_never_extends_integer_ceiling(nanoseconds):
    request = replace(_request(), operation_deadline_ns=nanoseconds)

    assert deadline_nanoseconds(request.operation_deadline) <= nanoseconds


@pytest.mark.parametrize("relation", ["same_login", "same_sid", "server", "database"])
def test_request_rejects_invalid_observer_target_relation(relation):
    request = _request()
    observer, target = request.observer_admission, request.target_admission
    if relation == "same_login":
        target = replace(target, login=observer.login)
    elif relation == "same_sid":
        target = replace(
            target,
            login=replace(target.login, sid=observer.login.sid, original_sid=observer.login.original_sid),
        )
    elif relation == "server":
        target = replace(target, server=replace(target.server, server_name="other"))
    else:
        target = replace(target, database=replace(target.database, database_id=target.database.database_id + 1))

    with pytest.raises(ValueError, match="writer_observer_wire_invalid"):
        replace(request, target_admission=target)
