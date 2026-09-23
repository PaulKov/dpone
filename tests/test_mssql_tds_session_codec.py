"""Closed durable/IPC session identity representation, independent of SQL I/O."""

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from dpone.contracts.mssql_tds_session import (
    TdsRemoteSessionIdentity,
    decode_session_identity,
    encode_session_identity,
)

IDENTITY = TdsRemoteSessionIdentity(
    UUID("11111111-1111-1111-1111-111111111111"),
    72,
    datetime(2026, 1, 2, 3, 4, 5, 6000),
    datetime(2026, 1, 2, 3, 4, 5, 9000),
    b"n" * 31 + b"\0",
    b"a" * 32,
)
RECORD = {
    "schema": "dpone.tds.remote-session.v1",
    "connection_id": "11111111-1111-1111-1111-111111111111",
    "session_id": 72,
    "connect_time": "2026-01-02T03:04:05.006000",
    "login_time": "2026-01-02T03:04:05.009000",
    "nonce": "6e" * 31 + "00",
    "authority_sha256": "61" * 32,
}


def test_frozen_record_and_canonical_encoding():
    expected = json.dumps(RECORD, sort_keys=True, separators=(",", ":")).encode()
    assert encode_session_identity(IDENTITY) == expected
    assert decode_session_identity(expected) == IDENTITY
    assert decode_session_identity(json.dumps(RECORD, indent=2).encode()) == IDENTITY


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema", "dpone.tds.remote-session.v2"),
        ("session_id", True),
        ("session_id", 72.0),
        ("session_id", "72"),
        ("session_id", 0),
        ("session_id", 32768),
        ("connection_id", "11111111111111111111111111111111"),
        ("connection_id", "{11111111-1111-1111-1111-111111111111}"),
        ("connection_id", "00000000-0000-0000-0000-000000000000"),
        ("connect_time", "2026-01-02 03:04:05.006000"),
        ("connect_time", "2026-01-02T03:04:05.006"),
        ("login_time", "2026-01-02T03:04:05.009000+00:00"),
        ("login_time", "2026-01-02T03:04:05.009000Z"),
        ("login_time", "1900-01-01T00:00:00.000000"),
        ("nonce", "00" * 32),
        ("nonce", "6E" * 32),
        ("nonce", "6e " * 32),
        ("nonce", ["6e"] * 32),
        ("authority_sha256", "61" * 31),
        ("authority_sha256", None),
    ],
)
def test_noncanonical_or_invalid_value_rejected(field, value):
    with pytest.raises(ValueError, match="mssql_native.tds_session_record_invalid"):
        decode_session_identity(json.dumps(dict(RECORD, **{field: value})).encode())


@pytest.mark.parametrize("field", tuple(RECORD))
def test_missing_field_rejected(field):
    record = dict(RECORD)
    del record[field]
    with pytest.raises(ValueError):
        decode_session_identity(json.dumps(record).encode())


@pytest.mark.parametrize(
    "body",
    [
        b"{}",
        b"[]",
        b"null",
        b"{",
        b"\xff",
        b" " * 1025,
        b'{"schema":"x","schema":"y"}',
        b'{"session_id":NaN}',
    ],
)
def test_closed_bounded_json(body):
    with pytest.raises(ValueError, match="mssql_native.tds_session_record_invalid"):
        decode_session_identity(body)


def test_extra_fields_and_private_values_never_enter_error():
    record = dict(RECORD, password="private-canary")
    with pytest.raises(ValueError) as caught:
        decode_session_identity(json.dumps(record).encode())
    assert str(caught.value) == "mssql_native.tds_session_record_invalid"
    assert caught.value.__cause__ is None


@pytest.mark.parametrize("body", ["", bytearray(b"{}"), memoryview(b"{}"), None])
def test_non_bytes_rejected(body):
    with pytest.raises(ValueError):
        decode_session_identity(body)


def test_encode_rejects_mapping_and_timezone_aware_record():
    with pytest.raises(ValueError):
        encode_session_identity(RECORD)
    with pytest.raises(ValueError):
        TdsRemoteSessionIdentity(
            IDENTITY.connection_id,
            72,
            datetime(2026, 1, 1, tzinfo=UTC),
            IDENTITY.login_time,
            IDENTITY.nonce,
            IDENTITY.authority_sha256,
        )
