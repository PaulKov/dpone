"""Immutable remote-session continuity observations, never SQL mutation authority."""

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


def require_session_nonce(nonce: bytes) -> None:
    """Require a nonzero, fixed-size nonce; the composition root supplies randomness."""
    if type(nonce) is not bytes or len(nonce) != 32 or not any(nonce):
        raise ValueError("mssql_native.tds_session_nonce_invalid")


@dataclass(frozen=True, slots=True)
class TdsRemoteSessionIdentity:
    """Bind physical connection, login, nonce and canonical SQL authority facts.

    SQL timestamps remain naive server values; they are not interpreted as UTC.
    A connection UUID may survive pooling. Neither it nor the mutable nonce alone
    authenticates a session or proves remote settlement. The coordinator must
    persist the complete record before separately authorizing any mutation.
    """

    connection_id: UUID
    session_id: int
    connect_time: datetime
    login_time: datetime
    nonce: bytes
    authority_sha256: bytes

    def __post_init__(self) -> None:
        require_session_nonce(self.nonce)
        if type(self.connection_id) is not UUID or self.connection_id.int == 0:
            raise ValueError("mssql_native.tds_session_identity_invalid")
        if type(self.session_id) is not int or not 1 <= self.session_id <= 32767:
            raise ValueError("mssql_native.tds_session_identity_invalid")
        for stamp in (self.connect_time, self.login_time):
            if type(stamp) is not datetime or stamp.tzinfo is not None or stamp <= datetime(1900, 1, 1):
                raise ValueError("mssql_native.tds_session_identity_invalid")
        if type(self.authority_sha256) is not bytes or len(self.authority_sha256) != 32:
            raise ValueError("mssql_native.tds_session_identity_invalid")


@dataclass(frozen=True, slots=True)
class TdsRestrictedRemoteSessionIdentity:
    """Least-privilege session identity with a client diagnostic UUID.

    ``client_connection_id`` is the ODBC same-handle/no-reconnect diagnostic
    identifier.  It is deliberately nominally distinct from SQL Server's
    ``sys.dm_exec_connections.connection_id`` and must never be used in a DMV
    UUID predicate.  Independent settlement uses the exact SPID/login epoch.
    """

    client_connection_id: UUID
    session_id: int
    login_time: datetime
    nonce: bytes
    authority_sha256: bytes

    def __post_init__(self) -> None:
        require_session_nonce(self.nonce)
        if type(self.client_connection_id) is not UUID or self.client_connection_id.int == 0:
            raise ValueError("mssql_native.tds_restricted_session_identity_invalid")
        if type(self.session_id) is not int or not 1 <= self.session_id <= 32767:
            raise ValueError("mssql_native.tds_restricted_session_identity_invalid")
        if (
            type(self.login_time) is not datetime
            or self.login_time.tzinfo is not None
            or self.login_time <= datetime(1900, 1, 1)
        ):
            raise ValueError("mssql_native.tds_restricted_session_identity_invalid")
        if type(self.authority_sha256) is not bytes or len(self.authority_sha256) != 32:
            raise ValueError("mssql_native.tds_restricted_session_identity_invalid")


def encode_session_identity(identity: TdsRemoteSessionIdentity) -> bytes:
    """Encode the closed v1 record for durable registration and phase IPC.

    Authority facts are represented by their canonical digest, not server names
    or principal details. Timestamps have six fractional digits and no invented
    timezone; nonce bytes retain leading and trailing zeros.
    """
    if type(identity) is not TdsRemoteSessionIdentity:
        raise ValueError("mssql_native.tds_session_record_invalid")
    return canonical_json_bytes(
        {
            "schema": "dpone.tds.remote-session.v1",
            "connection_id": str(identity.connection_id),
            "session_id": identity.session_id,
            "connect_time": identity.connect_time.isoformat(timespec="microseconds"),
            "login_time": identity.login_time.isoformat(timespec="microseconds"),
            "nonce": identity.nonce.hex(),
            "authority_sha256": identity.authority_sha256.hex(),
        }
    )


def decode_session_identity(payload: bytes) -> TdsRemoteSessionIdentity:
    """Reject duplicate/extra fields and noncanonical scalar spellings before use."""
    try:
        if type(payload) is not bytes or len(payload) > 1024:
            raise ValueError("record_size")
        value = strict_json_object(payload)
        if (
            set(value)
            != {"schema", "connection_id", "session_id", "connect_time", "login_time", "nonce", "authority_sha256"}
            or value["schema"] != "dpone.tds.remote-session.v1"
        ):
            raise ValueError("record_fields")
        for field in ("connection_id", "connect_time", "login_time", "nonce", "authority_sha256"):
            if type(value[field]) is not str:
                raise ValueError("scalar_type")
        identity = TdsRemoteSessionIdentity(
            UUID(value["connection_id"]),
            value["session_id"],
            datetime.fromisoformat(value["connect_time"]),
            datetime.fromisoformat(value["login_time"]),
            bytes.fromhex(value["nonce"]),
            bytes.fromhex(value["authority_sha256"]),
        )
        # JSON whitespace is immaterial; scalar aliases are not allowed to give
        # one stored identity multiple timestamp, UUID or binary spellings.
        if strict_json_object(encode_session_identity(identity)) != value:
            raise ValueError("noncanonical_scalars")
        return identity
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError("mssql_native.tds_session_record_invalid") from None


def encode_restricted_session_identity(identity: TdsRestrictedRemoteSessionIdentity) -> bytes:
    """Encode the closed restricted-session record without server UUID claims."""
    if type(identity) is not TdsRestrictedRemoteSessionIdentity:
        raise ValueError("mssql_native.tds_restricted_session_record_invalid")
    identity.__post_init__()
    return canonical_json_bytes(
        {
            "schema": "dpone.tds.restricted-remote-session.v1",
            "client_connection_id": str(identity.client_connection_id),
            "session_id": identity.session_id,
            "login_time": identity.login_time.isoformat(timespec="microseconds"),
            "nonce": identity.nonce.hex(),
            "authority_sha256": identity.authority_sha256.hex(),
        }
    )


def decode_restricted_session_identity(payload: bytes) -> TdsRestrictedRemoteSessionIdentity:
    """Decode one canonical client-ID/SPID/login-epoch identity."""
    try:
        if type(payload) is not bytes or len(payload) > 1024:
            raise ValueError("record_size")
        value = strict_json_object(payload)
        if (
            set(value) != {"schema", "client_connection_id", "session_id", "login_time", "nonce", "authority_sha256"}
            or value["schema"] != "dpone.tds.restricted-remote-session.v1"
        ):
            raise ValueError("record_fields")
        for field in ("client_connection_id", "login_time", "nonce", "authority_sha256"):
            if type(value[field]) is not str:
                raise ValueError("scalar_type")
        identity = TdsRestrictedRemoteSessionIdentity(
            UUID(value["client_connection_id"]),
            value["session_id"],
            datetime.fromisoformat(value["login_time"]),
            bytes.fromhex(value["nonce"]),
            bytes.fromhex(value["authority_sha256"]),
        )
        if strict_json_object(encode_restricted_session_identity(identity)) != value:
            raise ValueError("noncanonical_scalars")
        return identity
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError("mssql_native.tds_restricted_session_record_invalid") from None


TdsSessionIdentity = TdsRemoteSessionIdentity | TdsRestrictedRemoteSessionIdentity


def encode_session_continuity_identity(identity: TdsSessionIdentity) -> bytes:
    """Dispatch without converting one identity domain into the other."""
    if type(identity) is TdsRemoteSessionIdentity:
        return encode_session_identity(identity)
    if type(identity) is TdsRestrictedRemoteSessionIdentity:
        return encode_restricted_session_identity(identity)
    raise ValueError("mssql_native.tds_session_record_invalid")


def decode_session_continuity_identity(payload: bytes) -> TdsSessionIdentity:
    """Decode either closed schema while preserving its nominal type."""
    try:
        schema = strict_json_object(payload).get("schema")
    except (ValueError, TypeError, RecursionError):
        raise ValueError("mssql_native.tds_session_record_invalid") from None
    if schema == "dpone.tds.remote-session.v1":
        return decode_session_identity(payload)
    if schema == "dpone.tds.restricted-remote-session.v1":
        return decode_restricted_session_identity(payload)
    raise ValueError("mssql_native.tds_session_record_invalid")


def coordinator_authority_digest(authority_values: Sequence[Any]) -> bytes:
    """Encode the original fourteen-field coordinator authority without SQL effects.

    Order: four server names; database name, ID and UUID; current login name/SID;
    original login name/SID; database user name, ID and SID. This preserves the
    existing ASCII JSON list and digest domain, including its historical text
    limits. It is distinct from SqlClient session authority. Matching bytes alone
    does not authenticate a caller or establish remote-session exclusion.
    """
    if len(authority_values) != 14:
        raise ValueError("authority_width_invalid")
    row = authority_values
    for index in (0, 1, 2, 3, 4, 7, 9, 11):
        value = row[index]
        if type(value) is not str or not 1 <= len(value) <= 128 or any(ord(c) < 32 for c in value):
            raise ValueError("authority_text_invalid")
        value.encode("utf-8", errors="strict")
    for index in (5, 12):
        if type(row[index]) is not int or not 1 <= row[index] <= 2**31 - 1:
            raise ValueError("authority_integer_invalid")
    if type(row[6]) is not UUID or not row[6].int:
        raise ValueError("database_incarnation_invalid")
    for index in (8, 10, 13):
        if type(row[index]) is not bytes or not 1 <= len(row[index]) <= 85:
            raise ValueError("authority_sid_invalid")
    authority = list(row)
    authority[6] = str(row[6])
    for index in (8, 10, 13):
        authority[index] = row[index].hex()
    payload = json.dumps(["dpone.tds.session-authority.v1", *authority], ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("ascii")).digest()
