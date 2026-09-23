"""Compatibility exports for restricted-session departure encoding."""

from dpone.contracts.mssql_sqlclient_restricted_session_departure import (
    decode_restricted_session_departure,
    encode_restricted_session_departure,
)

__all__ = ("decode_restricted_session_departure", "encode_restricted_session_departure")
