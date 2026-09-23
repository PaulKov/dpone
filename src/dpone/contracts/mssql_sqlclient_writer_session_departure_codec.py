"""Compatibility exports for writer-session departure encoding."""

from dpone.contracts.mssql_sqlclient_writer_session_departure import _admission as _admission  # noqa: F401
from dpone.contracts.mssql_sqlclient_writer_session_departure import (
    decode_writer_session_departure,
    encode_writer_session_departure,
)

__all__ = ("decode_writer_session_departure", "encode_writer_session_departure")
