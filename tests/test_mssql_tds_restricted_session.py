"""Restricted session continuity avoids SQL Server 2022 server-state grants."""

from datetime import datetime
from uuid import UUID

import pytest

from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.adapters.mssql_tds_restricted_session import (
    _INITIALIZE,
    _OBSERVE,
    TdsRestrictedWriterSession,
)
from dpone.adapters.mssql_tds_session import TdsSessionObservationError

NONCE = b"n" * 32
CONNECTION = UUID("11111111-1111-1111-1111-111111111111")
DATABASE = UUID("22222222-2222-2222-2222-222222222222")
STAMP = datetime(2026, 1, 1, 12, 0, 0)


def observation():
    return [
        72,
        STAMP,
        NONCE,
        0,
        0,
        0,
        "server",
        "machine",
        "instance",
        "replica",
        "database",
        5,
        DATABASE,
        "login",
        b"sid",
        "original",
        b"originalsid",
        "user",
        1,
        b"usersid",
    ]


class Cursor:
    def __init__(self):
        self.row = observation()
        self.description = None
        self.rows = []

    def execute(self, sql, *parameters):
        self.rows = [] if sql == _INITIALIZE else [self.row]
        self.description = None if sql == _INITIALIZE else (("column",),)

    def nextset(self):
        assert not self.rows
        return None

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


def connection(cursor):
    return TdsSqlConnection(
        object(),
        cursor,
        client_connection_id=CONNECTION,
        client_session_id=72,
    )


def test_restricted_identity_uses_client_connection_id_and_own_session_only():
    assert "dm_exec_connections" not in _OBSERVE
    assert "dm_exec_requests" not in _OBSERVE
    assert "dm_tran_session_transactions" not in _OBSERVE
    cursor = Cursor()
    guard = TdsRestrictedWriterSession(connection(cursor))
    identity = guard.initialize(NONCE)
    assert (identity.client_connection_id, identity.session_id) == (CONNECTION, 72)
    assert identity.login_time == STAMP
    guard.require_same(identity)


def test_restricted_identity_rejects_client_and_server_spid_disagreement():
    cursor = Cursor()
    cursor.row[0] = 73
    with pytest.raises(TdsSessionObservationError, match="unavailable"):
        TdsRestrictedWriterSession(connection(cursor)).initialize(NONCE)
