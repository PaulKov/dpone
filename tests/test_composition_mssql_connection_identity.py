"""Independent database and service pins must match on the business connection."""

from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.adapters.composition_mssql_connection_identity import require_mssql_connection_identity
from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_SCHEMA_VERSION
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin

SERVICE = "11111111-1111-1111-1111-111111111111"
PIN = MssqlDatabaseAuthorityPin("warehouse", 7, "2026-01-01T00:00:00", UUID("22222222-2222-2222-2222-222222222222"))
DATABASE = (PIN.database_name, PIN.database_id, str(PIN.database_guid), PIN.create_token, 0)
MARKER = (1, COMPOSITION_MSSQL_SCHEMA_VERSION, SERVICE)


class Cursor:
    def __init__(self, rows):
        self.rows, self.queries, self.closed = iter(rows), [], False

    def execute(self, sql, *params):
        self.queries.append((sql, params))

    def fetchall(self):
        return next(self.rows)

    def close(self):
        self.closed = True


def verify(cursor):
    require_mssql_connection_identity(
        SimpleNamespace(cursor=lambda: cursor),
        pins=(PIN,),
        control_database="control",
        control_schema="custom",
        service_id=SERVICE,
    )


def test_exact_database_and_control_marker_checked_on_same_connection():
    cursor = Cursor([[DATABASE], [MARKER]])
    verify(cursor)
    assert cursor.closed
    assert cursor.queries[0][1] == ("warehouse",)
    assert "[control].[custom].[composition_authority]" in cursor.queries[1][0]


@pytest.mark.parametrize(
    "rows",
    [
        [[], [MARKER]],
        [[(*DATABASE[:-1], 1)], [MARKER]],
        [[DATABASE, DATABASE], [MARKER]],
        [[DATABASE], [(1, COMPOSITION_MSSQL_SCHEMA_VERSION, "33333333-3333-3333-3333-333333333333")]],
        [[DATABASE], []],
    ],
)
def test_missing_foreign_or_duplicate_original_rejected(rows):
    cursor = Cursor(rows)
    with pytest.raises(CompositionAdmissionError):
        verify(cursor)
    assert cursor.closed
