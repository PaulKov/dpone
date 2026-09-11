"""Native imports retain the exact shared SQL-control capability and behavior."""

from dpone.adapters import dbapi_lifecycle
from dpone.adapters import dbt_workspace_mssql_activation_connection as legacy
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


def test_native_connection_boundary_is_an_identity_preserving_compatibility_export():
    assert legacy.WorkspaceActivationConnection is SqlControlConnection
    assert legacy.WorkspaceActivationCursor is SqlControlCursor
    assert legacy.row is dbapi_lifecycle.row
    assert legacy.close is dbapi_lifecycle.close
    assert legacy.rollback is dbapi_lifecycle.rollback
