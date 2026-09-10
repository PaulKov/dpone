"""Offline capture provenance contracts; observed metadata never certifies SQL."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dpone.adapters.composition_mssql_gate_schema import login_trigger_sql
from tests.integration.composition import mssql_gate_live_provisioning as provisioning
from tests.integration.composition import mssql_store_live_support as support
from tests.integration.composition.mssql_gate_live_provisioning import ProvisionedGate, SqlFailure


def observed_cursor(rows):
    return SimpleNamespace(
        execute=Mock(), description=("columns",), fetchall=Mock(return_value=rows), nextset=Mock(return_value=False)
    )


@pytest.mark.parametrize("version", ["16.0.4265.3", None])
def test_capture_preserves_actual_nondefault_database_session_and_unknown_version(version):
    cursor = observed_cursor([("owned", 130, 32, version, 59)])
    assert support.catalog_context(cursor, "owned") == {
        "database": "owned",
        "compatibility_level": 130,
        "options_mask": 32,
        "product_version": version,
        "product_version_status": "UNVERIFIED" if version is None else "OBSERVED",
        "session_id": 59,
    }
    sql = cursor.execute.call_args.args[0]
    assert "DB_NAME()" in sql and "d.compatibility_level" in sql and "@@OPTIONS" in sql and "@@SPID" in sql
    assert "SERVERPROPERTY('ProductVersion')" in sql and "d.database_id=DB_ID()" in sql
    assert cursor.nextset.call_count == 1


@pytest.mark.parametrize("rows", [[], [("owned",)], [("owned", 130, 32, None, 59)] * 2])
def test_absent_or_ambiguous_capture_is_refused(rows):
    with pytest.raises(RuntimeError, match="^synthetic_catalog_context$"):
        support.catalog_context(observed_cursor(rows), "owned")


@pytest.mark.parametrize(
    "index,value",
    [
        (0, "foreign"),
        (0, "owned\n"),
        (1, None),
        (1, True),
        (1, 256),
        (2, None),
        (2, True),
        (2, -1),
        (3, "private-driver"),
        (3, "1" * 129),
        (4, 0),
        (4, True),
    ],
)
def test_untrusted_observation_cannot_be_defaulted_into_provenance(index, value):
    row = ["owned", 130, 32, "16.0.4265.3", 59]
    row[index] = value
    with pytest.raises(RuntimeError, match="^synthetic_catalog_context$"):
        support.catalog_context(observed_cursor([row]), "owned")


def test_delayed_provenance_query_failure_cannot_return_partial_observation():
    cursor = observed_cursor([("owned", 130, 32, None, 59)])
    cursor.nextset.side_effect = RuntimeError("delayed failure")
    with pytest.raises(RuntimeError, match="^delayed failure$"):
        support.catalog_context(cursor, "owned")


def check_provisioning_connections(monkeypatch, fail_permission):
    environment = ProvisionedGate(SimpleNamespace(database="owned_control"))
    connections, statements = [], []

    def connect(*, database=None):
        connection = SimpleNamespace(database=database or "owned_control", closed=False)
        connection.close = lambda: setattr(connection, "closed", True)
        cursor = SimpleNamespace(description=None, nextset=lambda: False, close=lambda: None)
        cursor.execute = lambda statement, *parameters: observe(connection, statement, *parameters)
        connection.cursor = lambda: cursor
        connections.append(connection)
        return connection

    def observe(connection, statement, *_):
        statements.append((connection.database, statement))
        if statement.startswith("GRANT VIEW") and connection.database != "master":
            raise SqlFailure(4621)
        if fail_permission and statement == f"GRANT {fail_permission} TO [dpone_gate_reader];":
            raise SqlFailure(229)
        if "ON ALL SERVER" in statement:
            assert connection.database == "master"
        if "CREATE USER" in statement or statement.startswith("GRANT CONNECT"):
            assert connection.database == "owned_control"
        return (("sa", b"controller"),) if statement.startswith("SELECT ORIGINAL_LOGIN") else ()

    monkeypatch.setattr(environment, "connect", connect)
    monkeypatch.setattr(provisioning, "execute", observe)
    # This role/connection test supplies catalog results; capture has separate contracts.
    monkeypatch.setattr(provisioning, "capture_gate_check_catalog", lambda *_: {})
    monkeypatch.setattr(provisioning, "catalog_context", lambda *_: {})
    if fail_permission:
        with pytest.raises(SqlFailure) as failure:
            environment.install()
        assert failure.value.code == 229
    else:
        environment.install()
    server_grants = [(database, sql) for database, sql in statements if sql.startswith("GRANT VIEW")]
    permissions = ("VIEW SERVER STATE", "VIEW ANY DEFINITION", "VIEW SERVER PERFORMANCE STATE")
    expected = permissions[:2] if fail_permission else permissions
    assert server_grants == [("master", f"GRANT {permission} TO [dpone_gate_reader];") for permission in expected]
    if not fail_permission:
        assert ("master", login_trigger_sql("owned_control", environment.schema)) in statements
    lifeline = environment.lifeline
    assert lifeline is not None and lifeline.database == "owned_control" and not lifeline.closed
    assert all(connection.closed for connection in connections if connection is not environment.lifeline)
    environment.close()
