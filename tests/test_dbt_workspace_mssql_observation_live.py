"""Disposable SQL Server certification for real catalog semantics and cleanup."""

from __future__ import annotations

import os
from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_sqlserver_macro_authority_baseline import DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256
from dpone.contracts.dbt_workspace_observation import (
    MssqlWorkspaceObservationRequest,
    WorkspaceObservationError,
    WorkspaceObservationLimits,
)
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.dbt_workspace_mssql_observer import MssqlWorkspaceCatalogObserver

pytestmark = [pytest.mark.integration_mssql, pytest.mark.integration_live]
_ENABLED = os.environ.get("DPONE_RUN_DBT_WORKSPACE_MSSQL_OBSERVATION_LIVE") == "1"
_DATABASES = {"DponeObservationCI": "SQL_Latin1_General_CP1_CI_AS", "DponeObservationCS": "Latin1_General_100_CS_AS"}
_READER = "dpone_observation_reader"


def _pyodbc():
    try:
        import pyodbc
    except ImportError:
        pytest.fail("Enabled workspace SQL Server certification requires pyodbc")
    if os.environ.get("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server") not in pyodbc.drivers():
        pytest.fail("Enabled workspace SQL Server certification requires the configured ODBC driver")
    pyodbc.pooling = False
    return pyodbc


def _raw_connection(database="master", *, user=None, password=None):
    module = _pyodbc()
    driver = os.environ.get("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")
    host = os.environ.get("DPONE_IT_MSSQL_HOST", "127.0.0.1")
    port = os.environ.get("DPONE_IT_MSSQL_PORT_FORWARD", "51433")
    user = user or os.environ.get("DPONE_IT_MSSQL_USER", "sa")
    password = password or os.environ["DPONE_IT_MSSQL_PASSWORD"]
    return module.connect(
        f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=10;",
        autocommit=True,
    )


def _drop_disposable_reader(cursor):
    """Fence only this fixture's sessions before removing its disposable login."""

    session_ids = [
        int(row[0])
        for row in cursor.execute(
            "SELECT session_id FROM sys.dm_exec_sessions WHERE login_name=? AND session_id<>@@SPID", _READER
        ).fetchall()
    ]
    for session_id in session_ids:
        cursor.execute(f"KILL {session_id}")
    cursor.execute(f"IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name=N'{_READER}') DROP LOGIN {_READER}")


@pytest.fixture(scope="module")
def live_catalogs():
    if not _ENABLED:
        pytest.skip("Set DPONE_RUN_DBT_WORKSPACE_MSSQL_OBSERVATION_LIVE=1")
    admin = _raw_connection()
    cursor = admin.cursor()
    try:
        for database, collation in _DATABASES.items():
            cursor.execute(
                f"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]; END"
            )
            cursor.execute(f"CREATE DATABASE [{database}] COLLATE {collation}")
            db = _raw_connection(database)
            db.execute("CREATE SCHEMA obs")
            db.execute("CREATE TABLE obs.[Orders] (id int NOT NULL)")
            db.execute("CREATE VIEW obs.AbsentCase AS SELECT id FROM obs.[Orders]")
            if database.endswith("CI"):
                db.execute(f"CREATE VIEW obs.LeftView AS SELECT id FROM [{database}].obs.[Orders]")
                db.execute(f"CREATE VIEW obs.RightView AS SELECT id FROM [{database}].obs.[Orders]")
                db.execute(
                    f"CREATE VIEW obs.BottomView AS SELECT l.id FROM [{database}].obs.LeftView l JOIN [{database}].obs.RightView r ON r.id=l.id"
                )
                db.execute("CREATE VIEW obs.TwoPartControl AS SELECT id FROM obs.[Orders]")
                db.execute("CREATE TABLE obs.[漢] (id int NOT NULL)")
            db.close()
        _drop_disposable_reader(cursor)
        cursor.execute(
            "CREATE LOGIN dpone_observation_reader WITH PASSWORD='Disposable.Reader.Pw.2026!', CHECK_POLICY=OFF"
        )
        cursor.execute("GRANT VIEW ANY DATABASE TO dpone_observation_reader")
        cursor.execute("GRANT VIEW SERVER STATE TO dpone_observation_reader")
        db = _raw_connection("DponeObservationCI")
        db.execute("CREATE USER dpone_observation_reader FOR LOGIN dpone_observation_reader")
        db.execute("DENY VIEW DEFINITION TO dpone_observation_reader")
        db.close()
        yield
    finally:
        _drop_disposable_reader(cursor)
        for database in _DATABASES:
            cursor.execute(
                f"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]; END"
            )
        admin.close()


def _pin(database):
    connection = _raw_connection(database)
    row = connection.execute(
        "SELECT DB_ID(), CONVERT(nvarchar(33), create_date, 126), CONVERT(nvarchar(36), database_guid) FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id=d.database_id WHERE d.name=?",
        database,
    ).fetchone()
    connection.close()
    return MssqlDatabaseAuthorityPin(database, int(row[0]), str(row[1]), UUID(str(row[2])))


def _connection(database, *, reader=False):
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(
            host=os.environ.get("DPONE_IT_MSSQL_HOST", "127.0.0.1"),
            port=int(os.environ.get("DPONE_IT_MSSQL_PORT_FORWARD", "51433")),
            database=database,
            username="dpone_observation_reader" if reader else os.environ.get("DPONE_IT_MSSQL_USER", "sa"),
            password="Disposable.Reader.Pw.2026!" if reader else os.environ["DPONE_IT_MSSQL_PASSWORD"],
            driver=os.environ.get("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            trust_server_certificate=True,
            query_timeout=10,
        ),
        safe_metadata={},
        descriptor=ResolvedConnectionDescriptor("mssql", {}),
    )


def _write(database, relation):
    return DbtRelationWrite(
        "dbt/live", "observe", f"model.live.{relation}", "model", "mssql", "warehouse", database, "obs", relation
    )


def _request(database, *writes, limits=None):
    return MssqlWorkspaceObservationRequest(
        "sha256:" + "a" * 64,
        "sha256:" + "b" * 64,
        DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
        _pin(database),
        database,
        (database,),
        tuple(writes),
        limits or WorkspaceObservationLimits(),
    )


def _object_count(database):
    connection = _raw_connection(database)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM sys.objects").fetchone()[0])
    finally:
        connection.close()


@pytest.mark.parametrize("database,same_class", [("DponeObservationCI", True), ("DponeObservationCS", False)])
def test_live_catalog_uses_actual_ci_cs_semantics_and_preserves_read_only_state(live_catalogs, database, same_class):
    before = _object_count(database)
    request = _request(
        database,
        _write(database, "Orders"),
        _write(database, "orders"),
        replace(_write(database, "DoesNotExist"), database=None),
    )
    result = MssqlWorkspaceCatalogObserver().observe(request, _connection(database))
    after = _object_count(database)
    classes = [slot.equivalence_class for slot in result.slots[:2]]
    assert (classes[0] == classes[1]) is same_class
    assert result.slots[2].object_id is None
    assert before == after
    if same_class:
        assert {edge.object_name for edge in result.dependencies} == {"LeftView", "RightView", "BottomView"}
        # CI-equivalent spellings are both source rows; raw edge budgets retain
        # both traversals even though child view nodes are visited only once.
        assert len(result.dependencies) == 6
        assert "TwoPartControl" not in {edge.object_name for edge in result.dependencies}


def test_live_catalog_fails_closed_for_permissions_unicode_and_incomplete_graph(live_catalogs):
    database = "DponeObservationCI"
    observer = MssqlWorkspaceCatalogObserver()
    request = _request(database, _write(database, "Orders"))
    with pytest.raises(WorkspaceObservationError, match="metadata_permission"):
        observer.observe(request, _connection(database, reader=True))
    with pytest.raises(WorkspaceObservationError, match="macro_literal"):
        observer.observe(_request(database, _write(database, "漢")), _connection(database))
    with pytest.raises(WorkspaceObservationError, match="edge_budget"):
        observer.observe(replace(request, limits=replace(request.limits, max_edges=1)), _connection(database))
    with pytest.raises(WorkspaceObservationError, match="depth_budget"):
        observer.observe(replace(request, limits=replace(request.limits, max_depth=1)), _connection(database))
