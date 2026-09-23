"""Actual existing SQL authority handle with a complete-result scripted cursor."""

import json
from dataclasses import replace
from time import monotonic
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.adapters import mssql_sqlclient_grant_catalog as module
from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import OWN_INCARNATION_SQL
from dpone.adapters.mssql_sqlclient_observer_sql import ADMISSION_SQL
from dpone.adapters.mssql_sqlclient_stage_catalog_sql import COLUMNS_SQL, FEATURES_SQL, OBJECT_SQL
from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.adapters.mssql_tds_coordinator_sql import TdsCoordinatorSql
from dpone.contracts.mssql_sqlclient_observation import SqlClientPrincipalResolution, SqlClientSessionAuthority
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from tests.mssql_sqlclient_departure_v2_fixtures import sample
from tests.test_mssql_sqlclient_observation import admission
from tests.test_mssql_sqlclient_observer_incarnation_adapter import own_row
from tests.test_mssql_sqlclient_stage_identity import stage
from tests.test_mssql_sqlclient_stage_locator import DATABASE, LOCATOR, OPERATION, OWNER, PARENT, SERVER
from tests.test_mssql_tds_coordinator import PROCESS
from tests.test_mssql_tds_coordinator_sql import Cursor as AuthorityCursor
from tests.test_mssql_tds_session import NONCE


def contexts():
    writer = admission()
    writer = replace(
        writer,
        server=SERVER,
        database=replace(
            writer.database,
            database_name=DATABASE.name,
            database_id=DATABASE.database_id,
            database_guid=str(DATABASE.database_guid),
        ),
    )
    management = replace(
        writer,
        login=replace(
            writer.login,
            principal_id=301,
            name="manager",
            sid="cc",
            original_name="manager",
            original_sid="cc",
            is_sysadmin=True,
        ),
    )
    own = sample().observer
    own = replace(
        own,
        authority=SqlClientSessionAuthority(
            management.server,
            management.database,
            management.login,
            management.transport,
            SqlClientPrincipalResolution("sysadmin_dbo", 1, "dbo", management.database.owner_sid),
        ),
        visibility=replace(own.visibility, database_id=DATABASE.database_id),
    )
    return writer, management, own


class Cursor(AuthorityCursor):
    def __init__(self):
        super().__init__()
        self.writer, self.management, self.own = contexts()
        self.stage = replace(
            stage(),
            database_guid=DATABASE.database_guid,
            database_id=DATABASE.database_id,
            database_name=DATABASE.name,
            schema_name=PARENT.schema,
            table_name=PARENT.table,
            owner_binding=PARENT.owner_binding,
            object_nonce=LOCATOR.object_nonce,
        )
        self.database = [
            DATABASE.name,
            DATABASE.database_id,
            DATABASE.database_guid,
            self.stage.schema_id,
            PARENT.schema,
        ]
        own = self.own
        self.session_row[0:4] = [own.connection_id, own.session_id, own.connect_time, own.login_time]
        self.session_row[7] = own.connection_id
        self.session_row[13:20] = [
            SERVER.server_name,
            SERVER.machine_name,
            SERVER.instance_name,
            SERVER.physical_machine_name,
            DATABASE.name,
            DATABASE.database_id,
            DATABASE.database_guid,
        ]
        self.session_row[20:27] = ["manager", b"\xcc", "manager", b"\xcc", "dbo", 1, b"\xbb"]
        self.permissions = [(1, self.stage.object_id, 0, 5, 1, "SL", "SELECT", "G")]
        self.mutate = lambda sql, rows: rows
        self.extra = None
        self.fetch_error = False
        self.current_sql = None
        self.profile_reads = 0

    def execute(self, sql, *args):
        self.current_sql = sql
        e, a = self.stage, self.writer
        if sql == OWN_INCARNATION_SQL:
            self.profile_reads += 1
            row = own_row(self.own)
            row[51], row[57:62] = 1, [None] * 5
            rows = [row]
        elif sql == ADMISSION_SQL:
            rows = [
                (
                    *vars_server(a.server),
                    a.database.database_id,
                    a.database.database_name,
                    UUID(a.database.database_guid),
                    bytes.fromhex(a.database.owner_sid),
                    a.login.principal_id,
                    a.login.name,
                    bytes.fromhex(a.login.sid),
                    int(a.login.is_sysadmin),
                    "SQL_LOGIN",
                )
            ]
        elif sql == module.PRINCIPALS_SQL:
            rows = [
                (0, "public", b"\x00", "DATABASE_ROLE", "NONE"),
                (5, "writer_user", b"\xaa", "SQL_USER", "INSTANCE"),
            ]
        elif sql == module.PERMISSION_COUNT_SQL:
            rows = [(len(self.permissions),)]
        elif sql == module.PERMISSIONS_SQL:
            rows = self.permissions
        elif sql == module.MEMBER_SQL:
            rows = [(e.object_id, e.schema_id, e.schema_name, e.table_name, "USER_TABLE", e.create_date)]
        elif sql == module.BATCH_MEMBERS_SQL:
            ids = json.loads(args[0])
            rows = [
                (
                    object_id,
                    e.schema_id,
                    e.schema_name,
                    e.table_name if object_id == e.object_id else f"unrelated_{object_id}",
                    "USER_TABLE" if object_id == e.object_id else "VIEW",
                    e.create_date,
                )
                for object_id in ids
            ]
        elif sql == OBJECT_SQL:
            rows = [(e.object_id, e.table_name, e.create_date, e.owner_binding, str(e.object_nonce), 0, 0, 0, 0)]
        elif sql == module.BATCH_OBJECTS_SQL:
            ids = json.loads(args[0])
            rows = [
                (object_id, e.table_name, e.create_date, e.owner_binding, str(e.object_nonce), 0, 0, 0, 0)
                for object_id in ids
            ]
        elif sql == FEATURES_SQL:
            rows = [(0,) * 12]
        elif sql == module.BATCH_FEATURES_SQL:
            rows = [(object_id, *((0,) * 12)) for object_id in json.loads(args[0])]
        elif sql == COLUMNS_SQL:
            rows = [
                (
                    c.ordinal,
                    c.name,
                    c.type.value,
                    c.nullable,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.collation,
                    0,
                    False,
                    False,
                )
                for c in e.columns
            ]
        elif sql == module.BATCH_COLUMNS_SQL:
            rows = [
                (
                    object_id,
                    c.ordinal,
                    c.name,
                    c.type.value,
                    c.nullable,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.collation,
                    0,
                    False,
                    False,
                )
                for object_id in json.loads(args[0])
                for c in e.columns
            ]
        else:
            return super().execute(sql, *args)
        self.calls.append((sql, args))
        self.rows = [tuple(r) for r in self.mutate(sql, rows)]

    def fetchone(self):
        if self.fetch_error and self.current_sql == module.PERMISSIONS_SQL:
            raise OSError("synthetic driver failure")
        return super().fetchone()

    def nextset(self):
        return self.extra


def vars_server(server):
    return (server.server_name, server.machine_name, server.instance_name, server.physical_machine_name)


def sql_owner(cursor=None):
    cursor = cursor or Cursor()
    connection = TdsSqlConnection(SimpleNamespace(close=lambda: None), cursor)
    sql = TdsCoordinatorSql(connection, replace(OPERATION, command=TdsCoordinatorCommand.OBSERVE), OWNER, PROCESS)
    sql.acquire(NONCE, deadline=monotonic() + 5)
    return sql


def test_permission_sql_retains_all_states_classes_minors_and_uses_parameters():
    cursor = Cursor()
    sql = sql_owner(cursor)
    cursor.permissions = [(1, -1, 3, 0, 1, "SL", "SELECT", "D"), (42, 0, 0, 5, 1, "AB", "UNKNOWN FUTURE", "R")]
    catalog = module.SqlClientGrantCatalog(sql, deadline=monotonic() + 2)
    assert catalog.permissions(5, 0, limit=10) == cursor.permissions
    call = next(p for q, p in cursor.calls if q == module.PERMISSIONS_SQL)
    assert call == (11, 5, 0)
    assert "WHERE grantee_principal_id IN (?,?)" in module.PERMISSIONS_SQL
    assert (
        "SELECT TOP (?) class,major_id,minor_id,grantee_principal_id,grantor_principal_id,type,permission_name,state"
        in module.PERMISSIONS_SQL
    )


@pytest.mark.parametrize(
    "fault", ["omitted", "extra_result", "limit", "driver", "bad_count", "count_drift", "extra_row"]
)
def test_incomplete_inventory_poisoned_without_retry(fault):
    cursor = Cursor()
    sql = sql_owner(cursor)
    catalog = module.SqlClientGrantCatalog(sql, deadline=monotonic() + 2)
    count = 0

    def mutate(statement, rows):
        nonlocal count
        if statement == module.PERMISSION_COUNT_SQL:
            count += 1
            if fault == "bad_count":
                return [(True,)]
            if fault == "count_drift" and count == 2:
                return [(0,)]
        if statement == module.PERMISSIONS_SQL:
            if fault == "omitted":
                return []
            if fault == "extra_row":
                return rows * 2
        return rows

    cursor.mutate = mutate
    if fault == "extra_result":
        cursor.extra = True
    elif fault == "limit":
        cursor.permissions *= 2
    elif fault == "driver":
        cursor.fetch_error = True
    with pytest.raises((RuntimeError, ValueError)):
        catalog.permissions(5, 0, limit=1)
    before = len(cursor.calls)
    with pytest.raises(RuntimeError):
        catalog.permissions(5, 0, limit=1)
    assert len(cursor.calls) == before


def test_identifier_bytes_are_only_parameters():
    sql = sql_owner()
    catalog = module.SqlClientGrantCatalog(sql, deadline=monotonic() + 2)
    name = "odd]; DROP DATABASE x;--"
    catalog.principals(name, b"\xaa")
    catalog.object_properties(1, name)
    assert [(q, p) for q, p in sql.cursor.calls if name in p] == [
        (module.PRINCIPALS_SQL, (name, b"\xaa")),
        (OBJECT_SQL, (1, name)),
    ]


def test_actual_nchar4_permission_codes_are_canonicalized_at_catalog_boundary():
    cursor = Cursor()
    cursor.permissions = [(0, 0, 0, 0, 1, code, "CATALOG PERMISSION", "G") for code in ("CO  ", "SL  ", "VWCK", "VWCM")]
    observed = module.SqlClientGrantCatalog(sql_owner(cursor), deadline=monotonic() + 2).permissions(5, 0, limit=4)
    assert [row[5] for row in observed] == ["CO", "SL", "VWCK", "VWCM"]


def test_exact_maximum_permission_rows_are_complete():
    cursor = Cursor()
    cursor.permissions = [(42, index, 0, 5, 1, "AB  ", "UNCLASSIFIED", "D") for index in range(4096)]
    observed = module.SqlClientGrantCatalog(sql_owner(cursor), deadline=monotonic() + 3).permissions(5, 0, limit=4096)
    assert [row[5] for row in observed] == ["AB"] * 4096


def test_batch_member_projection_uses_one_fixed_openjson_statement_for_1024_ids():
    cursor = Cursor()
    catalog = module.SqlClientGrantCatalog(sql_owner(cursor), deadline=monotonic() + 3)
    object_ids = tuple(range(1, 1025))
    rows = catalog.members(object_ids)
    assert len(rows) == 1024
    calls = [(sql, args) for sql, args in cursor.calls if sql == module.BATCH_MEMBERS_SQL]
    assert calls == [(module.BATCH_MEMBERS_SQL, (json.dumps(object_ids, separators=(",", ":")),))]
    assert "OPENJSON(?)" in module.BATCH_MEMBERS_SQL
    assert "OPENJSON(?)" in module.BATCH_OBJECTS_SQL
    assert "OPENJSON(?)" in module.BATCH_FEATURES_SQL
    assert "OPENJSON(?)" in module.BATCH_COLUMNS_SQL


@pytest.mark.parametrize("ids", [(), (2, 1), (1, 1), (True,), (0,), tuple(range(1, 83))])
def test_column_batch_rejects_noncanonical_or_over_bound_ids(ids):
    catalog = module.SqlClientGrantCatalog(sql_owner(), deadline=monotonic() + 2)
    with pytest.raises(ValueError, match="^mssql_native.sqlclient_grant_catalog_unavailable$"):
        catalog.columns_batch(ids)


def test_extra_resultset_and_owner_reentry_poison_before_more_effects():
    cursor = Cursor()
    catalog = module.SqlClientGrantCatalog(sql_owner(cursor), deadline=monotonic() + 2)
    original = cursor.fetchone
    reentered = False

    def fetch():
        nonlocal reentered
        if cursor.current_sql == module.PERMISSIONS_SQL and not reentered:
            reentered = True
            with pytest.raises(RuntimeError):
                catalog.permissions(5, 0, limit=1)
        return original()

    cursor.fetchone = fetch
    with pytest.raises(RuntimeError):
        catalog.permissions(5, 0, limit=1)
    assert reentered


def test_catalog_facade_delegates_fixed_query_projection_responsibility():
    from dpone.adapters.mssql_sqlclient_grant_catalog_projection import SqlClientGrantCatalogProjection

    assert issubclass(module.SqlClientGrantCatalog, SqlClientGrantCatalogProjection)
    assert "members" not in module.SqlClientGrantCatalog.__dict__
    assert "members" in SqlClientGrantCatalogProjection.__dict__
