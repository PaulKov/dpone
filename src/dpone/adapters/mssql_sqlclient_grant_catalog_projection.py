"""Fixed SQL Server grant-catalog projections over an owned observation lease."""

import json
from abc import abstractmethod
from contextlib import AbstractContextManager
from typing import Any

from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import OWN_INCARNATION_SQL
from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
from dpone.adapters.mssql_sqlclient_observer_incarnation import require_own_incarnation_statement
from dpone.adapters.mssql_sqlclient_observer_sql import ADMISSION_SQL
from dpone.adapters.mssql_sqlclient_stage_catalog_sql import COLUMNS_SQL, FEATURES_SQL, OBJECT_SQL

PRINCIPALS_SQL = """
SELECT principal_id,name,sid,type_desc,authentication_type_desc
FROM sys.database_principals WHERE name=? OR sid=? OR name=N'public'
ORDER BY principal_id,name,sid,type_desc,authentication_type_desc;
"""
PERMISSION_COUNT_SQL = """
SELECT COUNT_BIG(*) FROM sys.database_permissions WHERE grantee_principal_id IN (?,?);
"""
PERMISSIONS_SQL = """
SELECT TOP (?) class,major_id,minor_id,grantee_principal_id,grantor_principal_id,type,permission_name,state
FROM sys.database_permissions WHERE grantee_principal_id IN (?,?)
ORDER BY class,major_id,minor_id,grantee_principal_id,grantor_principal_id,type,permission_name,state;
"""
MEMBER_SQL = """
SELECT o.object_id,o.schema_id,s.name,o.name,o.type_desc,o.create_date
FROM sys.objects o LEFT JOIN sys.schemas s ON s.schema_id=o.schema_id WHERE o.object_id=?;
"""
BATCH_MEMBERS_SQL = """
WITH ids AS (SELECT CONVERT(int,[value]) AS object_id FROM OPENJSON(?))
SELECT o.object_id,o.schema_id,s.name,o.name,o.type_desc,o.create_date
FROM ids JOIN sys.objects o ON o.object_id=ids.object_id
LEFT JOIN sys.schemas s ON s.schema_id=o.schema_id ORDER BY o.object_id;
"""
BATCH_OBJECTS_SQL = """
WITH ids AS (SELECT CONVERT(int,[value]) AS object_id FROM OPENJSON(?))
SELECT t.object_id,t.name,t.create_date,own.value,inc.value,
 CONVERT(int,t.is_memory_optimized)+t.temporal_type+CONVERT(int,t.is_filetable),
 (SELECT COUNT(*) FROM sys.indexes i WHERE i.object_id=t.object_id AND i.index_id>0),
 (SELECT COUNT(*) FROM sys.triggers tr WHERE tr.parent_id=t.object_id),
 (SELECT COUNT(*) FROM sys.objects o WHERE o.parent_object_id=t.object_id)
FROM ids JOIN sys.tables t ON t.object_id=ids.object_id
LEFT JOIN sys.extended_properties own ON own.class=1 AND own.major_id=t.object_id AND own.minor_id=0 AND own.name=N'dpone_native_owner'
LEFT JOIN sys.extended_properties inc ON inc.class=1 AND inc.major_id=t.object_id AND inc.minor_id=0 AND inc.name=N'dpone_tds_incarnation'
ORDER BY t.object_id;
"""
BATCH_FEATURES_SQL = """
WITH ids AS (SELECT CONVERT(int,[value]) AS object_id FROM OPENJSON(?))
SELECT t.object_id,CONVERT(int,t.is_replicated),CONVERT(int,t.is_merge_published),
 CONVERT(int,t.is_sync_tran_subscribed),CONVERT(int,t.has_replication_filter),
 CONVERT(int,t.is_tracked_by_cdc),CONVERT(int,t.is_remote_data_archive_enabled),
 CONVERT(int,t.is_node),CONVERT(int,t.is_edge),CONVERT(int,t.ledger_type),
 CONVERT(int,t.is_dropped_ledger_table),
 (SELECT COUNT_BIG(*) FROM sys.external_tables e WHERE e.object_id=t.object_id),
 (SELECT COUNT_BIG(*) FROM sys.security_predicates p WHERE p.target_object_id=t.object_id)
FROM ids JOIN sys.tables t ON t.object_id=ids.object_id ORDER BY t.object_id;
"""
BATCH_COLUMNS_SQL = """
WITH ids AS (SELECT CONVERT(int,[value]) AS object_id FROM OPENJSON(?))
SELECT ids.object_id,c.column_id,c.name,t.name,c.is_nullable,c.max_length,c.precision,c.scale,c.collation_name,
 CONVERT(int,c.is_identity)+CONVERT(int,c.is_computed)+CONVERT(int,c.is_sparse)+CONVERT(int,c.is_column_set)
 +CONVERT(int,c.is_hidden)+CONVERT(int,c.is_filestream)+c.generated_always_type+CONVERT(int,c.is_masked)
 +COALESCE(c.encryption_type,0)+c.default_object_id+c.rule_object_id,
 t.is_user_defined,t.is_assembly_type
FROM ids CROSS APPLY (
 SELECT TOP (101) sc.* FROM sys.columns sc WHERE sc.object_id=ids.object_id ORDER BY sc.column_id
) c LEFT JOIN sys.types t ON t.user_type_id=c.user_type_id
ORDER BY ids.object_id,c.column_id;
"""
_ERROR = "mssql_native.sqlclient_grant_catalog_unavailable"
_BATCH_COLUMN_OBJECTS = 81


def _object_ids(value: tuple[int, ...], *, limit: int = 1024) -> str:
    if (
        type(value) is not tuple
        or not 1 <= len(value) <= limit
        or any(type(item) is not int or not 1 <= item <= 2**31 - 1 for item in value)
        or tuple(sorted(set(value))) != value
    ):
        raise ValueError(_ERROR)
    return json.dumps(value, separators=(",", ":"))


def _permission_row(row: tuple[Any, ...]) -> tuple[Any, ...]:
    """Detach SQL Server's fixed-width nchar(4) code into canonical text."""
    if type(row) is not tuple or len(row) != 8 or type(row[5]) is not str:
        raise ValueError(_ERROR)
    code = row[5].rstrip(" ")
    if not code or len(code) > 4:
        raise ValueError(_ERROR)
    return (*row[:5], code, *row[6:])


class SqlClientGrantCatalogProjection:
    """Fixed projection API; the concrete catalog owns lease and I/O semantics."""

    _session: ObservationCursor

    @abstractmethod
    def _operation(self) -> AbstractContextManager[object]: ...

    @abstractmethod
    def _rows(self, statement: str, parameters: tuple[Any, ...] = (), *, limit: int = 1) -> list[Any]: ...

    @abstractmethod
    def _rows_within(
        self, lease: object, statement: str, parameters: tuple[Any, ...] = (), *, limit: int = 1
    ) -> list[Any]: ...

    @abstractmethod
    def _permissions_within(self, lease: object, writer_id: int, public_id: int, *, limit: int) -> list[Any]: ...

    def own_incarnation(self, statement: str) -> list[Any]:
        try:
            require_own_incarnation_statement(statement)
        except ValueError:
            self._session.fail()
            raise ValueError(_ERROR) from None
        return self.read_own_incarnation()

    def read_own_incarnation(self) -> list[Any]:
        return self._rows(OWN_INCARNATION_SQL)

    def writer_admission(self, database_id: int, login: str) -> list[Any]:
        return self._rows(ADMISSION_SQL, (database_id, login))

    def principals(self, name: str, sid: bytes) -> list[Any]:
        return self._rows(PRINCIPALS_SQL, (name, sid), limit=2)

    def permissions(self, writer_id: int, public_id: int, *, limit: int) -> list[Any]:
        with self._operation() as lease:
            return self._permissions_within(lease, writer_id, public_id, limit=limit)

    def member(self, object_id: int) -> list[Any]:
        return self._rows(MEMBER_SQL, (object_id,))

    def members(self, object_ids: tuple[int, ...]) -> list[Any]:
        return self._rows(BATCH_MEMBERS_SQL, (_object_ids(object_ids),), limit=len(object_ids))

    def object_properties(self, schema_id: int, table: str) -> list[Any]:
        return self._rows(OBJECT_SQL, (schema_id, table))

    def object_properties_batch(self, object_ids: tuple[int, ...]) -> list[Any]:
        return self._rows(BATCH_OBJECTS_SQL, (_object_ids(object_ids),), limit=len(object_ids))

    def features(self, object_id: int) -> list[Any]:
        return self._rows(FEATURES_SQL, (object_id,))

    def features_batch(self, object_ids: tuple[int, ...]) -> list[Any]:
        return self._rows(BATCH_FEATURES_SQL, (_object_ids(object_ids),), limit=len(object_ids))

    def columns(self, object_id: int) -> list[Any]:
        return self._rows(COLUMNS_SQL, (object_id,), limit=100)

    def columns_batch(self, object_ids: tuple[int, ...]) -> list[Any]:
        payload = _object_ids(object_ids, limit=_BATCH_COLUMN_OBJECTS)
        return self._rows(BATCH_COLUMNS_SQL, (payload,), limit=len(object_ids) * 101)

    def _read_within(self, lease: object, opcode: str, arguments: dict) -> list[Any]:
        """Finite dispatch over adapter-owned statements under the session lease."""
        if opcode == "OWN_INCARNATION":
            return self._rows_within(lease, OWN_INCARNATION_SQL)
        if opcode == "WRITER_ADMISSION":
            return self._rows_within(lease, ADMISSION_SQL, (arguments["database_id"], arguments["login"]))
        if opcode == "PRINCIPALS":
            return self._rows_within(
                lease, PRINCIPALS_SQL, (arguments["name"], bytes.fromhex(arguments["sid"])), limit=2
            )
        if opcode == "PERMISSIONS":
            return self._permissions_within(lease, **arguments)
        if opcode == "MEMBER":
            return self._rows_within(lease, MEMBER_SQL, (arguments["object_id"],))
        if opcode == "BATCH_MEMBERS":
            ids = tuple(arguments["object_ids"])
            return self._rows_within(lease, BATCH_MEMBERS_SQL, (_object_ids(ids),), limit=len(ids))
        if opcode == "OBJECT_PROPERTIES":
            return self._rows_within(lease, OBJECT_SQL, (arguments["schema_id"], arguments["table"]))
        if opcode == "BATCH_OBJECTS":
            ids = tuple(arguments["object_ids"])
            return self._rows_within(lease, BATCH_OBJECTS_SQL, (_object_ids(ids),), limit=len(ids))
        if opcode == "FEATURES":
            return self._rows_within(lease, FEATURES_SQL, (arguments["object_id"],))
        if opcode == "BATCH_FEATURES":
            ids = tuple(arguments["object_ids"])
            return self._rows_within(lease, BATCH_FEATURES_SQL, (_object_ids(ids),), limit=len(ids))
        if opcode == "COLUMNS":
            return self._rows_within(lease, COLUMNS_SQL, (arguments["object_id"],), limit=100)
        if opcode == "BATCH_COLUMNS":
            ids = tuple(arguments["object_ids"])
            return self._rows_within(
                lease, BATCH_COLUMNS_SQL, (_object_ids(ids, limit=_BATCH_COLUMN_OBJECTS),), limit=len(ids) * 101
            )
        raise ValueError(_ERROR)
