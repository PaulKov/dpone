"""Administrator-only initial generation ledger and shared physical quota install."""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.native_generation_mssql_queries import generation_procedure, generation_procedure_name
from dpone.adapters.native_originals_mssql import native_control_schema
from dpone.contracts.native_identity import OriginalRef
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class MssqlNativeGenerationSchemaMigration:
    """Enroll an approved profile against one immutable physical capacity account.

    Existing workspace and native-original migrations must already be installed.
    Additional profile versions share the same guard account; no runtime path can
    enroll itself or reset charged capacity. All references are authenticated
    provisioning inputs, not new authority roots created by this migration.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], SqlControlConnection],
        control_schema: str,
        control_authority: OriginalRef,
        runtime_database_principal: str,
        physical_guard: str,
        resource_authority: OriginalRef,
        capacity_bytes: int,
        trusted_profile: OriginalRef,
        max_generation_bytes: int,
    ) -> None:
        self._schema = native_control_schema(control_schema)
        if type(physical_guard) is not str or not 0 < len(physical_guard) <= 512:
            raise ValueError("physical_guard must identify an existing physical resource")
        if type(runtime_database_principal) is not str or not 0 < len(runtime_database_principal) <= 128:
            raise ValueError("runtime_database_principal must identify an existing user")
        for limit in (capacity_bytes, max_generation_bytes):
            if type(limit) is not int or not 1 <= limit <= 9223372036854775807:
                raise ValueError("capacity must be a positive SQL bigint")
        if max_generation_bytes > capacity_bytes:
            raise ValueError("per-generation capacity exceeds physical capacity")
        for reference in (control_authority, resource_authority, trusted_profile):
            if type(reference) is not OriginalRef:
                raise TypeError("provisioning requires exact original references")
            reference.__post_init__()
        self._parameters = (
            physical_guard,
            resource_authority.locator.encode("utf-8"),
            resource_authority.sha256.encode("ascii"),
            capacity_bytes,
            trusted_profile.locator.encode("utf-8"),
            trusted_profile.sha256.encode("ascii"),
            max_generation_bytes,
            control_authority.locator.encode("utf-8"),
            control_authority.sha256.encode("ascii"),
            runtime_database_principal,
        )
        self._principal = "[" + runtime_database_principal.replace("]", "]]") + "]"
        self._connect = connection_factory

    def apply(self) -> None:
        """Atomically install fixed procedures and retain exact registration replay."""
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(self._tables())
            cursor.execute(self._verify_tables())
            for operation in ("reserve", "bind", "read"):
                name = f"[{self._schema}].[{generation_procedure_name(operation)}]"
                definition = generation_procedure(self._schema, operation)
                cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?))", name)
                existing = dbapi_lifecycle.row(cursor)
                if existing is None or len(existing) != 1:
                    raise RuntimeError("cannot inspect generation procedure")
                if existing[0] is None:
                    cursor.execute(definition)
                elif existing[0] != definition:
                    raise RuntimeError("existing generation procedure differs from V1")
            cursor.execute(self._registration(), *self._parameters)
            for table in ("native_generation_capacity_v1", "native_generation_profiles_v1", "native_generations_v1"):
                cursor.execute(
                    f"DENY SELECT, INSERT, UPDATE, DELETE, ALTER, TAKE OWNERSHIP ON "
                    f"OBJECT::[{self._schema}].[{table}] TO {self._principal}"
                )
            cursor.execute(f"DENY ALTER ON SCHEMA::[{self._schema}] TO {self._principal}")
            for operation in ("reserve", "bind", "read"):
                cursor.execute(
                    f"GRANT EXECUTE ON OBJECT::[{self._schema}].[{generation_procedure_name(operation)}] TO {self._principal}"
                )
            connection.commit()
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)

    def _tables(self) -> str:
        s = self._schema
        return f"""SET NOCOUNT ON; SET XACT_ABORT ON;
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name=N'{s}' AND principal_id=1)
 OR OBJECT_ID(N'[{s}].[native_original_authorities_v1]',N'U') IS NULL
 OR OBJECT_ID(N'[{s}].[dbt_workspace_attempt_guards]',N'U') IS NULL
 THROW 51310, 'DPONE_NATIVE_GENERATION_PREREQUISITE_MISSING', 1;
IF OBJECT_ID(N'[{s}].[native_generation_capacity_v1]',N'U') IS NULL
CREATE TABLE [{s}].[native_generation_capacity_v1] (
 guard_hash binary(32) NOT NULL PRIMARY KEY,
 guard_id varbinary(1024) NOT NULL,
 resource_locator varbinary(max) NOT NULL,
 resource_digest varbinary(71) NOT NULL,
 capacity_bytes bigint NOT NULL,
 charged_bytes bigint NOT NULL,
 CHECK (capacity_bytes>0 AND charged_bytes>=0 AND charged_bytes<=capacity_bytes)
);
IF OBJECT_ID(N'[{s}].[native_generation_profiles_v1]',N'U') IS NULL
CREATE TABLE [{s}].[native_generation_profiles_v1] (
 guard_hash binary(32) NOT NULL,
 profile_hash binary(32) NOT NULL,
 profile_locator varbinary(max) NOT NULL,
 profile_digest varbinary(71) NOT NULL,
 authority_locator varbinary(max) NOT NULL,
 authority_digest varbinary(71) NOT NULL,
 max_generation_bytes bigint NOT NULL CHECK (max_generation_bytes>0),
 PRIMARY KEY (guard_hash,profile_hash)
);
IF OBJECT_ID(N'[{s}].[native_generations_v1]',N'U') IS NULL
CREATE TABLE [{s}].[native_generations_v1] (
 generation_id uniqueidentifier NOT NULL PRIMARY KEY,
 guard_hash binary(32) NOT NULL,
 guard_epoch bigint NOT NULL,
 revision bigint NOT NULL,
 authority_locator varbinary(max) NOT NULL,
 authority_digest varbinary(71) NOT NULL,
 reservation_locator varbinary(max) NOT NULL,
 reservation_digest varbinary(71) NOT NULL,
 request varbinary(max) NOT NULL,
 executor varbinary(max) NULL,
 CHECK (guard_epoch>0 AND revision=CASE WHEN executor IS NULL THEN 1 ELSE 2 END)
);"""

    def _verify_tables(self) -> str:
        """Reject incompatible retained ledgers, including weakened constraints."""
        definitions = {
            "native_generation_capacity_v1": (
                "('guard_hash','binary',32,0),('guard_id','varbinary',1024,0),"
                "('resource_locator','varbinary',-1,0),('resource_digest','varbinary',71,0),"
                "('capacity_bytes','bigint',8,0),('charged_bytes','bigint',8,0)",
                "('guard_hash',1)",
                1,
                "capacity_bytes>0andcharged_bytes>=0andcharged_bytes<=capacity_bytes",
            ),
            "native_generation_profiles_v1": (
                "('guard_hash','binary',32,0),('profile_hash','binary',32,0),('profile_locator','varbinary',-1,0),"
                "('profile_digest','varbinary',71,0),('authority_locator','varbinary',-1,0),"
                "('authority_digest','varbinary',71,0),('max_generation_bytes','bigint',8,0)",
                "('guard_hash',1),('profile_hash',2)",
                2,
                "max_generation_bytes>0",
            ),
            "native_generations_v1": (
                "('generation_id','uniqueidentifier',16,0),('guard_hash','binary',32,0),('guard_epoch','bigint',8,0),"
                "('revision','bigint',8,0),('authority_locator','varbinary',-1,0),('authority_digest','varbinary',71,0),"
                "('reservation_locator','varbinary',-1,0),('reservation_digest','varbinary',71,0),"
                "('request','varbinary',-1,0),('executor','varbinary',-1,1)",
                "('generation_id',1)",
                1,
                "guard_epoch>0andrevision=casewhenexecutorisnullthen1else2end",
            ),
        }
        statements = []
        for table, (columns, keys, key_count, check) in definitions.items():
            statements.append(f"""DECLARE @object int=OBJECT_ID(N'[{self._schema}].[{table}]',N'U');
IF @object IS NULL OR NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id
 WHERE t.object_id=@object AND COALESCE(t.principal_id,s.principal_id)=1 AND t.temporal_type=0 AND t.is_memory_optimized=0)
 OR EXISTS (SELECT 1 FROM sys.triggers WHERE parent_id=@object)
 OR EXISTS (SELECT 1 FROM sys.columns WHERE object_id=@object
 AND (is_computed=1 OR is_identity=1 OR default_object_id<>0 OR user_type_id<>system_type_id))
 OR EXISTS (SELECT name,TYPE_NAME(user_type_id),max_length,is_nullable FROM sys.columns WHERE object_id=@object
 EXCEPT SELECT * FROM (VALUES {columns}) e(name,kind,length,nullable))
 OR EXISTS (SELECT * FROM (VALUES {columns}) e(name,kind,length,nullable)
 EXCEPT SELECT name,TYPE_NAME(user_type_id),max_length,is_nullable FROM sys.columns WHERE object_id=@object)
 OR (SELECT COUNT(*) FROM sys.indexes WHERE object_id=@object AND is_primary_key=1
 AND is_unique=1 AND is_disabled=0 AND has_filter=0 AND ignore_dup_key=0)<>1
 OR (SELECT COUNT(*) FROM sys.index_columns ic JOIN sys.indexes i
 ON i.object_id=ic.object_id AND i.index_id=ic.index_id
 WHERE ic.object_id=@object AND i.is_primary_key=1 AND ic.key_ordinal>0)<>{key_count}
 OR EXISTS (SELECT * FROM (VALUES {keys}) e(name,ordinal)
 EXCEPT SELECT c.name,ic.key_ordinal FROM sys.index_columns ic JOIN sys.indexes i
 ON i.object_id=ic.object_id AND i.index_id=ic.index_id
 JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
 WHERE ic.object_id=@object AND i.is_primary_key=1 AND ic.key_ordinal>0)
 OR (SELECT COUNT(*) FROM sys.check_constraints WHERE parent_object_id=@object)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id=@object
 AND is_disabled=0 AND is_not_trusted=0 AND is_not_for_replication=0
 AND LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(definition,' ',''),'(',''),')',''),'[',''),']',''))=N'{check}')
 THROW 51310, 'DPONE_NATIVE_GENERATION_EXISTING_SCHEMA_MISMATCH', 1;""")
        return "\n".join(statement.replace("@object", f"@object_{index}") for index, statement in enumerate(statements))

    def _registration(self) -> str:
        s = self._schema
        return f"""DECLARE @guard nvarchar(512)=?,@resource_locator varbinary(max)=?,@resource_digest varbinary(71)=?,
@capacity bigint=?,@profile_locator varbinary(max)=?,@profile_digest varbinary(71)=?,@maximum bigint=?,
@authority_locator varbinary(max)=?,@authority_digest varbinary(71)=?,@principal sysname=?;
DECLARE @guard_bytes varbinary(1024)=CONVERT(varbinary(1024),@guard);
DECLARE @guard_hash binary(32)=HASHBYTES('SHA2_256',@guard_bytes);
DECLARE @profile_hash binary(32)=HASHBYTES('SHA2_256',@profile_locator);
IF NOT EXISTS (SELECT 1 FROM [{s}].[native_original_authorities_v1] a WITH (HOLDLOCK)
 JOIN sys.database_principals p ON p.principal_id=a.runtime_principal_id AND p.sid=a.runtime_principal_sid
 WHERE p.name=@principal AND a.authority_hash=HASHBYTES('SHA2_256',@authority_locator)
 AND a.authority_locator=@authority_locator AND DATALENGTH(a.authority_locator)=DATALENGTH(@authority_locator)
 AND a.authority_digest=@authority_digest)
 THROW 51310, 'DPONE_NATIVE_GENERATION_PRINCIPAL_UNREGISTERED', 1;
IF NOT EXISTS (SELECT 1 FROM [{s}].[semantic_refresh_guards] WITH (HOLDLOCK)
 WHERE CONVERT(varbinary(max),resource_id)=@guard_bytes)
 THROW 51310, 'DPONE_NATIVE_GENERATION_PHYSICAL_GUARD_MISSING', 1;
IF NOT EXISTS (SELECT 1 FROM [{s}].[native_generation_capacity_v1] WITH (UPDLOCK,HOLDLOCK) WHERE guard_hash=@guard_hash)
 INSERT INTO [{s}].[native_generation_capacity_v1]
 (guard_hash,guard_id,resource_locator,resource_digest,capacity_bytes,charged_bytes)
 VALUES (@guard_hash,@guard_bytes,@resource_locator,@resource_digest,@capacity,0);
IF NOT EXISTS (SELECT 1 FROM [{s}].[native_generation_capacity_v1]
 WHERE guard_hash=@guard_hash AND guard_id=@guard_bytes AND DATALENGTH(guard_id)=DATALENGTH(@guard_bytes)
 AND resource_locator=@resource_locator AND DATALENGTH(resource_locator)=DATALENGTH(@resource_locator)
 AND resource_digest=@resource_digest AND capacity_bytes=@capacity)
 THROW 51310, 'DPONE_NATIVE_GENERATION_CAPACITY_REGISTRATION_CONFLICT', 1;
IF NOT EXISTS (SELECT 1 FROM [{s}].[native_generation_profiles_v1] WITH (UPDLOCK,HOLDLOCK)
 WHERE guard_hash=@guard_hash AND profile_hash=@profile_hash)
 INSERT INTO [{s}].[native_generation_profiles_v1]
 (guard_hash,profile_hash,profile_locator,profile_digest,authority_locator,authority_digest,max_generation_bytes)
 VALUES (@guard_hash,@profile_hash,@profile_locator,@profile_digest,@authority_locator,@authority_digest,@maximum);
IF NOT EXISTS (SELECT 1 FROM [{s}].[native_generation_profiles_v1]
 WHERE guard_hash=@guard_hash AND profile_hash=@profile_hash
 AND profile_locator=@profile_locator AND DATALENGTH(profile_locator)=DATALENGTH(@profile_locator)
 AND profile_digest=@profile_digest AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator) AND authority_digest=@authority_digest
 AND max_generation_bytes=@maximum)
 THROW 51310, 'DPONE_NATIVE_GENERATION_PROFILE_REGISTRATION_CONFLICT', 1;"""
