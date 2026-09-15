"""Administrator-only initial generation ledger and shared physical quota install."""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.native_generation_mssql_queries import generation_procedure_name
from dpone.adapters.native_generation_mssql_upgrade import (
    FREEZE_INSPECTION_OPERATIONS,
    GENERATION_ADMISSION_CHECK,
    GENERATION_COMPLETION_CHECK,
    upgrade_generation_ledger,
)
from dpone.contracts.mssql_object_name import native_control_schema
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
        self._principal_name = runtime_database_principal
        self._connect = connection_factory

    def apply(self) -> None:
        """Atomically install fixed procedures and retain exact registration replay."""
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT OBJECT_ID(N'[{self._schema}].[native_generations_v1]',N'U'), "
                f"COL_LENGTH(N'[{self._schema}].[native_generations_v1]',N'writer_admission'), "
                f"(SELECT COUNT(*) FROM sys.tables WHERE schema_id=SCHEMA_ID(N'{self._schema}') "
                "AND name IN ('native_generations_v1','native_generation_capacity_v1','native_generation_profiles_v1')), "
                f"COL_LENGTH(N'[{self._schema}].[native_generations_v1]',N'phase')"
            )
            layout = dbapi_lifecycle.row(cursor)
            if layout is None or len(layout) != 4:
                raise RuntimeError("cannot inspect retained generation layout")
            fresh, legacy = layout[0] is None, layout[1] is None
            if layout[2] != (0 if fresh else 3):
                raise RuntimeError("partial generation ledger cannot be silently repaired")
            cursor.execute(self._runtime_principal_preflight(), *self._parameters[-3:])
            if fresh:
                cursor.execute(self._tables())
            upgrade_generation_ledger(
                cursor,
                schema=self._schema,
                legacy=legacy,
                fresh=fresh,
                legacy_verification=self._verify_tables(extended=False),
                current_verification=self._verify_tables(extended=True),
                completed=layout[3] is not None,
                completion_verification=self._verify_tables(completed=True),
                freeze_enabled=True,
                inspection_enabled=True,
            )
            cursor.execute(self._registration(), *self._parameters)
            for table in ("native_generation_capacity_v1", "native_generation_profiles_v1", "native_generations_v1"):
                cursor.execute(
                    f"DENY SELECT, INSERT, UPDATE, DELETE, ALTER, TAKE OWNERSHIP ON "
                    f"OBJECT::[{self._schema}].[{table}] TO {self._principal}"
                )
            cursor.execute(f"DENY ALTER ON SCHEMA::[{self._schema}] TO {self._principal}")
            for operation in FREEZE_INSPECTION_OPERATIONS:
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

    def _runtime_principal_preflight(self) -> str:
        """Verify the retained caller and its effective role before ledger DDL."""
        principal = self._principal_name.replace("'", "''")
        return f"""DECLARE @authority_locator varbinary(max)=?,@authority_digest varbinary(71)=?,@principal sysname=?;
IF NOT EXISTS (SELECT 1 FROM [{self._schema}].[native_original_authorities_v1] a
 JOIN sys.database_principals p ON p.principal_id=a.runtime_principal_id AND p.sid=a.runtime_principal_sid
 WHERE p.name=@principal AND a.authority_hash=HASHBYTES('SHA2_256',@authority_locator)
 AND a.authority_locator=@authority_locator AND DATALENGTH(a.authority_locator)=DATALENGTH(@authority_locator)
 AND a.authority_digest=@authority_digest AND a.schema_version=1)
 THROW 51310, 'DPONE_NATIVE_GENERATION_PRINCIPAL_UNREGISTERED', 1;
DECLARE @overprivileged bit=0;
EXECUTE AS USER=N'{principal}';
BEGIN TRY
 IF IS_MEMBER('db_owner')=1 OR HAS_PERMS_BY_NAME(N'{self._schema}',N'SCHEMA',N'ALTER')=1
 SET @overprivileged=1;
 REVERT;
END TRY
BEGIN CATCH
 REVERT;
 THROW;
END CATCH;
IF @overprivileged=1 THROW 51310, 'DPONE_NATIVE_GENERATION_RUNTIME_PRINCIPAL_INVALID', 1;"""

    def _verify_tables(self, *, extended: bool = True, completed: bool = False) -> str:
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
        if extended:
            columns, keys, key_count, _ = definitions["native_generations_v1"]
            definitions["native_generations_v1"] = (
                columns + ",('writer_admission','varchar',6,0),('outcome','varchar',7,0),"
                "('admission_sequence','bigint',8,0),('admission_closure','varbinary',-1,1)",
                keys,
                key_count,
                "engine_compared",
            )
        if completed:
            if not extended:
                raise ValueError("completion table verification requires admission columns")
            columns, keys, key_count, check = definitions["native_generations_v1"]
            definitions["native_generations_v1"] = (
                columns + ",('phase','varchar',8,0),('completion_payload','varbinary',-1,1),"
                "('completion_locator','varbinary',-1,1),('completion_digest','varbinary',71,1),"
                "('frozen_payload','varbinary',-1,1),('frozen_locator','varbinary',-1,1),('frozen_digest','varbinary',71,1)",
                keys,
                key_count,
                check,
            )
        statements = [self._expected_admission_check(completed=completed)] if extended else []
        for table, (columns, keys, key_count, check) in definitions.items():
            check = check.replace("'", "''")
            if extended and table == "native_generations_v1":
                check_match = (
                    "CONVERT(varbinary(max),definition)=CONVERT(varbinary(max),@expected_admission_check) "
                    "AND DATALENGTH(definition)=DATALENGTH(@expected_admission_check)"
                )
            else:
                check_match = (
                    "LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(definition,' ',''),'(',''),')',''),"
                    f"'[',''),']',''))=N'{check}'"
                )
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
 AND {check_match})
 THROW 51310, 'DPONE_NATIVE_GENERATION_EXISTING_SCHEMA_MISMATCH', 1;""")
        return "\n".join(statement.replace("@object", f"@object_{index}") for index, statement in enumerate(statements))

    @staticmethod
    def _expected_admission_check(*, completed: bool = False) -> str:
        """Ask SQL Server to normalize its own predicate without discarding grouping.

        The session-local empty table holds no user data and is dropped in the
        same batch. Comparing exact engine-rendered definitions rejects weakened
        AND/OR regroupings that token-only normalization would accept.
        """
        columns = (
            ""
            if not completed
            else """phase varchar(8) NOT NULL,
 completion_payload varbinary(max) NULL,completion_locator varbinary(max) NULL,completion_digest varbinary(71) NULL,
 frozen_payload varbinary(max) NULL,frozen_locator varbinary(max) NULL,frozen_digest varbinary(71) NULL,"""
        )
        check = GENERATION_COMPLETION_CHECK if completed else GENERATION_ADMISSION_CHECK
        return f"""CREATE TABLE #dpone_native_admission_check (
 guard_epoch bigint NOT NULL, revision bigint NOT NULL, executor varbinary(max) NULL,
 writer_admission varchar(6) NOT NULL, outcome varchar(7) NOT NULL,
 admission_sequence bigint NOT NULL, admission_closure varbinary(max) NULL,
 {columns}
 CHECK ({check}));
DECLARE @expected_admission_check nvarchar(max)=(SELECT definition FROM tempdb.sys.check_constraints
 WHERE parent_object_id=OBJECT_ID(N'tempdb..#dpone_native_admission_check'));
DROP TABLE #dpone_native_admission_check;
IF @expected_admission_check IS NULL THROW 51310, 'DPONE_NATIVE_GENERATION_EXPECTED_CHECK_MISSING', 1;"""

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
