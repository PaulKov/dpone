"""Explicit administrator installation of protected native-original SQL objects."""

from __future__ import annotations

from collections.abc import Callable

from dpone.adapters import dbapi_lifecycle
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_identity import OriginalRef
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class MssqlNativeOriginalSchemaMigration:
    """Install exact V1 objects and a previously authenticated authority registration.

    Only the platform provisioning owner calls this entry point, with a privileged
    fresh connection to the selected native control database. The reference is an
    authenticated input, never a trust root created by this migration. The schema
    must already exist and be owned by dbo. Runtime installation is forbidden.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], SqlControlConnection],
        control_schema: str,
        control_authority: OriginalRef,
        runtime_database_principal: str,
    ) -> None:
        self._schema = native_control_schema(control_schema)
        if type(control_authority) is not OriginalRef:
            raise TypeError("control_authority must be an authenticated OriginalRef")
        self._authority = OriginalRef(control_authority.locator, control_authority.sha256)
        if type(runtime_database_principal) is not str or not 0 < len(runtime_database_principal) <= 128:
            raise ValueError("runtime_database_principal must name an existing database user")
        self._principal = runtime_database_principal
        self._connect = connection_factory

    def apply(self) -> None:
        """Atomically install or acknowledge exact existing objects/registration."""
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(self._tables())
            cursor.execute(self._verify_tables())
            for operation in ("bind", "resolve"):
                name = f"[{self._schema}].[native_original_{operation}_v1]"
                definition = self._procedure(operation)
                cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?))", name)
                existing = dbapi_lifecycle.row(cursor)
                if existing is None or len(existing) != 1:
                    raise RuntimeError("cannot inspect native original procedure definition")
                if existing[0] is None:
                    cursor.execute(definition)
                elif existing[0] != definition:
                    raise RuntimeError("installed native original procedure differs from exact V1 definition")
            cursor.execute(
                self._registration(),
                self._authority.locator.encode("utf-8"),
                self._authority.sha256.encode("ascii"),
                self._principal,
            )
            # Quote a catalog-resolved principal as an identifier, never interpolate raw input.
            principal = "[" + self._principal.replace("]", "]]") + "]"
            for table in ("native_original_authorities_v1", "native_original_bindings_v1"):
                cursor.execute(
                    f"DENY SELECT, INSERT, UPDATE, DELETE, ALTER, TAKE OWNERSHIP ON "
                    f"OBJECT::[{self._schema}].[{table}] TO {principal}"
                )
            cursor.execute(f"DENY ALTER ON SCHEMA::[{self._schema}] TO {principal}")
            for operation in ("bind", "resolve"):
                cursor.execute(
                    f"GRANT EXECUTE ON OBJECT::[{self._schema}].[native_original_{operation}_v1] TO {principal}"
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
    THROW 51280, 'DPONE_NATIVE_REQUIRES_EXISTING_DBO_CONTROL_SCHEMA', 1;
IF OBJECT_ID(N'[{s}].[native_original_authorities_v1]', N'U') IS NULL
CREATE TABLE [{s}].[native_original_authorities_v1] (
    authority_hash binary(32) NOT NULL PRIMARY KEY,
    authority_locator varbinary(max) NOT NULL,
    authority_digest varbinary(71) NOT NULL,
    runtime_principal_id int NOT NULL,
    runtime_principal_sid varbinary(85) NOT NULL,
    schema_version int NOT NULL CHECK (schema_version=1)
);
IF OBJECT_ID(N'[{s}].[native_original_bindings_v1]', N'U') IS NULL
CREATE TABLE [{s}].[native_original_bindings_v1] (
    locator_hash binary(32) NOT NULL PRIMARY KEY,
    locator varbinary(max) NOT NULL,
    authority_locator varbinary(max) NOT NULL,
    authority_digest varbinary(71) NOT NULL,
    payload_digest varbinary(71) NOT NULL,
    subject varbinary(max) NOT NULL,
    kind varbinary(128) NOT NULL,
    binding varbinary(max) NOT NULL CHECK (DATALENGTH(binding)>=1 AND DATALENGTH(binding)<=1048576)
);"""

    def _verify_tables(self) -> str:
        """Reject incompatible existing objects without repairing or replacing them."""
        definitions = {
            "native_original_authorities_v1": (
                "('authority_hash','binary',32),('authority_locator','varbinary',-1),"
                "('authority_digest','varbinary',71),('runtime_principal_id','int',4),"
                "('runtime_principal_sid','varbinary',85),('schema_version','int',4)",
                "authority_hash",
                "schema_version=1",
            ),
            "native_original_bindings_v1": (
                "('locator_hash','binary',32),('locator','varbinary',-1),"
                "('authority_locator','varbinary',-1),('authority_digest','varbinary',71),"
                "('payload_digest','varbinary',71),('subject','varbinary',-1),"
                "('kind','varbinary',128),('binding','varbinary',-1)",
                "locator_hash",
                "datalengthbinding>=1anddatalengthbinding<=1048576",
            ),
        }
        statements = []
        for table, (columns, key, check) in definitions.items():
            statements.append(f"""BEGIN
DECLARE @object int=OBJECT_ID(N'[{self._schema}].[{table}]',N'U');
IF @object IS NULL OR NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id
    WHERE t.object_id=@object AND COALESCE(t.principal_id,s.principal_id)=1 AND t.temporal_type=0
    AND t.is_memory_optimized=0)
 OR EXISTS (SELECT 1 FROM sys.triggers WHERE parent_id=@object)
 OR EXISTS (SELECT 1 FROM sys.columns WHERE object_id=@object AND
    (is_nullable=1 OR is_computed=1 OR is_identity=1 OR default_object_id<>0 OR user_type_id<>system_type_id))
 OR EXISTS (SELECT name,TYPE_NAME(user_type_id),max_length FROM sys.columns WHERE object_id=@object
    EXCEPT SELECT * FROM (VALUES {columns}) AS expected(name,kind,length))
 OR EXISTS (SELECT * FROM (VALUES {columns}) AS expected(name,kind,length)
    EXCEPT SELECT name,TYPE_NAME(user_type_id),max_length FROM sys.columns WHERE object_id=@object)
 OR (SELECT COUNT(*) FROM sys.indexes WHERE object_id=@object AND is_primary_key=1
    AND is_unique=1 AND is_disabled=0 AND has_filter=0 AND ignore_dup_key=0)<>1
 OR (SELECT COUNT(*) FROM sys.index_columns ic JOIN sys.indexes i
    ON i.object_id=ic.object_id AND i.index_id=ic.index_id
    WHERE ic.object_id=@object AND i.is_primary_key=1 AND ic.key_ordinal>0)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.index_columns ic JOIN sys.indexes i
    ON i.object_id=ic.object_id AND i.index_id=ic.index_id
    JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
    WHERE ic.object_id=@object AND i.is_primary_key=1 AND ic.key_ordinal=1 AND c.name=N'{key}')
 OR (SELECT COUNT(*) FROM sys.check_constraints WHERE parent_object_id=@object)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id=@object
    AND is_disabled=0 AND is_not_trusted=0 AND is_not_for_replication=0
    AND LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(definition,' ',''),'(',''),')',''),'[',''),']',''))=N'{check}')
    THROW 51290, 'DPONE_NATIVE_EXISTING_TABLE_SCHEMA_MISMATCH', 1;
END;""")
        # Separate batches avoid duplicate variable declarations in one SQL scope.
        return "\n".join(statement.replace("@object", f"@object_{index}") for index, statement in enumerate(statements))

    def _registration(self) -> str:
        s = self._schema
        return f"""SET NOCOUNT ON;
DECLARE @locator varbinary(max)=?, @digest varbinary(max)=?, @principal sysname=?;
DECLARE @id int, @sid varbinary(85);
SELECT @id=principal_id, @sid=sid FROM sys.database_principals
WHERE name=@principal AND type IN ('S','U','E') AND principal_id>4;
IF @id IS NULL OR @sid IS NULL OR IS_ROLEMEMBER('db_owner', @principal)=1
    THROW 51281, 'DPONE_NATIVE_REQUIRES_LEAST_PRIVILEGE_DATABASE_USER', 1;
DECLARE @hash binary(32)=HASHBYTES('SHA2_256', @locator);
IF EXISTS (SELECT 1 FROM [{s}].[native_original_authorities_v1] WITH (UPDLOCK,HOLDLOCK)
           WHERE authority_hash=@hash)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM [{s}].[native_original_authorities_v1]
        WHERE authority_hash=@hash AND authority_locator=@locator
        AND DATALENGTH(authority_locator)=DATALENGTH(@locator)
        AND authority_digest=@digest AND DATALENGTH(authority_digest)=DATALENGTH(@digest)
        AND runtime_principal_id=@id AND runtime_principal_sid=@sid AND schema_version=1)
        THROW 51282, 'DPONE_NATIVE_AUTHORITY_REGISTRATION_CONFLICT', 1;
END
ELSE INSERT [{s}].[native_original_authorities_v1]
    (authority_hash,authority_locator,authority_digest,runtime_principal_id,runtime_principal_sid,schema_version)
    VALUES (@hash,@locator,@digest,@id,@sid,1);"""

    def _procedure(self, operation: str) -> str:
        s = self._schema
        extra = ", @binding varbinary(max)" if operation == "bind" else ""
        lock = "UPDLOCK,HOLDLOCK" if operation == "bind" else "HOLDLOCK"
        insert = ""
        if operation == "bind":
            insert = f"""
IF @binding IS NULL OR DATALENGTH(@binding) NOT BETWEEN 1 AND @max_bytes
    THROW 51283, 'DPONE_NATIVE_BINDING_BOUND_INVALID', 1;
IF NOT EXISTS (SELECT 1 FROM [{s}].[native_original_bindings_v1] WITH (UPDLOCK,HOLDLOCK)
               WHERE locator_hash=@hash)
    INSERT [{s}].[native_original_bindings_v1]
    (locator_hash,locator,authority_locator,authority_digest,payload_digest,subject,kind,binding)
    VALUES (@hash,@locator,@authority_locator,@authority_digest,@payload_digest,@subject,@kind,@binding);
IF NOT EXISTS (SELECT 1 FROM [{s}].[native_original_bindings_v1]
               WHERE locator_hash=@hash AND binding=@binding AND DATALENGTH(binding)=DATALENGTH(@binding))
    THROW 51284, 'DPONE_NATIVE_IMMUTABLE_BINDING_CONFLICT', 1;
"""
        return f"""CREATE PROCEDURE [{s}].[native_original_{operation}_v1]
    @authority_locator varbinary(max), @authority_digest varbinary(max),
    @locator varbinary(max), @payload_digest varbinary(max),
    @subject varbinary(max), @kind varbinary(max), @max_bytes int{extra}
AS
BEGIN
SET NOCOUNT ON; SET XACT_ABORT ON;
IF @@TRANCOUNT=0 BEGIN TRANSACTION;
IF @@TRANCOUNT <> 1 OR @max_bytes IS NULL OR @max_bytes NOT BETWEEN 1 AND 1048576
    THROW 51285, 'DPONE_NATIVE_REQUIRES_BOUNDED_OWNED_TRANSACTION', 1;
IF @authority_locator IS NULL OR DATALENGTH(@authority_locator) NOT BETWEEN 1 AND 4096
 OR @authority_digest IS NULL OR DATALENGTH(@authority_digest)<>71
 OR @locator IS NULL OR DATALENGTH(@locator) NOT BETWEEN 1 AND 4096
 OR @payload_digest IS NULL OR DATALENGTH(@payload_digest)<>71
 OR @subject IS NULL OR DATALENGTH(@subject) NOT BETWEEN 1 AND 1048576
 OR @kind IS NULL OR DATALENGTH(@kind) NOT BETWEEN 1 AND 128
    THROW 51286, 'DPONE_NATIVE_IDENTITY_BOUND_INVALID', 1;
IF IS_SRVROLEMEMBER('sysadmin')=1 OR IS_MEMBER('db_owner')=1
 OR HAS_PERMS_BY_NAME(N'{s}', 'SCHEMA', 'ALTER')=1
    THROW 51287, 'DPONE_NATIVE_RUNTIME_PRINCIPAL_OVERPRIVILEGED', 1;
IF NOT EXISTS (
 SELECT 1 FROM [{s}].[native_original_authorities_v1] WITH (HOLDLOCK)
 WHERE authority_hash=HASHBYTES('SHA2_256',@authority_locator)
 AND authority_locator=@authority_locator AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest)
 AND runtime_principal_id=USER_ID()
 AND runtime_principal_sid=(SELECT sid FROM sys.database_principals WHERE principal_id=USER_ID())
 AND schema_version=1)
    THROW 51288, 'DPONE_NATIVE_AUTHORITY_OR_CALLER_MISMATCH', 1;
DECLARE @hash binary(32)=HASHBYTES('SHA2_256',@locator);
{insert}
IF NOT EXISTS (SELECT 1 FROM [{s}].[native_original_bindings_v1] WITH ({lock})
 WHERE locator_hash=@hash AND locator=@locator AND DATALENGTH(locator)=DATALENGTH(@locator)
 AND authority_locator=@authority_locator AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest)
 AND payload_digest=@payload_digest AND DATALENGTH(payload_digest)=DATALENGTH(@payload_digest)
 AND subject=@subject AND DATALENGTH(subject)=DATALENGTH(@subject)
 AND kind=@kind AND DATALENGTH(kind)=DATALENGTH(@kind)
 AND DATALENGTH(binding)<=@max_bytes)
    THROW 51289, 'DPONE_NATIVE_BINDING_IDENTITY_MISMATCH_OR_ABSENT', 1;
SELECT binding FROM [{s}].[native_original_bindings_v1] WHERE locator_hash=@hash;
END"""
