"""Grant an issued target login only the protected cross-database fence entrypoint.

This capability is injected for transfer workers after ordinary login creation.
It never grants access to ledger tables or controller roles. The procedure and
its owner-execution boundary must already be installed by the administrator.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.adapters.composition_mssql_transaction_fence_schema import (
    TRANSFER_PROCEDURE,
    require_transaction_fence_schema,
)
from dpone.adapters.dbapi_lifecycle import row
from dpone.contracts.composition_identity import CompositionAdmissionError

if TYPE_CHECKING:
    from dpone.adapters.composition_mssql_issuance import MssqlIssuedCredentials
    from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger


class MssqlCompositionTransferAccess:
    """One exact user/SID, no role memberships and only EXECUTE on the fence."""

    def grant(self, ledger: CompositionMssqlLedger, credentials: MssqlIssuedCredentials) -> None:
        transaction = ledger.require_transaction()
        require_transaction_fence_schema(ledger.cursor, ledger.schema)
        ledger.require_transaction(transaction)
        ledger.cursor.execute(
            "DECLARE @name sysname=?, @sid binary(16)=?, @schema sysname=?, @procedure sysname=?; "
            "IF SUSER_SID(@name)<>@sid OR SUSER_SID(@name) IS NULL "
            "OR EXISTS (SELECT 1 FROM sys.database_principals WHERE name=@name OR sid=@sid) "
            "THROW 51000, 'DPONE_COMPOSITION_TRANSFER_USER', 1; "
            "DECLARE @sql nvarchar(max)=N'CREATE USER '+QUOTENAME(@name)+N' FOR LOGIN '+QUOTENAME(@name)+ "
            "N'; GRANT EXECUTE ON OBJECT::'+QUOTENAME(@schema)+N'.'+QUOTENAME(@procedure)+N' TO '+QUOTENAME(@name); "
            "EXEC sys.sp_executesql @sql;",
            credentials.login_name,
            credentials.login_sid,
            ledger.schema,
            TRANSFER_PROCEDURE,
        )
        self.require(ledger, credentials)
        ledger.require_transaction(transaction)

    def require(self, ledger: CompositionMssqlLedger, credentials: MssqlIssuedCredentials) -> None:
        """Audit exact rights, allowing only read-only built-in public catalogs.

        Negative object IDs plus IsMSShipped exclude user-created objects,
        including all control tables. Built-in grants confer no ledger writes.
        """
        transaction = ledger.require_transaction()
        require_transaction_fence_schema(ledger.cursor, ledger.schema)
        ledger.cursor.execute(
            "SELECT p.name,p.sid,p.type,p.authentication_type, "
            "(SELECT COUNT(*) FROM sys.database_role_members r WHERE r.member_principal_id=p.principal_id), "
            "(SELECT COUNT(*) FROM sys.database_permissions x WHERE x.grantee_principal_id=p.principal_id "
            "AND NOT (x.state='G' AND ((x.class=0 AND x.permission_name='CONNECT') OR "
            "(x.class=1 AND x.major_id=OBJECT_ID(?) AND x.minor_id=0 AND x.permission_name='EXECUTE')))), "
            "(SELECT COUNT(*) FROM sys.database_permissions x WHERE x.grantee_principal_id=p.principal_id "
            "AND x.state='G' AND x.class=1 AND x.major_id=OBJECT_ID(?) AND x.minor_id=0 "
            "AND x.permission_name='EXECUTE'), "
            "(SELECT COUNT(*) FROM sys.database_permissions x "
            "WHERE x.grantee_principal_id=DATABASE_PRINCIPAL_ID('public') "
            "AND NOT (x.state='G' AND ((x.class=0 AND x.permission_name IN "
            "('CONNECT','VIEW ANY COLUMN ENCRYPTION KEY DEFINITION','VIEW ANY COLUMN MASTER KEY DEFINITION')) "
            "OR (x.class=1 AND x.major_id<0 AND x.minor_id=0 AND OBJECTPROPERTYEX(x.major_id,'IsMSShipped')=1 "
            "AND x.permission_name IN ('SELECT','VIEW DEFINITION'))))) "
            "FROM sys.database_principals p WHERE p.name=? OR p.sid=?;",
            f"[{ledger.schema}].[{TRANSFER_PROCEDURE}]",
            f"[{ledger.schema}].[{TRANSFER_PROCEDURE}]",
            credentials.login_name,
            credentials.login_sid,
        )
        expected = (credentials.login_name, credentials.login_sid, "S", 1, 0, 0, 1, 0)
        if row(ledger.cursor) != expected or ledger.cursor.fetchone() is not None:
            raise CompositionAdmissionError("transfer_control_user_policy")
        ledger.require_transaction(transaction)
