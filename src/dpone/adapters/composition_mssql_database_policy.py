"""Observe the complete bounded SQL target policy before writer issuance.

Every catalog predicate produces a fixed diagnostic code; a declared policy
violation is distinct from an unavailable or uncertain driver operation.
"""

from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.dbapi_lifecycle import row
from dpone.contracts.composition_control import CompositionAdmissionError

_POLICY_REASONS = (
    "role_owner",
    "additional_role",
    "object_type",
    "object_owner",
    "unmanaged_object",
    "assembly",
    "user_authentication",
    "ambient_permission",
    "role_membership",
    "schema_owner",
    "server_principal",
    "public_server_permission",
)


def require_database_policy(
    ledger: CompositionMssqlLedger, *, database: str, writer_role: str, schemas: tuple[str, ...], guard_id: str
) -> None:
    """Reject ambient writers and require the bounded role's exact permission set.

    The controller owns the database; dbo owns roles and managed objects. Ordinary bindings
    are SQL instance users with read-only direct grants, optionally db_datareader.
    No other user-defined role, executable module or assembly is admitted.
    """
    for name in (database, writer_role, *schemas):
        require_control_schema(name)
    db = f"[{database}]"
    gates = ledger.table("login_gates")
    ledger.cursor.execute(
        f"SELECT p.class, p.major_id, s.name, p.permission_name, p.state FROM {db}.sys.database_permissions p "
        f"JOIN {db}.sys.database_principals u ON p.grantee_principal_id=u.principal_id "
        f"LEFT JOIN {db}.sys.schemas s ON p.class=3 AND p.major_id=s.schema_id WHERE u.name=?;",
        writer_role,
    )
    observed = set(tuple(value) for value in ledger.cursor.fetchall())
    expected: set[tuple[int, int, str | None, str, str]] = {
        (0, 0, None, "CREATE TABLE", "G"),
        (0, 0, None, "CREATE VIEW", "G"),
    }
    ledger.cursor.execute(f"SELECT schema_id, name FROM {db}.sys.schemas;")
    schema_ids = {name: schema_id for schema_id, name in ledger.cursor.fetchall()}
    for name in schemas:
        if name not in schema_ids:
            raise CompositionAdmissionError("login_schema_missing")
        expected.update(
            (3, schema_ids[name], name, permission, "G")
            for permission in ("SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "ALTER", "VIEW DEFINITION")
        )
    if observed != expected:
        raise CompositionAdmissionError("login_role_permissions")
    ledger.cursor.execute(
        f"""DECLARE @role sysname = ?, @guard varchar(71) = ?;
SELECT CASE
WHEN NOT EXISTS (SELECT 1 FROM {db}.sys.database_principals
    WHERE name=@role AND type='R' AND is_fixed_role=0 AND owning_principal_id=1) THEN 1
WHEN EXISTS (SELECT 1 FROM {db}.sys.database_principals WHERE type='R' AND is_fixed_role=0 AND name NOT IN ('public', @role)) THEN 2
WHEN EXISTS (SELECT 1 FROM {db}.sys.objects WHERE is_ms_shipped=0 AND type NOT IN ('U','V','PK','UQ','F','D','C','IT')) THEN 3
WHEN EXISTS (SELECT 1 FROM {db}.sys.objects WHERE is_ms_shipped=0 AND principal_id IS NOT NULL AND principal_id<>1) THEN 4
WHEN EXISTS (SELECT 1 FROM {db}.sys.objects o JOIN {db}.sys.schemas s ON o.schema_id=s.schema_id
    WHERE o.is_ms_shipped=0 AND NOT EXISTS (SELECT 1 FROM {ledger.table("mssql_managed_schemas")} m
        WHERE m.guard_id=@guard AND m.schema_name=s.name COLLATE Latin1_General_100_BIN2)) THEN 5
WHEN EXISTS (SELECT 1 FROM {db}.sys.assemblies WHERE is_user_defined=1) THEN 6
WHEN EXISTS (SELECT 1 FROM {db}.sys.database_principals u WHERE u.principal_id>4 AND u.type<>'R'
    AND (u.type<>'S' OR u.authentication_type<>1)) THEN 7
WHEN EXISTS (SELECT 1 FROM {db}.sys.database_permissions p
    JOIN {db}.sys.database_principals u ON p.grantee_principal_id=u.principal_id
    WHERE u.name<>@role AND u.name NOT IN ('dbo','sys','INFORMATION_SCHEMA')
      AND (p.permission_name NOT IN ('SELECT','VIEW DEFINITION','CONNECT',
          'VIEW ANY COLUMN ENCRYPTION KEY DEFINITION','VIEW ANY COLUMN MASTER KEY DEFINITION')
          OR p.state NOT IN ('G','D') OR (u.name='guest' AND p.permission_name='CONNECT' AND p.state='G'))) THEN 8
WHEN EXISTS (SELECT 1 FROM {db}.sys.database_role_members m
    JOIN {db}.sys.database_principals u ON u.principal_id=m.member_principal_id
    JOIN {db}.sys.database_principals r ON r.principal_id=m.role_principal_id
    LEFT JOIN {gates} g ON g.login_sid=u.sid
    WHERE NOT ((u.principal_id=1 AND u.name='dbo' AND u.sid=SUSER_SID(ORIGINAL_LOGIN())
               AND r.name='db_owner' AND r.is_fixed_role=1)
          OR (r.name=@role AND g.login_sid IS NOT NULL AND u.name COLLATE Latin1_General_100_BIN2=g.login_name)
          OR (r.name='db_datareader' AND g.login_sid IS NULL))) THEN 9
WHEN EXISTS (SELECT 1 FROM {db}.sys.schemas s JOIN {db}.sys.database_principals u ON s.principal_id=u.principal_id
    WHERE s.schema_id<16384 AND u.principal_id>4) THEN 10
WHEN EXISTS (SELECT 1 FROM {db}.sys.database_principals u JOIN sys.server_principals p ON p.sid=u.sid
    LEFT JOIN {gates} g ON g.login_sid=p.sid WHERE u.principal_id>4 AND g.login_sid IS NULL
    AND (p.type<>'S' OR EXISTS (SELECT 1 FROM sys.server_role_members m WHERE m.member_principal_id=p.principal_id)
         OR EXISTS (SELECT 1 FROM sys.server_permissions x WHERE x.grantee_principal_id=p.principal_id
             AND NOT (x.permission_name IN ('CONNECT SQL','VIEW ANY DEFINITION') AND x.state='G')))) THEN 11
WHEN EXISTS (SELECT 1 FROM sys.server_permissions x JOIN sys.server_principals p
    ON x.grantee_principal_id=p.principal_id WHERE p.name='public'
    AND NOT (x.state='G' AND ((x.class=100 AND x.permission_name IN ('CONNECT SQL','VIEW ANY DATABASE'))
        OR (x.class=105 AND x.permission_name='CONNECT' AND x.major_id IN
            (SELECT endpoint_id FROM sys.endpoints WHERE type=2)))))
 THEN 12
ELSE 0 END;""",
        writer_role,
        guard_id,
    )
    observed_policy = row(ledger.cursor)
    if observed_policy != (0,) or type(observed_policy[0]) is not int:
        index = observed_policy[0] if observed_policy and len(observed_policy) == 1 else None
        reason = (
            _POLICY_REASONS[index - 1] if type(index) is int and 1 <= index <= len(_POLICY_REASONS) else "observation"
        )
        raise CompositionAdmissionError("login_database_policy_" + reason)
