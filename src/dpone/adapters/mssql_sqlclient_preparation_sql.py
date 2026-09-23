"""Literal, unqualified SQL16/17 preparation profile; no dynamic query discovery.

Hashes identify implemented statements, not independently qualified permissions.
Production baseline admission remains closed pending separate control evidence.
"""

from hashlib import sha256

from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import OWN_INCARNATION_SQL
from dpone.contracts.mssql_tds_api import OWNERSHIP_LABELS

ENV_SQL = """SELECT CONVERT(nvarchar(64),SERVERPROPERTY('ProductVersion')),
CONVERT(nvarchar(256),SERVERPROPERTY('Edition')),CONVERT(int,SERVERPROPERTY('EngineEdition')),
CONVERT(int,SERVERPROPERTY('ProductMajorVersion')),d.compatibility_level,d.collation_name,
d.database_id,d.name,d.containment,CONVERT(int,d.is_trustworthy_on),CONVERT(int,d.is_db_chaining_on),
l.principal_id,l.name,l.sid,l.type_desc,CONVERT(int,l.is_disabled),u.principal_id,u.name,u.sid,
u.type_desc,u.authentication_type_desc,d.owner_sid
FROM sys.databases d CROSS JOIN sys.server_principals l JOIN sys.database_principals u ON u.sid=l.sid
WHERE d.database_id=DB_ID() AND l.principal_id=? AND u.principal_id=?"""
_OWNER_CATALOGS = (
    ("sys.server_role_members", "member_principal_id", "login"),
    ("sys.database_role_members", "member_principal_id", "user"),
    ("sys.databases", "owner_sid", "sid"),
    ("sys.server_principals", "owning_principal_id", "login"),
    ("sys.database_principals", "owning_principal_id", "user"),
    ("sys.schemas", "principal_id", "user"),
    ("sys.objects", "principal_id", "user"),
    *(
        (name, "principal_id", "user")
        for name in (
            "sys.types",
            "sys.xml_schema_collections",
            "sys.assemblies",
            "sys.certificates",
            "sys.asymmetric_keys",
            "sys.symmetric_keys",
            "sys.fulltext_catalogs",
            "sys.fulltext_stoplists",
            "sys.registered_search_property_lists",
            "sys.service_message_types",
            "sys.service_contracts",
            "sys.services",
            "sys.remote_service_bindings",
            "sys.routes",
            "sys.database_scoped_credentials",
        )
    ),
    ("sys.endpoints", "principal_id", "login"),
    ("sys.external_libraries", "principal_id", "user"),
    ("sys.external_languages", "principal_id", "user"),
)
OWNERSHIP_SQL = tuple(
    (f"SELECT N'{label}',COUNT_BIG(*) FROM {catalog} WHERE {column}=?", subject)
    for label, (catalog, column, subject) in zip(OWNERSHIP_LABELS, _OWNER_CATALOGS, strict=True)
)
SERVER_SUBJECT = """p.grantee_principal_id=? OR p.grantee_principal_id=(
SELECT principal_id FROM sys.server_principals WHERE name=N'public' AND type=N'R')"""
SERVER_COUNT_SQL = "SELECT COUNT_BIG(*) FROM sys.server_permissions p WHERE " + SERVER_SUBJECT
SERVER_PERMISSIONS_SQL = (
    """SELECT p.class,p.major_id,p.grantee_principal_id,p.grantor_principal_id,
p.type,p.permission_name,p.state,p.class_desc,
CASE WHEN p.class=105 THEN e.name ELSE NULL END
FROM sys.server_permissions p LEFT JOIN sys.endpoints e ON p.class=105 AND e.endpoint_id=p.major_id
WHERE """
    + SERVER_SUBJECT
    + " ORDER BY p.class,p.major_id,p.grantee_principal_id,p.grantor_principal_id,p.type,p.state"
)
STAGE_SECURITY_SQL = "SELECT ?,COUNT_BIG(*) FROM sys.security_predicates WHERE target_object_id=?"
EFFECTIVE_SQL = (
    """SET NOCOUNT ON;
DECLARE @preparation_entered bit=0,@preparation_revert_attempted bit=0,@preparation_login sysname=?;
BEGIN TRY
 EXECUTE AS LOGIN=@preparation_login;
 SET @preparation_entered=1;
 BEGIN TRY
  SELECT SUSER_SNAME(),SUSER_SID(),USER_ID(),USER_NAME(),DB_ID(),DB_NAME(),
   IS_SRVROLEMEMBER(N'sysadmin'),IS_ROLEMEMBER(N'db_owner'),ORIGINAL_LOGIN();
  SELECT principal_id,sid,name,type,usage FROM sys.login_token ORDER BY principal_id,type,usage;
  SELECT principal_id,sid,name,type,usage FROM sys.user_token ORDER BY principal_id,type,usage;
  SELECT entity_name,subentity_name,permission_name FROM sys.fn_my_permissions(NULL,N'SERVER')
   ORDER BY entity_name,subentity_name,permission_name;
  SELECT entity_name,subentity_name,permission_name FROM sys.fn_my_permissions(NULL,N'DATABASE')
   ORDER BY entity_name,subentity_name,permission_name;
 END TRY
 BEGIN CATCH
  IF @preparation_entered=1 AND @preparation_revert_attempted=0
  BEGIN
   SET @preparation_revert_attempted=1;
   REVERT;
  END;
  THROW;
 END CATCH;
 SET @preparation_revert_attempted=1;
 REVERT;
END TRY
BEGIN CATCH
 THROW;
END CATCH;
"""
    + OWN_INCARNATION_SQL
)


def query_hashes() -> dict[str, str]:
    """Actual literal producer hashes; these alone never qualify a deployment."""
    statements = {
        "environment": ENV_SQL,
        "server_count": SERVER_COUNT_SQL,
        "server_permissions": SERVER_PERMISSIONS_SQL,
        "stage_security": STAGE_SECURITY_SQL,
        "effective": EFFECTIVE_SQL,
    }
    statements.update({label: statement for label, (statement, _) in zip(OWNERSHIP_LABELS, OWNERSHIP_SQL, strict=True)})
    for name, count, query, *_ in IDENTITY_SQL:
        statements[name + "_count"], statements[name] = count, query
    return {name: sha256(statement.encode("utf-8")).hexdigest() for name, statement in statements.items()}


# DISTINCT lookup keys are derived from the same fixed permission subjects.
_DATABASE_SUBJECT = "p.grantee_principal_id=? OR p.grantee_principal_id=0"
_SERVER_IDS = (
    "SELECT p.grantee_principal_id AS id FROM sys.server_permissions p WHERE "
    + SERVER_SUBJECT
    + " UNION SELECT p.grantor_principal_id FROM sys.server_permissions p WHERE "
    + SERVER_SUBJECT
    + " UNION SELECT ? UNION SELECT principal_id FROM sys.server_principals WHERE name=N'public' AND type=N'R'"
)
_DATABASE_IDS = (
    "SELECT p.grantee_principal_id AS id FROM sys.database_permissions p WHERE "
    + _DATABASE_SUBJECT
    + " UNION SELECT p.grantor_principal_id FROM sys.database_permissions p WHERE "
    + _DATABASE_SUBJECT
    + " UNION SELECT ? UNION SELECT 0 UNION SELECT 1"
)
_DATABASE_KEYS = (
    "SELECT DISTINCT p.class,p.major_id,p.minor_id FROM sys.database_permissions p WHERE " + _DATABASE_SUBJECT
)
IDENTITY_SQL: tuple[tuple[str, str, str, str, int, int], ...] = (
    (
        "server_principals",
        "SELECT COUNT_BIG(*) FROM (" + _SERVER_IDS + ") k",
        "SELECT k.id,s.name,s.sid,s.type_desc,CONVERT(int,s.is_disabled) FROM ("
        + _SERVER_IDS
        + ") k LEFT JOIN sys.server_principals s ON s.principal_id=k.id ORDER BY k.id",
        "login",
        3,
        8194,
    ),
    (
        "database_principals",
        "SELECT COUNT_BIG(*) FROM (" + _DATABASE_IDS + ") k",
        "SELECT k.id,s.name,s.sid,s.type_desc,s.authentication_type_desc FROM ("
        + _DATABASE_IDS
        + ") k LEFT JOIN sys.database_principals s ON s.principal_id=k.id ORDER BY k.id",
        "user",
        3,
        8194,
    ),
    (
        "database_securables",
        "SELECT COUNT_BIG(*) FROM (" + _DATABASE_KEYS + ") k",
        """SELECT k.class,k.major_id,k.minor_id,
CASE k.class WHEN 0 THEN N'DATABASE' WHEN 1 THEN N'OBJECT_OR_COLUMN' WHEN 3 THEN N'SCHEMA' WHEN 4 THEN N'DATABASE_PRINCIPAL' ELSE N'UNSUPPORTED' END,
CASE WHEN k.class=1 THEN COALESCE(os.name,OBJECT_SCHEMA_NAME(k.major_id)) ELSE NULL END,
CASE k.class WHEN 1 THEN COALESCE(o.name,OBJECT_NAME(k.major_id)) WHEN 3 THEN s.name WHEN 4 THEN u.name ELSE NULL END,
CASE WHEN k.class=1 AND k.minor_id<>0 THEN COALESCE(c.name,COL_NAME(k.major_id,k.minor_id)) ELSE NULL END,
CASE WHEN k.class=1 THEN COALESCE(CONVERT(int,o.is_ms_shipped),CONVERT(int,OBJECTPROPERTYEX(k.major_id,N'IsMSShipped'))) ELSE NULL END
FROM ("""
        + _DATABASE_KEYS
        + """ ) k
LEFT JOIN sys.all_objects o ON k.class=1 AND o.object_id=k.major_id
LEFT JOIN sys.schemas os ON os.schema_id=o.schema_id
LEFT JOIN sys.all_columns c ON k.class=1 AND c.object_id=k.major_id AND c.column_id=k.minor_id
LEFT JOIN sys.schemas s ON k.class=3 AND s.schema_id=k.major_id
LEFT JOIN sys.database_principals u ON k.class=4 AND u.principal_id=k.major_id
ORDER BY k.class,k.major_id,k.minor_id""",
        "user",
        1,
        4096,
    ),
)

_ENDPOINT_KEYS = (
    "SELECT DISTINCT p.major_id AS id FROM sys.server_permissions p WHERE p.class=105 AND (" + SERVER_SUBJECT + ")"
)
IDENTITY_SQL += (
    (
        "server_endpoints",
        "SELECT COUNT_BIG(*) FROM (" + _ENDPOINT_KEYS + ") k",
        "SELECT k.id,e.name,e.type_desc,e.protocol_desc,e.state_desc,CONVERT(int,e.is_admin_endpoint),CONVERT(int,CASE WHEN e.endpoint_id<65536 THEN 1 ELSE 0 END) FROM ("
        + _ENDPOINT_KEYS
        + ") k LEFT JOIN sys.endpoints e ON e.endpoint_id=k.id ORDER BY k.id",
        "login",
        1,
        4096,
    ),
)
