"""Fixed read-only SQL used only by the isolated restricted-login helper."""

CONTEXT_SQL = """
SELECT SUSER_SNAME(),LOWER(CONVERT(varchar(170),SUSER_SID(),2)),ORIGINAL_LOGIN(),
 LOWER(CONVERT(varchar(170),SUSER_SID(ORIGINAL_LOGIN()),2)),
 (SELECT authenticating_database_id FROM sys.dm_exec_sessions WHERE session_id=@@SPID),
 DB_ID(),DB_NAME(),USER_ID(),USER_NAME(),
 LOWER(CONVERT(varchar(170),(SELECT sid FROM sys.database_principals WHERE principal_id=USER_ID()),2)),
 CONVERT(bit,ISNULL(IS_SRVROLEMEMBER(N'sysadmin'),0)),
 CONVERT(bit,ISNULL(IS_ROLEMEMBER(N'db_owner'),0)),CONVERT(bit,CASE WHEN USER_ID()=1 THEN 1 ELSE 0 END),
 CONVERT(int,1),@@TRANCOUNT,@@TRANCOUNT,
 XACT_STATE(),CONVERT(bit,CASE WHEN (@@OPTIONS & 2)<>0 THEN 1 ELSE 0 END);
"""

LOGIN_TOKEN_SQL = """
SELECT TOP (9) principal_id,LOWER(CONVERT(varchar(170),sid,2)),name,type,usage
FROM sys.login_token ORDER BY principal_id,name,type,usage;
"""

USER_TOKEN_SQL = """
SELECT TOP (9) principal_id,LOWER(CONVERT(varchar(170),sid,2)),name,type,usage
FROM sys.user_token ORDER BY principal_id,name,type,usage;
"""

SERVER_PERMISSIONS_SQL = """
SELECT TOP (129) NULLIF(entity_name,N''),NULLIF(subentity_name,N''),permission_name
FROM sys.fn_my_permissions(NULL,N'SERVER')
ORDER BY entity_name,subentity_name,permission_name;
"""

DATABASE_PERMISSIONS_SQL = """
SELECT TOP (129) NULLIF(entity_name,N''),NULLIF(subentity_name,N''),permission_name
FROM sys.fn_my_permissions(NULL,N'DATABASE')
ORDER BY entity_name,subentity_name,permission_name;
"""

STAGE_PERMISSIONS_SQL = """
SELECT DISTINCT TOP (4) permission_name
FROM sys.fn_my_permissions(?,N'OBJECT')
ORDER BY permission_name;
"""

DATABASE_SQL = """
SELECT d.database_id,d.name,r.database_guid
FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id=d.database_id
WHERE d.database_id=DB_ID();
"""
