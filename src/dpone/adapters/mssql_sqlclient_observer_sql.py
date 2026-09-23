"""Fixed read-only SQL for a dedicated observer in the admitted database.

The current DB_ID is only a catalog lookup-location guard. Writer identity uses
s.database_id and explicit target joins. No nonce/transport filter may hide a
competing connection. Metadata grants may be scoped; missing rows fail closed.
"""

VISIBILITY_SQL = """
SELECT CONVERT(int, SERVERPROPERTY('ProductMajorVersion')),
       CONVERT(int, SERVERPROPERTY('EngineEdition')),
       HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE'),
       HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER PERFORMANCE STATE'),
       DB_ID();
"""

WRITER_SQL = """
SELECT c.connection_id, c.session_id, c.connect_time, s.login_time,
       c.parent_connection_id, c.net_transport, c.protocol_type, c.auth_scheme,
       (SELECT COUNT(*) FROM sys.dm_exec_connections child
        WHERE child.parent_connection_id = c.connection_id),
       s.status, CONVERT(int, s.is_user_process), s.open_transaction_count,
       (SELECT COUNT(*) FROM sys.dm_tran_session_transactions t
        WHERE t.session_id = s.session_id),
       (SELECT COUNT(*) FROM sys.dm_exec_requests r WHERE r.session_id = s.session_id),
       s.context_info,
       CONVERT(nvarchar(128), SERVERPROPERTY('ServerName')),
       CONVERT(nvarchar(128), SERVERPROPERTY('MachineName')),
       COALESCE(CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName')), N'MSSQLSERVER'),
       CONVERT(nvarchar(128), SERVERPROPERTY('ComputerNamePhysicalNetBIOS')),
       s.database_id, d.name, recovery.database_guid, d.owner_sid,
       p.principal_id, p.name, p.sid, s.original_login_name, s.original_security_id,
       s.authenticating_database_id, IS_SRVROLEMEMBER('sysadmin', s.login_name),
       p.type_desc, c.encrypt_option, s.login_name, s.security_id, original.name
FROM sys.dm_exec_connections c
LEFT JOIN sys.dm_exec_sessions s ON s.session_id = c.session_id
LEFT JOIN sys.databases d ON d.database_id = s.database_id
LEFT JOIN sys.database_recovery_status recovery ON recovery.database_id = s.database_id
LEFT JOIN sys.server_principals p ON p.sid = s.security_id
LEFT JOIN sys.server_principals original ON original.sid = s.original_security_id
WHERE c.session_id = ?;
"""

PRINCIPALS_SQL = """
SELECT principal_id, name, sid, type_desc, authentication_type_desc
FROM sys.database_principals
WHERE principal_id = 1 OR sid = ?;
"""

ADMISSION_SQL = """
SELECT CONVERT(nvarchar(128), SERVERPROPERTY('ServerName')),
       CONVERT(nvarchar(128), SERVERPROPERTY('MachineName')),
       COALESCE(CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName')), N'MSSQLSERVER'),
       CONVERT(nvarchar(128), SERVERPROPERTY('ComputerNamePhysicalNetBIOS')),
       d.database_id, d.name, recovery.database_guid, d.owner_sid,
       p.principal_id, p.name, p.sid, IS_SRVROLEMEMBER('sysadmin', p.name), p.type_desc
FROM sys.databases d
LEFT JOIN sys.database_recovery_status recovery ON recovery.database_id = d.database_id
CROSS JOIN sys.server_principals p
WHERE d.database_id = ? AND p.name = ?;
"""
