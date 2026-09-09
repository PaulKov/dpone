"""Parameterized read-only catalog batches for workspace target observations.

CATALOG_DEFAULT delegates identifier equivalence to SQL Server. No dynamic SQL,
Python case folding, NOLOCK, or speculative outgoing dependency traversal.
"""

HEADER_SQL = """
/* dpone:workspace:header */
DECLARE @databases nvarchar(max) = ?;
SELECT DB_ID() AS database_id, DB_NAME() AS database_name,
       (SELECT COUNT(*) FROM OPENJSON(@databases) WITH (
           database_name nvarchar(128) '$.database'
       ) WHERE DB_ID(database_name) IS NULL OR DB_ID(database_name) <> DB_ID())
           AS database_reference_mismatch_count,
       d.containment, d.compatibility_level,
       CONVERT(int, SERVERPROPERTY('EngineEdition')) AS engine_edition,
       CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')) AS engine_version,
       CONVERT(nvarchar(128), SERVERPROPERTY('ServerName')) AS server_name,
       CONVERT(nvarchar(128), SERVERPROPERTY('MachineName')) AS machine_name,
       CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName')) AS instance_name,
       CONVERT(nvarchar(128), SERVERPROPERTY('ComputerNamePhysicalNetBIOS')) AS physical_name,
       CONVERT(nvarchar(128), SERVERPROPERTY('Collation')) AS server_collation,
       CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation')) AS database_collation,
       CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(
           CONVERT(nvarchar(128), N'x') COLLATE CATALOG_DEFAULT, 'Collation')) AS catalog_collation,
       HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION') AS can_view_definition,
       HAS_PERMS_BY_NAME('sys.sql_expression_dependencies', 'OBJECT', 'SELECT') AS can_select_dependencies,
       s.original_security_id AS original_sid,
       SUSER_SID() AS effective_sid, p.sid AS database_sid
FROM sys.databases AS d
JOIN sys.dm_exec_sessions AS s ON s.session_id = @@SPID
LEFT JOIN sys.database_principals AS p ON p.principal_id = DATABASE_PRINCIPAL_ID()
WHERE d.database_id = DB_ID();
"""

SLOTS_SQL = """
/* dpone:workspace:slots */
DECLARE @cap int = ?;
DECLARE @slots nvarchar(max) = ?;
WITH requested AS (
    SELECT slot_id, database_arg, schema_name, relation_name
    FROM OPENJSON(@slots) WITH (
        slot_id int '$.slot_id', database_arg nvarchar(128) '$.database',
        schema_name nvarchar(128) '$.schema', relation_name nvarchar(128) '$.relation'
    )
), classified AS (
    SELECT *, DENSE_RANK() OVER (
        ORDER BY schema_name COLLATE CATALOG_DEFAULT, relation_name COLLATE CATALOG_DEFAULT
    ) AS equivalence_class
    FROM requested
)
SELECT TOP (@cap) i.slot_id, i.equivalence_class, DB_ID(i.database_arg) AS database_id,
       s.schema_id, s.name AS schema_name, o.object_id, o.name AS object_name,
       RTRIM(o.type) AS object_type, o.is_ms_shipped,
       CONVERT(nvarchar(33), o.create_date, 126) AS create_token,
       CONVERT(nvarchar(33), o.modify_date, 126) AS modify_token,
       CASE WHEN EXISTS (
           SELECT 1 FROM (VALUES(i.database_arg), (i.schema_name), (i.relation_name)) AS names(value)
           WHERE CONVERT(varbinary(max), names.value) != CONVERT(varbinary(max),
               CONVERT(nvarchar(max), CONVERT(varchar(max), names.value COLLATE DATABASE_DEFAULT)))
       ) THEN 0 ELSE 1 END AS literal_roundtrip
FROM classified AS i
LEFT JOIN sys.schemas AS s ON s.name = i.schema_name COLLATE CATALOG_DEFAULT
LEFT JOIN sys.objects AS o
  ON o.schema_id = s.schema_id AND o.name = i.relation_name COLLATE CATALOG_DEFAULT
ORDER BY i.slot_id;
"""

DEPENDENCIES_SQL = """
/* dpone:workspace:incoming */
DECLARE @cap int = ?;
DECLARE @frontier nvarchar(max) = ?;
WITH f AS (
    SELECT frontier_id, database_arg, schema_arg, relation_arg
    FROM OPENJSON(@frontier) WITH (
        frontier_id int '$.frontier_id', database_arg nvarchar(128) '$.database',
        schema_arg nvarchar(128) '$.schema', relation_arg nvarchar(128) '$.relation'
    )
)
SELECT TOP (@cap) f.frontier_id, o.object_id, s.name AS schema_name, o.name AS object_name,
       CONVERT(nvarchar(33), o.create_date, 126) AS create_token,
       CONVERT(nvarchar(33), o.modify_date, 126) AS modify_token,
       d.referenced_server_name AS referenced_server,
       d.referenced_database_name AS referenced_database,
       d.referenced_schema_name AS referenced_schema,
       d.referenced_entity_name AS referenced_entity,
       d.referenced_id, d.referencing_minor_id, d.referenced_minor_id,
       CASE WHEN EXISTS (
           SELECT 1 FROM (VALUES(s.name), (o.name)) AS names(value)
           WHERE CONVERT(varbinary(max), names.value) != CONVERT(varbinary(max),
               CONVERT(nvarchar(max), CONVERT(varchar(max), names.value COLLATE DATABASE_DEFAULT)))
       ) THEN 0 ELSE 1 END AS literal_roundtrip
FROM f
JOIN sys.sql_expression_dependencies AS d
  ON d.referenced_database_name = f.database_arg COLLATE CATALOG_DEFAULT
 AND d.referenced_schema_name = f.schema_arg COLLATE CATALOG_DEFAULT
 AND d.referenced_entity_name = f.relation_arg COLLATE CATALOG_DEFAULT
JOIN sys.objects AS o ON o.object_id = d.referencing_id
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
WHERE o.type = 'V'
ORDER BY f.frontier_id, o.object_id, d.referencing_minor_id, d.referenced_minor_id;
"""
