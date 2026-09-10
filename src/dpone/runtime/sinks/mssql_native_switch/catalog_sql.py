"""Explicit SQL Server 2022 catalog projection for the finite v1 profile.

No wildcard metadata reads: the parser requires every projected field. Other
server majors fail before this version-specific query. Definitions are retained
in fingerprints; unsupported dependencies never become assumed empty metadata.
"""

DATABASE_SQL = """
SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int) AS major,
       CAST(SERVERPROPERTY('EngineEdition') AS int) AS edition,
       HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION') AS visible,
       HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW ANY DEFINITION') AS server_visible,
       (SELECT COUNT_BIG(*) FROM sys.triggers WHERE parent_class = 0 AND is_disabled = 0) AS database_triggers,
       (SELECT COUNT_BIG(*) FROM sys.server_triggers WHERE is_disabled = 0) AS server_triggers,
       DB_ID() AS database_id, DB_NAME() AS name,
       CONVERT(nvarchar(36), service_broker_guid) + ':' +
       CONVERT(nvarchar(33), create_date, 126) AS generation
FROM sys.databases WHERE database_id = DB_ID()
"""

# Nested FOR JSON queries keep ordered, explicit catalog sections together.
TABLE_SQL = """
DECLARE @schema sysname = ?, @table sysname = ?;
SELECT (
 SELECT t.object_id, s.schema_id, s.name AS schema_name, t.name,
 CONVERT(nvarchar(33), t.create_date, 126) AS created_at,
 CONVERT(nvarchar(33), t.modify_date, 126) AS modified_at,
 COALESCE(t.principal_id, s.principal_id) AS principal_id,
 CONVERT(nvarchar(4000), ep.value) AS owner_tag,
 t.temporal_type, t.is_memory_optimized, t.is_filetable, t.is_replicated,
 t.has_replication_filter, t.is_merge_published, t.is_sync_tran_subscribed,
 t.is_tracked_by_cdc, t.is_remote_data_archive_enabled, t.is_external,
 t.ledger_type, t.is_node, t.is_edge, t.filestream_data_space_id,
 t.lob_data_space_id,
 JSON_QUERY((SELECT c.column_id, c.name, ty.name AS type_name,
    c.system_type_id, c.user_type_id, c.max_length, c.precision, c.scale,
    c.is_nullable, c.collation_name, c.is_identity, c.is_computed, c.is_sparse,
    c.is_column_set, c.is_filestream, c.generated_always_type, c.is_hidden,
    c.is_masked, c.encryption_type, ty.is_user_defined, ty.is_assembly_type,
    c.xml_collection_id, c.is_rowguidcol, c.is_ansi_padded, c.default_object_id, c.rule_object_id
    FROM sys.columns c JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE c.object_id = t.object_id ORDER BY c.column_id
    FOR JSON PATH, INCLUDE_NULL_VALUES)) AS columns,
 JSON_QUERY((SELECT i.index_id, i.name, i.type, i.is_unique, i.data_space_id,
    i.is_disabled, i.is_hypothetical, i.has_filter, i.filter_definition,
    i.is_primary_key, i.is_unique_constraint, i.ignore_dup_key, i.fill_factor,
    i.is_padded, i.allow_row_locks, i.allow_page_locks,
    JSON_QUERY((SELECT ic.column_id, ic.index_column_id, ic.key_ordinal,
      ic.partition_ordinal, ic.is_descending_key, ic.is_included_column
      FROM sys.index_columns ic WHERE ic.object_id = t.object_id AND ic.index_id = i.index_id
      ORDER BY ic.index_column_id FOR JSON PATH, INCLUDE_NULL_VALUES)) AS columns
    FROM sys.indexes i WHERE i.object_id = t.object_id ORDER BY i.index_id
    FOR JSON PATH, INCLUDE_NULL_VALUES)) AS indexes,
 JSON_QUERY((SELECT ps.data_space_id AS scheme_id, ps.function_id, pf.name,
    pf.boundary_value_on_right AS range_right, pp.system_type_id, pp.user_type_id,
    pp.max_length, pp.precision, pp.scale,
    JSON_QUERY((SELECT rv.boundary_id, CASE WHEN SQL_VARIANT_PROPERTY(rv.value, 'BaseType') = 'date'
        THEN CONVERT(nvarchar(40), CONVERT(date, rv.value), 126)
        WHEN SQL_VARIANT_PROPERTY(rv.value, 'BaseType') = 'datetime2'
        THEN CONVERT(nvarchar(40), CONVERT(datetime2(7), rv.value), 126) ELSE NULL END AS value
      FROM sys.partition_range_values rv WHERE rv.function_id = pf.function_id
      ORDER BY rv.boundary_id FOR JSON PATH, INCLUDE_NULL_VALUES)) AS boundaries
    FROM sys.indexes i JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
    JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
    WHERE i.object_id = t.object_id AND i.index_id IN (0, 1)
    FOR JSON PATH, INCLUDE_NULL_VALUES)) AS functions,
 JSON_QUERY((SELECT p.index_id, p.partition_number, p.partition_id, p.hobt_id,
    p.data_compression, p.xml_compression, dds.data_space_id AS filegroup_id
    FROM sys.partitions p JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
    LEFT JOIN sys.destination_data_spaces dds ON i.data_space_id = dds.partition_scheme_id
      AND p.partition_number = dds.destination_id
    WHERE p.object_id = t.object_id ORDER BY p.index_id, p.partition_number
    FOR JSON PATH, INCLUDE_NULL_VALUES)) AS partitions,
 JSON_QUERY((SELECT o.object_id, o.type, OBJECT_DEFINITION(o.object_id) AS definition
    FROM sys.objects o WHERE o.parent_object_id = t.object_id
    ORDER BY o.object_id FOR JSON PATH, INCLUDE_NULL_VALUES)) AS children,
 (SELECT COUNT_BIG(*) FROM sys.foreign_keys WHERE referenced_object_id = t.object_id) AS incoming_fks,
 (SELECT COUNT_BIG(*) FROM sys.sql_expression_dependencies
    WHERE referenced_id = t.object_id AND is_schema_bound_reference = 1) AS bound_dependencies,
 (SELECT COUNT_BIG(*) FROM sys.security_predicates WHERE target_object_id = t.object_id) AS security_predicates,
 (SELECT COUNT_BIG(*) FROM sys.change_tracking_tables WHERE object_id = t.object_id) AS change_tracking,
 (SELECT COUNT_BIG(*) FROM sys.fulltext_indexes WHERE object_id = t.object_id) AS fulltext_indexes
 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
 LEFT JOIN sys.extended_properties ep ON ep.class = 1 AND ep.major_id = t.object_id
    AND ep.minor_id = 0 AND ep.name = N'dpone.native_switch.owner.v1'
 WHERE s.name = @schema AND t.name = @table
 FOR JSON PATH, INCLUDE_NULL_VALUES
) AS metadata
"""

TRANSACTION_SQL = """
SELECT XACT_STATE() AS state, @@TRANCOUNT AS depth, @@SPID AS session_id,
       DB_ID() AS database_id, @@OPTIONS & 16384 AS xact_abort,
       transaction_isolation_level AS isolation
FROM sys.dm_exec_sessions WHERE session_id = @@SPID
"""
