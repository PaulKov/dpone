"""Canonical owned-stage catalog projections shared with CREATE.

The CREATE object projection is unchanged; the column projection uses a LEFT
JOIN so an unknown type cannot hide its physical column. Additional current
stage gates use an explicit SQL Server 16–17 candidate profile; live qualification
is separate. No SQL in this module grants privileges or changes an object.
"""

OBJECT_SQL = """
SELECT t.object_id,t.name,t.create_date,own.value,inc.value,
 CONVERT(int,t.is_memory_optimized)+t.temporal_type+CONVERT(int,t.is_filetable),
 (SELECT COUNT(*) FROM sys.indexes i WHERE i.object_id=t.object_id AND i.index_id>0),
 (SELECT COUNT(*) FROM sys.triggers tr WHERE tr.parent_id=t.object_id),
 (SELECT COUNT(*) FROM sys.objects o WHERE o.parent_object_id=t.object_id)
FROM sys.tables t
LEFT JOIN sys.extended_properties own ON own.class=1 AND own.major_id=t.object_id AND own.minor_id=0 AND own.name=N'dpone_native_owner'
LEFT JOIN sys.extended_properties inc ON inc.class=1 AND inc.major_id=t.object_id AND inc.minor_id=0 AND inc.name=N'dpone_tds_incarnation'
WHERE t.schema_id=? AND t.name=?;
"""
COLUMNS_SQL = """
SELECT TOP (101) c.column_id,c.name,t.name,c.is_nullable,c.max_length,c.precision,c.scale,c.collation_name,
 CONVERT(int,c.is_identity)+CONVERT(int,c.is_computed)+CONVERT(int,c.is_sparse)+CONVERT(int,c.is_column_set)
 +CONVERT(int,c.is_hidden)+CONVERT(int,c.is_filestream)+c.generated_always_type+CONVERT(int,c.is_masked)
 +COALESCE(c.encryption_type,0)+c.default_object_id+c.rule_object_id,
 t.is_user_defined,t.is_assembly_type
FROM sys.columns c LEFT JOIN sys.types t ON t.user_type_id=c.user_type_id
WHERE c.object_id=? ORDER BY c.column_id;
"""

STAGE_VISIBILITY_SQL = """
SELECT CONVERT(int,SERVERPROPERTY('ProductMajorVersion')),
 HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION'),
 HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','ALTER ANY SECURITY POLICY'),
 HAS_PERMS_BY_NAME(?,'OBJECT','VIEW DEFINITION');
"""
SCHEMA_SQL = "SELECT schema_id,name FROM sys.schemas WHERE schema_id=?;"
FEATURES_SQL = """
SELECT CONVERT(int,t.is_replicated),CONVERT(int,t.is_merge_published),
 CONVERT(int,t.is_sync_tran_subscribed),CONVERT(int,t.has_replication_filter),
 CONVERT(int,t.is_tracked_by_cdc),CONVERT(int,t.is_remote_data_archive_enabled),
 CONVERT(int,t.is_node),CONVERT(int,t.is_edge),CONVERT(int,t.ledger_type),
 CONVERT(int,t.is_dropped_ledger_table),
 (SELECT COUNT_BIG(*) FROM sys.external_tables e WHERE e.object_id=t.object_id),
 (SELECT COUNT_BIG(*) FROM sys.security_predicates p WHERE p.target_object_id=t.object_id)
FROM sys.tables t WHERE t.object_id=?;
"""
