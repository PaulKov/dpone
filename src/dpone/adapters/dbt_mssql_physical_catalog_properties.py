"""Finite SQL Server 2022 forbidden-property coverage for catalog observation.

Every branch runs for an observation, including disabled constraints, policies
and triggers. An empty result means this registry found no occurrence, conditional
on the separately verified metadata-visibility permission inventory. It never
proves absence of cross-database or dynamic SQL references.
"""

# Entries are closed SQL, not caller supplied expressions. Names are stable wire
# codes; multiple occurrences intentionally remain multiple rows (UNION ALL).
_PROPERTIES = (
    ("CHECK_CONSTRAINT", "sys.check_constraints", "parent_object_id=@object_id", "object_id", "NULL"),
    ("KEY_CONSTRAINT", "sys.key_constraints", "parent_object_id=@object_id", "object_id", "NULL"),
    ("FOREIGN_KEY_INBOUND", "sys.foreign_keys", "referenced_object_id=@object_id", "object_id", "NULL"),
    ("FOREIGN_KEY_OUTBOUND", "sys.foreign_keys", "parent_object_id=@object_id", "object_id", "NULL"),
    ("DEFAULT_CONSTRAINT", "sys.default_constraints", "parent_object_id=@object_id", "object_id", "parent_column_id"),
    ("DML_TRIGGER", "sys.triggers", "parent_class=1 AND parent_id=@object_id", "object_id", "NULL"),
    (
        "OBJECT_PERMISSION",
        "sys.database_permissions",
        "class=1 AND major_id=@object_id AND minor_id=0",
        "major_id",
        "NULL",
    ),
    (
        "COLUMN_PERMISSION",
        "sys.database_permissions",
        "class=1 AND major_id=@object_id AND minor_id<>0",
        "major_id",
        "minor_id",
    ),
    (
        "EXTENDED_PROPERTY",
        "sys.extended_properties",
        "(class IN (1,7) AND major_id=@object_id) OR (class=1 AND major_id IN (SELECT object_id FROM sys.objects WHERE parent_object_id=@object_id))",
        "major_id",
        "minor_id",
    ),
    ("USER_STATISTIC", "sys.stats", "object_id=@object_id AND user_created=1", "object_id", "NULL"),
    ("FULLTEXT_INDEX", "sys.fulltext_indexes", "object_id=@object_id", "object_id", "NULL"),
    ("CHANGE_TRACKING", "sys.change_tracking_tables", "object_id=@object_id", "object_id", "NULL"),
    ("ROW_SECURITY_PREDICATE", "sys.security_predicates", "target_object_id=@object_id", "object_id", "NULL"),
    (
        "LEGACY_BOUND_DEFAULT",
        "sys.columns c JOIN sys.types t ON t.user_type_id=c.user_type_id",
        "c.object_id=@object_id AND (c.default_object_id<>0 OR t.default_object_id<>0)",
        "c.object_id",
        "c.column_id",
    ),
    (
        "LEGACY_BOUND_RULE",
        "sys.columns c JOIN sys.types t ON t.user_type_id=c.user_type_id",
        "c.object_id=@object_id AND (c.rule_object_id<>0 OR t.rule_object_id<>0)",
        "c.object_id",
        "c.column_id",
    ),
    (
        "OBJECT_OWNER",
        "sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id",
        "o.object_id=@object_id AND COALESCE(o.principal_id,s.principal_id)<>1",
        "o.object_id",
        "NULL",
    ),
    ("SYSTEM_OBJECT", "sys.objects", "object_id=@object_id AND is_ms_shipped<>0", "object_id", "NULL"),
    ("TABLE_EXTERNAL", "sys.tables", "object_id=@object_id AND is_external<>0", "object_id", "NULL"),
    (
        "TABLE_REPLICATION",
        "sys.tables",
        "object_id=@object_id AND (is_replicated<>0 OR has_replication_filter<>0 OR is_merge_published<>0 OR is_sync_tran_subscribed<>0 OR has_unchecked_assembly_data<>0)",
        "object_id",
        "NULL",
    ),
    ("TABLE_CDC", "sys.tables", "object_id=@object_id AND is_tracked_by_cdc<>0", "object_id", "NULL"),
    (
        "TABLE_REMOTE_ARCHIVE",
        "sys.tables",
        "object_id=@object_id AND is_remote_data_archive_enabled<>0",
        "object_id",
        "NULL",
    ),
    (
        "TABLE_HISTORY",
        "sys.tables",
        "object_id=@object_id AND (history_table_id IS NOT NULL OR temporal_type<>0)",
        "object_id",
        "NULL",
    ),
    (
        "TABLE_LEDGER",
        "sys.tables",
        "object_id=@object_id AND (ledger_type<>0 OR ledger_view_id IS NOT NULL OR is_dropped_ledger_table<>0)",
        "object_id",
        "NULL",
    ),
    (
        "TABLE_OPTIONS",
        "sys.tables",
        "object_id=@object_id AND (text_in_row_limit<>0 OR large_value_types_out_of_row<>0 OR lock_on_bulk_load<>0 OR uses_ansi_nulls<>1 OR lock_escalation<>0)",
        "object_id",
        "NULL",
    ),
    ("COLUMN_ROWGUID", "sys.columns", "object_id=@object_id AND is_rowguidcol<>0", "object_id", "column_id"),
    ("COLUMN_FILESTREAM", "sys.columns", "object_id=@object_id AND is_filestream<>0", "object_id", "column_id"),
    (
        "COLUMN_XML",
        "sys.columns",
        "object_id=@object_id AND (is_xml_document<>0 OR xml_collection_id<>0)",
        "object_id",
        "column_id",
    ),
    (
        "COLUMN_REPLICATION",
        "sys.columns",
        "object_id=@object_id AND (is_replicated<>0 OR is_non_sql_subscribed<>0 OR is_merge_published<>0 OR is_dts_replicated<>0)",
        "object_id",
        "column_id",
    ),
    ("COLUMN_GRAPH", "sys.columns", "object_id=@object_id AND graph_type IS NOT NULL", "object_id", "column_id"),
    (
        "COLUMN_LEDGER",
        "sys.columns",
        "object_id=@object_id AND (ledger_view_column_type IS NOT NULL OR is_dropped_ledger_column<>0)",
        "object_id",
        "column_id",
    ),
    (
        "COLUMN_ENCRYPTION_METADATA",
        "sys.columns",
        "object_id=@object_id AND (encryption_algorithm_name IS NOT NULL OR column_encryption_key_id IS NOT NULL OR column_encryption_key_database_name IS NOT NULL)",
        "object_id",
        "column_id",
    ),
    (
        "INDEX_OPTIONS",
        "sys.indexes",
        "object_id=@object_id AND (fill_factor<>0 OR is_padded<>0 OR ignore_dup_key<>0 OR optimize_for_sequential_key<>0 OR (compression_delay IS NOT NULL AND compression_delay<>0) OR allow_row_locks<>CASE WHEN type=5 THEN 0 ELSE 1 END OR allow_page_locks<>CASE WHEN type=5 THEN 0 ELSE 1 END)",
        "object_id",
        "NULL",
    ),
    ("INDEX_RESUMABLE_OPERATION", "sys.index_resumable_operations", "object_id=@object_id", "object_id", "NULL"),
    ("PARTITION_XML_COMPRESSION", "sys.partitions", "object_id=@object_id AND xml_compression<>0", "object_id", "NULL"),
    (
        "DEPENDENCY_CLASS_UNSUPPORTED",
        "sys.sql_expression_dependencies",
        "(referencing_id=@object_id OR referenced_id=@object_id OR (referenced_id IS NULL "
        "AND referenced_server_name IS NULL AND (referenced_database_name IS NULL OR referenced_database_name=DB_NAME()) "
        "AND (referenced_schema_name IS NULL OR referenced_schema_name=@model_schema) "
        "AND referenced_entity_name=@object_name)) AND (referencing_class<>1 OR referenced_class<>1)",
        "referencing_id",
        "referencing_minor_id",
    ),
)

FORBIDDEN_PROPERTY_CODES = frozenset(item[0] for item in _PROPERTIES)


def forbidden_property_query() -> str:
    """Return the complete occurrence query; caller must bound before sorting."""
    return "\nUNION ALL\n".join(
        f"SELECT CAST('{code}' AS varchar(40)) AS property_code, "
        f"CAST({object_id} AS int) AS related_object_id, "
        f"CAST({column_id} AS int) AS related_column_id FROM {source} WHERE {predicate}"
        for code, source, predicate, object_id, column_id in _PROPERTIES
    )
