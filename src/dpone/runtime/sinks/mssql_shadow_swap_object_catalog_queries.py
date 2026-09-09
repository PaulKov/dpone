"""SQL projection for MSSQL shadow-swap residual object metadata."""

from __future__ import annotations

TABLE_FLAGS = (
    "is_replicated",
    "has_replication_filter",
    "is_merge_published",
    "is_sync_tran_subscribed",
    "is_tracked_by_cdc",
    "is_remote_data_archive_enabled",
    "ansi_nulls_disabled",
    "filestream_enabled",
    "large_values_out_of_row",
    "text_in_row_enabled",
)
FEATURE_COUNTS = (
    "unsupported_extended_properties",
    "security_predicates",
    "fulltext_indexes",
    "change_tracking_tables",
    "non_simple_columns",
    "schema_bound_references",
)


def residual_catalog_sql(prefix: str) -> str:
    """Render the database-qualified residual table-surface query."""

    return f"""
        SELECT base.name AS base_data_space_name, base.type_desc AS base_data_space_type,
               lob.name AS lob_data_space_name, lob.type_desc AS lob_data_space_type,
               tab.is_replicated, tab.has_replication_filter, tab.is_merge_published,
               tab.is_sync_tran_subscribed, tab.is_tracked_by_cdc,
               tab.is_remote_data_archive_enabled,
               CONVERT(bit, CASE WHEN tab.uses_ansi_nulls=0 THEN 1 ELSE 0 END) AS ansi_nulls_disabled,
               CONVERT(bit, CASE WHEN tab.filestream_data_space_id > 0 THEN 1 ELSE 0 END) AS filestream_enabled,
               tab.large_value_types_out_of_row AS large_values_out_of_row,
               CONVERT(bit, CASE WHEN tab.text_in_row_limit > 0 THEN 1 ELSE 0 END) AS text_in_row_enabled,
               (SELECT COUNT_BIG(*) FROM {prefix}sys.extended_properties AS ep
                WHERE ep.class=1 AND ep.major_id=tab.object_id
                  AND ep.name NOT IN (?, ?, ?, ?, ?, ?)) AS unsupported_extended_properties,
               (SELECT COUNT_BIG(*) FROM {prefix}sys.security_predicates AS sp
                WHERE sp.target_object_id=tab.object_id) AS security_predicates,
               (SELECT COUNT_BIG(*) FROM {prefix}sys.fulltext_indexes AS ft
                WHERE ft.object_id=tab.object_id) AS fulltext_indexes,
               (SELECT COUNT_BIG(*) FROM {prefix}sys.change_tracking_tables AS ct
                WHERE ct.object_id=tab.object_id) AS change_tracking_tables,
               (SELECT COUNT_BIG(*) FROM {prefix}sys.columns AS c
                LEFT JOIN {prefix}sys.masked_columns AS mc
                  ON mc.object_id=c.object_id AND mc.column_id=c.column_id
                WHERE c.object_id=tab.object_id AND
                  (c.is_hidden=1 OR c.is_column_set=1 OR c.is_filestream=1
                   OR c.encryption_type IS NOT NULL OR c.xml_collection_id<>0
                   OR c.rule_object_id<>0 OR ISNULL(mc.is_masked, 0)=1)) AS non_simple_columns,
               (SELECT COUNT_BIG(DISTINCT sed.referencing_id)
                FROM {prefix}sys.sql_expression_dependencies AS sed
                INNER JOIN {prefix}sys.sql_modules AS sm ON sm.object_id=sed.referencing_id
                WHERE sed.referenced_id=tab.object_id AND sm.is_schema_bound=1) AS schema_bound_references
        FROM {prefix}sys.tables AS tab
        INNER JOIN {prefix}sys.schemas AS s ON s.schema_id=tab.schema_id
        LEFT JOIN {prefix}sys.indexes AS bi ON bi.object_id=tab.object_id AND bi.index_id IN (0, 1)
        LEFT JOIN {prefix}sys.data_spaces AS base ON base.data_space_id=bi.data_space_id
        LEFT JOIN {prefix}sys.data_spaces AS lob ON lob.data_space_id=tab.lob_data_space_id
        WHERE s.name=? AND tab.name=?
        /* dpone_shadow_swap_residual_catalog_v1 */
        """


def index_options_catalog_sql(
    prefix: str,
    *,
    modern: bool,
    ordered_columnstore: bool,
) -> str:
    """Render index-option SQL for the detected SQL Server capabilities."""

    sequential = "i.optimize_for_sequential_key" if modern else "CONVERT(bit, 0)"
    order_columns = "MAX(CONVERT(int, ic.column_store_order_ordinal))" if ordered_columnstore else "0"
    return f"""
        SELECT i.name, i.is_padded, i.allow_row_locks, i.allow_page_locks,
               i.compression_delay, {sequential} AS optimize_for_sequential_key,
               {order_columns} AS columnstore_order_columns
        FROM {prefix}sys.indexes AS i
        INNER JOIN {prefix}sys.tables AS tab ON tab.object_id=i.object_id
        INNER JOIN {prefix}sys.schemas AS s ON s.schema_id=tab.schema_id
        LEFT JOIN {prefix}sys.index_columns AS ic
          ON ic.object_id=i.object_id AND ic.index_id=i.index_id
        WHERE s.name=? AND tab.name=? AND i.index_id>0
        GROUP BY i.index_id, i.name, i.is_padded, i.allow_row_locks,
                 i.allow_page_locks, i.compression_delay, {sequential}
        ORDER BY i.index_id
        /* dpone_shadow_swap_index_options_v1 */
        """


__all__ = [
    "FEATURE_COUNTS",
    "TABLE_FLAGS",
    "index_options_catalog_sql",
    "residual_catalog_sql",
]
