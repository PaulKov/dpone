"""Read-only exact admission of the v1 SQL Server publication catalog."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.adapters.mssql_publication_catalog_ddl import (
    EVENT_COLUMNS,
    EVENT_TABLE,
    SLOT_COLUMNS,
    SLOT_TABLE,
    immutable_event_trigger,
    quote_identifier,
)

if TYPE_CHECKING:
    from dpone.ports.mssql_publication import PublicationAuthorityBinding, PublicationCatalogReader


def require_publication_catalog(connector: PublicationCatalogReader, *, binding: PublicationAuthorityBinding) -> None:
    """Reject missing, extended or drifted objects without attempting repair.

    This checks structure, not an exclusion fence against privileged operators.
    Endpoint identity and all-writer admission remain composition requirements.
    """
    db = quote_identifier(binding.database)
    tables = f"{db}.sys.tables t JOIN {db}.sys.schemas s ON s.schema_id=t.schema_id"
    where = "s.name=? AND t.name IN (?, ?)"
    params = (binding.schema, SLOT_TABLE, EVENT_TABLE)
    try:
        columns = connector.get_records(
            "SELECT t.name,c.column_id,c.name,ty.name,c.max_length,c.is_nullable,c.collation_name,c.scale "
            f"FROM {tables} JOIN {db}.sys.columns c ON c.object_id=t.object_id "
            f"JOIN {db}.sys.types ty ON ty.user_type_id=c.user_type_id WHERE {where}",
            params,
        )
        expected = [
            (table, i, *column)
            for table, fields in ((SLOT_TABLE, SLOT_COLUMNS), (EVENT_TABLE, EVENT_COLUMNS))
            for i, column in enumerate(fields, 1)
        ]
        if sorted(map(tuple, columns)) != sorted(expected):
            raise ValueError("columns")
        indexes = connector.get_records(
            "SELECT t.name,i.name,i.is_unique,i.is_primary_key,i.is_disabled,i.has_filter,i.ignore_dup_key,"
            "ic.key_ordinal,c.name,i.type "
            f"FROM {tables} JOIN {db}.sys.indexes i ON i.object_id=t.object_id "
            f"JOIN {db}.sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id "
            f"JOIN {db}.sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id WHERE {where}",
            params,
        )
        expected_indexes = [
            (SLOT_TABLE, "pk_publication_authority", 1, 1, 0, 0, 0, 1, "slot_key", 1),
            (EVENT_TABLE, "pk_publication_events", 1, 1, 0, 0, 0, 1, "slot_key", 1),
            (EVENT_TABLE, "pk_publication_events", 1, 1, 0, 0, 0, 2, "revision", 1),
            (EVENT_TABLE, "uq_publication_write", 1, 0, 0, 0, 0, 1, "write_id", 2),
        ]
        if sorted(map(tuple, indexes)) != sorted(expected_indexes):
            raise ValueError("indexes")
        triggers = connector.get_records(
            "SELECT t.name,tr.name,tr.is_disabled,tr.is_instead_of_trigger,m.definition "
            f"FROM {tables} JOIN {db}.sys.triggers tr ON tr.parent_id=t.object_id "
            f"LEFT JOIN {db}.sys.sql_modules m ON m.object_id=tr.object_id WHERE {where}",
            params,
        )
        if len(triggers) != 1 or tuple(triggers[0][:4]) != (EVENT_TABLE, "dpone_publication_events_immutable", 0, 0):
            raise ValueError("trigger")
        if _normalized(triggers[0][4]) != _normalized(immutable_event_trigger(schema=binding.schema)):
            raise ValueError("trigger definition")
        others = connector.get_records(
            f"SELECT t.name,o.name,o.type FROM {tables} JOIN {db}.sys.objects o ON o.parent_object_id=t.object_id "
            f"WHERE {where} AND o.type NOT IN ('PK','UQ','TR') "
            "UNION ALL "
            f"SELECT t.name,t.name,'unsupported' FROM {db}.sys.tables t "
            f"WHERE t.object_id IN (OBJECT_ID('{db}.{quote_identifier(binding.schema)}.[{SLOT_TABLE}]'),"
            f"OBJECT_ID('{db}.{quote_identifier(binding.schema)}.[{EVENT_TABLE}]')) AND "
            "(t.is_memory_optimized=1 OR t.temporal_type<>0 OR EXISTS "
            f"(SELECT 1 FROM {db}.sys.columns c WHERE c.object_id=t.object_id "
            "AND (c.is_computed=1 OR c.is_identity=1 OR c.generated_always_type<>0)) OR EXISTS "
            f"(SELECT 1 FROM {db}.sys.triggers tr WHERE tr.parent_id=t.object_id AND tr.is_not_for_replication=1))",
            params,
        )
        if others:
            raise ValueError("unsupported catalog objects")
    except Exception:
        raise ValueError("publication catalog is absent, incompatible or unverifiable") from None


def _normalized(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("missing trigger definition")
    return " ".join(value.split())
