"""Exact catalog policy for MSSQL backfill shadow publication."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.runtime.sinks.mssql_backfill_publication_names import (
    publication_contract_suffixes,
    publication_table_names,
)
from dpone.runtime.support.mssql_object_name import (
    MSSQLObjectName,
    mssql_sp_rename_statement,
    quote_mssql_identifier,
)

MssqlPublicationTarget = MSSQLObjectName
SHADOW_OWNER_PROPERTY = "dpone_backfill_run_key"


def publication_target(
    *,
    database: str,
    schema: str,
    table: str,
) -> MssqlPublicationTarget:
    """Build the exact physical target used by the publication adapter."""

    return MSSQLObjectName.from_parts(
        database=database,
        schema=schema,
        table=table,
        strict=True,
    )


def publication_names(
    load_config: Any,
    *,
    run_key: str,
    artifact_scope: str,
) -> tuple[MssqlPublicationTarget, MssqlPublicationTarget, MssqlPublicationTarget]:
    """Resolve exact live, shadow and backup coordinates for one campaign."""

    live = publication_target(
        database=str(getattr(load_config, "target_database", "") or ""),
        schema=str(load_config.target_schema),
        table=str(load_config.target_table),
    )
    shadow, backup = publication_table_names(
        live.table,
        run_key=run_key,
        artifact_scope=artifact_scope,
    )
    return live, live.with_table(shadow), live.with_table(backup)


def publication_campaign_contract(load_config: Any, *, policy: Any) -> dict[str, Any]:
    """Describe artifact naming without requiring a planned campaign key."""

    live, shadow, backup = publication_names(
        load_config,
        run_key="",
        artifact_scope="stable",
    )
    scope = policy.publication.artifact_scope
    base = {
        "kind": "dpone.mssql_backfill_shadow_publication.v1",
        "mode": policy.publication.mode,
        "retain_backup": policy.publication.retain_backup,
        "target": live.dataset,
    }
    if scope == "stable":
        return {**base, "shadow": shadow.dataset, "backup": backup.dataset}
    shadow_suffix, backup_suffix = publication_contract_suffixes(scope)
    return {
        **base,
        "artifact_scope": scope,
        "shadow_suffix": shadow_suffix,
        "backup_suffix": backup_suffix,
    }


def publication_rename_statement(target: MssqlPublicationTarget, new_table: str) -> str:
    """Render a database-scoped atomic publication rename."""

    return mssql_sp_rename_statement(target, new_table)


def quote_publication_identifier(value: str) -> str:
    """Quote one SQL Server identifier for publication-owned SQL."""

    return quote_mssql_identifier(value)


@dataclass(frozen=True, slots=True)
class MssqlBackfillIndex:
    """Supported physical index that must survive initial publication."""

    name: str
    type_desc: str
    unique: bool
    key_columns: tuple[str, ...]

    @property
    def is_columnstore(self) -> bool:
        return self.type_desc == "CLUSTERED COLUMNSTORE"

    def create_sql(self, target: MssqlPublicationTarget) -> str:
        name = quote_mssql_identifier(self.name)
        if self.is_columnstore:
            return f"CREATE CLUSTERED COLUMNSTORE INDEX {name} ON {target.quoted()}"
        columns = ", ".join(quote_mssql_identifier(column) for column in self.key_columns)
        unique = "UNIQUE " if self.unique else ""
        return f"CREATE {unique}NONCLUSTERED INDEX {name} ON {target.quoted()} ({columns})"


def read_supported_indexes(
    connector: Any,
    target: MssqlPublicationTarget,
) -> tuple[MssqlBackfillIndex, ...]:
    """Read and fail closed on physical designs the publisher cannot preserve."""

    prefix = f"{quote_mssql_identifier(target.database)}." if target.database else ""
    rows = connector.get_records(
        "SELECT i.index_id, i.name AS index_name, i.type_desc, i.is_unique, "
        "i.is_primary_key, i.is_unique_constraint, i.is_disabled, i.is_hypothetical, "
        "i.has_filter, i.filter_definition, ic.key_ordinal, ic.is_included_column, "
        "ic.is_descending_key, c.name AS column_name "
        f"FROM {prefix}sys.indexes AS i "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = i.object_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        f"LEFT JOIN {prefix}sys.index_columns AS ic "
        "ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        f"LEFT JOIN {prefix}sys.columns AS c "
        "ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "WHERE s.name = ? AND t.name = ? AND i.index_id > 0 "
        "ORDER BY i.index_id, ic.key_ordinal, ic.index_column_id",
        (target.schema, target.table),
        as_dict=True,
    )
    grouped: dict[int, list[Mapping[str, Any]]] = {}
    for raw in rows or ():
        row = dict(raw)
        grouped.setdefault(int(row.get("index_id") or 0), []).append(row)
    indexes = tuple(_supported_index(index_rows) for _, index_rows in sorted(grouped.items()))
    if sum(index.is_columnstore for index in indexes) > 1:
        raise RuntimeError("mssql_backfill_publication.multiple_clustered_columnstore_indexes")
    return indexes


def catalog_columns(connector: Any, target: MssqlPublicationTarget) -> tuple[tuple[Any, ...], ...]:
    """Return ordered structural column evidence, excluding object-local ids."""

    prefix = f"{quote_mssql_identifier(target.database)}." if target.database else ""
    rows = connector.get_records(
        "SELECT c.column_id, c.name, ty.name AS type_name, c.max_length, c.precision, c.scale, "
        "c.is_nullable, c.is_computed, c.is_identity, c.collation_name, c.default_object_id "
        f"FROM {prefix}sys.columns AS c "
        f"INNER JOIN {prefix}sys.types AS ty ON ty.user_type_id = c.user_type_id "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = c.object_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name = ? ORDER BY c.column_id",
        (target.schema, target.table),
        as_dict=True,
    )
    fields = (
        "column_id",
        "name",
        "type_name",
        "max_length",
        "precision",
        "scale",
        "is_nullable",
        "is_computed",
        "is_identity",
        "collation_name",
        "default_object_id",
    )
    return tuple(tuple(row.get(field) for field in fields) for row in rows or ())


def require_matching_columns(
    connector: Any,
    live: MssqlPublicationTarget,
    shadow: MssqlPublicationTarget,
) -> None:
    live_columns = catalog_columns(connector, live)
    shadow_columns = catalog_columns(connector, shadow)
    if not live_columns or live_columns != shadow_columns:
        raise RuntimeError("mssql_backfill_publication.shadow_catalog_mismatch")
    if any(bool(row[7]) or bool(row[8]) or int(row[10] or 0) for row in live_columns):
        raise RuntimeError("mssql_backfill_publication.unsupported_column_behavior")


def require_matching_indexes(
    connector: Any,
    live: MssqlPublicationTarget,
    shadow: MssqlPublicationTarget,
) -> tuple[MssqlBackfillIndex, ...]:
    expected = read_supported_indexes(connector, live)
    actual = read_supported_indexes(connector, shadow)
    if actual != expected:
        raise RuntimeError("mssql_backfill_publication.shadow_index_mismatch")
    return expected


def require_unique_key_index(
    connector: Any,
    target: MssqlPublicationTarget,
    unique_key: Any,
) -> MssqlBackfillIndex:
    """Require the live target's exact ascending unique-key authority."""

    keys = (unique_key,) if isinstance(unique_key, str) else tuple(unique_key or ())
    normalized = tuple(str(key).strip() for key in keys)
    if not normalized or any(not key for key in normalized):
        raise RuntimeError("mssql_backfill_publication.unique_key_required")
    matches = tuple(
        index for index in read_supported_indexes(connector, target) if index.unique and index.key_columns == normalized
    )
    if len(matches) != 1:
        raise RuntimeError("mssql_backfill_publication.live_unique_key_index_missing")
    return matches[0]


def require_shadow_owner(
    connector: Any,
    shadow: MssqlPublicationTarget,
    *,
    run_key: str,
) -> None:
    """Require the exact server-side campaign owner on one shadow."""

    if read_shadow_owner(connector, shadow) != run_key:
        raise RuntimeError("mssql_backfill_publication.shadow_owner_mismatch")


def read_shadow_owner(
    connector: Any,
    target: MssqlPublicationTarget,
) -> str | None:
    """Read the immutable campaign marker left on a shadow-swapped table."""

    prefix = f"{quote_mssql_identifier(target.database)}." if target.database else ""
    rows = connector.get_records(
        "SELECT CONVERT(nvarchar(128), ep.value) AS run_key "
        f"FROM {prefix}sys.extended_properties AS ep "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id=ep.major_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id=t.schema_id "
        "WHERE ep.minor_id=0 AND ep.name=? AND s.name=? AND t.name=?",
        (SHADOW_OWNER_PROPERTY, target.schema, target.table),
        as_dict=True,
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError("mssql_backfill_publication.shadow_owner_invalid")
    owner = str(rows[0].get("run_key") or "").strip()
    if not owner:
        raise RuntimeError("mssql_backfill_publication.shadow_owner_invalid")
    return owner


def _supported_index(rows: Sequence[Mapping[str, Any]]) -> MssqlBackfillIndex:
    first = rows[0]
    type_desc = str(first.get("type_desc") or "").strip().upper()
    name = str(first.get("index_name") or "").strip()
    if not name or bool(first.get("is_disabled")) or bool(first.get("is_hypothetical")):
        raise RuntimeError("mssql_backfill_publication.unsupported_index_state")
    if bool(first.get("has_filter")) or first.get("filter_definition") not in (None, ""):
        raise RuntimeError("mssql_backfill_publication.filtered_index_unsupported")
    if type_desc == "CLUSTERED COLUMNSTORE":
        if bool(first.get("is_unique")) or bool(first.get("is_primary_key")) or bool(first.get("is_unique_constraint")):
            raise RuntimeError("mssql_backfill_publication.columnstore_contract_invalid")
        return MssqlBackfillIndex(name=name, type_desc=type_desc, unique=False, key_columns=())
    if type_desc != "NONCLUSTERED" or not bool(first.get("is_unique")):
        raise RuntimeError("mssql_backfill_publication.index_type_unsupported")
    ordered = sorted(rows, key=lambda row: int(row.get("key_ordinal") or 0))
    if any(
        int(row.get("key_ordinal") or 0) < 1
        or bool(row.get("is_included_column"))
        or bool(row.get("is_descending_key"))
        for row in ordered
    ):
        raise RuntimeError("mssql_backfill_publication.index_shape_unsupported")
    keys = tuple(str(row.get("column_name") or "") for row in ordered)
    if any(not key for key in keys):
        raise RuntimeError("mssql_backfill_publication.index_column_missing")
    return MssqlBackfillIndex(name=name, type_desc=type_desc, unique=True, key_columns=keys)


__all__ = [
    "MssqlBackfillIndex",
    "MssqlPublicationTarget",
    "SHADOW_OWNER_PROPERTY",
    "catalog_columns",
    "publication_rename_statement",
    "publication_target",
    "quote_publication_identifier",
    "read_shadow_owner",
    "read_supported_indexes",
    "require_matching_columns",
    "require_matching_indexes",
    "require_shadow_owner",
    "require_unique_key_index",
]
