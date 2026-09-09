"""SQL Server catalog authority and normalized observation construction."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from dpone.contracts.dbt_semantic_refresh_catalog_types import (
    SqlServerCatalogSnapshot,
    SqlServerDependencyEdge,
    SqlServerObjectMetadata,
)
from dpone.contracts.semantic_refresh_core import require_digest, require_text, semantic_refresh_sha256

RelationIdentity = tuple[str, str, str]


@dataclass(frozen=True, slots=True)
class SemanticRefreshCatalogAuthority:
    """Protected database/connection and DDL-exclusivity receipt subject."""

    database_name: str
    connection_ref: str
    ddl_exclusivity_receipt_sha256: str
    catalog_authority_receipt_sha256: str

    def __post_init__(self) -> None:
        require_text(self.database_name, "database_name")
        require_text(self.connection_ref, "connection_ref")
        require_digest(self.ddl_exclusivity_receipt_sha256, "ddl_exclusivity_receipt_sha256")
        require_digest(self.catalog_authority_receipt_sha256, "catalog_authority_receipt_sha256")

    @property
    def authority_sha256(self) -> str:
        return semantic_refresh_sha256(self.to_dict())

    def to_dict(self) -> dict[str, str]:
        return {
            "catalog_authority_receipt_sha256": self.catalog_authority_receipt_sha256,
            "connection_ref": self.connection_ref,
            "database_name": self.database_name,
            "ddl_exclusivity_receipt_sha256": self.ddl_exclusivity_receipt_sha256,
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshCatalogObservation:
    """Adapter-owned catalog rows and exact requested relation resolution."""

    snapshot: SqlServerCatalogSnapshot
    resolved_relations: tuple[tuple[RelationIdentity, int], ...]
    target_relation: RelationIdentity
    target_object_id: int


def build_sqlserver_catalog_observation(
    *,
    authority: SemanticRefreshCatalogAuthority,
    read_relations: tuple[RelationIdentity, ...],
    target_relation: RelationIdentity,
    metadata_row: Sequence[object] | None,
    catalog_rows: Sequence[Sequence[object]],
) -> SemanticRefreshCatalogObservation:
    """Validate raw sys-catalog rows and build the immutable proof observation."""

    if metadata_row is None or len(metadata_row) != 2:
        raise ValueError("SQL Server metadata-visibility row is missing")
    database = str(metadata_row[0] or "")
    if database.casefold() != authority.database_name.casefold():
        raise ValueError("SQL Server catalog database differs from authority")
    objects = _catalog_objects(database, catalog_rows)
    by_relation = {(item.database.casefold(), item.schema.casefold(), item.name.casefold()): item for item in objects}
    normalized_reads = tuple(sorted(_normalize_relation(item, database) for item in read_relations))
    normalized_target = _normalize_relation(target_relation, database)
    try:
        resolved = tuple((relation, by_relation[relation].object_id) for relation in normalized_reads)
        target_object_id = by_relation[normalized_target].object_id
    except KeyError as exc:
        raise ValueError("SQL Server catalog omitted a compiled relation identity") from exc
    return SemanticRefreshCatalogObservation(
        SqlServerCatalogSnapshot(database, metadata_row[1] in {1, True}, False, objects),
        resolved,
        normalized_target,
        target_object_id,
    )


def _catalog_objects(
    database: str,
    rows: Sequence[Sequence[object]],
) -> tuple[SqlServerObjectMetadata, ...]:
    grouped: dict[int, tuple[Sequence[object], list[SqlServerDependencyEdge]]] = {}
    for row in rows:
        if len(row) != 14:
            raise ValueError("SQL Server catalog row shape is invalid")
        object_id = _positive_int(row[0], "object_id")
        record, edges = grouped.setdefault(object_id, (row, []))
        if tuple(record[:4]) != tuple(row[:4]) or tuple(record[5:8]) != tuple(row[5:8]):
            raise ValueError("SQL Server catalog object rows conflict")
        if row[13] in {1, True}:
            raise ValueError("SQL Server catalog definition budget was exceeded")
        if row[8] is not None:
            edges.append(
                SqlServerDependencyEdge(
                    object_id,
                    (_positive_int(row[9], "referenced_id") if row[9] is not None else None),
                    str(row[10]) if row[10] is not None else None,
                    str(row[11]) if row[11] is not None else database,
                    row[12] in {1, True},
                )
            )
    result = [
        SqlServerObjectMetadata(
            object_id=object_id,
            database=database,
            schema=str(row[1]),
            name=str(row[2]),
            object_type=str(row[3]),
            definition=(str(row[4]) if row[4] is not None else None),
            dependency_edges=tuple(sorted(set(edges), key=_catalog_edge_key)),
            schema_bound=row[5] in {1, True},
            encrypted=row[6] in {1, True},
            has_computed_columns=row[7] in {1, True},
        )
        for object_id, (row, edges) in grouped.items()
    ]
    return tuple(sorted(result, key=lambda item: item.object_id))


def _normalize_relation(value: RelationIdentity, database: str) -> RelationIdentity:
    if not isinstance(value, tuple) or len(value) != 3:
        raise ValueError("relation identity must contain database/schema/name")
    catalog, schema, name = value
    normalized = ((catalog or database).casefold(), schema.casefold(), name.casefold())
    if any(not item for item in normalized) or normalized[0] != database.casefold():
        raise ValueError("relation identity is not exact and same-database")
    return normalized


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"SQL Server {field} is invalid")
    return value


def _catalog_edge_key(edge: SqlServerDependencyEdge) -> tuple[int, int, str, str]:
    return (
        edge.referencing_object_id,
        edge.referenced_object_id or -1,
        edge.referenced_server or "",
        edge.referenced_database or "",
    )


__all__ = [
    "RelationIdentity",
    "SemanticRefreshCatalogAuthority",
    "SemanticRefreshCatalogObservation",
    "build_sqlserver_catalog_observation",
]
