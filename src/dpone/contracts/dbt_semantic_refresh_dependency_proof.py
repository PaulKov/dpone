"""Bounded SQL Server read-dependency proof for semantic refresh V2."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.dbt_contract_validation import canonical_fingerprint, sha256_bytes
from dpone.contracts.dbt_semantic_refresh_catalog_types import (
    SqlServerCatalogSnapshot,
    SqlServerDependencyEdge,
    SqlServerDependencyLimits,
    SqlServerObjectMetadata,
)
from dpone.contracts.dbt_semantic_refresh_common import (
    ProofStatus,
    SemanticRefreshProofIssue,
    nonconformant,
    proof_status,
    unverified,
)

READ_DEPENDENCY_PROOF_SCHEMA = "dpone.semantic-refresh-read-dependency-proof.v1"
_MODULE_TYPES = frozenset({"VIEW", "SQL_INLINE_TABLE_VALUED_FUNCTION"})


@dataclass(frozen=True, slots=True)
class ReadDependencyProof:
    """Resolved base tables, module definitions and ordered catalog edges."""

    status: ProofStatus
    resolved_base_object_ids: tuple[int, ...]
    ordered_edges: tuple[tuple[int, int], ...]
    module_definition_digests: tuple[tuple[int, str], ...]
    catalog_sha256: str
    proof_sha256: str
    issues: tuple[SemanticRefreshProofIssue, ...]
    schema: str = READ_DEPENDENCY_PROOF_SCHEMA


@dataclass(slots=True)
class _Traversal:
    snapshot: SqlServerCatalogSnapshot
    objects: dict[int, SqlServerObjectMetadata]
    target_object_id: int
    limits: SqlServerDependencyLimits
    visited: set[int]
    active: set[int]
    bases: set[int]
    edges: set[tuple[int, int]]
    definitions: dict[int, str]
    issues: list[SemanticRefreshProofIssue]
    definition_bytes: int = 0

    def visit(self, object_id: int, depth: int) -> None:
        if object_id == self.target_object_id:
            self.issues.append(_nonconformant("TARGET_READ_UNSUPPORTED", "target_object_id"))
            return
        if object_id in self.active:
            self.issues.append(_nonconformant("DEPENDENCY_CYCLE", "dependency_edges"))
            return
        if object_id in self.visited:
            return
        if depth > self.limits.max_depth or len(self.visited) >= self.limits.max_nodes:
            self.issues.append(_nonconformant("DEPENDENCY_BUDGET_EXCEEDED", "limits"))
            return
        metadata = self.objects.get(object_id)
        if metadata is None:
            self.issues.append(_unverified("CATALOG_UNVERIFIED", "objects"))
            return
        if _reserved_relation_name(metadata.name):
            self.issues.append(
                _nonconformant(
                    "RESERVED_RELATION_NAMESPACE",
                    "objects",
                )
            )
            return
        self.visited.add(object_id)
        self.active.add(object_id)
        if metadata.database.casefold() != self.snapshot.database.casefold():
            self.issues.append(_nonconformant("CROSS_DATABASE_READ", "database"))
        elif metadata.object_type == "USER_TABLE":
            self._visit_table(metadata)
        elif metadata.object_type in _MODULE_TYPES:
            self._visit_module(metadata, depth)
        else:
            self.issues.append(_nonconformant("OBJECT_TYPE_UNSUPPORTED", "object_type"))
        self.active.remove(object_id)

    def _visit_table(self, metadata: SqlServerObjectMetadata) -> None:
        if metadata.has_computed_columns:
            self.issues.append(_nonconformant("COMPUTED_COLUMN_UNSUPPORTED", "has_computed_columns"))
            return
        if metadata.dependency_edges:
            self.issues.append(_unverified("CATALOG_UNVERIFIED", "dependency_edges"))
            return
        self.bases.add(metadata.object_id)

    def _visit_module(self, metadata: SqlServerObjectMetadata, depth: int) -> None:
        if metadata.encrypted or not metadata.schema_bound:
            self.issues.append(_nonconformant("MODULE_UNSUPPORTED", "schema_bound"))
            return
        if not isinstance(metadata.definition, str) or not metadata.definition.strip():
            self.issues.append(_unverified("CATALOG_UNVERIFIED", "definition"))
            return
        normalized = " ".join(metadata.definition.split())
        compact = normalized.casefold().replace(" ", "")
        if "openquery(" in compact or "openrowset(" in compact:
            self.issues.append(_nonconformant("MODULE_UNSUPPORTED", "definition"))
            return
        self.definition_bytes += len(normalized.encode("utf-8"))
        if self.definition_bytes > self.limits.max_definition_bytes:
            self.issues.append(_nonconformant("DEPENDENCY_BUDGET_EXCEEDED", "max_definition_bytes"))
            return
        self.definitions[metadata.object_id] = sha256_bytes(normalized.encode("utf-8"))
        for edge in sorted(metadata.dependency_edges, key=_edge_key):
            self._visit_edge(metadata.object_id, edge, depth)

    def _visit_edge(self, source: int, edge: SqlServerDependencyEdge, depth: int) -> None:
        if edge.referencing_object_id != source or edge.referenced_object_id is None:
            self.issues.append(_unverified("CATALOG_UNVERIFIED", "dependency_edges"))
            return
        if edge.caller_dependent:
            self.issues.append(_nonconformant("CALLER_DEPENDENCY_UNSUPPORTED", "caller_dependent"))
            return
        if edge.referenced_server:
            self.issues.append(_nonconformant("CROSS_SERVER_READ", "referenced_server"))
            return
        if not edge.referenced_database or edge.referenced_database.casefold() != self.snapshot.database.casefold():
            self.issues.append(_nonconformant("CROSS_DATABASE_READ", "referenced_database"))
            return
        target = edge.referenced_object_id
        self.edges.add((source, target))
        if len(self.edges) > self.limits.max_edges:
            self.issues.append(_nonconformant("DEPENDENCY_BUDGET_EXCEEDED", "max_edges"))
            return
        self.visit(target, depth + 1)


def prove_sqlserver_read_dependencies(
    snapshot: SqlServerCatalogSnapshot,
    *,
    root_object_ids: tuple[int, ...],
    target_object_id: int,
    limits: SqlServerDependencyLimits,
) -> ReadDependencyProof:
    """Resolve the bounded same-database module closure to admitted base tables."""

    roots = _object_ids(root_object_ids, "root_object_ids")
    if isinstance(target_object_id, bool) or not isinstance(target_object_id, int) or target_object_id <= 0:
        raise ValueError("target_object_id must be a positive integer")
    issues: list[SemanticRefreshProofIssue] = []
    if snapshot.metadata_visible is not True:
        issues.append(_unverified("CATALOG_UNVERIFIED", "metadata_visible"))
    if snapshot.ddl_exclusivity_proven is not True:
        issues.append(_unverified("CATALOG_UNVERIFIED", "ddl_exclusivity_proven"))
    objects = {metadata.object_id: metadata for metadata in snapshot.objects}
    if len(objects) != len(snapshot.objects):
        issues.append(_unverified("CATALOG_UNVERIFIED", "objects"))
    traversal = _Traversal(
        snapshot,
        objects,
        target_object_id,
        limits,
        set(),
        set(),
        set(),
        set(),
        {},
        issues,
    )

    if not issues:
        for root in roots:
            traversal.visit(root, 0)
    ordered_issues = tuple(sorted(set(issues), key=lambda issue: (issue.field, issue.code)))
    ordered_edges = tuple(sorted(traversal.edges))
    definitions = tuple(sorted(traversal.definitions.items()))
    catalog_payload = {
        "database": snapshot.database.casefold(),
        "visited_object_ids": sorted(traversal.visited),
        "resolved_base_object_ids": sorted(traversal.bases),
        "ordered_edges": [list(edge) for edge in ordered_edges],
        "module_definition_digests": [list(item) for item in definitions],
    }
    catalog_digest = canonical_fingerprint(catalog_payload)
    proof_payload = {
        "schema": READ_DEPENDENCY_PROOF_SCHEMA,
        "root_object_ids": list(roots),
        "target_object_id": target_object_id,
        "limits": {
            "max_depth": limits.max_depth,
            "max_nodes": limits.max_nodes,
            "max_edges": limits.max_edges,
            "max_definition_bytes": limits.max_definition_bytes,
        },
        "catalog_sha256": catalog_digest,
        "issues": [issue.to_jsonable() for issue in ordered_issues],
    }
    return ReadDependencyProof(
        proof_status(ordered_issues),
        tuple(sorted(traversal.bases)),
        ordered_edges,
        definitions,
        catalog_digest,
        canonical_fingerprint(proof_payload),
        ordered_issues,
    )


class SqlServerReadDependencyProver:
    """Injectable pure bounded traversal capability for catalog proof services."""

    def prove(
        self,
        snapshot: SqlServerCatalogSnapshot,
        *,
        root_object_ids: tuple[int, ...],
        target_object_id: int,
        limits: SqlServerDependencyLimits,
    ) -> ReadDependencyProof:
        return prove_sqlserver_read_dependencies(
            snapshot,
            root_object_ids=root_object_ids,
            target_object_id=target_object_id,
            limits=limits,
        )


def _object_ids(values: object, field: str) -> tuple[int, ...]:
    if (
        not isinstance(values, tuple)
        or not values
        or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in values)
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{field} must be a non-empty tuple of unique positive integers")
    return tuple(sorted(values))


def _edge_key(edge: SqlServerDependencyEdge) -> tuple[int, int]:
    return edge.referencing_object_id, edge.referenced_object_id or -1


def _reserved_relation_name(value: str) -> bool:
    normalized = value.casefold()
    return (
        normalized.endswith("__dbt_tmp")
        or normalized.startswith("dpone_sr_before_")
        or normalized.startswith("dpone_sr_after_")
    )


def _nonconformant(suffix: str, field: str) -> SemanticRefreshProofIssue:
    return nonconformant(
        f"DPONE_DBT_V2_{suffix}",
        field,
        "SQL Server read dependency is outside the admitted bounded policy",
    )


def _unverified(suffix: str, field: str) -> SemanticRefreshProofIssue:
    return unverified(
        f"DPONE_DBT_V2_{suffix}",
        field,
        "SQL Server catalog authority is incomplete; an empty dependency set is not accepted",
    )


__all__ = [
    "READ_DEPENDENCY_PROOF_SCHEMA",
    "ReadDependencyProof",
    "SqlServerCatalogSnapshot",
    "SqlServerDependencyEdge",
    "SqlServerDependencyLimits",
    "SqlServerObjectMetadata",
    "SqlServerReadDependencyProver",
    "prove_sqlserver_read_dependencies",
]
