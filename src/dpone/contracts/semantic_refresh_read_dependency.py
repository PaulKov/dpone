"""Bounded SQL Server read-dependency proof for semantic refresh."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    require_closed_mapping,
    require_digest,
    require_enum,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_types import ClosureStatus

READ_DEPENDENCY_PROOF_SCHEMA = "dpone.semantic-refresh-read-dependency-proof.v1"
_DIGEST_FIELD = "read_dependency_proof_sha256"
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "status",
        "model_unique_id",
        "database_name",
        "compiled_sql_sha256",
        "catalog_snapshot_sha256",
        "normalized_definitions_sha256",
        "policy_sha256",
        "dependency_edges",
        "base_relation_object_ids",
        "max_depth",
        "max_nodes",
        "max_edges",
        "max_definition_bytes",
    }
)
_EDGE_FIELDS = frozenset({"from_object_id", "to_object_id", "dependency_kind"})


class ReadDependencyKind(str, Enum):  # noqa: UP042
    """Only SQL Server object kinds admitted by the V2 dependency policy."""

    BASE_TABLE = "BASE_TABLE"
    SCHEMABOUND_VIEW = "SCHEMABOUND_VIEW"
    SCHEMABOUND_INLINE_TVF = "SCHEMABOUND_INLINE_TVF"


@dataclass(frozen=True, order=True, slots=True)
class ReadDependencyEdge:
    """One normalized SQL Server object-identity dependency edge."""

    from_object_id: int
    to_object_id: int
    dependency_kind: ReadDependencyKind

    def __post_init__(self) -> None:
        require_positive_int(self.from_object_id, "from_object_id")
        require_positive_int(self.to_object_id, "to_object_id")
        if not isinstance(self.dependency_kind, ReadDependencyKind):
            raise SemanticRefreshContractError("dependency_kind is unsupported")

    @classmethod
    def from_mapping(cls, value: object) -> ReadDependencyEdge:
        """Parse one closed normalized edge."""

        raw = require_closed_mapping(value, "dependency_edge", required=_EDGE_FIELDS)
        return cls(
            require_positive_int(raw.get("from_object_id"), "from_object_id"),
            require_positive_int(raw.get("to_object_id"), "to_object_id"),
            require_enum(raw.get("dependency_kind"), "dependency_kind", ReadDependencyKind),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed public edge mapping."""

        return {
            "dependency_kind": self.dependency_kind.value,
            "from_object_id": self.from_object_id,
            "to_object_id": self.to_object_id,
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshReadDependencyProof(SemanticRefreshDocumentCodec):
    """Catalog-bound recursive dependency closure with explicit budgets."""

    status: ClosureStatus
    model_unique_id: str
    database_name: str
    compiled_sql_sha256: str
    catalog_snapshot_sha256: str
    normalized_definitions_sha256: str
    policy_sha256: str
    dependency_edges: tuple[ReadDependencyEdge, ...]
    base_relation_object_ids: tuple[int, ...]
    max_depth: int
    max_nodes: int
    max_edges: int
    max_definition_bytes: int
    read_dependency_proof_sha256: str
    schema: str = READ_DEPENDENCY_PROOF_SCHEMA

    schema_id: ClassVar[str] = READ_DEPENDENCY_PROOF_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        if not isinstance(self.status, ClosureStatus):
            raise SemanticRefreshContractError("read dependency proof status is unsupported")
        require_text(self.model_unique_id, "model_unique_id")
        require_text(self.database_name, "database_name")
        for field_name in (
            "compiled_sql_sha256",
            "catalog_snapshot_sha256",
            "normalized_definitions_sha256",
            "policy_sha256",
        ):
            require_digest(getattr(self, field_name), field_name)
        for field_name in ("max_depth", "max_nodes", "max_edges", "max_definition_bytes"):
            require_positive_int(getattr(self, field_name), field_name)
        _validate_closure(self)
        validate_digest(self._unsigned(), self.read_dependency_proof_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        status: ClosureStatus,
        model_unique_id: str,
        database_name: str,
        compiled_sql_sha256: str,
        catalog_snapshot_sha256: str,
        normalized_definitions_sha256: str,
        policy_sha256: str,
        dependency_edges: tuple[ReadDependencyEdge, ...],
        base_relation_object_ids: tuple[int, ...],
        max_depth: int,
        max_nodes: int,
        max_edges: int,
        max_definition_bytes: int,
    ) -> SemanticRefreshReadDependencyProof:
        """Build a canonical bounded closure proof."""

        edges = _canonical_edges(dependency_edges)
        bases = _canonical_object_ids(base_relation_object_ids)
        values = (
            status,
            model_unique_id,
            database_name,
            compiled_sql_sha256,
            catalog_snapshot_sha256,
            normalized_definitions_sha256,
            policy_sha256,
            edges,
            bases,
            max_depth,
            max_nodes,
            max_edges,
            max_definition_bytes,
        )
        return cls(*values, semantic_refresh_sha256(_unsigned_mapping(*values)))

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshReadDependencyProof:
        """Parse a strict dependency proof and recompute its digest."""

        raw = require_closed_mapping(value, "read_dependency_proof", required=_FIELDS)
        raw_edges = raw.get("dependency_edges")
        if not isinstance(raw_edges, Sequence) or isinstance(raw_edges, str | bytes):
            raise SemanticRefreshContractError("dependency_edges must be an array")
        raw_bases = raw.get("base_relation_object_ids")
        if not isinstance(raw_bases, Sequence) or isinstance(raw_bases, str | bytes):
            raise SemanticRefreshContractError("base_relation_object_ids must be an array")
        return cls(
            status=require_enum(raw.get("status"), "status", ClosureStatus),
            model_unique_id=require_text(raw.get("model_unique_id"), "model_unique_id"),
            database_name=require_text(raw.get("database_name"), "database_name"),
            compiled_sql_sha256=require_digest(raw.get("compiled_sql_sha256"), "compiled_sql_sha256"),
            catalog_snapshot_sha256=require_digest(raw.get("catalog_snapshot_sha256"), "catalog_snapshot_sha256"),
            normalized_definitions_sha256=require_digest(
                raw.get("normalized_definitions_sha256"), "normalized_definitions_sha256"
            ),
            policy_sha256=require_digest(raw.get("policy_sha256"), "policy_sha256"),
            dependency_edges=tuple(ReadDependencyEdge.from_mapping(edge) for edge in raw_edges),
            base_relation_object_ids=tuple(require_positive_int(item, "base_relation_object_id") for item in raw_bases),
            max_depth=require_positive_int(raw.get("max_depth"), "max_depth"),
            max_nodes=require_positive_int(raw.get("max_nodes"), "max_nodes"),
            max_edges=require_positive_int(raw.get("max_edges"), "max_edges"),
            max_definition_bytes=require_positive_int(raw.get("max_definition_bytes"), "max_definition_bytes"),
            read_dependency_proof_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            self.status,
            self.model_unique_id,
            self.database_name,
            self.compiled_sql_sha256,
            self.catalog_snapshot_sha256,
            self.normalized_definitions_sha256,
            self.policy_sha256,
            self.dependency_edges,
            self.base_relation_object_ids,
            self.max_depth,
            self.max_nodes,
            self.max_edges,
            self.max_definition_bytes,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {**self._unsigned(), self.digest_field: self.read_dependency_proof_sha256}


def _canonical_edges(values: tuple[ReadDependencyEdge, ...]) -> tuple[ReadDependencyEdge, ...]:
    if not isinstance(values, tuple) or any(not isinstance(item, ReadDependencyEdge) for item in values):
        raise SemanticRefreshContractError("dependency_edges must be a tuple of ReadDependencyEdge")
    if len(values) != len(set(values)):
        raise SemanticRefreshContractError("dependency_edges must be unique")
    return tuple(sorted(values))


def _canonical_object_ids(values: tuple[int, ...]) -> tuple[int, ...]:
    if not isinstance(values, tuple):
        raise SemanticRefreshContractError("base_relation_object_ids must be a tuple")
    normalized = tuple(require_positive_int(item, "base_relation_object_id") for item in values)
    if normalized != tuple(sorted(set(normalized))):
        raise SemanticRefreshContractError("base_relation_object_ids must be sorted and unique")
    return normalized


def _validate_closure(proof: SemanticRefreshReadDependencyProof) -> None:
    if proof.dependency_edges != _canonical_edges(proof.dependency_edges):
        raise SemanticRefreshContractError("dependency_edges must be canonically ordered")
    _canonical_object_ids(proof.base_relation_object_ids)
    nodes = {
        object_id for edge in proof.dependency_edges for object_id in (edge.from_object_id, edge.to_object_id)
    } | set(proof.base_relation_object_ids)
    if len(proof.dependency_edges) > proof.max_edges or len(nodes) > proof.max_nodes:
        raise SemanticRefreshContractError("read dependency closure exceeds its bound policy")
    if proof.status is ClosureStatus.PROVEN and not proof.base_relation_object_ids:
        raise SemanticRefreshContractError("a PROVEN dependency closure requires an admitted base table")


def _unsigned_mapping(
    status: ClosureStatus,
    model_unique_id: str,
    database_name: str,
    compiled_sql_sha256: str,
    catalog_snapshot_sha256: str,
    normalized_definitions_sha256: str,
    policy_sha256: str,
    dependency_edges: tuple[ReadDependencyEdge, ...],
    base_relation_object_ids: tuple[int, ...],
    max_depth: int,
    max_nodes: int,
    max_edges: int,
    max_definition_bytes: int,
) -> dict[str, object]:
    return {
        "base_relation_object_ids": list(base_relation_object_ids),
        "catalog_snapshot_sha256": catalog_snapshot_sha256,
        "compiled_sql_sha256": compiled_sql_sha256,
        "database_name": database_name,
        "dependency_edges": [edge.to_dict() for edge in dependency_edges],
        "max_definition_bytes": max_definition_bytes,
        "max_depth": max_depth,
        "max_edges": max_edges,
        "max_nodes": max_nodes,
        "model_unique_id": model_unique_id,
        "normalized_definitions_sha256": normalized_definitions_sha256,
        "policy_sha256": policy_sha256,
        "schema": READ_DEPENDENCY_PROOF_SCHEMA,
        "status": status.value,
    }


__all__ = [
    "READ_DEPENDENCY_PROOF_SCHEMA",
    "ReadDependencyEdge",
    "ReadDependencyKind",
    "SemanticRefreshReadDependencyProof",
]
