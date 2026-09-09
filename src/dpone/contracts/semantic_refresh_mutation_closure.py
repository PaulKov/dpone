"""Exact dbt mutation-closure contract for semantic refresh."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    canonical_string_set,
    require_closed_mapping,
    require_enum,
    require_sorted_unique_strings,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_types import ClosureStatus

MUTATION_CLOSURE_SCHEMA = "dpone.semantic-refresh-mutation-closure.v1"
_DIGEST_FIELD = "mutation_closure_sha256"
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "status",
        "selectors",
        "indirect_selection",
        "selected_node_ids",
        "selected_mutating_node_ids",
        "selected_read_only_node_ids",
        "unclassified_mutating_node_ids",
    }
)


@dataclass(frozen=True, slots=True)
class SemanticRefreshMutationClosure(SemanticRefreshDocumentCodec):
    """Closed exact selection and mutating/read-only classification proof."""

    status: ClosureStatus
    selectors: tuple[str, ...]
    selected_node_ids: tuple[str, ...]
    selected_mutating_node_ids: tuple[str, ...]
    selected_read_only_node_ids: tuple[str, ...]
    unclassified_mutating_node_ids: tuple[str, ...]
    mutation_closure_sha256: str
    indirect_selection: str = "empty"
    schema: str = MUTATION_CLOSURE_SCHEMA

    schema_id: ClassVar[str] = MUTATION_CLOSURE_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        if not isinstance(self.status, ClosureStatus):
            raise SemanticRefreshContractError("mutation closure status is unsupported")
        for values, field_name, allow_empty in (
            (self.selectors, "selectors", False),
            (self.selected_node_ids, "selected_node_ids", False),
            (self.selected_mutating_node_ids, "selected_mutating_node_ids", False),
            (self.selected_read_only_node_ids, "selected_read_only_node_ids", True),
            (self.unclassified_mutating_node_ids, "unclassified_mutating_node_ids", True),
        ):
            require_sorted_unique_strings(list(values), field_name, allow_empty=allow_empty)
        if self.indirect_selection != "empty":
            raise SemanticRefreshContractError("semantic refresh requires indirect_selection=empty")
        for selector in self.selectors:
            if "+" in selector:
                raise SemanticRefreshContractError("semantic refresh selectors must be exact and exclude +")
        if set(self.selected_node_ids) != set(self.selected_mutating_node_ids) | set(self.selected_read_only_node_ids):
            raise SemanticRefreshContractError("selected nodes must equal the classified closure")
        if set(self.selected_mutating_node_ids) & set(self.selected_read_only_node_ids):
            raise SemanticRefreshContractError("mutating and read-only closure nodes must be disjoint")
        if not set(self.unclassified_mutating_node_ids).issubset(self.selected_mutating_node_ids):
            raise SemanticRefreshContractError("unclassified mutation nodes must be selected mutating nodes")
        if self.status is ClosureStatus.PROVEN and self.unclassified_mutating_node_ids:
            raise SemanticRefreshContractError("a PROVEN mutation closure cannot contain unclassified mutations")
        validate_digest(self._unsigned(), self.mutation_closure_sha256, self.digest_field)

    @classmethod
    def build(
        cls,
        *,
        status: ClosureStatus,
        selectors: tuple[str, ...],
        selected_node_ids: tuple[str, ...],
        selected_mutating_node_ids: tuple[str, ...],
        selected_read_only_node_ids: tuple[str, ...],
        unclassified_mutating_node_ids: tuple[str, ...],
    ) -> SemanticRefreshMutationClosure:
        """Build a canonical exact-selection closure."""

        values = _canonical_values(
            selectors,
            selected_node_ids,
            selected_mutating_node_ids,
            selected_read_only_node_ids,
            unclassified_mutating_node_ids,
        )
        unsigned = _unsigned_mapping(status, *values)
        return cls(
            status=status,
            selectors=values[0],
            selected_node_ids=values[1],
            selected_mutating_node_ids=values[2],
            selected_read_only_node_ids=values[3],
            unclassified_mutating_node_ids=values[4],
            mutation_closure_sha256=semantic_refresh_sha256(unsigned),
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshMutationClosure:
        """Parse a strict closure and recompute its digest."""

        raw = require_closed_mapping(value, "mutation_closure", required=_FIELDS)
        return cls(
            status=require_enum(raw.get("status"), "status", ClosureStatus),
            selectors=require_sorted_unique_strings(raw.get("selectors"), "selectors"),
            selected_node_ids=require_sorted_unique_strings(raw.get("selected_node_ids"), "selected_node_ids"),
            selected_mutating_node_ids=require_sorted_unique_strings(
                raw.get("selected_mutating_node_ids"), "selected_mutating_node_ids"
            ),
            selected_read_only_node_ids=require_sorted_unique_strings(
                raw.get("selected_read_only_node_ids"), "selected_read_only_node_ids", allow_empty=True
            ),
            unclassified_mutating_node_ids=require_sorted_unique_strings(
                raw.get("unclassified_mutating_node_ids"),
                "unclassified_mutating_node_ids",
                allow_empty=True,
            ),
            mutation_closure_sha256=str(raw.get(_DIGEST_FIELD)),
            indirect_selection=str(raw.get("indirect_selection")),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def _unsigned(self) -> dict[str, object]:
        return _unsigned_mapping(
            self.status,
            self.selectors,
            self.selected_node_ids,
            self.selected_mutating_node_ids,
            self.selected_read_only_node_ids,
            self.unclassified_mutating_node_ids,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        return {**self._unsigned(), self.digest_field: self.mutation_closure_sha256}


def _canonical_values(
    selectors: tuple[str, ...],
    selected_node_ids: tuple[str, ...],
    selected_mutating_node_ids: tuple[str, ...],
    selected_read_only_node_ids: tuple[str, ...],
    unclassified_mutating_node_ids: tuple[str, ...],
) -> tuple[tuple[str, ...], ...]:
    return (
        canonical_string_set(selectors, "selectors"),
        canonical_string_set(selected_node_ids, "selected_node_ids"),
        canonical_string_set(selected_mutating_node_ids, "selected_mutating_node_ids"),
        canonical_string_set(selected_read_only_node_ids, "selected_read_only_node_ids", allow_empty=True),
        canonical_string_set(
            unclassified_mutating_node_ids,
            "unclassified_mutating_node_ids",
            allow_empty=True,
        ),
    )


def _unsigned_mapping(
    status: ClosureStatus,
    selectors: tuple[str, ...],
    selected_node_ids: tuple[str, ...],
    selected_mutating_node_ids: tuple[str, ...],
    selected_read_only_node_ids: tuple[str, ...],
    unclassified_mutating_node_ids: tuple[str, ...],
) -> dict[str, object]:
    return {
        "indirect_selection": "empty",
        "schema": MUTATION_CLOSURE_SCHEMA,
        "selected_mutating_node_ids": list(selected_mutating_node_ids),
        "selected_node_ids": list(selected_node_ids),
        "selected_read_only_node_ids": list(selected_read_only_node_ids),
        "selectors": list(selectors),
        "status": status.value,
        "unclassified_mutating_node_ids": list(unclassified_mutating_node_ids),
    }


__all__ = ["MUTATION_CLOSURE_SCHEMA", "SemanticRefreshMutationClosure"]
