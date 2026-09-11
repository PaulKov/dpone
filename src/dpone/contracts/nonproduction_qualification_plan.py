"""Closed qualification action originals; serialization grants no execution.

Predecessor IDs describe an intended graph. The pair contract checks its entire
scope and generation order; protected observations and lifecycle remain later.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Any

from dpone.contracts.nonproduction_authority import NonproductionLimits
from dpone.contracts.nonproduction_plan_values import (
    NonproductionPlanEffect,
    NonproductionPlanOriginal,
    NonproductionSourceBound,
)
from dpone.contracts.nonproduction_scope import (
    NonproductionAuthorityError,
    NonproductionRouteDimensions,
    canonical_document,
    digest,
    document_sha256,
    exact_fields,
    ordered,
    parse_document,
    sequence,
    text,
)
from dpone.contracts.strict_json import strict_json_object

_SCHEMA = "dpone.nonproduction-qualification-plan.v1"
_ITEM_SCHEMA = "dpone.nonproduction-qualification-work-item.v1"


@dataclass(frozen=True, slots=True)
class _WorkItem:
    work_item_id: str
    action: str
    fixture_id: str
    predecessor_ids: tuple[str, ...]
    effects: tuple[NonproductionPlanEffect, ...]
    limits: NonproductionLimits

    def __post_init__(self) -> None:
        text(self.work_item_id)
        text(self.fixture_id)
        if type(self.predecessor_ids) is not tuple:
            raise NonproductionAuthorityError("plan_graph")
        for item_id in self.predecessor_ids:
            text(item_id)
        ordered(self.predecessor_ids, maximum=64, allow_empty=True)
        if (
            type(self.effects) is not tuple
            or not 1 <= len(self.effects) <= 8192
            or any(type(edge) is not NonproductionPlanEffect for edge in self.effects)
        ):
            raise NonproductionAuthorityError("plan_item")
        for edge in self.effects:
            edge.__post_init__()
        ordered(self.effects, maximum=8192)
        if type(self.limits) is not NonproductionLimits:
            raise NonproductionAuthorityError("limits")
        self.limits.__post_init__()

    def to_dict(self) -> dict[str, Any]:
        """Detached complete original, with no parent or stored self hash."""
        self.__post_init__()
        return strict_json_object(canonical_document(asdict(self)))

    @property
    def work_item_sha256(self) -> str:
        return document_sha256({"schema": _ITEM_SCHEMA, **self.to_dict()})


@dataclass(frozen=True, slots=True)
class NonproductionFixtureSeedItem(_WorkItem):
    """A fixed fixture step, never an arbitrary SQL or key-generation callback."""

    seed_step: str

    def __post_init__(self) -> None:
        _WorkItem.__post_init__(self)
        if (
            type(self.action) is not str
            or self.action != "fixture_seed"
            or text(self.seed_step) not in {"initial", "watermark_3"}
        ):
            raise NonproductionAuthorityError("plan_item")


@dataclass(frozen=True, slots=True)
class NonproductionRouteQualificationItem(_WorkItem):
    """One exact scope route and documentary source calculation obligations."""

    route: NonproductionRouteDimensions
    reviewed_case_original: NonproductionPlanOriginal
    source_bound: NonproductionSourceBound

    def __post_init__(self) -> None:
        _WorkItem.__post_init__(self)
        if type(self.action) is not str or self.action != "route_qualification":
            raise NonproductionAuthorityError("plan_item")
        if (
            type(self.route) is not NonproductionRouteDimensions
            or type(self.reviewed_case_original) is not NonproductionPlanOriginal
            or type(self.source_bound) is not NonproductionSourceBound
        ):
            raise NonproductionAuthorityError("plan_item")
        self.route.__post_init__()
        self.reviewed_case_original.__post_init__()
        self.source_bound.__post_init__()


@dataclass(frozen=True, slots=True)
class NonproductionSourceSealItem(_WorkItem):
    """Participant reads only; no export accounting capability is inferred."""

    retained_source_ids: tuple[str, ...]
    seal_recipe_original: NonproductionPlanOriginal

    def __post_init__(self) -> None:
        _WorkItem.__post_init__(self)
        if type(self.action) is not str or self.action != "source_seal":
            raise NonproductionAuthorityError("plan_item")
        if type(self.retained_source_ids) is not tuple:
            raise NonproductionAuthorityError("plan_item")
        for object_id in self.retained_source_ids:
            text(object_id)
        ordered(self.retained_source_ids, maximum=1)
        if (
            any(edge.access != "read" for edge in self.effects)
            or type(self.seal_recipe_original) is not NonproductionPlanOriginal
        ):
            raise NonproductionAuthorityError("plan_item")
        self.seal_recipe_original.__post_init__()


NonproductionQualificationWorkItem = (
    NonproductionFixtureSeedItem | NonproductionRouteQualificationItem | NonproductionSourceSealItem
)


def work_item_from_dict(value: object) -> NonproductionQualificationWorkItem:
    """Decode the closed union, rejecting every field from another action."""
    if type(value) is not dict or type(value.get("action")) is not str:
        raise NonproductionAuthorityError("plan_item")
    if value["action"] == "fixture_seed":
        kind: type[NonproductionQualificationWorkItem] = NonproductionFixtureSeedItem
    elif value["action"] == "route_qualification":
        kind = NonproductionRouteQualificationItem
    elif value["action"] == "source_seal":
        kind = NonproductionSourceSealItem
    else:
        raise NonproductionAuthorityError("plan_item")
    body = dict(exact_fields(value, {field.name for field in fields(kind)}))
    body["predecessor_ids"] = tuple(sequence(body["predecessor_ids"], maximum=64, allow_empty=True))
    body["effects"] = tuple(NonproductionPlanEffect.from_dict(edge) for edge in sequence(body["effects"], maximum=8192))
    body["limits"] = NonproductionLimits.from_dict(body["limits"])
    if kind is NonproductionRouteQualificationItem:
        body["route"] = NonproductionRouteDimensions.from_dict(body["route"])
        body["reviewed_case_original"] = NonproductionPlanOriginal.from_dict(body["reviewed_case_original"])
        body["source_bound"] = NonproductionSourceBound.from_dict(body["source_bound"])
    elif kind is NonproductionSourceSealItem:
        body["retained_source_ids"] = tuple(sequence(body["retained_source_ids"], maximum=1))
        body["seal_recipe_original"] = NonproductionPlanOriginal.from_dict(body["seal_recipe_original"])
    return kind(**body)


@dataclass(frozen=True, slots=True)
class NonproductionQualificationPlan:
    """All declared seed, route and seal items, sharing a finite total budget."""

    scope_sha256: str
    fixture_plan_sha256: str
    work_items: tuple[NonproductionQualificationWorkItem, ...]

    def __post_init__(self) -> None:
        digest(self.scope_sha256)
        digest(self.fixture_plan_sha256)
        if (
            type(self.work_items) is not tuple
            or not 1 <= len(self.work_items) <= 64
            or any(
                type(row)
                not in {NonproductionFixtureSeedItem, NonproductionRouteQualificationItem, NonproductionSourceSealItem}
                for row in self.work_items
            )
        ):
            raise NonproductionAuthorityError("plan_item")
        for row in self.work_items:
            row.__post_init__()
        ordered(tuple(row.work_item_id for row in self.work_items), maximum=64)
        if sum(len(row.effects) for row in self.work_items) > 8192:
            raise NonproductionAuthorityError("plan_limits")
        canonical_document(self._document())

    def _document(self) -> dict[str, Any]:
        return {
            "schema": _SCHEMA,
            "scope_sha256": self.scope_sha256,
            "fixture_plan_sha256": self.fixture_plan_sha256,
            "work_items": [row.to_dict() for row in self.work_items],
        }

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return self._document()

    def to_bytes(self) -> bytes:
        return canonical_document(self.to_dict())

    @property
    def qualification_plan_sha256(self) -> str:
        return document_sha256(self.to_dict())

    @classmethod
    def from_bytes(cls, raw: bytes, *, expected_sha256: str) -> NonproductionQualificationPlan:
        digest(expected_sha256)
        body = exact_fields(
            parse_document(raw, _SCHEMA), {"schema", "scope_sha256", "fixture_plan_sha256", "work_items"}
        )
        result = cls(
            body["scope_sha256"],
            body["fixture_plan_sha256"],
            tuple(work_item_from_dict(row) for row in sequence(body["work_items"], maximum=64)),
        )
        if result.qualification_plan_sha256 != expected_sha256:
            raise NonproductionAuthorityError("plan_originals")
        return result
