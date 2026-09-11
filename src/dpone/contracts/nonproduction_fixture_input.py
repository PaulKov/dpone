"""Closed expected seed inputs; no acquisition, generation or bound authority.

Construction precedes route/grant hashes. Readback compares a complete existing
plan pair and its source-writer ancestry, without observing any physical input.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any

from dpone.contracts.nonproduction_fixture_plan import BCP_PROFILE, PG_PROFILE, NonproductionFixturePlan
from dpone.contracts.nonproduction_plan_pair import NonproductionQualificationPlanOriginals
from dpone.contracts.nonproduction_plan_values import NonproductionPlanOriginal
from dpone.contracts.nonproduction_qualification_plan import (
    NonproductionFixtureSeedItem,
    NonproductionQualificationWorkItem,
    NonproductionRouteQualificationItem,
)
from dpone.contracts.nonproduction_scope import (
    NonproductionAuthorityError,
    canonical_document,
    digest,
    document_sha256,
    exact_fields,
    parse_document,
    sequence,
    text,
)

_SCHEMA = "dpone.nonproduction-fixture-input.v1"


@dataclass(frozen=True, slots=True)
class NonproductionFixtureInput:
    """One expected finite recipe generation, without an observed outcome."""

    profile: str
    fixture_id: str
    source_object_id: str
    parameters: tuple[tuple[str, int | bool], ...]
    recipe_original: NonproductionPlanOriginal
    seed_program: str
    implementation_originals: tuple[NonproductionPlanOriginal, ...]
    seed_steps: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if text(self.profile) not in {BCP_PROFILE, PG_PROFILE}:
            raise NonproductionAuthorityError("fixture_input_profile")
        text(self.fixture_id)
        text(self.source_object_id)
        if (
            type(self.parameters) is not tuple
            or len(self.parameters) != 1
            or type(self.parameters[0]) is not tuple
            or len(self.parameters[0]) != 2
        ):
            raise NonproductionAuthorityError("fixture_input_profile")
        key, value = self.parameters[0]
        key = text(key)
        bcp = self.profile == BCP_PROFILE
        if (
            key != ("row_count" if bcp else "watermark_key_3")
            or (bcp and (type(value) is not int or not 0 <= value <= 100000))
            or (not bcp and type(value) is not bool)
            or text(self.seed_program) != _program(self.profile)
        ):
            raise NonproductionAuthorityError("fixture_input_profile")
        if type(self.recipe_original) is not NonproductionPlanOriginal:
            raise NonproductionAuthorityError("fixture_input_originals")
        self.recipe_original.__post_init__()
        recipe_path = (
            "tools/mssql_clickhouse_bcp_native_fixtures.py"
            if bcp
            else "tests/integration/postgres/postgres_mssql_wide_fixtures.py"
        )
        if self.recipe_original.path != recipe_path:
            raise NonproductionAuthorityError("fixture_input_originals")
        if (
            type(self.implementation_originals) is not tuple
            or len(self.implementation_originals) != (1 if bcp else 2)
            or any(type(row) is not NonproductionPlanOriginal for row in self.implementation_originals)
        ):
            raise NonproductionAuthorityError("fixture_input_originals")
        for row in self.implementation_originals:
            row.__post_init__()
        names = ("bcp_fixture_recipe",) if bcp else ("postgres_fixture_recipe", "postgres_fixture_rows")
        if tuple(row.path for row in self.implementation_originals) != tuple(
            f"src/dpone/adapters/nonproduction_{name}.py" for name in names
        ):
            raise NonproductionAuthorityError("fixture_input_originals")
        if (
            type(self.seed_steps) is not tuple
            or not 1 <= len(self.seed_steps) <= 2
            or any(type(pair) is not tuple or len(pair) != 2 for pair in self.seed_steps)
        ):
            raise NonproductionAuthorityError("fixture_input_steps")
        for item_id, step in self.seed_steps:
            text(item_id)
            text(step)
        expected_steps = ("initial", "watermark_3") if not bcp and value and len(self.seed_steps) == 2 else ("initial",)
        if tuple(step for _, step in self.seed_steps) != expected_steps or len(
            {item_id for item_id, _ in self.seed_steps}
        ) != len(self.seed_steps):
            raise NonproductionAuthorityError("fixture_input_steps")
        canonical_document(self._document())

    def _document(self) -> dict[str, Any]:
        return {
            "schema": _SCHEMA,
            "profile": self.profile,
            "fixture_id": self.fixture_id,
            "source_object_id": self.source_object_id,
            "parameters": dict(self.parameters),
            "recipe_original": self.recipe_original.to_dict(),
            "seed_program": self.seed_program,
            "implementation_originals": [row.to_dict() for row in self.implementation_originals],
            "seed_steps": [{"work_item_id": item_id, "seed_step": step} for item_id, step in self.seed_steps],
        }

    def to_dict(self) -> dict[str, Any]:
        """Return detached exact values, retaining generation rather than ID order."""
        self.__post_init__()
        return self._document()

    def to_bytes(self) -> bytes:
        """Return canonical bounded UTF-8; these bytes authenticate nothing."""
        return canonical_document(self.to_dict())

    @property
    def input_sha256(self) -> str:
        return document_sha256(self.to_dict())

    @classmethod
    def from_bytes(cls, raw: bytes, *, expected_sha256: str) -> NonproductionFixtureInput:
        """Reopen the exact input original with unchanged strict parser failures."""
        digest(expected_sha256)
        body = exact_fields(parse_document(raw, _SCHEMA), {"schema", *(field.name for field in fields(cls))})
        if type(body["parameters"]) is not dict:
            raise NonproductionAuthorityError("fixture_input_profile")
        for key in body["parameters"]:
            text(key)
        steps = [exact_fields(row, {"work_item_id", "seed_step"}) for row in sequence(body["seed_steps"], maximum=2)]
        value = cls(
            body["profile"],
            body["fixture_id"],
            body["source_object_id"],
            tuple(sorted(body["parameters"].items())),
            NonproductionPlanOriginal.from_dict(body["recipe_original"]),
            body["seed_program"],
            tuple(
                NonproductionPlanOriginal.from_dict(row)
                for row in sequence(body["implementation_originals"], maximum=2)
            ),
            tuple((row["work_item_id"], row["seed_step"]) for row in steps),
        )
        if value.input_sha256 != expected_sha256:
            raise NonproductionAuthorityError("fixture_input_originals")
        return value

    def require_route(
        self, originals: NonproductionQualificationPlanOriginals, *, route_work_item_id: str
    ) -> NonproductionRouteQualificationItem:
        """Return the exact documentary route after complete original comparison."""
        raw = self.to_bytes()
        if type(originals) is not NonproductionQualificationPlanOriginals:
            raise NonproductionAuthorityError("fixture_input_reference")
        originals.__post_init__()
        text(route_work_item_id)
        rows = {row.work_item_id: row for row in originals.qualification_plan.work_items}
        route = rows.get(route_work_item_id)
        if type(route) is not NonproductionRouteQualificationItem:
            raise NonproductionAuthorityError("fixture_input_reference")
        fixture = next(row for row in originals.fixture_plan.fixtures if row.fixture_id == route.fixture_id)
        if (self.fixture_id, self.source_object_id, self.profile, self.parameters) != (
            fixture.fixture_id,
            dict(fixture.bindings)["source"],
            fixture.profile,
            fixture.parameters,
        ):
            raise NonproductionAuthorityError("fixture_input_reference")
        descriptor = route.source_bound.input_original
        if self.recipe_original != fixture.recipe_original or (descriptor.sha256, descriptor.size_bytes) != (
            self.input_sha256,
            len(raw),
        ):
            raise NonproductionAuthorityError("fixture_input_originals")
        if self.seed_steps != _source_steps(rows, route, source_object_id=self.source_object_id):
            raise NonproductionAuthorityError("fixture_input_steps")
        return route


def _program(profile: str) -> str:
    return "mssql_bcp_decimal_prefix_v1" if profile == BCP_PROFILE else "postgres_wide_explicit_rows_v1"


def _source_steps(
    rows: dict[str, NonproductionQualificationWorkItem],
    route: NonproductionRouteQualificationItem,
    *,
    source_object_id: str,
) -> tuple[tuple[str, str], ...]:
    """Project ancestors of an already validated, at-most-64-item complete DAG."""
    ancestors: set[str] = set()
    pending = list(route.predecessor_ids)
    while pending:
        item_id = pending.pop()
        if item_id not in ancestors:
            ancestors.add(item_id)
            pending.extend(rows[item_id].predecessor_ids)
    seeds = []
    for item_id in ancestors:
        row = rows[item_id]
        if any(edge.object_id == source_object_id and edge.access == "write" for edge in row.effects):
            if type(row) is not NonproductionFixtureSeedItem or row.fixture_id != route.fixture_id:
                raise NonproductionAuthorityError("fixture_input_writers")
            seeds.append((row.work_item_id, row.seed_step))
    return tuple(sorted(seeds, key=lambda pair: pair[1] != "initial"))


def describe_fixture_input(
    fixture_plan: NonproductionFixturePlan,
    *,
    fixture_id: str,
    seed_items: tuple[NonproductionFixtureSeedItem, ...],
    implementation_originals: tuple[NonproductionPlanOriginal, ...],
) -> NonproductionFixtureInput:
    """Construct expected input before route/pair/grant hashes can exist.

    The supplied seed originals need not form a complete campaign. Their actual
    source-writer ancestry is compared only during complete pair readback.
    """
    if type(fixture_plan) is not NonproductionFixturePlan:
        raise NonproductionAuthorityError("fixture_input_reference")
    fixture_plan.to_bytes()
    text(fixture_id)
    fixture = next((row for row in fixture_plan.fixtures if row.fixture_id == fixture_id), None)
    if fixture is None:
        raise NonproductionAuthorityError("fixture_input_reference")
    if (
        type(seed_items) is not tuple
        or not 1 <= len(seed_items) <= 2
        or any(type(row) is not NonproductionFixtureSeedItem for row in seed_items)
    ):
        raise NonproductionAuthorityError("fixture_input_steps")
    for row in seed_items:
        row.to_dict()
        if row.fixture_id != fixture_id:
            raise NonproductionAuthorityError("fixture_input_reference")
    return NonproductionFixtureInput(
        fixture.profile,
        fixture.fixture_id,
        dict(fixture.bindings)["source"],
        fixture.parameters,
        fixture.recipe_original,
        _program(fixture.profile),
        implementation_originals,
        tuple((row.work_item_id, row.seed_step) for row in seed_items),
    )


@dataclass(frozen=True, slots=True)
class FixtureTransportBound:
    """Complete-source SQL observation, not an execution or ownership permit.

    Values must come from the matching trusted accounting query under the same
    immutable source generation as export. This type does not establish that
    provenance; the caller retains the query, source identity and snapshot proof.
    """

    source_rows: int
    transport_bytes_upper_bound: int

    def __post_init__(self) -> None:
        if any(
            type(value) is not int or not 0 <= value < 2**63
            for value in (self.source_rows, self.transport_bytes_upper_bound)
        ):
            raise NonproductionAuthorityError("fixture_bound_observation")
        if (self.source_rows == 0) != (self.transport_bytes_upper_bound == 0):
            raise NonproductionAuthorityError("fixture_bound_observation")


def require_fixture_transport_bound(records: object, *, max_rows: int, max_bytes: int) -> FixtureTransportBound:
    """Reject incomplete/malformed or N+1 observations before starting transport.

    Limits are budgets, never expected row-count evidence. No clipping, coercion,
    row sampling or substitutions from fixture parameters are performed.
    """
    if any(type(value) is not int or not 0 <= value < 2**63 for value in (max_rows, max_bytes)):
        raise NonproductionAuthorityError("fixture_bound_limit")
    if type(records) is not list or len(records) != 1:
        raise NonproductionAuthorityError("fixture_bound_observation")
    row = exact_fields(records[0], {"source_rows", "transport_bytes_upper_bound"})
    result = FixtureTransportBound(row["source_rows"], row["transport_bytes_upper_bound"])
    if result.source_rows > max_rows or result.transport_bytes_upper_bound > max_bytes:
        raise NonproductionAuthorityError("fixture_bound_exceeded")
    return result
