"""Complete plan/grant/owner comparison, with no protected execution authority.

All projections below are declarations. Original observations remain opaque and
bound by the existing owner/operation contracts; no current truth is fabricated.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING

from dpone.contracts.composition_qualification_operation import (
    CompositionQualificationOperation,
    CompositionQualificationOwner,
)
from dpone.contracts.nonproduction_authority import NonproductionLimits, require_validity
from dpone.contracts.nonproduction_fixture_plan import (
    BCP_PROFILE,
    PG_PROFILE,
    PG_SEQUENCES,
    NonproductionFixture,
    NonproductionFixturePlan,
)
from dpone.contracts.nonproduction_grants import NonproductionQualificationGrant
from dpone.contracts.nonproduction_qualification_plan import (
    NonproductionFixtureSeedItem,
    NonproductionQualificationPlan,
    NonproductionQualificationWorkItem,
    NonproductionRouteQualificationItem,
    NonproductionSourceSealItem,
)
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError, NonproductionParticipant

if TYPE_CHECKING:
    from dpone.contracts.nonproduction_plan_values import NonproductionPlanObject


def _require_actions(
    fixtures: dict[str, NonproductionFixture], items: tuple[NonproductionQualificationWorkItem, ...]
) -> None:
    for row in items:
        fixture = fixtures[row.fixture_id]
        bindings = dict(fixture.bindings)
        source, target = bindings["source"], bindings["target"]
        effects = {(edge.object_id, edge.access) for edge in row.effects}
        if isinstance(row, NonproductionFixtureSeedItem):
            required = {(source, "write")}
            if row.seed_step == "watermark_3" and (
                fixture.profile != PG_PROFILE or not dict(fixture.parameters)["watermark_key_3"]
            ):
                raise NonproductionAuthorityError("plan_item")
            if fixture.profile == PG_PROFILE:
                required.add((bindings["enum"], "read"))
                if row.seed_step == "initial":
                    required |= {(bindings[key], "write") for key in ("enum", *PG_SEQUENCES)}
        elif isinstance(row, NonproductionRouteQualificationItem):
            required = {(source, "read"), (target, "write")}
            pair = ("mssql", "clickhouse") if fixture.profile == BCP_PROFILE else ("postgres", "mssql")
            profile = "mssql_bcp_native_file" if fixture.profile == BCP_PROFILE else "postgres_copy_payload"
            if (
                (row.route.source, row.route.sink) != pair
                or row.source_bound.source_object_id != source
                or row.source_bound.accounting_profile != profile
                or (source, "write") in effects
            ):
                raise NonproductionAuthorityError("plan_item")
        else:
            required = {(source, "read")}
            if row.retained_source_ids != (source,):
                raise NonproductionAuthorityError("plan_item")
        if not required <= effects:
            raise NonproductionAuthorityError("plan_item")


def _ancestors(items: tuple[NonproductionQualificationWorkItem, ...]) -> dict[str, set[str]]:
    rows = {row.work_item_id: row for row in items}
    result: dict[str, set[str]] = {}
    active: set[str] = set()

    def visit(item_id: str) -> set[str]:
        if item_id in active or item_id not in rows:
            raise NonproductionAuthorityError("plan_graph")
        if item_id not in result:
            active.add(item_id)
            parents: set[str] = set()
            for predecessor in rows[item_id].predecessor_ids:
                parents.update(visit(predecessor))
                parents.add(predecessor)
            active.remove(item_id)
            result[item_id] = parents
        return result[item_id]

    for item_id in rows:
        visit(item_id)
    return result


def _require_order(
    fixtures: dict[str, NonproductionFixture],
    objects: dict[str, NonproductionPlanObject],
    items: tuple[NonproductionQualificationWorkItem, ...],
) -> None:
    ancestors = _ancestors(items)
    seals = {row.work_item_id for row in items if isinstance(row, NonproductionSourceSealItem)}
    if any(ancestors[row.work_item_id] & seals for row in items if not isinstance(row, NonproductionSourceSealItem)):
        raise NonproductionAuthorityError("plan_graph")
    for fixture in fixtures.values():
        selected = [row for row in items if row.fixture_id == fixture.fixture_id]
        initial = [
            row.work_item_id
            for row in selected
            if isinstance(row, NonproductionFixtureSeedItem) and row.seed_step == "initial"
        ]
        watermark = [
            row for row in selected if isinstance(row, NonproductionFixtureSeedItem) and row.seed_step == "watermark_3"
        ]
        if len(initial) != 1 or len(watermark) > 1:
            raise NonproductionAuthorityError("plan_graph")
        if any(initial[0] not in ancestors[row.work_item_id] for row in selected if row.work_item_id != initial[0]):
            raise NonproductionAuthorityError("plan_graph")
        source = objects[dict(fixture.bindings)["source"]].identity
        writers = {
            row.work_item_id
            for row in items
            if any(edge.access == "write" and objects[edge.object_id].identity == source for edge in row.effects)
        }
        for left in writers:
            if any(
                left != right and left not in ancestors[right] and right not in ancestors[left] for right in writers
            ):
                raise NonproductionAuthorityError("plan_graph")
        for row in items:
            if isinstance(row, NonproductionRouteQualificationItem) and any(
                edge.access == "read" and objects[edge.object_id].identity == source for edge in row.effects
            ):
                if any(
                    writer == row.work_item_id
                    or writer not in ancestors[row.work_item_id]
                    and row.work_item_id not in ancestors[writer]
                    for writer in writers
                ):
                    raise NonproductionAuthorityError("plan_graph")
            if isinstance(row, NonproductionSourceSealItem) and row.fixture_id == fixture.fixture_id:
                if not writers <= ancestors[row.work_item_id]:
                    raise NonproductionAuthorityError("plan_graph")


def _participants(
    objects: dict[str, NonproductionPlanObject], items: tuple[NonproductionQualificationWorkItem, ...]
) -> tuple[NonproductionParticipant, ...]:
    union: dict[tuple[str, str, str], dict[str, set[str]]] = {}
    for row in items:
        for edge in row.effects:
            obj = objects[edge.object_id]
            union.setdefault(obj.domain, {"read": set(), "write": set()})[edge.access].add(obj.subject_sha256)
    return tuple(
        sorted(
            NonproductionParticipant(*domain, tuple(sorted(effects["read"])), tuple(sorted(effects["write"])))
            for domain, effects in union.items()
        )
    )


def _roles(
    fixture_plan: NonproductionFixturePlan, participants: tuple[NonproductionParticipant, ...]
) -> dict[tuple[str, str, str], str]:
    objects = {row.object_id: row for row in fixture_plan.objects}
    sources = [objects[dict(row.bindings)["source"]] for row in fixture_plan.fixtures]
    retained = {row.domain for row in sources}
    roles: dict[tuple[str, str, str], str] = {}
    for part in participants:
        domain = part.connector, part.service_id, part.physical_subject_sha256
        if domain in retained:
            if any(row.subject_sha256 not in part.read_relations for row in sources if row.domain == domain):
                raise NonproductionAuthorityError("plan_scope")
            roles[domain] = "mutation_and_retained_source" if part.write_relations else "retained_source"
        elif part.write_relations:
            roles[domain] = "mutation"
        else:
            raise NonproductionAuthorityError("plan_scope")
    return roles


@dataclass(frozen=True, slots=True)
class NonproductionQualificationPlanOriginals:
    """Original documentary pair; every comparison repeats complete closure."""

    fixture_plan: NonproductionFixturePlan
    qualification_plan: NonproductionQualificationPlan
    original_grant: NonproductionQualificationGrant

    def __post_init__(self) -> None:
        if (
            type(self.fixture_plan) is not NonproductionFixturePlan
            or type(self.qualification_plan) is not NonproductionQualificationPlan
            or type(self.original_grant) is not NonproductionQualificationGrant
        ):
            raise NonproductionAuthorityError("plan_originals")
        fixture, plan, grant = self.fixture_plan, self.qualification_plan, self.original_grant
        grant.to_bytes()
        if (
            fixture.scope_sha256,
            plan.scope_sha256,
            fixture.fixture_plan_sha256,
            plan.fixture_plan_sha256,
            plan.qualification_plan_sha256,
        ) != (
            grant.scope.scope_sha256,
            grant.scope.scope_sha256,
            grant.fixture_plan_sha256,
            grant.fixture_plan_sha256,
            grant.qualification_plan_sha256,
        ):
            raise NonproductionAuthorityError("plan_originals")
        objects = {row.object_id: row for row in fixture.objects}
        fixtures = {row.fixture_id: row for row in fixture.fixtures}
        items = plan.work_items
        if {row.fixture_id for row in items} != fixtures.keys() or {
            edge.object_id for row in items for edge in row.effects
        } != objects.keys():
            raise NonproductionAuthorityError("plan_reference")
        _require_actions(fixtures, items)
        _require_order(fixtures, objects, items)
        participants = _participants(objects, items)
        if participants != grant.scope.participants or {
            row.route for row in items if isinstance(row, NonproductionRouteQualificationItem)
        } != set(grant.scope.routes):
            raise NonproductionAuthorityError("plan_scope")
        _roles(fixture, participants)
        for row in items:
            lower = NonproductionLimits(
                **{
                    field.name: min(getattr(grant.limits, field.name), getattr(row.limits, field.name))
                    for field in fields(NonproductionLimits)
                }
            )
            if len(items) > lower.max_workloads:
                raise NonproductionAuthorityError("plan_limits")
            require_validity(grant.not_before, grant.expires_at, lower.max_validity_seconds)

    def require_operation(
        self, operation: CompositionQualificationOperation, *, owner: CompositionQualificationOwner
    ) -> NonproductionQualificationWorkItem:
        """Bind a selected item to complete original owner claims, not truth."""
        self.__post_init__()
        if type(owner) is not CompositionQualificationOwner:
            raise NonproductionAuthorityError("plan_owner")
        if type(operation) is not CompositionQualificationOperation:
            raise NonproductionAuthorityError("plan_operation")
        owner.to_bytes()
        for claim in owner.claims:
            claim.__post_init__()
        operation.require_owner(owner)
        grant = self.original_grant
        if (
            owner.qualification_run_id,
            owner.consumption_subject_sha256,
            owner.grant_sha256,
            owner.fixture_plan_sha256,
            owner.qualification_plan_sha256,
            owner.scope_sha256,
            owner.environment_id,
            owner.campaign_id,
        ) != (
            grant.qualification_run_id,
            grant.consumption_subject_sha256,
            grant.grant_sha256,
            grant.fixture_plan_sha256,
            grant.qualification_plan_sha256,
            grant.scope.scope_sha256,
            grant.scope.environment_id,
            grant.scope.campaign_id,
        ):
            raise NonproductionAuthorityError("plan_owner")
        roles = _roles(self.fixture_plan, grant.scope.participants)
        expected = {
            (
                part.connector,
                part.service_id,
                part.physical_subject_sha256,
                roles[part.connector, part.service_id, part.physical_subject_sha256],
                part.read_relations,
                part.write_relations,
            )
            for part in grant.scope.participants
        }
        actual = {
            (
                claim.connector,
                claim.service_id,
                claim.physical_subject_sha256,
                claim.role,
                claim.read_subjects,
                claim.write_subjects,
            )
            for claim in owner.claims
        }
        if actual != expected:
            raise NonproductionAuthorityError("plan_owner")
        item = next(
            (row for row in self.qualification_plan.work_items if row.work_item_id == operation.work_item_id), None
        )
        if item is None or (item.work_item_sha256, item.action) != (operation.work_item_sha256, operation.action):
            raise NonproductionAuthorityError("plan_item")
        objects = {row.object_id: row for row in self.fixture_plan.objects}
        if {objects[edge.object_id].guard_id for edge in item.effects} != {
            guard for guard, _ in operation.guard_epochs
        }:
            raise NonproductionAuthorityError("plan_operation")
        return item


def require_qualification_plan_originals(
    fixture_bytes: bytes, qualification_bytes: bytes, *, original_grant: NonproductionQualificationGrant
) -> NonproductionQualificationPlanOriginals:
    """Reopen exact original bytes against the supplied original grant only."""
    if type(original_grant) is not NonproductionQualificationGrant:
        raise NonproductionAuthorityError("plan_originals")
    original_grant.to_bytes()
    return NonproductionQualificationPlanOriginals(
        NonproductionFixturePlan.from_bytes(fixture_bytes, expected_sha256=original_grant.fixture_plan_sha256),
        NonproductionQualificationPlan.from_bytes(
            qualification_bytes, expected_sha256=original_grant.qualification_plan_sha256
        ),
        original_grant,
    )
