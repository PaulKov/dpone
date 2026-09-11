"""Full documentary graph, scope and exact original owner comparisons."""

from copy import deepcopy
from dataclasses import replace
from typing import Any

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.nonproduction_plan_pair import require_qualification_plan_originals
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError, canonical_document
from tests.nonproduction_authority_helpers import execution
from tests.nonproduction_plan_helpers import (
    SEQUENCES,
    declared,
    documents,
    effect,
    find_item,
    label_sha,
    order_documents,
    originals,
    owner_operation,
    sha,
)


@pytest.mark.parametrize("watermark", [False, True])
def test_full_pair_and_each_original_selected_item(watermark: bool) -> None:
    fixture, plan = documents(watermark=watermark)
    first, second, grant = originals(fixture, plan)
    pair = require_qualification_plan_originals(first, second, original_grant=grant)
    for row in plan["work_items"]:
        owner, operation = owner_operation(grant, row, fixture)
        assert pair.require_operation(operation, owner=owner).to_dict() == row


def test_route_missing_own_source_read_cannot_borrow_seal_read() -> None:
    fixture, plan = documents()
    row = find_item(plan, "bcp_route")
    row["effects"] = [edge for edge in row["effects"] if edge["access"] != "read"]
    first, second, grant = originals(fixture, plan)
    with pytest.raises(NonproductionAuthorityError, match="plan_item"):
        require_qualification_plan_originals(first, second, original_grant=grant)


def test_unordered_watermark_and_route_rejects_acyclic_plan() -> None:
    fixture, plan = documents(watermark=True)
    find_item(plan, "pg_watermark")["predecessor_ids"] = ["pg_initial"]
    first, second, grant = originals(fixture, plan)
    with pytest.raises(NonproductionAuthorityError, match="plan_graph"):
        require_qualification_plan_originals(first, second, original_grant=grant)


def test_original_observation_digest_is_not_synthesized() -> None:
    fixture, plan = documents()
    first, second, grant = originals(fixture, plan)
    pair = require_qualification_plan_originals(first, second, original_grant=grant)
    owner, operation = owner_operation(grant, find_item(plan, "bcp_seal"), fixture)
    claims = (replace(owner.claims[0], observation_sha256=grant.grant_sha256), *owner.claims[1:])
    with pytest.raises(CompositionAdmissionError):
        pair.require_operation(operation, owner=replace(owner, claims=claims))


@pytest.mark.parametrize("seals", [False, True])
def test_initial_watermark_then_unordered_final_reads_is_valid(seals: bool) -> None:
    fixture, plan = documents(watermark=True, seals=seals)
    find_item(plan, "pg_watermark")["predecessor_ids"] = ["pg_initial"]
    find_item(plan, "pg_route")["predecessor_ids"] = ["pg_watermark"]
    first, second, grant = originals(fixture, plan)
    pair = require_qualification_plan_originals(first, second, original_grant=grant)
    assert len(pair.qualification_plan.work_items) == (8 if seals else 6)


@pytest.mark.parametrize(
    "damage",
    [
        "missing_initial",
        "duplicate_initial",
        "duplicate_watermark",
        "disabled_watermark",
        "self",
        "unknown",
        "cycle",
        "route_no_initial",
        "watermark_no_initial",
        "seal_before_writer",
        "seal_before_other_fixture_seed",
        "cross_fixture_writer",
    ],
)
def test_generation_and_phase_order_rejects_incomplete_graph(damage: str) -> None:
    fixture, plan = documents(watermark=True)
    if damage == "missing_initial":
        plan["work_items"] = [row for row in plan["work_items"] if row["work_item_id"] != "bcp_initial"]
    elif damage in {"duplicate_initial", "duplicate_watermark"}:
        row = deepcopy(find_item(plan, "pg_initial" if damage == "duplicate_initial" else "pg_watermark"))
        row["work_item_id"] += "_duplicate"
        plan["work_items"].append(row)
    elif damage == "disabled_watermark":
        fixture["fixtures"][1]["parameters"]["watermark_key_3"] = False
    elif damage == "cross_fixture_writer":
        find_item(plan, "bcp_route")["effects"].append(effect("pg_source", "write", "helper"))
    else:
        name, parents = {
            "self": ("bcp_initial", ["bcp_initial"]),
            "unknown": ("bcp_route", ["unknown"]),
            "cycle": ("bcp_initial", ["bcp_route"]),
            "route_no_initial": ("bcp_route", []),
            "watermark_no_initial": ("pg_watermark", []),
            "seal_before_writer": ("pg_seal", ["pg_initial"]),
            "seal_before_other_fixture_seed": ("pg_initial", ["bcp_seal"]),
        }[damage]
        find_item(plan, name)["predecessor_ids"] = parents
    order_documents(fixture, plan)
    first, second, grant = originals(fixture, plan)
    with pytest.raises(NonproductionAuthorityError):
        require_qualification_plan_originals(first, second, original_grant=grant)


def test_cross_fixture_source_writer_is_allowed_only_in_complete_generation_order() -> None:
    fixture, plan = documents()
    row = find_item(plan, "bcp_route")
    row["effects"].append(effect("pg_source", "write", "helper"))
    row["predecessor_ids"].append("pg_initial")
    find_item(plan, "pg_route")["predecessor_ids"] = ["bcp_route"]
    order_documents(fixture, plan)
    first, second, grant = originals(fixture, plan)
    assert require_qualification_plan_originals(first, second, original_grant=grant).original_grant == grant


@pytest.mark.parametrize(
    ("name", "object_id", "access"),
    [
        ("bcp_route", "bcp_target", "write"),
        ("bcp_initial", "bcp_source", "write"),
        ("pg_initial", "pg_source", "write"),
        ("pg_initial", "enum", "read"),
        ("pg_initial", "enum", "write"),
        *[("pg_initial", key, "write") for key in SEQUENCES],
        ("pg_watermark", "enum", "read"),
    ],
)
def test_each_action_minimum_is_independent_of_overall_union(name: str, object_id: str, access: str) -> None:
    fixture, plan = documents(watermark=True)
    row = find_item(plan, name)
    row["effects"] = [edge for edge in row["effects"] if (edge["object_id"], edge["access"]) != (object_id, access)]
    # Keep other shape, union and ordering checks valid while isolating the missing minimum.
    if name in {"bcp_initial", "pg_initial"} and object_id == name.replace("_initial", "_source"):
        row["effects"].append(effect(object_id, "read", "source"))
        donor = "pg_initial" if name == "bcp_initial" else "bcp_initial"
        find_item(plan, donor)["effects"].append(effect(object_id, access, "helper"))
        find_item(plan, name.replace("_initial", "_route"))["predecessor_ids"].append(donor)
    else:
        find_item(plan, "pg_route_final")["effects"].append(effect(object_id, access, "helper"))
    order_documents(fixture, plan)
    first, second, grant = originals(fixture, plan)
    with pytest.raises(NonproductionAuthorityError, match="plan_item"):
        require_qualification_plan_originals(first, second, original_grant=grant)


@pytest.mark.parametrize(
    "damage",
    [
        "source_bound",
        "accounting",
        "route_dimension",
        "route_writes_source",
        "seal_source",
        "orphan_object",
        "missing_effect",
        "unknown_fixture",
    ],
)
def test_original_pair_rejects_mismatched_action_and_complete_closure(damage: str) -> None:
    fixture, plan = documents()
    if damage == "source_bound":
        find_item(plan, "bcp_route")["source_bound"]["source_object_id"] = "pg_source"
    elif damage == "accounting":
        find_item(plan, "bcp_route")["source_bound"]["accounting_profile"] = "postgres_copy_payload"
    elif damage == "route_dimension":
        find_item(plan, "bcp_route")["route"]["airflow_runtime_mode"] = "taskflow"
    elif damage == "route_writes_source":
        find_item(plan, "bcp_route")["effects"].append(effect("bcp_source", "write"))
    elif damage == "seal_source":
        find_item(plan, "bcp_seal")["retained_source_ids"] = ["pg_source"]
    elif damage == "orphan_object":
        obj = deepcopy(fixture["objects"][0])
        obj.update(
            object_id="orphan", subject_sha256=label_sha("orphan"), qualified_name=["database", "schema", "orphan"]
        )
        fixture["objects"].append(obj)
    elif damage == "missing_effect":
        for row in plan["work_items"]:
            row["effects"] = [edge for edge in row["effects"] if edge["object_id"] != "enum"]
    else:
        find_item(plan, "bcp_route")["fixture_id"] = "unknown"
    order_documents(fixture, plan)
    first, second, grant = originals(fixture, plan)
    with pytest.raises(NonproductionAuthorityError):
        require_qualification_plan_originals(first, second, original_grant=grant)


@pytest.mark.parametrize("field", ["max_workloads", "max_validity_seconds"])
@pytest.mark.parametrize("lower_at", ["grant", "item"])
def test_structural_lower_ceiling_cannot_be_expanded(field: str, lower_at: str) -> None:
    fixture, plan = documents()
    lower = 5 if field == "max_workloads" else 3599
    if lower_at == "item":
        find_item(plan, "bcp_route")["limits"][field] = lower
    first, second, grant = originals(fixture, plan)
    if lower_at == "grant" and field == "max_validity_seconds":
        with pytest.raises(NonproductionAuthorityError):
            replace(grant, limits=replace(grant.limits, **{field: lower}))
        return
    if lower_at == "grant":
        grant = replace(grant, limits=replace(grant.limits, **{field: lower}))
    with pytest.raises(NonproductionAuthorityError):
        require_qualification_plan_originals(first, second, original_grant=grant)


@pytest.mark.parametrize("field", ["fixture_plan_sha256", "qualification_plan_sha256", "scope_sha256"])
def test_pair_requires_original_grant_hashes(field: str) -> None:
    fixture, plan = documents()
    first, second, grant = originals(fixture, plan)
    if field == "scope_sha256":
        fixture[field] = label_sha("wrong")
        first = canonical_document(fixture)
        plan["fixture_plan_sha256"] = sha(first)
        second = canonical_document(plan)
        grant = replace(grant, fixture_plan_sha256=sha(first), qualification_plan_sha256=sha(second))
    else:
        grant = (
            replace(grant, fixture_plan_sha256=label_sha("wrong"))
            if field == "fixture_plan_sha256"
            else replace(grant, qualification_plan_sha256=label_sha("wrong"))
        )
    with pytest.raises(NonproductionAuthorityError):
        require_qualification_plan_originals(first, second, original_grant=grant)


def test_execution_grant_cannot_replace_original_qualification_grant() -> None:
    fixture, plan = documents()
    first, second, _ = originals(fixture, plan)
    wrong: Any = execution()
    with pytest.raises(NonproductionAuthorityError, match="plan_originals"):
        require_qualification_plan_originals(first, second, original_grant=wrong)


@pytest.mark.parametrize(
    "damage",
    [
        "missing_guard",
        "extra_guard",
        "action",
        "item",
        "hash",
        "full_owner_reads",
        "role",
        "owner_run",
        "owner_environment",
    ],
)
def test_selected_operation_requires_full_original_owner_and_exact_item(damage: str) -> None:
    fixture, plan = documents()
    first, second, grant = originals(fixture, plan)
    pair = require_qualification_plan_originals(first, second, original_grant=grant)
    owner, operation = owner_operation(grant, find_item(plan, "bcp_route"), fixture)
    if damage == "missing_guard":
        operation = replace(operation, guard_epochs=operation.guard_epochs[:1])
    elif damage == "extra_guard":
        operation = replace(operation, guard_epochs=tuple((claim.guard_id, 7) for claim in owner.claims))
    elif damage == "action":
        operation = replace(operation, action="source_seal")
    elif damage == "item":
        operation = replace(operation, work_item_id="missing")
    elif damage == "hash":
        operation = replace(operation, work_item_sha256=label_sha("wrong"))
    else:
        if damage in {"role", "full_owner_reads"}:
            index = next(i for i, claim in enumerate(owner.claims) if claim.connector == "postgres")
            claim = owner.claims[index]
            changed = (
                replace(claim, role="mutation")
                if damage == "role"
                else replace(claim, read_subjects=tuple(sorted((*claim.read_subjects, label_sha("extra")))))
            )
            owner = replace(owner, claims=(*owner.claims[:index], changed, *owner.claims[index + 1 :]))
        else:
            changed_id = "10000000-0000-4000-8000-000000000098"
            owner = (
                replace(owner, qualification_run_id=changed_id)
                if damage == "owner_run"
                else replace(owner, environment_id=changed_id)
            )
        operation = replace(
            operation,
            owner_subject_sha256=owner.subject_sha256,
            owner_key=owner.owner_key,
            qualification_run_id=owner.qualification_run_id,
        )
    with pytest.raises((NonproductionAuthorityError, CompositionAdmissionError)):
        pair.require_operation(operation, owner=owner)


@pytest.mark.parametrize("separate_domain", [False, True])
def test_arbitrary_read_never_fabricates_a_retained_source(separate_domain: bool) -> None:
    fixture, plan = documents()
    obj = declared("observer", "clickhouse")
    if separate_domain:
        obj["physical_subject_sha256"] = label_sha("observer domain")
    fixture["objects"].append(obj)
    find_item(plan, "bcp_route")["effects"].append(effect("observer", "read", "state"))
    order_documents(fixture, plan)
    first, second, grant = originals(fixture, plan)
    if separate_domain:
        with pytest.raises(NonproductionAuthorityError, match="plan_scope"):
            require_qualification_plan_originals(first, second, original_grant=grant)
    else:
        pair = require_qualification_plan_originals(first, second, original_grant=grant)
        owner, operation = owner_operation(grant, find_item(plan, "bcp_route"), fixture)
        assert next(claim for claim in owner.claims if claim.connector == "clickhouse").role == "mutation"
        assert pair.require_operation(operation, owner=owner).work_item_id == "bcp_route"


def test_pair_is_documentary_and_revalidates_before_readback(monkeypatch: pytest.MonkeyPatch) -> None:
    fixture, plan = documents()
    first, second, grant = originals(fixture, plan)

    def forbidden_io(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("codec attempted I/O")

    monkeypatch.setattr("builtins.open", forbidden_io)
    pair = require_qualification_plan_originals(first, second, original_grant=grant)
    owner, operation = owner_operation(grant, find_item(plan, "bcp_route"), fixture)
    assert pair.require_operation(operation, owner=owner).work_item_id == "bcp_route"
    assert operation.guard_epochs[0][1] == 7
    object.__setattr__(pair.qualification_plan.work_items[0].effects[0], "access", "delete")
    with pytest.raises(NonproductionAuthorityError):
        pair.require_operation(operation, owner=owner)


def test_opaque_observation_and_epochs_are_bound_without_claiming_truth() -> None:
    fixture, plan = documents()
    first, second, grant = originals(fixture, plan)
    pair = require_qualification_plan_originals(first, second, original_grant=grant)
    owner, operation = owner_operation(grant, find_item(plan, "bcp_seal"), fixture)
    owner = replace(
        owner,
        claims=tuple(
            replace(claim, observation_sha256=label_sha("another unverified original")) for claim in owner.claims
        ),
    )
    operation = replace(
        operation,
        owner_subject_sha256=owner.subject_sha256,
        guard_epochs=tuple((guard, 99) for guard, _ in operation.guard_epochs),
    )
    assert pair.require_operation(operation, owner=owner).work_item_id == "bcp_seal"
    assert all(claim.observation_sha256 == label_sha("another unverified original") for claim in owner.claims)


def test_full_signed_transport_has_no_invented_profile_whitelist() -> None:
    fixture, plan = documents()
    _, _, grant = originals(fixture, plan)
    dimensions = replace(grant.scope.routes[0], transport="explicit_signed_transport")
    subject = replace(grant.scope, routes=(dimensions, grant.scope.routes[1]))
    find_item(plan, "bcp_route")["route"]["transport"] = dimensions.transport
    fixture["scope_sha256"] = plan["scope_sha256"] = subject.scope_sha256
    first = canonical_document(fixture)
    plan["fixture_plan_sha256"] = sha(first)
    second = canonical_document(plan)
    grant = replace(grant, scope=subject, fixture_plan_sha256=sha(first), qualification_plan_sha256=sha(second))
    assert require_qualification_plan_originals(first, second, original_grant=grant).original_grant.scope == subject
