"""Exact closed item variants and schema-tagged original hashes."""

from copy import deepcopy
from dataclasses import replace
from typing import Any

import pytest

from dpone.contracts.nonproduction_plan_values import NonproductionPlanEffect
from dpone.contracts.nonproduction_qualification_plan import NonproductionQualificationPlan, work_item_from_dict
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError, canonical_document
from tests.nonproduction_plan_helpers import documents, find_item, label_sha, originals, sha


def test_all_item_hashes_cover_complete_schema_tagged_originals() -> None:
    fixture, plan = documents(watermark=True)
    _, raw, _ = originals(fixture, plan)
    value = NonproductionQualificationPlan.from_bytes(raw, expected_sha256=sha(raw))
    assert value.to_dict() == plan
    assert value.to_bytes() == raw
    for parsed, body in zip(value.work_items, plan["work_items"], strict=True):
        assert parsed.work_item_sha256 == sha(
            canonical_document({"schema": "dpone.nonproduction-qualification-work-item.v1", **body})
        )


@pytest.mark.parametrize(
    "extra", ["route", "source_bound", "retained_source_ids", "observation_sha256", "work_item_sha256"]
)
def test_seed_rejects_variant_leakage(extra: str) -> None:
    _, plan = documents()
    plan["work_items"][0][extra] = "unsupported"
    raw = canonical_document(plan)
    with pytest.raises(NonproductionAuthorityError):
        NonproductionQualificationPlan.from_bytes(raw, expected_sha256=sha(raw))


def test_plan_and_three_action_golden_hashes() -> None:
    fixture, plan = documents()
    _, raw, _ = originals(fixture, plan)
    value = NonproductionQualificationPlan.from_bytes(raw, expected_sha256=sha(raw))
    assert value.qualification_plan_sha256 == "sha256:0afb4ba14061cdee56ecb55ca9345764dd90b5ef5f190e166d86f0fefabe1b79"
    assert [row.work_item_sha256 for row in value.work_items[:3]] == [
        "sha256:2be10902510010394a6cd665f3167eb1a18fb756530a46ff220dbc42dc4405ac",
        "sha256:d3dc42656926654c1ba10af6740714eb3d852c24ba8f88034340ad7bc77071cd",
        "sha256:dba63a81cd2e47d18ab1705a91dace4d117b247c51082a0fbb9246f131653af1",
    ]


@pytest.mark.parametrize(
    ("name", "key", "value"),
    [
        ("bcp_initial", "seed_step", "random"),
        ("bcp_initial", "action", "dbt"),
        ("bcp_route", "seed_step", "initial"),
        ("bcp_route", "retained_source_ids", ["bcp_source"]),
        ("bcp_seal", "source_bound", {}),
        ("bcp_seal", "route", {}),
        ("bcp_seal", "retained_source_ids", []),
        ("bcp_seal", "retained_source_ids", ["a", "b"]),
    ],
)
def test_action_specific_fields_are_closed(name: str, key: str, value: Any) -> None:
    _, plan = documents()
    row = find_item(plan, name)
    row[key] = value
    with pytest.raises(NonproductionAuthorityError):
        work_item_from_dict(row)


def test_seal_never_accepts_participant_write() -> None:
    _, plan = documents()
    row = find_item(plan, "bcp_seal")
    row["effects"][0]["access"] = "write"
    with pytest.raises(NonproductionAuthorityError, match="plan_item"):
        work_item_from_dict(row)


@pytest.mark.parametrize(
    "field",
    [
        "max_validity_seconds",
        "max_workloads",
        "max_attempts",
        "max_source_rows",
        "max_source_bytes",
        "max_attempt_seconds",
    ],
)
@pytest.mark.parametrize("invalid", [0, -1, True, 1.0, "1", 2**63])
def test_every_item_limit_remains_strict(field: str, invalid: Any) -> None:
    _, plan = documents()
    row = plan["work_items"][0]
    row["limits"][field] = invalid
    with pytest.raises(NonproductionAuthorityError, match="limits"):
        work_item_from_dict(row)


@pytest.mark.parametrize(
    "damage",
    [
        "effects_unsorted",
        "effects_duplicate",
        "effects_empty",
        "parents_unsorted",
        "parents_duplicate",
        "items_unsorted",
        "items_duplicate",
        "unknown_field",
        "missing_field",
    ],
)
def test_arrays_and_fields_reject_instead_of_normalizing(damage: str) -> None:
    _, plan = documents()
    row = find_item(plan, "bcp_route")
    if damage == "effects_unsorted":
        row["effects"].reverse()
    elif damage == "effects_duplicate":
        row["effects"].insert(0, row["effects"][0])
    elif damage == "effects_empty":
        row["effects"] = []
    elif damage == "parents_unsorted":
        row["predecessor_ids"] = ["z", "a"]
    elif damage == "parents_duplicate":
        row["predecessor_ids"] *= 2
    elif damage == "items_unsorted":
        plan["work_items"].reverse()
    elif damage == "items_duplicate":
        plan["work_items"].insert(0, plan["work_items"][0])
    elif damage == "unknown_field":
        plan["parent_sha256"] = label_sha("cycle")
    else:
        del plan["fixture_plan_sha256"]
    raw = canonical_document(plan)
    with pytest.raises(NonproductionAuthorityError):
        NonproductionQualificationPlan.from_bytes(raw, expected_sha256=sha(raw))


@pytest.mark.parametrize(
    "changed",
    [
        "limits",
        "predecessor_ids",
        "effects",
        "recipe",
        "case",
        "derivation",
        "projection",
        "input",
        "source",
        "fixture_id",
        "logical_id",
    ],
)
def test_complete_item_hash_covers_every_decision(changed: str) -> None:
    _, plan = documents()
    name = "bcp_seal" if changed == "recipe" else "bcp_route"
    body = deepcopy(find_item(plan, name))
    old = work_item_from_dict(body)
    if changed == "limits":
        body["limits"]["max_attempts"] -= 1
    elif changed == "predecessor_ids":
        body["predecessor_ids"] = ["pg_initial"]
    elif changed == "effects":
        body["effects"][0]["purpose"] = "helper"
    elif changed == "recipe":
        body["seal_recipe_original"]["sha256"] = label_sha("changed")
    elif changed == "case":
        body["reviewed_case_original"]["sha256"] = label_sha("changed")
    elif changed in {"derivation", "projection", "input"}:
        body["source_bound"][changed + "_original"]["sha256"] = label_sha("changed")
    elif changed == "source":
        body["source_bound"]["source_object_id"] = "different"
    else:
        body["fixture_id" if changed == "fixture_id" else "work_item_id"] = "logical/e\u0301\\x"
    value = work_item_from_dict(body)
    assert value.to_dict() == body
    assert value.work_item_sha256 != old.work_item_sha256


def test_total_item_and_effect_edge_caps() -> None:
    fixture, plan = documents()
    _, raw, _ = originals(fixture, plan)
    value = NonproductionQualificationPlan.from_bytes(raw, expected_sha256=sha(raw))
    seed = value.work_items[0]
    items = tuple(replace(seed, work_item_id=f"item{i:02}") for i in range(64))
    assert len(replace(value, work_items=items).work_items) == 64
    with pytest.raises(NonproductionAuthorityError):
        replace(value, work_items=(*items, replace(seed, work_item_id="item64")))
    edges = tuple(NonproductionPlanEffect(f"object{i:05}", "write", "fixture") for i in range(8192))
    whole = replace(seed, effects=edges)
    assert len(replace(value, work_items=(whole,)).work_items[0].effects) == 8192
    with pytest.raises(NonproductionAuthorityError, match="plan_limits"):
        replace(value, work_items=(whole, replace(seed, work_item_id="extra")))
    with pytest.raises(NonproductionAuthorityError):
        replace(seed, effects=(*edges, NonproductionPlanEffect("object8192", "write", "fixture")))


@pytest.mark.parametrize("damage", ["duplicate", "space", "utf8", "nonfinite", "schema", "digest", "oversize"])
def test_qualification_transport_remains_strict(damage: str) -> None:
    _, plan = documents()
    raw = canonical_document(plan)
    expected = sha(raw)
    if damage == "duplicate":
        raw = b'{"schema":"x",' + raw[1:]
    elif damage == "space":
        raw += b"\n"
    elif damage == "utf8":
        raw = b"\xff"
    elif damage == "nonfinite":
        raw = raw.replace(b'"max_attempts":128', b'"max_attempts":NaN')
    elif damage == "schema":
        raw = raw.replace(b"dpone.nonproduction-qualification-plan.v1", b"wrong.schema")
    elif damage == "digest":
        expected = label_sha("different")
    else:
        raw = b" " * (1024 * 1024 + 1)
    with pytest.raises(NonproductionAuthorityError):
        NonproductionQualificationPlan.from_bytes(raw, expected_sha256=expected)


def test_exact_one_mib_plan_original_and_next_byte() -> None:
    _, plan = documents()
    seed = deepcopy(plan["work_items"][0])
    seed["effects"] = [{"object_id": f"o{i:04}", "access": "write", "purpose": "fixture"} for i in range(8192)]
    plan["work_items"] = [seed]
    remaining = 1024 * 1024 - len(canonical_document(plan))
    quotient, remainder = divmod(remaining, len(seed["effects"]))
    for index, edge in enumerate(seed["effects"]):
        edge["object_id"] += "x" * (quotient + (index < remainder))
    raw = canonical_document(plan)
    assert len(raw) == 1024 * 1024
    value = NonproductionQualificationPlan.from_bytes(raw, expected_sha256=sha(raw))
    assert value.to_bytes() == raw
    oversized = raw.replace(b"o0000", b"o00000", 1)
    assert len(oversized) == 1024 * 1024 + 1
    with pytest.raises(NonproductionAuthorityError, match="document_budget"):
        NonproductionQualificationPlan.from_bytes(oversized, expected_sha256=sha(oversized))
