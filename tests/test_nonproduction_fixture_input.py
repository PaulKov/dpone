"""Expected input originals never observe a generation or permit execution."""

import json
from dataclasses import FrozenInstanceError, replace
from typing import Any, cast

import pytest

from dpone.contracts.nonproduction_fixture_input import NonproductionFixtureInput, describe_fixture_input
from dpone.contracts.nonproduction_fixture_plan import NonproductionFixturePlan
from dpone.contracts.nonproduction_plan_pair import (
    NonproductionQualificationPlanOriginals,
    require_qualification_plan_originals,
)
from dpone.contracts.nonproduction_plan_values import NonproductionPlanOriginal
from dpone.contracts.nonproduction_qualification_plan import NonproductionFixtureSeedItem, work_item_from_dict
from dpone.contracts.nonproduction_scope import MAX_DOCUMENT_BYTES, NonproductionAuthorityError, canonical_document
from tests.nonproduction_plan_helpers import descriptor, documents, effect, find_item, order_documents, originals, sha


def implementation(fixture_id: str) -> tuple[NonproductionPlanOriginal, ...]:
    names = ("bcp_fixture_recipe",) if fixture_id == "bcp" else ("postgres_fixture_recipe", "postgres_fixture_rows")
    return tuple(
        NonproductionPlanOriginal.from_dict(descriptor(f"src/dpone/adapters/nonproduction_{name}.py")) for name in names
    )


def described(fixture: dict[str, Any], plan: dict[str, Any], name: str) -> NonproductionFixtureInput:
    raw = canonical_document(fixture)
    parsed = NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw))
    fixture_id = "bcp" if name.startswith("bcp") else "pg"
    ids = [fixture_id + "_initial"] + (["pg_watermark"] if name.endswith("final") else [])
    return describe_fixture_input(
        parsed,
        fixture_id=fixture_id,
        seed_items=tuple(
            cast(NonproductionFixtureSeedItem, work_item_from_dict(find_item(plan, item_id))) for item_id in ids
        ),
        implementation_originals=implementation(fixture_id),
    )


def bound_pair(
    fixture: dict[str, Any], plan: dict[str, Any], name: str, value: NonproductionFixtureInput
) -> NonproductionQualificationPlanOriginals:
    find_item(plan, name)["source_bound"]["input_original"].update(
        sha256=value.input_sha256, size_bytes=len(value.to_bytes())
    )
    first, second, grant = originals(fixture, plan)
    return require_qualification_plan_originals(first, second, original_grant=grant)


@pytest.mark.parametrize("name", ["bcp_route", "pg_route", "pg_route_final"])
def test_documentary_construction_precedes_grant_and_readback_returns_original_item(name: str) -> None:
    fixture, plan = documents(watermark=True)
    value = described(fixture, plan, name)
    raw = value.to_bytes()
    assert NonproductionFixtureInput.from_bytes(raw, expected_sha256=sha(raw)) == value
    assert value.input_sha256 == sha(raw)
    pair = bound_pair(fixture, plan, name, value)
    selected = next(row for row in pair.qualification_plan.work_items if row.work_item_id == name)
    assert value.require_route(pair, route_work_item_id=name) is selected
    assert value.require_route(pair, route_work_item_id=name) is selected
    assert "grant" not in value.to_dict() and "qualification_plan_sha256" not in value.to_dict()


def test_generation_order_is_not_lexical_and_serialization_is_detached() -> None:
    fixture, plan = documents(watermark=True)
    value = described(fixture, plan, "pg_route_final")
    value = replace(value, seed_steps=(("z_initial", "initial"), ("a_watermark", "watermark_3")))
    raw = value.to_bytes()
    assert NonproductionFixtureInput.from_bytes(raw, expected_sha256=sha(raw)) == value
    body = value.to_dict()
    body["seed_steps"][0]["work_item_id"] = "changed"
    body["parameters"]["watermark_key_3"] = False
    assert value.to_bytes() == raw
    with pytest.raises(FrozenInstanceError):
        value.seed_program = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    "field",
    [
        "profile",
        "fixture_id",
        "source_object_id",
        "parameters",
        "recipe_original",
        "seed_program",
        "implementation_originals",
        "seed_steps",
    ],
)
def test_missing_or_unknown_document_fields_reject(field: str) -> None:
    fixture, plan = documents()
    body = described(fixture, plan, "bcp_route").to_dict()
    body.pop(field)
    with pytest.raises(NonproductionAuthorityError, match="fields$"):
        NonproductionFixtureInput.from_bytes(canonical_document(body), expected_sha256=sha(b"expected"))


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("profile", "unknown", "fixture_input_profile"),
        ("seed_program", "unknown", "fixture_input_profile"),
        ("parameters", (("row_count", True),), "fixture_input_profile"),
        ("parameters", (("row_count", -1),), "fixture_input_profile"),
        ("parameters", (("row_count", 100001),), "fixture_input_profile"),
        ("parameters", {"row_count": 1}, "fixture_input_profile"),
        ("seed_steps", [], "fixture_input_steps"),
        ("seed_steps", (("x", "watermark_3"),), "fixture_input_steps"),
        ("seed_steps", (("x", "initial"), ("x", "initial")), "fixture_input_steps"),
        ("implementation_originals", (), "fixture_input_originals"),
    ],
)
def test_new_failure_reasons_are_value_free(field: str, value: Any, reason: str) -> None:
    fixture, plan = documents()
    with pytest.raises(NonproductionAuthorityError) as caught:
        replace(described(fixture, plan, "bcp_route"), **{field: value})
    assert str(caught.value) == "DPONE_NONPRODUCTION_AUTHORITY_INVALID: " + reason


@pytest.mark.parametrize("damage", ["foreign_writer", "different_fixture_seed"])
def test_valid_complete_pair_unsupported_source_writer_is_isolated(damage: str) -> None:
    fixture, plan = documents()
    donor = find_item(plan, "bcp_route" if damage == "foreign_writer" else "bcp_initial")
    donor["effects"].append(effect("pg_source", "write", "helper"))
    donor["predecessor_ids"].append("pg_initial")
    find_item(plan, "pg_route")["predecessor_ids"] = [donor["work_item_id"]]
    order_documents(fixture, plan)
    value = described(fixture, plan, "pg_route")
    pair = bound_pair(fixture, plan, "pg_route", value)
    with pytest.raises(NonproductionAuthorityError, match="fixture_input_writers$"):
        value.require_route(pair, route_work_item_id="pg_route")


@pytest.mark.parametrize(
    "field,replacement",
    [("fixture_id", "missing"), ("source_object_id", "missing"), ("parameters", (("row_count", 11),))],
)
def test_selected_route_reference_is_exact(field: str, replacement: Any) -> None:
    fixture, plan = documents()
    value = replace(described(fixture, plan, "bcp_route"), **{field: replacement})
    pair = bound_pair(fixture, plan, "bcp_route", value)
    with pytest.raises(NonproductionAuthorityError, match="fixture_input_reference$"):
        value.require_route(pair, route_work_item_id="bcp_route")


@pytest.mark.parametrize("damage", ["digest", "size", "steps"])
def test_route_original_and_ancestor_steps_cannot_be_rebound(damage: str) -> None:
    fixture, plan = documents(watermark=True)
    value = described(fixture, plan, "pg_route_final")
    if damage == "steps":
        value = replace(value, seed_steps=(("pg_initial", "initial"),))
    pair = bound_pair(fixture, plan, "pg_route_final", value)
    if damage != "steps":
        doc = find_item(plan, "pg_route_final")["source_bound"]["input_original"]
        doc["sha256" if damage == "digest" else "size_bytes"] = sha(b"other") if damage == "digest" else 1
        first, second, grant = originals(fixture, plan)
        pair = require_qualification_plan_originals(first, second, original_grant=grant)
    with pytest.raises(
        NonproductionAuthorityError, match=("fixture_input_steps$" if damage == "steps" else "fixture_input_originals$")
    ):
        value.require_route(pair, route_work_item_id="pg_route_final")


def test_complete_pair_failures_are_not_wrapped() -> None:
    fixture, plan = documents()
    value = described(fixture, plan, "pg_route")
    pair = bound_pair(fixture, plan, "pg_route", value)
    object.__setattr__(pair.qualification_plan, "scope_sha256", sha(b"other"))
    with pytest.raises(NonproductionAuthorityError, match="plan_originals$"):
        value.require_route(pair, route_work_item_id="pg_route")


@pytest.mark.parametrize("rows", [0, 100000])
def test_input_parameter_boundaries(rows: int) -> None:
    fixture, plan = documents()
    fixture["fixtures"][0]["parameters"]["row_count"] = rows
    value = described(fixture, plan, "bcp_route")
    assert value.parameters == (("row_count", rows),)
    assert value.require_route(bound_pair(fixture, plan, "bcp_route", value), route_work_item_id="bcp_route")


@pytest.mark.parametrize(
    "damage,reason",
    [
        ("extra", "fields"),
        ("schema", "document"),
        ("whitespace", "document"),
        ("duplicate", "document"),
        ("nonfinite", "document"),
        ("invalid_utf8", "document"),
        ("empty", "document_budget"),
        ("bytearray", "document_budget"),
        ("oversize", "document_budget"),
        ("expected_digest", "digest"),
        ("digest_mismatch", "fixture_input_originals"),
        ("nested_extra", "fields"),
        ("array", "array"),
        ("parameter_array", "fixture_input_profile"),
    ],
)
def test_strict_original_parser_keeps_exact_existing_errors(damage: str, reason: str) -> None:
    fixture, plan = documents()
    body = described(fixture, plan, "bcp_route").to_dict()
    if damage == "extra":
        body["unknown"] = True
    if damage == "schema":
        body["schema"] = "another.schema"
    if damage == "nested_extra":
        body["seed_steps"][0]["unknown"] = True
    if damage == "array":
        body["seed_steps"] = {}
    if damage == "parameter_array":
        body["parameters"] = []
    raw: Any = canonical_document(body)
    variants = {
        "whitespace": raw + b" ",
        "duplicate": b'{"schema":"a","schema":"b"}',
        "nonfinite": b'{"schema":NaN}',
        "invalid_utf8": b"\xff",
        "empty": b"",
        "bytearray": bytearray(raw),
        "oversize": b"x" * (MAX_DOCUMENT_BYTES + 1),
    }
    raw = variants.get(damage, raw)
    expected = (
        "wrong"
        if damage == "expected_digest"
        else sha(b"different")
        if damage == "digest_mismatch"
        else sha(bytes(raw))
    )
    with pytest.raises(NonproductionAuthorityError, match=reason + "$"):
        NonproductionFixtureInput.from_bytes(raw, expected_sha256=expected)


@pytest.mark.parametrize("extra,reason", [(0, "fields"), (1, "document_budget")])
def test_exact_document_byte_ceiling_precedes_field_validation(extra: int, reason: str) -> None:
    fixture, plan = documents()
    body = described(fixture, plan, "bcp_route").to_dict()
    body["padding"] = ""

    def encode() -> bytes:
        return json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()

    body["padding"] = "x" * (MAX_DOCUMENT_BYTES - len(encode()) + extra)
    raw = encode()
    assert len(raw) == MAX_DOCUMENT_BYTES + extra
    with pytest.raises(NonproductionAuthorityError, match=reason + "$"):
        NonproductionFixtureInput.from_bytes(raw, expected_sha256=sha(raw))


@pytest.mark.parametrize("damage", ["reverse", "duplicate", "wrong_path", "wrong_type", "list"])
def test_exact_implementation_originals(damage: str) -> None:
    fixture, plan = documents(watermark=True)
    value = described(fixture, plan, "pg_route")
    rows: Any = value.implementation_originals
    if damage == "reverse":
        rows = rows[::-1]
    if damage == "duplicate":
        rows = (rows[0], rows[0])
    if damage == "wrong_path":
        rows = (replace(rows[0], path="other/recipe.py"), rows[1])
    if damage == "wrong_type":
        rows = (rows[0].to_dict(), rows[1])
    if damage == "list":
        rows = list(rows)
    with pytest.raises(NonproductionAuthorityError, match="fixture_input_originals$"):
        replace(value, implementation_originals=rows)


@pytest.mark.parametrize("method", ["to_dict", "to_bytes", "input_sha256", "require_route"])
def test_each_computation_revalidates_nested_originals(method: str) -> None:
    fixture, plan = documents()
    value = described(fixture, plan, "bcp_route")
    pair = bound_pair(fixture, plan, "bcp_route", value)
    object.__setattr__(value.recipe_original, "size_bytes", True)
    with pytest.raises(NonproductionAuthorityError, match="plan_originals$"):
        if method == "input_sha256":
            _ = value.input_sha256
        elif method == "require_route":
            value.require_route(pair, route_work_item_id="bcp_route")
        else:
            getattr(value, method)()


@pytest.mark.parametrize(
    "damage,reason",
    [
        ("plan", "fixture_input_reference"),
        ("fixture", "fixture_input_reference"),
        ("seed_type", "fixture_input_steps"),
        ("seed_list", "fixture_input_steps"),
        ("seed_fixture", "fixture_input_reference"),
        ("reverse_steps", "fixture_input_steps"),
        ("disabled_step", "fixture_input_steps"),
    ],
)
def test_constructor_references_and_steps_are_closed(damage: str, reason: str) -> None:
    fixture, plan = documents(watermark=damage != "disabled_step")
    raw = canonical_document(fixture)
    parsed: Any = NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw))
    initial = work_item_from_dict(find_item(plan, "pg_initial"))
    seed_items: Any = (initial,)
    if damage == "plan":
        parsed = {}
    if damage == "seed_type":
        seed_items = (find_item(plan, "pg_initial"),)
    if damage == "seed_list":
        seed_items = [initial]
    if damage == "seed_fixture":
        seed_items = (work_item_from_dict(find_item(plan, "bcp_initial")),)
    if damage in {"reverse_steps", "disabled_step"}:
        watermark = replace(cast(NonproductionFixtureSeedItem, initial), work_item_id="later", seed_step="watermark_3")
        seed_items = (watermark, initial) if damage == "reverse_steps" else (initial, watermark)
    with pytest.raises(NonproductionAuthorityError, match=reason + "$"):
        describe_fixture_input(
            parsed,
            fixture_id="missing" if damage == "fixture" else "pg",
            seed_items=seed_items,
            implementation_originals=implementation("pg"),
        )


@pytest.mark.parametrize("mode", ["reverse_ids", "enabled_without_step", "future_writer"])
def test_exact_selected_generation_uses_ancestors_only(mode: str) -> None:
    fixture, plan = documents(watermark=mode == "reverse_ids")
    name = "pg_route_final" if mode == "reverse_ids" else "pg_route"
    value = described(fixture, plan, name)
    if mode == "reverse_ids":
        rename = {"pg_initial": "z_initial", "pg_watermark": "a_watermark"}
        for row in plan["work_items"]:
            row["work_item_id"] = rename.get(row["work_item_id"], row["work_item_id"])
            row["predecessor_ids"] = [rename.get(key, key) for key in row["predecessor_ids"]]
        value = replace(value, seed_steps=(("z_initial", "initial"), ("a_watermark", "watermark_3")))
    elif mode == "enabled_without_step":
        fixture["fixtures"][1]["parameters"]["watermark_key_3"] = True
        value = replace(value, parameters=(("watermark_key_3", True),))
    else:
        donor = find_item(plan, "bcp_route")
        donor["effects"].append(effect("pg_source", "write", "helper"))
        donor["predecessor_ids"].append("pg_route")
        find_item(plan, "pg_seal")["predecessor_ids"] = ["bcp_route"]
    order_documents(fixture, plan)
    pair = bound_pair(fixture, plan, name, value)
    selected = value.require_route(pair, route_work_item_id=name)
    assert selected is next(row for row in pair.qualification_plan.work_items if row.work_item_id == name)


@pytest.mark.parametrize(
    "damage,reason",
    [
        ("pair", "fixture_input_reference"),
        ("route", "fixture_input_reference"),
        ("nonroute", "fixture_input_reference"),
        ("recipe", "fixture_input_originals"),
    ],
)
def test_route_selection_and_recipe_original_are_exact(damage: str, reason: str) -> None:
    fixture, plan = documents()
    value = described(fixture, plan, "bcp_route")
    if damage == "recipe":
        value = replace(value, recipe_original=replace(value.recipe_original, sha256=sha(b"different recipe")))
    pair: Any = bound_pair(fixture, plan, "bcp_route", value)
    if damage == "pair":
        pair = pair.to_dict() if hasattr(pair, "to_dict") else {}
    name = "missing" if damage == "route" else "bcp_initial" if damage == "nonroute" else "bcp_route"
    with pytest.raises(NonproductionAuthorityError, match=reason + "$"):
        value.require_route(pair, route_work_item_id=name)


def test_canonical_unicode_remains_exact_utf8() -> None:
    fixture, plan = documents()
    value = replace(described(fixture, plan, "bcp_route"), fixture_id="источник")
    raw = value.to_bytes()
    assert "источник".encode() in raw
    assert NonproductionFixtureInput.from_bytes(raw, expected_sha256=sha(raw)) == value
