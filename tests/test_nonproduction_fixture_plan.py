"""Closed fixed profiles and complete fixture originals."""

from copy import deepcopy
from dataclasses import replace
from typing import Any

import pytest

from dpone.contracts.nonproduction_fixture_plan import NonproductionFixturePlan
from dpone.contracts.nonproduction_plan_values import NonproductionPlanObject
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError, canonical_document
from tests.nonproduction_plan_helpers import declared, documents, label_sha, originals, sha


def test_fixture_roundtrip_and_detached_values() -> None:
    fixture, plan = documents()
    raw, _, grant = originals(fixture, plan)
    value = NonproductionFixturePlan.from_bytes(raw, expected_sha256=grant.fixture_plan_sha256)
    assert value.to_dict() == fixture
    assert value.to_bytes() == raw
    assert value.fixture_plan_sha256 == sha(raw)
    assert value.fixture_plan_sha256 == "sha256:50c4d722bcf2eecb41d11aa5ad239e98384abb56b28580ce04790b1480f482eb"
    detached = value.to_dict()
    detached["objects"].clear()
    assert value.to_bytes() == raw


@pytest.mark.parametrize("count", [0, 100000])
def test_bcp_declared_finite_rows(count: int) -> None:
    fixture, plan = documents()
    fixture["fixtures"][0]["parameters"]["row_count"] = count
    raw, _, _ = originals(fixture, plan)
    assert NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw)).to_bytes() == raw


@pytest.mark.parametrize("count", [True, False, -1, 100001, 1.0, "10", None])
def test_bcp_invalid_rows(count: Any) -> None:
    fixture, _ = documents()
    fixture["fixtures"][0]["parameters"]["row_count"] = count
    raw = canonical_document(fixture)
    with pytest.raises(NonproductionAuthorityError):
        NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw))


@pytest.mark.parametrize("field", ["object_id", "fixture_id"])
def test_revalidation_rejects_mutated_nested_identity_with_typed_error(field: str) -> None:
    fixture, plan = documents()
    raw, _, _ = originals(fixture, plan)
    value = NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw))
    nested = value.objects[0] if field == "object_id" else value.fixtures[0]
    object.__setattr__(nested, field, [])
    with pytest.raises(NonproductionAuthorityError):
        value.to_bytes()


@pytest.mark.parametrize(
    "damage",
    [
        "unknown_profile",
        "wrong_recipe",
        "extra_parameter",
        "missing_binding",
        "extra_binding",
        "same_binding",
        "wrong_source",
        "wrong_helper_kind",
        "duplicate_source",
        "duplicate_subject",
        "duplicate_name",
        "unsorted_objects",
        "unsorted_fixtures",
    ],
)
def test_fixture_rejects_incomplete_or_ambiguous_profile(damage: str) -> None:
    fixture, _ = documents()
    bcp, pg = fixture["fixtures"]
    enum = next(row for row in fixture["objects"] if row["object_id"] == "enum")
    if damage == "unknown_profile":
        bcp["profile"] = "dbt"
    elif damage == "wrong_recipe":
        bcp["recipe_original"]["path"] = "recipes/replacement.py"
    elif damage == "extra_parameter":
        pg["parameters"]["row_id"] = 3
    elif damage == "missing_binding":
        del pg["bindings"]["c_serial_sequence"]
    elif damage == "extra_binding":
        bcp["bindings"]["state"] = "enum"
    elif damage == "same_binding":
        pg["bindings"]["c_serial_sequence"] = pg["bindings"]["c_smallserial_sequence"]
    elif damage == "wrong_source":
        bcp["bindings"]["source"] = "pg_source"
    elif damage == "wrong_helper_kind":
        enum["object_kind"] = "table"
    elif damage == "duplicate_source":
        fixture["fixtures"].insert(1, {**deepcopy(bcp), "fixture_id": "bcp_duplicate"})
    elif damage in {"duplicate_subject", "duplicate_name"}:
        source = next(row for row in fixture["objects"] if row["object_id"] == "pg_source")
        enum["subject_sha256" if damage == "duplicate_subject" else "qualified_name"] = source[
            "subject_sha256" if damage == "duplicate_subject" else "qualified_name"
        ]
        if damage == "duplicate_name":
            enum["object_kind"] = "table"
    elif damage == "unsorted_objects":
        fixture["objects"].reverse()
    else:
        fixture["fixtures"].reverse()
    raw = canonical_document(fixture)
    with pytest.raises(NonproductionAuthorityError):
        NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw))


@pytest.mark.parametrize(
    "helper", ["enum", "c_smallserial_sequence", "c_serial_sequence", "c_identity_sequence", "c_bigserial_sequence"]
)
@pytest.mark.parametrize("field", ["service_id", "physical_subject_sha256", "database", "schema"])
def test_pg_helpers_match_source_domain_and_name_namespace(helper: str, field: str) -> None:
    fixture, _ = documents()
    obj = next(row for row in fixture["objects"] if row["object_id"] == helper)
    if field in {"database", "schema"}:
        obj["qualified_name"][0 if field == "database" else 1] = "another"
    else:
        obj[field] = "10000000-0000-4000-8000-000000000099" if field == "service_id" else label_sha("another")
    raw = canonical_document(fixture)
    with pytest.raises(NonproductionAuthorityError, match="plan_bindings"):
        NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw))


@pytest.mark.parametrize("value", [0, 1, "true", None])
def test_pg_watermark_flag_is_strict_boolean(value: Any) -> None:
    fixture, _ = documents()
    fixture["fixtures"][1]["parameters"]["watermark_key_3"] = value
    raw = canonical_document(fixture)
    with pytest.raises(NonproductionAuthorityError):
        NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw))


@pytest.mark.parametrize(
    "damage",
    ["duplicate_json", "noncanonical", "nonfinite", "utf8", "missing", "extra", "schema", "digest", "oversize"],
)
def test_strict_fixture_transport(damage: str) -> None:
    fixture, _ = documents()
    raw = canonical_document(fixture)
    expected = sha(raw)
    if damage == "duplicate_json":
        raw = b'{"schema":"x",' + raw[1:]
    elif damage == "noncanonical":
        raw += b"\n"
    elif damage == "nonfinite":
        raw = raw.replace(b'"row_count":10', b'"row_count":NaN')
    elif damage == "utf8":
        raw = b"\xff"
    elif damage == "oversize":
        raw = b" " * (1024 * 1024 + 1)
    elif damage == "digest":
        expected = label_sha("different")
    else:
        if damage == "missing":
            del fixture["objects"]
        elif damage == "extra":
            fixture["qualification_plan_sha256"] = label_sha("cycle")
        else:
            fixture["schema"] = "wrong"
        raw = canonical_document(fixture)
        expected = sha(raw)
    with pytest.raises(NonproductionAuthorityError):
        NonproductionFixturePlan.from_bytes(raw, expected_sha256=expected)


def test_physical_participant_budget_is_independent_of_object_count() -> None:
    fixture, _ = documents()
    raw = canonical_document(fixture)
    value = NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw))
    extras = tuple(
        NonproductionPlanObject.from_dict(
            {**declared(f"extra_{i:03}", "clickhouse"), "physical_subject_sha256": label_sha(f"domain{i}")}
        )
        for i in range(254)
    )
    at_limit = replace(value, objects=tuple(sorted((*value.objects, *extras[:253]), key=lambda row: row.object_id)))
    assert len({row.domain for row in at_limit.objects}) == 256
    with pytest.raises(NonproductionAuthorityError, match="plan_objects"):
        replace(value, objects=tuple(sorted((*value.objects, *extras), key=lambda row: row.object_id)))


def test_fixture_count_and_object_bound_have_explicit_limits() -> None:
    fixture, _ = documents()
    bcp = fixture["fixtures"][0]
    for index in range(63):
        source_id = f"extra{index:03}"
        fixture["objects"].append(declared(source_id, "mssql"))
        fixture["fixtures"].append(
            {**deepcopy(bcp), "fixture_id": source_id, "bindings": {"source": source_id, "target": "bcp_target"}}
        )
    fixture["objects"].sort(key=lambda row: row["object_id"])
    fixture["fixtures"].sort(key=lambda row: row["fixture_id"])
    extra = fixture["fixtures"].pop(-2)
    raw = canonical_document(fixture)
    value = NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw))
    assert len(value.fixtures) == 64
    fixture["fixtures"].insert(-1, extra)
    raw = canonical_document(fixture)
    with pytest.raises(NonproductionAuthorityError):
        NonproductionFixturePlan.from_bytes(raw, expected_sha256=sha(raw))
    # 8192 full object declarations exceed 1 MiB first; that stricter bound is intentional.
    objects = tuple(NonproductionPlanObject.from_dict(declared(f"object{i:05}", "mssql")) for i in range(8193))
    at_limit = tuple(sorted((*value.objects, *objects[: 8192 - len(value.objects)]), key=lambda row: row.object_id))
    with pytest.raises(NonproductionAuthorityError, match="document_budget"):
        replace(value, objects=at_limit)
    with pytest.raises(NonproductionAuthorityError, match="plan_objects"):
        NonproductionFixturePlan(value.scope_sha256, objects, value.fixtures)
