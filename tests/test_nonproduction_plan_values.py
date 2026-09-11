"""Strict documentary values never resolve physical names or original bytes."""

from dataclasses import replace
from typing import Any

import pytest

from dpone.contracts.nonproduction_plan_values import (
    NonproductionPlanEffect,
    NonproductionPlanObject,
    NonproductionPlanOriginal,
    NonproductionSourceBound,
)
from dpone.contracts.nonproduction_scope import MAX_DOCUMENT_BYTES, NonproductionAuthorityError
from tests.nonproduction_plan_helpers import declared, descriptor, documents, find_item, label_sha


@pytest.mark.parametrize("size", [1, MAX_DOCUMENT_BYTES])
def test_original_metadata_size_boundary(size: int) -> None:
    value = NonproductionPlanOriginal.from_dict({**descriptor(), "size_bytes": size})
    assert value.to_dict()["size_bytes"] == size


@pytest.mark.parametrize("size", [0, -1, True, False, 1.0, "1", None, MAX_DOCUMENT_BYTES + 1])
def test_original_size_rejects_coercion(size: Any) -> None:
    with pytest.raises(NonproductionAuthorityError):
        NonproductionPlanOriginal.from_dict({**descriptor(), "size_bytes": size})


@pytest.mark.parametrize(
    "path",
    [
        "/absolute",
        "a//b",
        "../secret",
        "a/./b",
        "a/../b",
        "a\\b",
        "https://a/b",
        "a/*",
        "a?x",
        "a#x",
        "é/file",
        "a" * 513,
    ],
)
def test_original_rejects_unsafe_paths(path: str) -> None:
    with pytest.raises(NonproductionAuthorityError):
        NonproductionPlanOriginal.from_dict(descriptor(path))


@pytest.mark.parametrize(("connector", "maximum"), [("postgres", 63), ("mssql", 128), ("clickhouse", 128)])
def test_exact_name_bounds(connector: str, maximum: int) -> None:
    body = declared("object", connector)
    body["qualified_name"][-1] = "a" * maximum
    value = NonproductionPlanObject.from_dict(body)
    assert value.qualified_name[-1] == "a" * maximum
    with pytest.raises(NonproductionAuthorityError):
        replace(value, qualified_name=(*value.qualified_name[:-1], "a" * (maximum + 1)))


@pytest.mark.parametrize("name", ["a.b", "../x", "*", "[x]", '"x"', "a b", "a;DROP", "é", "0abc", ""])
def test_names_are_explicit_safe_parts(name: str) -> None:
    body = declared("object", "postgres")
    body["qualified_name"][-1] = name
    with pytest.raises(NonproductionAuthorityError):
        NonproductionPlanObject.from_dict(body)


@pytest.mark.parametrize(
    ("connector", "kind"),
    [
        ("postgres", "enum"),
        ("postgres", "sequence"),
        ("postgres", "table"),
        ("mssql", "table"),
        ("clickhouse", "table"),
    ],
)
def test_closed_supported_object_kinds(connector: str, kind: str) -> None:
    body = declared("a", connector, kind)
    value = NonproductionPlanObject.from_dict(body)
    assert value.to_dict() == body
    assert value.identity == (*value.domain, body["subject_sha256"])


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("connector", "sqlite"),
        ("object_kind", "schema"),
        ("object_kind", "enum"),
        ("service_id", "not-a-uuid"),
        ("subject_sha256", "a" * 64),
        ("physical_subject_sha256", None),
        ("qualified_name", "a.b.c"),
        ("qualified_name", ["a", "b"]),
        ("qualified_name", ["a", "b", "c", "d"]),
    ],
)
def test_invalid_physical_shapes(key: str, value: Any) -> None:
    body = declared("a", "mssql")
    body[key] = value
    with pytest.raises(NonproductionAuthorityError):
        NonproductionPlanObject.from_dict(body)


@pytest.mark.parametrize("logical_id", ["route/é", "route/e\u0301", "route\\é"])
def test_logical_ids_preserve_exact_unicode_and_separators(logical_id: str) -> None:
    value = NonproductionPlanObject.from_dict({**declared("a", "postgres"), "object_id": logical_id})
    assert value.object_id == logical_id
    assert value.to_dict()["object_id"] == logical_id


@pytest.mark.parametrize(
    "kind", [NonproductionPlanObject, NonproductionPlanOriginal, NonproductionPlanEffect, NonproductionSourceBound]
)
@pytest.mark.parametrize("damage", ["missing", "extra", "wrong_type"])
def test_every_value_has_closed_fields(kind: Any, damage: str) -> None:
    _, plan = documents()
    choices = {
        NonproductionPlanObject: declared("a", "postgres"),
        NonproductionPlanOriginal: descriptor(),
        NonproductionPlanEffect: {"object_id": "a", "access": "read", "purpose": "helper"},
        NonproductionSourceBound: find_item(plan, "bcp_route")["source_bound"],
    }
    body: Any = choices[kind]
    if damage == "missing":
        del body[next(iter(body))]
    elif damage == "extra":
        body["caller_callback"] = "no"
    else:
        body = []
    with pytest.raises(NonproductionAuthorityError):
        kind.from_dict(body)


@pytest.mark.parametrize(
    ("access", "purpose"), [("READ", "source"), ("read", "permission"), ("delete", "target"), (True, "source")]
)
def test_effects_have_explicit_access_and_explanatory_purpose(access: Any, purpose: str) -> None:
    with pytest.raises(NonproductionAuthorityError):
        NonproductionPlanEffect("a", access, purpose)


def test_bound_descriptors_are_detached_and_never_computed_numbers() -> None:
    _, plan = documents()
    body = find_item(plan, "bcp_route")["source_bound"]
    value = NonproductionSourceBound.from_dict(body)
    body["input_original"]["sha256"] = label_sha("replacement")
    assert value.input_original.sha256 != body["input_original"]["sha256"]
    assert set(value.to_dict()) == {
        "source_object_id",
        "accounting_profile",
        "input_original",
        "projection_original",
        "derivation_original",
    }
    for extra in ("max_source_rows", "max_source_bytes", "schema_observation", "generation_id"):
        with pytest.raises(NonproductionAuthorityError):
            NonproductionSourceBound.from_dict({**value.to_dict(), extra: 0})
