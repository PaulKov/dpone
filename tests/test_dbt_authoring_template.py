"""Exact, immutable authoring coordinates with no inferred defaults."""

from copy import deepcopy
from dataclasses import FrozenInstanceError

import pytest

from dpone.contracts.dbt_authoring_template import DbtAuthoringTemplate, DbtSourceRelation
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_invocation import DbtInvocationTarget


def payload() -> dict:
    return {
        "project_name": "orders_demo",
        "invocation_target": {"database": "Development", "schema": "authoring"},
        "source_relation": {"database": "Source", "schema": "dbo", "name": "orders"},
    }


def test_round_trip_has_no_input_or_output_aliases() -> None:
    original = payload()
    template = DbtAuthoringTemplate.from_mapping(original)
    assert template.to_dict() == original
    original["source_relation"]["name"] = "changed"
    rendered = template.to_dict()
    rendered["invocation_target"]["schema"] = "changed"
    assert template.to_dict() == payload()
    assert template.invocation_target == DbtInvocationTarget("Development", "authoring")
    assert template.source_relation == DbtSourceRelation("Source", "dbo", "orders")
    with pytest.raises(FrozenInstanceError):
        template.project_name = "changed"
    with pytest.raises(FrozenInstanceError):
        template.source_relation.name = "changed"


@pytest.mark.parametrize("section", [None, "invocation_target", "source_relation"])
@pytest.mark.parametrize("mutation", ["missing", "extra", "not_mapping"])
def test_exact_object_shapes(section: str | None, mutation: str) -> None:
    value = payload()
    target = value if section is None else value[section]
    if mutation == "missing":
        target.pop(next(iter(target)))
    elif mutation == "extra":
        target["unexpected"] = "PRIVATE_SENTINEL"
    elif section is None:
        value = []
    else:
        value[section] = []
    with pytest.raises(DbtPublishingError) as caught:
        DbtAuthoringTemplate.from_mapping(value)
    assert "PRIVATE_SENTINEL" not in str(caught.value)


@pytest.mark.parametrize("field", ["project_name", "database", "schema", "name"])
@pytest.mark.parametrize("value", [None, True, 3, "", "a-b", "a b", "a\n", "9name", "é", "x" * 129])
def test_identifier_validation(field: str, value: object) -> None:
    data = payload()
    target = data if field == "project_name" else data["source_relation"]
    target[field] = value
    with pytest.raises(DbtPublishingError):
        DbtAuthoringTemplate.from_mapping(data)


@pytest.mark.parametrize("name", ["_", "a_1", "x" * 128])
def test_identifier_boundaries(name: str) -> None:
    data = payload()
    data["project_name"] = name
    data["source_relation"] = dict.fromkeys(("database", "schema", "name"), name)
    assert DbtAuthoringTemplate.from_mapping(data).to_dict() == data


@pytest.mark.parametrize("value", ["", " ", "-switch", "two words", "x\x00y", "x" * 257, True])
def test_invocation_target_preserves_existing_validation(value: object) -> None:
    data = payload()
    data["invocation_target"]["database"] = value
    with pytest.raises(DbtPublishingError) as caught:
        DbtAuthoringTemplate.from_mapping(data)
    assert caught.value.code == "DPONE_DBT_PACK_INVALID"


def test_direct_construction_does_not_accept_mutable_nested_mappings() -> None:
    data = deepcopy(payload())
    with pytest.raises(DbtPublishingError):
        DbtAuthoringTemplate(**data)


def test_direct_relation_construction_validates_identifiers() -> None:
    with pytest.raises(DbtPublishingError):
        DbtSourceRelation("Source", "dbo", "PRIVATE_SENTINEL invalid")
