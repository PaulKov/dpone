"""Selected filegroup names are policy input, never observed database authority."""

from copy import deepcopy

import pytest

from dpone.contracts.dbt_native_execution_policy import require_physical_filegroup_name
from dpone.contracts.dbt_publish_schema_contract_policy import policy_v3_schema
from dpone.contracts.dbt_publish_schema_contract_v4 import validate_native_policy_v4
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from tests.test_dbt_native_policy_v4 import native_policy


def validate(policy):
    return validate_native_policy_v4(encode_native_delivery_json(policy), max_bytes=1048576)


def test_legacy_policy_retains_exact_bytes_without_filegroup_default():
    policy = native_policy()
    before = encode_native_delivery_json(policy)
    legacy_schema = deepcopy(policy_v3_schema())
    decoded = validate(policy)
    native = decoded["profiles"]["local"]["native_execution"]
    assert "physical_filegroup" not in native
    assert encode_native_delivery_json(decoded) == before
    assert policy_v3_schema() == legacy_schema
    with pytest.raises(ValueError, match="physical_filegroup"):
        require_physical_filegroup_name(native)


@pytest.mark.parametrize("name", ["PRIMARY", "archive fg", "Mixed]Quote'", "данные", "x" * 128, "😀" * 64])
def test_explicit_name_is_retained_exactly_without_normalization(name):
    policy = native_policy()
    native = policy["profiles"]["local"]["native_execution"]
    native["physical_filegroup"] = {"name": name}
    decoded = validate(policy)
    assert decoded == policy
    assert require_physical_filegroup_name(decoded["profiles"]["local"]["native_execution"]) == name


@pytest.mark.parametrize(
    "selection", [None, {}, {"name": "fg", "data_space_id": 1}, {"name": "fg", "fallback": "PRIMARY"}, [], "PRIMARY"]
)
def test_selection_is_closed_and_requires_explicit_name(selection):
    policy = native_policy()
    native = policy["profiles"]["local"]["native_execution"]
    native["physical_filegroup"] = selection
    with pytest.raises(ValueError):
        validate(policy)
    with pytest.raises(ValueError, match="physical_filegroup"):
        require_physical_filegroup_name(native)


@pytest.mark.parametrize("name", [None, True, 1, "", "x" * 129, "😀" * 65, "a\nb", "a\x00b", "a\x7fb"])
def test_invalid_identifier_rejected_by_policy_and_explicit_consumer(name):
    policy = native_policy()
    native = policy["profiles"]["local"]["native_execution"]
    native["physical_filegroup"] = {"name": name}
    with pytest.raises(ValueError):
        validate(policy)
    with pytest.raises(ValueError, match="physical_filegroup"):
        require_physical_filegroup_name(native)


def test_schema_validation_alone_does_not_relax_sql_utf16_identifier_boundary():
    # JSON Schema maxLength counts scalar characters; SQL identifiers count UTF-16 units.
    policy = native_policy()
    policy["profiles"]["local"]["native_execution"]["physical_filegroup"] = {"name": "😀" * 64 + "x"}
    with pytest.raises(ValueError, match="physical_filegroup"):
        validate(policy)
