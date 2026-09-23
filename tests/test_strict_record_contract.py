"""Structural record decoding rejects scalar and field aliases without policy."""

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

import pytest

from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape, string_enum


@dataclass(frozen=True)
class Record:
    name: str
    count: int = 0


class Choice(StrEnum):
    VALUE = "value"


def test_exact_shape_preserves_input_and_requires_defaulted_fields():
    value = {"name": "sample", "count": 2}
    assert record_shape(Record, value) is value
    assert construct_record(Record, value) == Record("sample", 2)
    assert value == {"name": "sample", "count": 2}
    with pytest.raises(ValueError, match="record_fields"):
        construct_record(Record, {"name": "sample"})


@pytest.mark.parametrize("value", [None, [], True, {"name": "sample", "count": 2, "extra": 1}])
def test_non_records_and_unknown_fields_reject(value):
    with pytest.raises(ValueError, match="record_fields"):
        record_shape(Record, value)


def test_mapping_subclasses_do_not_supply_hidden_behavior():
    class Mapping(dict):
        pass

    with pytest.raises(ValueError, match="record_fields"):
        record_shape(Record, Mapping(name="sample", count=1))


@pytest.mark.parametrize("value", [True, 1, None, Choice.VALUE])
def test_enum_requires_exact_string(value):
    with pytest.raises(ValueError, match="enum_type"):
        string_enum(Choice, value)


def test_enum_decoding_preserves_declared_enum_domain():
    assert string_enum(Choice, "value") is Choice.VALUE
    with pytest.raises(ValueError):
        string_enum(Choice, "unknown")


@pytest.mark.parametrize("value", [UUID(int=1), None, True, 1])
def test_uuid_requires_exact_string(value):
    with pytest.raises(ValueError, match="uuid_type"):
        canonical_uuid(value)


@pytest.mark.parametrize("value", ["00000000000000000000000000000001", "{00000000-0000-0000-0000-000000000001}"])
def test_uuid_spelling_aliases_reject(value):
    with pytest.raises(ValueError, match="uuid_alias"):
        canonical_uuid(value)


def test_canonical_uuid_does_not_add_domain_nonzero_policy():
    for integer in (0, 1):
        value = UUID(int=integer)
        assert canonical_uuid(str(value)) == value
