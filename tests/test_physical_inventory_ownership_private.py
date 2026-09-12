"""Collection owners reject malformed inventory without normalization."""

import json
from dataclasses import fields, replace
from importlib import import_module
from pathlib import Path

import pytest

from dpone.contracts.mssql_r1_v3_codec import MssqlR1V3ContractError
from tests.test_postgres_mssql_r1_v3_physical_descriptor_contract import descriptor

BASELINE = json.loads((Path(__file__).parent / "fixtures/physical_inventory_baseline_v38_private.json").read_text())

INVENTORIES = (
    ("relations", "validate_table_inventory", "ordered_tables"),
    ("procedures", "validate_procedure_inventory", "ordered_procedures"),
    ("procedures", "validate_binding_inventory", "ordered_binding_module_templates"),
    ("resources", "validate_resource_inventory", "ordered_resource_declarations"),
    ("errors", "validate_migration_probe_inventory", "ordered_migration_probes"),
)


@pytest.mark.parametrize(("owner", "name", "field"), INVENTORIES)
def test_collection_owner_preserves_valid_inventory(owner, name, field):
    value = descriptor()
    original = value.canonical_bytes
    validate = getattr(import_module("dpone.contracts.mssql_r1_v3_physical_descriptor_" + owner), name)
    assert validate(getattr(value, field)) is None
    assert value.canonical_bytes == original


@pytest.mark.parametrize(("owner", "name", "field"), INVENTORIES)
@pytest.mark.parametrize("invalid", ["list", "tuple_subclass", "wrong_member", "empty", "duplicate"])
def test_collection_owner_matches_aggregate_rejection(owner, name, field, invalid):
    value = descriptor()
    values = getattr(value, field)

    class TupleSubclass(tuple):
        pass

    malformed = {
        "list": list(values),
        "tuple_subclass": TupleSubclass(values),
        "wrong_member": (object(),),
        "empty": (),
        "duplicate": (*values, values[0]),
    }[invalid]
    with pytest.raises(MssqlR1V3ContractError) as original:
        replace(value, **{field: malformed})
    validate = getattr(import_module("dpone.contracts.mssql_r1_v3_physical_descriptor_" + owner), name)
    with pytest.raises(type(original.value)) as moved:
        validate(malformed)
    expected = BASELINE["errors"][field + "/" + invalid]
    assert type(original.value).__name__ == expected["type"]
    assert str(original.value) == expected["message"]
    assert str(moved.value) == expected["message"]


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("ordered_tables", "ordered_procedures"),
        ("ordered_procedures", "ordered_binding_module_templates"),
        ("ordered_binding_module_templates", "ordered_resource_declarations"),
        ("ordered_resource_declarations", "ordered_migration_probes"),
    ],
)
def test_aggregate_keeps_inventory_error_precedence(first, second):
    value = descriptor()
    with pytest.raises(MssqlR1V3ContractError) as first_error:
        replace(value, **{first: []})
    with pytest.raises(MssqlR1V3ContractError) as combined:
        replace(value, **{first: [], second: []})
    assert str(combined.value) == str(first_error.value)


def test_descriptor_retains_frozen_baseline_bytes_and_constructor():
    from dpone.contracts.mssql_r1_v3_physical_schema_descriptor import PROCEDURE_NAMES, TABLE_NAMES

    value = descriptor()
    assert value.digest.hex() == BASELINE["descriptor_digest"]
    assert [field.name for field in fields(value)] == BASELINE["constructor_fields"]
    assert list(TABLE_NAMES) == BASELINE["table_names"]
    assert list(PROCEDURE_NAMES) == BASELINE["procedure_names"]


@pytest.mark.parametrize("later", ["ordered_procedures", "ordered_binding_module_templates"])
def test_schema_check_follows_all_ordered_inventory_checks(later):
    value = descriptor()
    table = value.ordered_tables[0]
    altered = replace(table, portable_object=replace(table.portable_object, schema_name="other_schema"))
    with pytest.raises(MssqlR1V3ContractError) as expected:
        replace(value, **{later: []})
    with pytest.raises(MssqlR1V3ContractError) as actual:
        replace(value, ordered_tables=(altered, *value.ordered_tables[1:]), **{later: []})
    assert str(actual.value) == str(expected.value)


@pytest.mark.parametrize("kind", ["resources", "probes"])
def test_unique_but_reversed_inventory_is_not_normalized(kind):
    value = descriptor()
    if kind == "resources":
        from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import validate_resource_inventory

        validate = validate_resource_inventory
        values = value.ordered_resource_declarations
        message = "resource declarations must be a nonempty canonical set"
    else:
        from dpone.contracts.mssql_r1_v3_physical_descriptor_errors import validate_migration_probe_inventory

        validate = validate_migration_probe_inventory
        probe = value.ordered_migration_probes[0]
        values = tuple(sorted((probe, replace(probe, probe_id="another_probe")), key=lambda item: item.canonical_bytes))
        message = "migration probes must be a nonempty canonical set"
    malformed = tuple(reversed(values))
    before = tuple(item.canonical_bytes for item in malformed)
    with pytest.raises(MssqlR1V3ContractError) as error:
        validate(malformed)
    assert str(error.value) == message
    assert tuple(item.canonical_bytes for item in malformed) == before


def test_probe_casefold_identity_is_checked_after_canonical_payload_uniqueness():
    from dpone.contracts.mssql_r1_v3_physical_descriptor_errors import validate_migration_probe_inventory

    probe = descriptor().ordered_migration_probes[0]
    from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import (
        MssqlR1MigrationDispositionV1,
        MssqlR1MigrationObservationKindV1,
    )

    altered = replace(
        probe,
        observation_kind=MssqlR1MigrationObservationKindV1.EXACT_SCHEMA2,
        disposition=MssqlR1MigrationDispositionV1.COEXIST,
    )
    values = tuple(sorted((probe, altered), key=lambda item: item.canonical_bytes))
    assert len({item.canonical_bytes for item in values}) == 2
    with pytest.raises(MssqlR1V3ContractError) as error:
        validate_migration_probe_inventory(values)
    assert str(error.value) == "migration probe IDs are not semantically unique"


def test_probe_rejects_uppercase_identity_before_collection_admission():
    probe = descriptor().ordered_migration_probes[0]
    with pytest.raises(MssqlR1V3ContractError) as error:
        replace(probe, probe_id=probe.probe_id.upper())
    assert str(error.value) == "migration probe ID must be bounded lowercase ASCII"
