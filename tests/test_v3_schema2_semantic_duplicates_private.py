"""Schema2 semantic-identity collisions with distinct canonical fixture bytes.

Each mutation preserves the relevant structural ordering so the specific semantic
uniqueness guard is exercised. Permission-effect identity remains a separate
pending contract question and is deliberately not asserted by this test module.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_physical_schema_descriptor import MssqlR1PhysicalSchemaDescriptorV1
from dpone.contracts.mssql_r1_v3_schema_inventory import (
    MssqlR1IndexDirectionV3,
    MssqlR1SchemaIndexKeyV3,
    MssqlR1SchemaIndexV3,
)
from tests.test_postgres_mssql_r1_v3_physical_descriptor_contract import descriptor


@pytest.fixture(scope="module")
def physical_descriptor() -> MssqlR1PhysicalSchemaDescriptorV1:
    return descriptor()


def test_column_names_reject_distinct_bytes_with_contiguous_ordinals(
    physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
) -> None:
    table = physical_descriptor.ordered_tables[0].portable_object
    first = table.ordered_columns[0]
    second = replace(first, ordinal=2)
    assert first.canonical_bytes != second.canonical_bytes

    with pytest.raises(MssqlR1V3ContractError, match="columns names contain duplicates"):
        replace(table, ordered_columns=(first, second))


def test_column_ordinals_reject_distinct_names_at_same_ordinal(
    physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
) -> None:
    table = physical_descriptor.ordered_tables[0].portable_object
    first = table.ordered_columns[0]
    second = replace(first, name="copy_column")
    assert first.canonical_bytes != second.canonical_bytes

    with pytest.raises(MssqlR1V3ContractError, match="columns ordinals must be contiguous"):
        replace(table, ordered_columns=(first, second))


def test_constraint_names_reject_distinct_canonically_ordered_definitions(
    physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
) -> None:
    table = next(t.portable_object for t in physical_descriptor.ordered_tables if t.portable_object.ordered_constraints)
    first = table.ordered_constraints[0]
    second = replace(first, definition_digest=bytes(value ^ 1 for value in first.definition_digest))
    assert first.canonical_bytes != second.canonical_bytes
    values = tuple(sorted((first, second), key=lambda item: item.canonical_bytes))

    with pytest.raises(MssqlR1V3ContractError, match="constraints names contain duplicates"):
        replace(table, ordered_constraints=values)


def test_index_names_reject_distinct_canonically_ordered_flags(
    physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
) -> None:
    table = physical_descriptor.ordered_tables[0].portable_object
    first = MssqlR1SchemaIndexV3(
        "synthetic_duplicate_index",
        True,
        False,
        (MssqlR1SchemaIndexKeyV3(table.ordered_columns[0].name, MssqlR1IndexDirectionV3.ASC),),
        (),
        None,
        True,
    )
    second = replace(first, enabled=False)
    assert first.canonical_bytes != second.canonical_bytes
    values = tuple(sorted((first, second), key=lambda item: item.canonical_bytes))

    with pytest.raises(MssqlR1V3ContractError, match="indexes names contain duplicates"):
        replace(table, ordered_indexes=values)


def test_parameter_ordinals_reject_distinct_names_at_same_ordinal(
    physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
) -> None:
    procedure = next(
        p.portable_object for p in physical_descriptor.ordered_procedures if p.portable_object.ordered_parameters
    )
    first = procedure.ordered_parameters[0]
    second = replace(first, name="copy_parameter")
    assert first.canonical_bytes != second.canonical_bytes

    with pytest.raises(MssqlR1V3ContractError, match="parameters ordinals must be contiguous"):
        replace(procedure, ordered_parameters=(first, second))


def test_parameter_names_reject_distinct_bytes_with_contiguous_ordinals(
    physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
) -> None:
    procedure = next(
        p.portable_object for p in physical_descriptor.ordered_procedures if p.portable_object.ordered_parameters
    )
    first = procedure.ordered_parameters[0]
    second = replace(first, ordinal=2)
    assert first.canonical_bytes != second.canonical_bytes

    with pytest.raises(MssqlR1V3ContractError, match="parameters names contain duplicates"):
        replace(procedure, ordered_parameters=(first, second))


def test_object_coordinates_reject_distinct_portable_bodies(
    physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
) -> None:
    contract = physical_descriptor.expected_schema_contract
    table = physical_descriptor.ordered_tables[0].portable_object
    first_column = table.ordered_columns[0]
    changed_column = replace(first_column, nullable=not first_column.nullable)
    changed_table = replace(table, ordered_columns=(changed_column, *table.ordered_columns[1:]))
    assert table.canonical_bytes != changed_table.canonical_bytes
    objects = tuple(
        sorted(
            (*contract.ordered_objects, changed_table),
            key=lambda item: (item.schema_name.encode(), item.object_name.encode()),
        )
    )

    with pytest.raises(MssqlR1V3ContractError, match="portable objects must use strict coordinate order"):
        replace(contract, ordered_objects=objects)


def test_codec_identity_rejects_distinct_canonically_ordered_implementations(
    physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
) -> None:
    contract = physical_descriptor.expected_schema_contract
    first = contract.ordered_supported_codecs[0]
    second = replace(first, implementation_digest=bytes(value ^ 1 for value in first.implementation_digest))
    assert first.canonical_bytes != second.canonical_bytes
    codecs = tuple(sorted((first, second), key=lambda item: item.canonical_bytes))

    with pytest.raises(MssqlR1V3ContractError, match="supported codec identities contain duplicates"):
        replace(contract, ordered_supported_codecs=codecs)
