from __future__ import annotations

from dataclasses import fields, replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_registered_target_catalog import (
    MssqlR1RegisteredTargetCatalogV1,
    MssqlR1RegisteredTargetColumnRefV1,
)
from dpone.contracts.mssql_r1_v3_registered_target_catalog_items import (
    MssqlR1ClosedTargetFeatureObservationV1,
    MssqlR1RegisteredTargetColumnV1,
    MssqlR1RegisteredTargetIndexV1,
)
from dpone.contracts.postgres_mssql_type_derivation import derive_type_decision
from dpone.contracts.postgres_mssql_type_target_enums import (
    PostgresMssqlLengthKindV1,
    PostgresMssqlSourceScalarFamilyV1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import PostgresMssqlSourceScalarShapeV1

BINDING = UUID("10000000-0000-0000-0000-000000000001")
OBJECT = UUID("20000000-0000-0000-0000-000000000002")
GENERATION = UUID("30000000-0000-0000-0000-000000000003")


def _int_target():
    source = PostgresMssqlSourceScalarShapeV1(
        PostgresMssqlSourceScalarFamilyV1.INT4,
        23,
        -1,
        PostgresMssqlLengthKindV1.NOT_APPLICABLE,
        None,
        None,
        None,
    )
    return derive_type_decision(source, maximum_input_bytes=32).target_shape


def _catalog() -> MssqlR1RegisteredTargetCatalogV1:
    column = MssqlR1RegisteredTargetColumnV1(
        1,
        "order_id",
        "sys",
        "int",
        "sys",
        "int",
        _int_target(),
        4,
        10,
        0,
        False,
        False,
        False,
        False,
        False,
        0,
        None,
        None,
    )
    primary = MssqlR1RegisteredTargetIndexV1(
        1,
        "PK_orders",
        "clustered",
        True,
        True,
        False,
        False,
        False,
        False,
        None,
        (1,),
        (False,),
        (),
    )
    return MssqlR1RegisteredTargetCatalogV1(
        "dpone-mssql-r1-registered-target-catalog-1",
        BINDING,
        OBJECT,
        GENERATION,
        "ordinary_disk_rowstore_v1",
        "warehouse",
        "dbo",
        "orders",
        41,
        1,
        "Latin1_General_100_BIN2",
        (column,),
        primary,
        (),
        MssqlR1ClosedTargetFeatureObservationV1.empty(),
    )


def registered_target_catalog() -> MssqlR1RegisteredTargetCatalogV1:
    """Build the exact admitted catalog shared by cross-contract tests."""

    return _catalog()


_CATALOG_CONTRACT_CLASSES = (
    MssqlR1RegisteredTargetColumnV1,
    MssqlR1RegisteredTargetIndexV1,
    MssqlR1ClosedTargetFeatureObservationV1,
    MssqlR1RegisteredTargetCatalogV1,
    MssqlR1RegisteredTargetColumnRefV1,
)
CATALOG_STRUCTURAL_WRONG_TYPE_CASES = tuple(
    (f"must_reject__structural_wrong_type__{contract.__name__}__{field.name}", index, field.name)
    for index, contract in enumerate(_CATALOG_CONTRACT_CLASSES)
    for field in fields(contract)
)


def _catalog_contract_values():
    catalog = _catalog()
    return (
        catalog.ordered_columns[0],
        catalog.primary_key,
        catalog.feature_observation,
        catalog,
        MssqlR1RegisteredTargetColumnRefV1.from_catalog(catalog, 1),
    )


def test_registered_catalog_round_trips_and_issues_bound_ref() -> None:
    catalog = _catalog()

    assert MssqlR1RegisteredTargetCatalogV1.from_canonical_bytes(catalog.canonical_bytes) == catalog
    assert len(catalog.digest) == 32
    reference = MssqlR1RegisteredTargetColumnRefV1.from_catalog(catalog, 1)
    assert reference.target_catalog_digest == catalog.digest
    assert reference.primary_key_ordinal == 1
    assert MssqlR1RegisteredTargetColumnRefV1.from_canonical_bytes(reference.canonical_bytes) == reference
    assert MssqlR1RegisteredTargetCatalogV1.create(catalog.canonical_bytes) == catalog


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: replace(value, primary_key=replace(value.primary_key, unique=False)),
        lambda value: replace(value, primary_key=replace(value.primary_key, ordered_descending=(True,))),
        lambda value: replace(value, primary_key=replace(value.primary_key, ordered_included_column_ordinals=(1,))),
        lambda value: replace(
            value,
            feature_observation=replace(value.feature_observation, ordered_trigger_digests=(b"a" * 32,)),
        ),
        lambda value: replace(value, ordered_columns=(replace(value.ordered_columns[0], identity=True),)),
    ),
    ids=(
        "must_reject__primary_key_not_unique",
        "must_reject__primary_key_descending",
        "must_reject__primary_key_included_column",
        "must_reject__trigger",
        "must_reject__identity_column",
    ),
)
def test_catalog_rejects_behavior_widening(mutation) -> None:
    with pytest.raises(MssqlR1V3ContractError):
        mutation(_catalog())


def test_catalog_rejects_structural_substitutes_and_bad_refs() -> None:
    catalog = _catalog()
    with pytest.raises(MssqlR1V3ContractError):
        replace(catalog, ordered_columns=list(catalog.ordered_columns))  # type: ignore[arg-type]
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1RegisteredTargetColumnRefV1.from_catalog(catalog, 2)


def test_catalog_enforces_exact_sql_identifier_and_bigint_boundaries() -> None:
    catalog = _catalog()
    supplementary_128_units = "😀" * 64

    assert replace(catalog, object_name=supplementary_128_units).object_name == supplementary_128_units
    with pytest.raises(MssqlR1V3ContractError):
        replace(catalog, object_name=supplementary_128_units + "a")
    assert replace(catalog, target_contract_revision=2**63 - 1).target_contract_revision == 2**63 - 1
    with pytest.raises(MssqlR1V3ContractError):
        replace(catalog, target_contract_revision=2**63)


def test_target_column_ref_requires_catalog_owned_construction_and_revalidation() -> None:
    catalog = _catalog()
    reference = MssqlR1RegisteredTargetColumnRefV1.from_catalog(catalog, 1)
    foreign = replace(catalog, target_contract_revision=2)

    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1RegisteredTargetColumnRefV1(
            reference.ordinal,
            reference.name,
            reference.nullable,
            reference.scalar_shape,
            reference.target_catalog_digest,
            reference.primary_key_ordinal,
        )
    reference.validate_against_catalog(catalog)
    with pytest.raises(MssqlR1V3ContractError):
        reference.validate_against_catalog(foreign)


@pytest.mark.parametrize(
    ("case_id", "contract_index", "field_name"),
    CATALOG_STRUCTURAL_WRONG_TYPE_CASES,
    ids=[case[0] for case in CATALOG_STRUCTURAL_WRONG_TYPE_CASES],
)
def test_catalog_contract_rejects_structural_wrong_types(case_id, contract_index, field_name) -> None:
    value = _catalog_contract_values()[contract_index]

    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1V3ContractError):
        replace(value, **{field_name: object()})


CATALOG_VALID_DISTINCT_REGISTRY = ("valid_distinct__target_contract_revision",)


@pytest.mark.parametrize("case_id", CATALOG_VALID_DISTINCT_REGISTRY)
def test_catalog_valid_distinct_mutations_change_digest(case_id) -> None:
    before = _catalog()
    after = replace(before, target_contract_revision=2)

    assert case_id.startswith("valid_distinct__")
    assert before.digest != after.digest
