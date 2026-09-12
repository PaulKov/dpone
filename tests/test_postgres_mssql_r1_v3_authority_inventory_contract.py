from __future__ import annotations

import hashlib
from dataclasses import fields, replace
from functools import lru_cache
from types import NoneType
from typing import get_args, get_type_hints

import pytest

from dpone.contracts.mssql_r1_v3_codec import decode_canonical_bytes
from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_registered_target_catalog import (
    MssqlR1RegisteredTargetCatalogV1,
    MssqlR1RegisteredTargetColumnRefV1,
)
from dpone.contracts.mssql_r1_v3_registered_target_catalog_items import (
    MssqlR1RegisteredTargetColumnV1,
    MssqlR1RegisteredTargetIndexV1,
)
from dpone.contracts.mssql_r1_v3_registration_rotation import (
    MssqlR1RegistrationRotationTransitionEvidenceV1,
)
from dpone.contracts.mssql_r1_v3_verified_target_authority import (
    MssqlR1ActiveRegistrationHeadObservationV1,
    MssqlR1RegistrationAdmissionEvidenceV1,
    MssqlR1RotationStableTargetAuthorityV1,
)
from dpone.contracts.postgres_mssql_type_authority import (
    PostgresMssqlSourceColumnRefV1,
    PostgresMssqlTypePolicyAuthorityV1,
)
from dpone.contracts.postgres_mssql_type_derivation import PostgresMssqlTypeDecisionAuthorityV1, derive_type_decision
from dpone.contracts.postgres_mssql_type_target_enums import (
    MssqlR1TargetScalarFamilyV1,
    MssqlR1TypeTargetAuthorityError,
    MssqlR1TypeTargetRecoveryClass,
    PostgresMssqlCodecV1,
    PostgresMssqlEqualityPolicyV1,
    PostgresMssqlLengthKindV1,
    PostgresMssqlNormalizationV1,
    PostgresMssqlSourceScalarFamilyV1,
    PostgresMssqlValueGuardV1,
    reject,
)
from dpone.contracts.postgres_mssql_type_target_shapes import (
    MssqlR1CanonicalTargetScalarShapeV1,
    PostgresMssqlSourceScalarShapeV1,
    PostgresMssqlValueAdmissionAuthorityV1,
)
from tests.postgres_mssql_r1_v3_authority_inventory_support import (
    CANONICAL_LAYOUT,
    CROSS_AUTHORITY_FIELDS,
    CROSS_AUTHORITY_MUST_REJECT_CASES,
    DATACLASS_FIELD_MUST_REJECT_CASES,
    ENUM_MEMBER_INVENTORY,
    FIELD_INVENTORY,
    OPTIONAL_ARM_CASES,
    OPTIONAL_ARM_DISPOSITIONS,
    field_index,
    foreign_candidate,
)
from tests.postgres_mssql_r1_v3_type_target_test_support import rotation_case, source_shape


def _all_source_shapes() -> tuple[PostgresMssqlSourceScalarShapeV1, ...]:
    numeric_typmod = ((10 << 16) | 2) + 4
    return (
        source_shape(PostgresMssqlSourceScalarFamilyV1.BOOL, 16),
        source_shape(PostgresMssqlSourceScalarFamilyV1.INT2, 21),
        source_shape(PostgresMssqlSourceScalarFamilyV1.INT4, 23),
        source_shape(PostgresMssqlSourceScalarFamilyV1.INT8, 20),
        source_shape(PostgresMssqlSourceScalarFamilyV1.NUMERIC, 1700, numeric_typmod, precision=10, scale=2),
        source_shape(PostgresMssqlSourceScalarFamilyV1.FLOAT4, 700),
        source_shape(PostgresMssqlSourceScalarFamilyV1.FLOAT8, 701),
        source_shape(PostgresMssqlSourceScalarFamilyV1.UUID, 2950),
        source_shape(PostgresMssqlSourceScalarFamilyV1.DATE, 1082),
        source_shape(PostgresMssqlSourceScalarFamilyV1.TIME, 1083, 6, precision=6),
        source_shape(PostgresMssqlSourceScalarFamilyV1.TIMESTAMP, 1114, 6, precision=6),
        source_shape(PostgresMssqlSourceScalarFamilyV1.TIMESTAMPTZ, 1184, 6, precision=6),
        source_shape(PostgresMssqlSourceScalarFamilyV1.TEXT, 25),
        source_shape(PostgresMssqlSourceScalarFamilyV1.VARCHAR, 1043, 14, maximum_characters=10),
        source_shape(PostgresMssqlSourceScalarFamilyV1.BYTEA, 17),
    )


@lru_cache
def _context() -> dict[str, object]:
    source = source_shape(PostgresMssqlSourceScalarFamilyV1.INT4, 23)
    decision = derive_type_decision(source, maximum_input_bytes=32)
    policy = PostgresMssqlTypePolicyAuthorityV1.create((decision,), (source,))
    source_ref = policy.source_column_ref(ordinal=1, name="order_id", nullable=False, source_shape=source)
    case = rotation_case()
    catalog, payload, verification, physical, security, predecessor = case[:6]
    receipt, successor, predecessor_admission, successor_admission, lock = case[8:]
    transition = MssqlR1RegistrationRotationTransitionEvidenceV1.create(
        predecessor_admission, successor_admission, receipt, physical, security, lock
    )
    values = {
        PostgresMssqlSourceScalarShapeV1: source,
        MssqlR1CanonicalTargetScalarShapeV1: decision.target_shape,
        PostgresMssqlValueAdmissionAuthorityV1: decision.value_admission,
        PostgresMssqlTypeDecisionAuthorityV1: decision,
        PostgresMssqlTypePolicyAuthorityV1: policy,
        PostgresMssqlSourceColumnRefV1: source_ref,
        MssqlR1RegisteredTargetColumnV1: catalog.ordered_columns[0],
        MssqlR1RegisteredTargetIndexV1: catalog.primary_key,
        type(catalog.feature_observation): catalog.feature_observation,
        MssqlR1RegisteredTargetCatalogV1: catalog,
        MssqlR1RegisteredTargetColumnRefV1: MssqlR1RegisteredTargetColumnRefV1.from_catalog(catalog, 1),
        MssqlR1RotationStableTargetAuthorityV1: predecessor_admission.stable_target,
        MssqlR1ActiveRegistrationHeadObservationV1: predecessor,
        MssqlR1RegistrationAdmissionEvidenceV1: predecessor_admission,
        MssqlR1RegistrationRotationTransitionEvidenceV1: transition,
    }
    return {
        "values": values,
        "catalog": catalog,
        "payload": payload,
        "verification": verification,
        "physical": physical,
        "security": security,
        "predecessor": predecessor,
        "predecessor_admission": predecessor_admission,
        "successor_admission": successor_admission,
        "lock": lock,
        "policy": policy,
    }


def _values() -> dict[type, object]:
    return _context()["values"]  # type: ignore[return-value]


def _enum_owner_fields() -> dict[tuple[type, object], tuple[object, int]]:
    owners: dict[tuple[type, object], tuple[object, int]] = {}
    for shape in _all_source_shapes():
        decision = derive_type_decision(shape, maximum_input_bytes=1_048_576)
        values = (
            (PostgresMssqlSourceScalarFamilyV1, shape.family, shape, 0),
            (PostgresMssqlLengthKindV1, shape.length_kind, shape, 3),
            (MssqlR1TargetScalarFamilyV1, decision.target_shape.family, decision.target_shape, 0),
            (PostgresMssqlCodecV1, decision.codec, decision, 4),
            (PostgresMssqlNormalizationV1, decision.normalization, decision, 5),
            (PostgresMssqlEqualityPolicyV1, decision.equality_policy, decision, 8),
            (PostgresMssqlValueGuardV1, decision.value_admission.guard, decision.value_admission, 0),
        )
        for enum_type, member, owner, index in values:
            owners.setdefault((enum_type, member), (owner, index))
    return owners


ENUM_OWNER_CASES = tuple(
    (f"valid_distinct__enum_owner__{enum_type.__name__}__{name}", enum_type, name)
    for enum_type, names in ENUM_MEMBER_INVENTORY.items()
    if enum_type is not MssqlR1TypeTargetRecoveryClass
    for name in names
)
RECOVERY_CLASS_CASES = (
    ("must_reject__classification__permanent", "invalid_facet", MssqlR1TypeTargetRecoveryClass.PERMANENT_INPUT_ERROR),
    (
        "must_reject__classification__refresh",
        "active_head_stale",
        MssqlR1TypeTargetRecoveryClass.REFRESH_AUTHORITY_AND_RETRY,
    ),
    (
        "must_reject__classification__operator",
        "authority_splice",
        MssqlR1TypeTargetRecoveryClass.OPERATOR_INTERVENTION,
    ),
)


def _admit_with_catalog(catalog) -> None:
    context = _context()
    MssqlR1RegistrationAdmissionEvidenceV1.create(
        context["payload"],
        context["verification"],
        context["predecessor"],
        catalog,
        context["physical"],
        context["security"],
        context["lock"],
    )


def _validate_cross_splice(contract: type, field_name: str, candidate: object) -> None:
    context = _context()
    catalog = context["catalog"]
    admission = context["predecessor_admission"]
    if contract is PostgresMssqlTypeDecisionAuthorityV1:
        original = _values()[PostgresMssqlTypeDecisionAuthorityV1]
        spliced = replace(original, **{field_name: candidate})
        PostgresMssqlTypePolicyAuthorityV1.create((spliced,), (original.source_shape,))
    elif contract is PostgresMssqlTypePolicyAuthorityV1:
        _values()[PostgresMssqlSourceColumnRefV1].validate_against_policy(candidate)
    elif contract is PostgresMssqlSourceColumnRefV1:
        candidate.validate_against_policy(context["policy"])
    elif contract is MssqlR1RegisteredTargetColumnV1:
        _admit_with_catalog(replace(catalog, ordered_columns=(candidate,)))
    elif contract is MssqlR1RegisteredTargetCatalogV1:
        _admit_with_catalog(candidate)
    elif contract is MssqlR1RegisteredTargetColumnRefV1:
        candidate.validate_against_catalog(catalog)
    elif contract is MssqlR1RotationStableTargetAuthorityV1:
        replace(admission, stable_target=candidate).validate_against_authorities(
            context["physical"], context["security"], context["lock"]
        )
    elif contract is MssqlR1ActiveRegistrationHeadObservationV1:
        MssqlR1RegistrationAdmissionEvidenceV1.create(
            context["payload"],
            context["verification"],
            candidate,
            catalog,
            context["physical"],
            context["security"],
            context["lock"],
        )
    elif contract is MssqlR1RegistrationAdmissionEvidenceV1:
        candidate.validate_against_authorities(context["physical"], context["security"], context["lock"])
    else:
        predecessor = context["predecessor_admission"]
        successor = context["successor_admission"]
        if field_name == "predecessor_head":
            predecessor = successor
        else:
            successor = predecessor
        candidate.validate_against_authorities(
            predecessor,
            successor,
            context["physical"],
            context["security"],
            context["lock"],
        )


def _optional_arm_owners() -> dict[type, tuple[object, ...]]:
    decisions = tuple(derive_type_decision(shape, maximum_input_bytes=1_048_576) for shape in _all_source_shapes())
    catalog = _context()["catalog"]
    payload_column = replace(catalog.ordered_columns[0], ordinal=2, name="payload", nullable=True)
    secondary = MssqlR1RegisteredTargetIndexV1(
        2, "IX_orders_payload", "nonclustered", False, False, False, False, False, False, None, (2,), (False,), ()
    )
    non_key_catalog = replace(
        catalog,
        ordered_columns=(*catalog.ordered_columns, payload_column),
        ordered_secondary_indexes=(secondary,),
    )
    return {
        PostgresMssqlSourceScalarShapeV1: _all_source_shapes(),
        MssqlR1CanonicalTargetScalarShapeV1: tuple(decision.target_shape for decision in decisions),
        PostgresMssqlValueAdmissionAuthorityV1: tuple(decision.value_admission for decision in decisions),
        MssqlR1RegisteredTargetColumnRefV1: (
            MssqlR1RegisteredTargetColumnRefV1.from_catalog(catalog, 1),
            MssqlR1RegisteredTargetColumnRefV1.from_catalog(non_key_catalog, 2),
        ),
    }


def test_dataclass_field_inventory_is_exact_and_every_field_is_executable() -> None:
    assert len(FIELD_INVENTORY) == 15
    assert sum(map(len, FIELD_INVENTORY.values())) == 154
    assert len(DATACLASS_FIELD_MUST_REJECT_CASES) == 154
    for contract, expected_fields in FIELD_INVENTORY.items():
        assert tuple(field.name for field in fields(contract)) == expected_fields


@pytest.mark.parametrize(
    ("case_id", "contract", "field_name"),
    DATACLASS_FIELD_MUST_REJECT_CASES,
    ids=[case[0] for case in DATACLASS_FIELD_MUST_REJECT_CASES],
)
def test_each_dataclass_field_has_a_named_rejection_vector(case_id, contract, field_name) -> None:
    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1V3ContractError):
        replace(_values()[contract], **{field_name: object()})


def test_enum_member_inventory_is_exact_and_owner_bound() -> None:
    assert len(ENUM_MEMBER_INVENTORY) == 8
    assert sum(map(len, ENUM_MEMBER_INVENTORY.values())) == 76
    for enum_type, expected_names in ENUM_MEMBER_INVENTORY.items():
        assert tuple(member.name for member in enum_type) == expected_names
    assert set(_enum_owner_fields()) == {
        (enum_type, enum_type[name])
        for enum_type, names in ENUM_MEMBER_INVENTORY.items()
        if enum_type is not MssqlR1TypeTargetRecoveryClass
        for name in names
    }


@pytest.mark.parametrize(
    ("case_id", "enum_type", "member_name"),
    ENUM_OWNER_CASES,
    ids=[case[0] for case in ENUM_OWNER_CASES],
)
def test_each_enum_member_is_bound_to_owning_canonical_field(case_id, enum_type, member_name) -> None:
    member = enum_type[member_name]
    owner, index = _enum_owner_fields()[(enum_type, member)]
    alternate = next(candidate for candidate in enum_type if candidate is not member)
    alternate_owner, alternate_index = _enum_owner_fields()[(enum_type, alternate)]
    domain, field_count = CANONICAL_LAYOUT[type(owner)]
    values = list(decode_canonical_bytes(owner.canonical_bytes, domain, field_count=field_count))
    alternate_domain, alternate_count = CANONICAL_LAYOUT[type(alternate_owner)]
    alternate_values = decode_canonical_bytes(
        alternate_owner.canonical_bytes,
        alternate_domain,
        field_count=alternate_count,
    )

    assert case_id.startswith("valid_distinct__")
    assert type(owner).from_canonical_bytes(owner.canonical_bytes) == owner
    assert type(alternate_owner).from_canonical_bytes(alternate_owner.canonical_bytes) == alternate_owner
    assert values[index] == member.value
    assert alternate_values[alternate_index] == alternate.value
    assert hashlib.sha256(owner.canonical_bytes).digest() != hashlib.sha256(alternate_owner.canonical_bytes).digest()


@pytest.mark.parametrize(
    ("case_id", "reason", "recovery_class"),
    RECOVERY_CLASS_CASES,
    ids=[case[0] for case in RECOVERY_CLASS_CASES],
)
def test_each_recovery_enum_member_is_reached_by_real_classification(case_id, reason, recovery_class) -> None:
    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1TypeTargetAuthorityError) as raised:
        reject(reason)
    assert raised.value.recovery_class is recovery_class


def test_optional_arm_inventory_is_exact_and_classified() -> None:
    discovered = {
        (contract, field_name)
        for contract in FIELD_INVENTORY
        for field_name, annotation in get_type_hints(contract).items()
        if NoneType in get_args(annotation)
    }
    assert discovered == set(OPTIONAL_ARM_DISPOSITIONS)
    assert len(OPTIONAL_ARM_CASES) == 13


@pytest.mark.parametrize(
    ("case_id", "contract", "field_name", "disposition"),
    OPTIONAL_ARM_CASES,
    ids=[case[0] for case in OPTIONAL_ARM_CASES],
)
def test_each_optional_arm_has_a_valid_distinct_pair_or_rejection_assertion(
    case_id, contract, field_name, disposition
) -> None:
    assert case_id.startswith(f"{disposition}__")
    if disposition == "must_reject":
        replacement = None if field_name == "maximum_bytes" else b"x" * 32
        with pytest.raises(MssqlR1V3ContractError):
            replace(_values()[contract], **{field_name: replacement})
        return
    owners = _optional_arm_owners()[contract]
    none_owner = next(owner for owner in owners if getattr(owner, field_name) is None)
    value_owner = next(owner for owner in owners if getattr(owner, field_name) is not None)
    domain, field_count = CANONICAL_LAYOUT[contract]
    none_fields = decode_canonical_bytes(none_owner.canonical_bytes, domain, field_count=field_count)
    value_fields = decode_canonical_bytes(value_owner.canonical_bytes, domain, field_count=field_count)
    index = field_index(contract, field_name)
    assert contract.from_canonical_bytes(none_owner.canonical_bytes) == none_owner
    assert contract.from_canonical_bytes(value_owner.canonical_bytes) == value_owner
    assert none_fields[index] is None
    assert value_fields[index] is not None
    assert hashlib.sha256(none_owner.canonical_bytes).digest() != hashlib.sha256(value_owner.canonical_bytes).digest()


def test_cross_authority_inventory_is_complete_and_field_bound() -> None:
    assert len(CROSS_AUTHORITY_MUST_REJECT_CASES) == sum(map(len, CROSS_AUTHORITY_FIELDS.values()))
    assert len({case[0] for case in CROSS_AUTHORITY_MUST_REJECT_CASES}) == len(CROSS_AUTHORITY_MUST_REJECT_CASES)
    for case_id, contract, field_name in CROSS_AUTHORITY_MUST_REJECT_CASES:
        assert case_id.startswith("must_reject__")
        assert field_name in FIELD_INVENTORY[contract]


@pytest.mark.parametrize(
    ("case_id", "contract", "field_name"),
    CROSS_AUTHORITY_MUST_REJECT_CASES,
    ids=[case[0] for case in CROSS_AUTHORITY_MUST_REJECT_CASES],
)
def test_each_cross_authority_reference_executes_type_correct_splice(case_id, contract, field_name) -> None:
    candidate = foreign_candidate(contract, field_name, _context())
    candidate_contract = type(candidate) if contract is PostgresMssqlTypeDecisionAuthorityV1 else contract
    assert candidate_contract.from_canonical_bytes(candidate.canonical_bytes) == candidate
    if contract is PostgresMssqlTypeDecisionAuthorityV1:
        assert candidate != getattr(_values()[contract], field_name)
    if contract is PostgresMssqlSourceColumnRefV1:
        original = _values()[contract]
        assert getattr(candidate, field_name) != getattr(original, field_name)
        assert all(
            getattr(candidate, field.name) == getattr(original, field.name)
            for field in fields(contract)
            if field.name != field_name
        )
    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1V3ContractError):
        _validate_cross_splice(contract, field_name, candidate)
