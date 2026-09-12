from __future__ import annotations

from dataclasses import fields, replace
from functools import lru_cache
from uuid import UUID

from dpone.contracts.mssql_r1_v3_registered_target_catalog import (
    MssqlR1RegisteredTargetCatalogV1,
    MssqlR1RegisteredTargetColumnRefV1,
)
from dpone.contracts.mssql_r1_v3_registered_target_catalog_items import (
    MssqlR1ClosedTargetFeatureObservationV1,
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
    MssqlR1TypeTargetRecoveryClass,
    PostgresMssqlCodecV1,
    PostgresMssqlEqualityPolicyV1,
    PostgresMssqlLengthKindV1,
    PostgresMssqlNormalizationV1,
    PostgresMssqlSourceScalarFamilyV1,
    PostgresMssqlValueGuardV1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import (
    MssqlR1CanonicalTargetScalarShapeV1,
    PostgresMssqlSourceScalarShapeV1,
    PostgresMssqlValueAdmissionAuthorityV1,
)
from tests.postgres_mssql_r1_v3_type_target_test_support import source_shape

FIELD_INVENTORY = {
    PostgresMssqlSourceScalarShapeV1: tuple(
        "family source_type_oid source_typmod length_kind precision scale maximum_characters".split()
    ),
    MssqlR1CanonicalTargetScalarShapeV1: tuple(
        "family length_kind precision scale maximum_utf16_units maximum_bytes collation".split()
    ),
    PostgresMssqlValueAdmissionAuthorityV1: tuple("guard maximum_input_bytes maximum_utf16_units".split()),
    PostgresMssqlTypeDecisionAuthorityV1: tuple(
        "decision_id source_shape stage_shape target_shape codec normalization value_admission loss_policy "
        "equality_policy hash_policy".split()
    ),
    PostgresMssqlTypePolicyAuthorityV1: ("policy_version", "ordered_decisions"),
    PostgresMssqlSourceColumnRefV1: tuple("ordinal name nullable source_shape type_policy_digest".split()),
    MssqlR1RegisteredTargetColumnV1: tuple(
        "ordinal name system_type_schema system_type_name user_type_schema user_type_name scalar_shape max_length "
        "precision scale nullable identity computed sparse rowguidcol generated_always_type "
        "default_definition_digest computed_definition_digest".split()
    ),
    MssqlR1RegisteredTargetIndexV1: tuple(
        "ordinal name index_kind unique primary_key unique_constraint disabled hypothetical ignore_dup_key "
        "filter_definition_digest ordered_key_column_ordinals ordered_descending "
        "ordered_included_column_ordinals".split()
    ),
    MssqlR1ClosedTargetFeatureObservationV1: tuple(
        "temporal_type ledger_type memory_optimized durability_desc filetable graph_node graph_edge "
        "ordered_trigger_digests ordered_inbound_foreign_key_digests ordered_outbound_foreign_key_digests "
        "ordered_check_constraint_digests ordered_indexed_view_dependency_digests "
        "ordered_encrypted_column_ordinals".split()
    ),
    MssqlR1RegisteredTargetCatalogV1: tuple(
        "catalog_version target_binding_uuid target_object_uuid physical_generation_uuid target_object_profile "
        "database_name schema_name object_name object_id target_contract_revision database_collation "
        "ordered_columns primary_key ordered_secondary_indexes feature_observation".split()
    ),
    MssqlR1RegisteredTargetColumnRefV1: tuple(
        "ordinal name nullable scalar_shape target_catalog_digest primary_key_ordinal".split()
    ),
    MssqlR1RotationStableTargetAuthorityV1: tuple(
        "profile_id capability_tuple_digest resolved_profile_digest route_source_authority_sha256 "
        "target_object_profile catalog_projection_version revocation_revision target_binding_uuid "
        "target_object_uuid recovery_domain_uuid recovery_domain_epoch server_instance_identity_sha256 "
        "database_guid database_family_guid recovery_fork_guid database_name_digest schema_name_digest "
        "object_name_digest database_name schema_name object_name object_id physical_generation_uuid "
        "catalog_contract_digest target_contract_revision".split()
    ),
    MssqlR1ActiveRegistrationHeadObservationV1: tuple(
        "target_binding_uuid physical_coordinate_digest target_object_uuid active_registration_id "
        "active_registration_revision active_registration_payload_digest registration_verification_policy_digest "
        "registration_verification_receipt_digest revocation_revision registered_physical_authority_digest "
        "schema_contract_digest permission_contract_digest last_control_receipt_id last_control_receipt_digest "
        "projection_revision updated_at row_token observation_contract_digest schema_lock_binding_digest "
        "observed_at".split()
    ),
    MssqlR1RegistrationAdmissionEvidenceV1: tuple(
        "registration_payload registration_verification active_head observed_at stable_target target_catalog".split()
    ),
    MssqlR1RegistrationRotationTransitionEvidenceV1: tuple(
        "predecessor_head successor_head rotation_control_receipt_payload rotation_control_receipt_digest".split()
    ),
}

DATACLASS_FIELD_MUST_REJECT_CASES = tuple(
    (f"must_reject__wrong_type__{contract.__name__}__{field_name}", contract, field_name)
    for contract, field_names in FIELD_INVENTORY.items()
    for field_name in field_names
)

ENUM_MEMBER_INVENTORY = {
    PostgresMssqlSourceScalarFamilyV1: tuple(
        "BOOL INT2 INT4 INT8 NUMERIC FLOAT4 FLOAT8 UUID DATE TIME TIMESTAMP TIMESTAMPTZ TEXT VARCHAR BYTEA".split()
    ),
    MssqlR1TargetScalarFamilyV1: tuple(
        "BIT SMALLINT INT BIGINT DECIMAL REAL FLOAT_53 UNIQUEIDENTIFIER DATE TIME DATETIME2 DATETIMEOFFSET NVARCHAR VARBINARY".split()
    ),
    PostgresMssqlLengthKindV1: ("NOT_APPLICABLE", "BOUNDED", "MAXIMUM"),
    PostgresMssqlCodecV1: tuple(
        "BOOL_ASCII_V1 SIGNED_INTEGER_ASCII_V1 DECIMAL_FIXED_ASCII_V1 "
        "RYU_BINARY32_SHORTEST_ASCII_V1 RYU_BINARY64_SHORTEST_ASCII_V1 UUID_LOWER_ASCII_V1 "
        "ISO_DATE_ASCII_V1 ISO_TIME_ASCII_V1 ISO_TIMESTAMP_ASCII_V1 ISO_UTC_TIMESTAMP_ASCII_V1 "
        "UTF8_TO_UTF16LE_V1 RAW_BINARY_V1".split()
    ),
    PostgresMssqlNormalizationV1: tuple("IDENTITY CANONICAL_POSITIVE_ZERO UTC_INSTANT UNICODE_SCALAR_IDENTITY".split()),
    PostgresMssqlEqualityPolicyV1: tuple(
        "BOOLEAN_EXACT INTEGER_EXACT DECIMAL_EXACT IEEE_VALUE_AFTER_POSITIVE_ZERO UUID_OCTETS DATE_EXACT "
        "TIME_EXACT TIMESTAMP_EXACT UTC_INSTANT_EXACT UNICODE_CODEPOINT_EXACT BINARY_OCTETS".split()
    ),
    PostgresMssqlValueGuardV1: tuple(
        "BOOLEAN_DOMAIN INT2_RANGE INT4_RANGE INT8_RANGE DECIMAL_SHAPE_AND_VALUE FINITE_FLOAT4 FINITE_FLOAT8 "
        "UUID_DOMAIN SQLSERVER_DATE_RANGE SQLSERVER_TIME_PRECISION SQLSERVER_DATETIME2_RANGE_PRECISION "
        "SQLSERVER_DATETIMEOFFSET_RANGE_PRECISION UTF8_AND_UTF16_CAPACITY BINARY_CAPACITY".split()
    ),
    MssqlR1TypeTargetRecoveryClass: tuple(
        "PERMANENT_INPUT_ERROR REFRESH_AUTHORITY_AND_RETRY OPERATOR_INTERVENTION".split()
    ),
}

OPTIONAL_ARM_DISPOSITIONS = {
    (PostgresMssqlSourceScalarShapeV1, "precision"): "valid_distinct",
    (PostgresMssqlSourceScalarShapeV1, "scale"): "valid_distinct",
    (PostgresMssqlSourceScalarShapeV1, "maximum_characters"): "valid_distinct",
    (MssqlR1CanonicalTargetScalarShapeV1, "precision"): "valid_distinct",
    (MssqlR1CanonicalTargetScalarShapeV1, "scale"): "valid_distinct",
    (MssqlR1CanonicalTargetScalarShapeV1, "maximum_utf16_units"): "valid_distinct",
    (MssqlR1CanonicalTargetScalarShapeV1, "maximum_bytes"): "must_reject",
    (MssqlR1CanonicalTargetScalarShapeV1, "collation"): "valid_distinct",
    (PostgresMssqlValueAdmissionAuthorityV1, "maximum_utf16_units"): "valid_distinct",
    (MssqlR1RegisteredTargetColumnV1, "default_definition_digest"): "must_reject",
    (MssqlR1RegisteredTargetColumnV1, "computed_definition_digest"): "must_reject",
    (MssqlR1RegisteredTargetIndexV1, "filter_definition_digest"): "must_reject",
    (MssqlR1RegisteredTargetColumnRefV1, "primary_key_ordinal"): "valid_distinct",
}

OPTIONAL_ARM_CASES = tuple(
    (f"{disposition}__optional_arm__{contract.__name__}__{field_name}", contract, field_name, disposition)
    for (contract, field_name), disposition in OPTIONAL_ARM_DISPOSITIONS.items()
)

CROSS_AUTHORITY_FIELDS = {
    PostgresMssqlTypeDecisionAuthorityV1: ("source_shape", "stage_shape", "target_shape", "value_admission"),
    PostgresMssqlTypePolicyAuthorityV1: ("ordered_decisions",),
    PostgresMssqlSourceColumnRefV1: ("source_shape", "type_policy_digest"),
    MssqlR1RegisteredTargetColumnV1: ("scalar_shape",),
    MssqlR1RegisteredTargetCatalogV1: (
        "target_binding_uuid",
        "target_object_uuid",
        "physical_generation_uuid",
        "ordered_columns",
        "primary_key",
        "ordered_secondary_indexes",
    ),
    MssqlR1RegisteredTargetColumnRefV1: ("scalar_shape", "target_catalog_digest"),
    MssqlR1RotationStableTargetAuthorityV1: tuple(
        "capability_tuple_digest resolved_profile_digest route_source_authority_sha256 target_binding_uuid "
        "target_object_uuid recovery_domain_uuid server_instance_identity_sha256 database_guid database_family_guid "
        "recovery_fork_guid database_name_digest schema_name_digest object_name_digest physical_generation_uuid "
        "catalog_contract_digest".split()
    ),
    MssqlR1ActiveRegistrationHeadObservationV1: tuple(
        "target_binding_uuid physical_coordinate_digest target_object_uuid active_registration_id "
        "active_registration_payload_digest registration_verification_policy_digest "
        "registration_verification_receipt_digest registered_physical_authority_digest schema_contract_digest "
        "permission_contract_digest observation_contract_digest schema_lock_binding_digest".split()
    ),
    MssqlR1RegistrationAdmissionEvidenceV1: (
        "registration_payload",
        "registration_verification",
        "active_head",
        "stable_target",
        "target_catalog",
    ),
    MssqlR1RegistrationRotationTransitionEvidenceV1: (
        "predecessor_head",
        "successor_head",
    ),
}

CROSS_AUTHORITY_MUST_REJECT_CASES = tuple(
    (f"must_reject__cross_authority__{contract.__name__}__{field_name}", contract, field_name)
    for contract, field_names in CROSS_AUTHORITY_FIELDS.items()
    for field_name in field_names
)

CANONICAL_LAYOUT = {
    PostgresMssqlSourceScalarShapeV1: (b"dpone-postgres-mssql-source-scalar-shape-v1\0", 7),
    MssqlR1CanonicalTargetScalarShapeV1: (b"dpone-mssql-r1-target-scalar-shape-v1\0", 7),
    PostgresMssqlValueAdmissionAuthorityV1: (b"dpone-postgres-mssql-value-admission-authority-v1\0", 3),
    PostgresMssqlTypeDecisionAuthorityV1: (b"dpone-postgres-mssql-type-decision-authority-v1\0", 10),
    PostgresMssqlTypePolicyAuthorityV1: (b"dpone-postgres-mssql-type-policy-authority-v1\0", 2),
    PostgresMssqlSourceColumnRefV1: (b"dpone-postgres-mssql-source-column-ref-v1\0", 5),
    MssqlR1RegisteredTargetColumnV1: (b"dpone-mssql-r1-registered-target-column-v1\0", 18),
    MssqlR1RegisteredTargetCatalogV1: (b"dpone-mssql-r1-registered-target-catalog-v1\0", 15),
    MssqlR1RegisteredTargetColumnRefV1: (b"dpone-mssql-r1-registered-target-column-ref-v1\0", 6),
    MssqlR1RotationStableTargetAuthorityV1: (b"dpone-mssql-r1-rotation-stable-target-authority-v1\0", 25),
    MssqlR1ActiveRegistrationHeadObservationV1: (
        b"dpone-mssql-r1-active-registration-head-observation-v1\0",
        20,
    ),
    MssqlR1RegistrationAdmissionEvidenceV1: (b"dpone-mssql-r1-registration-admission-evidence-v1\0", 6),
    MssqlR1RegistrationRotationTransitionEvidenceV1: (
        b"dpone-mssql-r1-registration-rotation-transition-evidence-v1\0",
        4,
    ),
}


def field_index(contract: type, field_name: str) -> int:
    return tuple(field.name for field in fields(contract)).index(field_name)


def _type_correct_foreign(value: object) -> object:
    if type(value) is bytes:
        return bytes((value[0] ^ 1,)) + value[1:]
    if type(value) is UUID:
        return UUID(int=(value.int + 1) % (2**128))
    raise AssertionError(f"cross-authority value has no type-correct mutator: {type(value)!r}")


@lru_cache
def _foreign_type_authorities():
    source = source_shape(PostgresMssqlSourceScalarFamilyV1.INT8, 20)
    decision = derive_type_decision(source, maximum_input_bytes=64)
    policy = PostgresMssqlTypePolicyAuthorityV1.create((decision,), (source,))
    reference = policy.source_column_ref(ordinal=1, name="order_id", nullable=False, source_shape=source)
    return decision, policy, reference


def _foreign_column(values: dict[type, object]) -> MssqlR1RegisteredTargetColumnV1:
    target = _foreign_type_authorities()[0].target_shape
    return replace(
        values[MssqlR1RegisteredTargetColumnV1],
        system_type_name="bigint",
        user_type_name="bigint",
        scalar_shape=target,
        max_length=8,
        precision=19,
    )


def _foreign_catalog(
    field_name: str,
    catalog: MssqlR1RegisteredTargetCatalogV1,
) -> MssqlR1RegisteredTargetCatalogV1:
    if field_name in {"target_binding_uuid", "target_object_uuid", "physical_generation_uuid"}:
        return replace(catalog, **{field_name: _type_correct_foreign(getattr(catalog, field_name))})
    if field_name == "ordered_columns":
        return replace(catalog, ordered_columns=(replace(catalog.ordered_columns[0], name="foreign_key"),))
    if field_name == "primary_key":
        return replace(catalog, primary_key=replace(catalog.primary_key, name="PK_orders_foreign"))
    secondary = MssqlR1RegisteredTargetIndexV1(
        2, "IX_orders_foreign", "nonclustered", False, False, False, False, False, False, None, (1,), (False,), ()
    )
    return replace(catalog, ordered_secondary_indexes=(secondary,))


def foreign_candidate(contract: type, field_name: str, context: dict[str, object]):
    values = context["values"]
    catalog = context["catalog"]
    if contract is PostgresMssqlTypeDecisionAuthorityV1:
        return getattr(_foreign_type_authorities()[0], field_name)
    if contract is PostgresMssqlTypePolicyAuthorityV1:
        return _foreign_type_authorities()[1]
    if contract is PostgresMssqlSourceColumnRefV1:
        original = values[PostgresMssqlSourceColumnRefV1]
        foreign_reference = _foreign_type_authorities()[2]
        return PostgresMssqlSourceColumnRefV1._from_policy(
            original.ordinal,
            original.name,
            original.nullable,
            foreign_reference.source_shape if field_name == "source_shape" else original.source_shape,
            foreign_reference.type_policy_digest if field_name == "type_policy_digest" else original.type_policy_digest,
        )
    if contract is MssqlR1RegisteredTargetColumnV1:
        return _foreign_column(values)
    if contract is MssqlR1RegisteredTargetCatalogV1:
        return _foreign_catalog(field_name, catalog)
    if contract is MssqlR1RegisteredTargetColumnRefV1:
        foreign_catalog = (
            replace(catalog, ordered_columns=(_foreign_column(values),))
            if field_name == "scalar_shape"
            else replace(catalog, target_contract_revision=2)
        )
        return MssqlR1RegisteredTargetColumnRefV1.from_catalog(foreign_catalog, 1)
    if contract in {MssqlR1RotationStableTargetAuthorityV1, MssqlR1ActiveRegistrationHeadObservationV1}:
        owner = values[contract]
        return replace(owner, **{field_name: _type_correct_foreign(getattr(owner, field_name))})
    if contract is MssqlR1RegistrationAdmissionEvidenceV1:
        admission = context["predecessor_admission"]
        if field_name == "registration_payload":
            replacement = context["successor_admission"].registration_payload
        elif field_name == "registration_verification":
            replacement = context["successor_admission"].registration_verification
        elif field_name == "active_head":
            replacement = context["successor_admission"].active_head
        elif field_name == "stable_target":
            stable = admission.stable_target
            replacement = replace(stable, capability_tuple_digest=_type_correct_foreign(stable.capability_tuple_digest))
        else:
            replacement = replace(catalog, target_contract_revision=2)
        return replace(admission, **{field_name: replacement})
    return values[MssqlR1RegistrationRotationTransitionEvidenceV1]
