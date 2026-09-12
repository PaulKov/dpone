from __future__ import annotations

import hashlib
import inspect
from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError, canonical_identifier_digest
from dpone.contracts.mssql_r1_v3_registration import (
    MssqlTargetRegistrationPayloadV1,
    MssqlTargetRegistrationVerificationV1,
    RegistrationActionV1,
)
from dpone.contracts.mssql_r1_v3_schema_attestation import (
    MSSQL_R1_SCHEMA_CONTRACT_VERSION,
    MssqlR1SchemaAttestationV3,
    MssqlR1SchemaContractV3,
)
from dpone.contracts.mssql_r1_v3_schema_inventory import (
    MssqlR1ConstraintKindV3,
    MssqlR1ExtendedPropertyV3,
    MssqlR1FixedResultV3,
    MssqlR1IndexDirectionV3,
    MssqlR1ModuleOptionsV3,
    MssqlR1NoResultV3,
    MssqlR1ParameterDirectionV3,
    MssqlR1PortableSchemaObjectV3,
    MssqlR1PortableTriggerV3,
    MssqlR1ReferencedObjectV3,
    MssqlR1ResultCardinalityV3,
    MssqlR1ResultColumnV3,
    MssqlR1SchemaColumnV3,
    MssqlR1SchemaConstraintV3,
    MssqlR1SchemaIndexKeyV3,
    MssqlR1SchemaIndexV3,
    MssqlR1SchemaObjectKindV3,
    MssqlR1SchemaProcedureParameterV3,
    MssqlR1SignerProfileKindV3,
    MssqlR1StageScanTemplateV3,
    MssqlR1SupportedCodecEntryV3,
    module_definition_digest,
    require_schema_identifier,
)
from dpone.contracts.mssql_r1_v3_schema_observation import (
    MssqlR1ModuleSignatureObservationV3,
    MssqlR1ObservedPermissionV3,
    MssqlR1ObservedPrincipalAuthorityKindV3,
    MssqlR1ObservedPrincipalV3,
    MssqlR1ObservedRoleMembershipV3,
    MssqlR1ObservedSchemaObjectV3,
    MssqlR1ObservedSchemaV3,
    MssqlR1ObservedTriggerIdentityV3,
    raw_crypt_property_digest,
    raw_thumbprint_digest,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import canonical_bytes as schema_canonical_bytes
from dpone.contracts.mssql_r1_v3_schema_security import (
    MssqlR1AuthenticationTypeV3,
    MssqlR1PermissionEffectV3,
    MssqlR1PermissionRuleV3,
    MssqlR1PermissionScopeV3,
    MssqlR1PermissionSourceV3,
    MssqlR1PrincipalAuthoritySetV3,
    MssqlR1PrincipalBindingV3,
    MssqlR1PrincipalTypeV3,
    MssqlR1SignerProfileV3,
    MssqlR1SubjectRoleV3,
    raw_sid_digest,
)
from tests.schema1_wire_fixture_private import load_fixture

FIXTURES = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def _digest(seed: int) -> bytes:
    return hashlib.sha256(str(seed).encode()).digest()


def _sorted(*items):
    return tuple(sorted(items, key=lambda item: item.canonical_bytes))


def _registration(*, binding: UUID = UUID(int=101), server: bytes = _digest(1)) -> MssqlTargetRegistrationPayloadV1:
    return MssqlTargetRegistrationPayloadV1(
        registration_id=UUID(int=102),
        registration_action=RegistrationActionV1.INITIAL,
        predecessor_registration_id=None,
        expected_active_registration_revision=None,
        issued_at=NOW,
        expires_at=NOW + timedelta(days=1),
        nonce=b"n" * 16,
        profile_id="postgres-mssql-r1",
        capability_tuple_digest=_digest(2),
        resolved_profile_digest=_digest(3),
        route_source_authority_sha256=_digest(4),
        target_object_profile="ordinary_disk_rowstore_v1",
        catalog_projection_version="dpone-mssql-target-catalog-v1",
        revocation_revision=0,
        target_binding_uuid=binding,
        target_object_uuid=UUID(int=103),
        recovery_domain_uuid=UUID(int=104),
        recovery_domain_epoch=1,
        server_instance_identity_sha256=server,
        database_guid=UUID(int=105),
        database_family_guid=UUID(int=106),
        recovery_fork_guid=UUID(int=107),
        database_name_digest=canonical_identifier_digest("warehouse"),
        schema_name_digest=canonical_identifier_digest("dbo"),
        object_name_digest=canonical_identifier_digest("orders"),
        database_name="warehouse",
        schema_name="dbo",
        object_name="orders",
        object_id=37,
        physical_generation_uuid=UUID(int=108),
        catalog_contract_digest=_digest(5),
        target_contract_revision=1,
    )


def _verification(
    registration: MssqlTargetRegistrationPayloadV1 | None = None,
) -> MssqlTargetRegistrationVerificationV1:
    payload = registration or _registration()
    return MssqlTargetRegistrationVerificationV1(
        registration_payload_digest=payload.payload_digest,
        signature_bundle_digest=_digest(6),
        signer_identity_digest=_digest(7),
        trusted_root_digest=_digest(8),
        cosign_policy_digest=_digest(9),
        verifier_version="test-verifier-v1",
        verified_at=NOW,
        certificate_identity_digest=_digest(10),
        certificate_issuer_digest=_digest(11),
        payload_bytes=payload.canonical_bytes,
    )


def _bindings() -> tuple[MssqlR1PrincipalBindingV3, ...]:
    return _sorted(
        MssqlR1PrincipalBindingV3(
            MssqlR1SubjectRoleV3.LOADER,
            "loader_user",
            _digest(20),
            MssqlR1PrincipalTypeV3.SQL_USER,
            MssqlR1AuthenticationTypeV3.DATABASE,
            None,
            (),
        ),
        MssqlR1PrincipalBindingV3(
            MssqlR1SubjectRoleV3.OBSERVER,
            "observer_user",
            _digest(21),
            MssqlR1PrincipalTypeV3.EXTERNAL_USER,
            MssqlR1AuthenticationTypeV3.EXTERNAL,
            None,
            (),
        ),
        MssqlR1PrincipalBindingV3(
            MssqlR1SubjectRoleV3.PROVISIONER,
            "provisioner_user",
            _digest(22),
            MssqlR1PrincipalTypeV3.WINDOWS_USER,
            MssqlR1AuthenticationTypeV3.WINDOWS,
            _digest(32),
            (),
        ),
        MssqlR1PrincipalBindingV3(
            MssqlR1SubjectRoleV3.RUNTIME,
            "runtime_user",
            _digest(23),
            MssqlR1PrincipalTypeV3.SQL_USER,
            MssqlR1AuthenticationTypeV3.INSTANCE,
            _digest(33),
            (),
        ),
    )


def _portable_contract() -> MssqlR1SchemaContractV3:
    unsigned = MssqlR1ModuleOptionsV3("caller", True, True, False, False, MssqlR1SignerProfileKindV3.NONE)
    attestor = replace(unsigned, signer_profile=MssqlR1SignerProfileKindV3.ATTESTOR)
    result = MssqlR1FixedResultV3(
        MssqlR1ResultCardinalityV3.ZERO_OR_ONE,
        (MssqlR1ResultColumnV3(1, "accepted", "bit", 1, 1, 0, False, None),),
    )
    procedure = MssqlR1PortableSchemaObjectV3(
        MssqlR1SchemaObjectKindV3.PROCEDURE,
        "dpone_authority",
        "consume_effect_v3",
        module_definition_digest("SELECT 1\n"),
        (),
        (),
        (),
        (MssqlR1SchemaProcedureParameterV3(1, "effect_key", "varbinary", 32, 0, 0, MssqlR1ParameterDirectionV3.INPUT),),
        result,
        attestor,
        (),
        (MssqlR1ExtendedPropertyV3("dpone_contract", _digest(40)),),
    )
    constraint = MssqlR1SchemaConstraintV3(
        "pk_receipt",
        MssqlR1ConstraintKindV3.PRIMARY_KEY,
        ("receipt_id",),
        None,
        _digest(41),
        True,
        True,
    )
    index = MssqlR1SchemaIndexV3(
        "ix_receipt",
        True,
        False,
        (MssqlR1SchemaIndexKeyV3("receipt_id", MssqlR1IndexDirectionV3.ASC),),
        (),
        None,
        True,
    )
    trigger = MssqlR1PortableTriggerV3(
        "dpone_authority",
        "protect_receipt",
        "dpone_authority",
        "effect_receipt_v3",
        "instead_of",
        ("update", "delete"),
        True,
        _digest(42),
        unsigned,
    )
    table = MssqlR1PortableSchemaObjectV3(
        MssqlR1SchemaObjectKindV3.TABLE,
        "dpone_authority",
        "effect_receipt_v3",
        None,
        (MssqlR1SchemaColumnV3(1, "receipt_id", "uniqueidentifier", 16, 0, 0, False, None, False, False),),
        (constraint,),
        (index,),
        (),
        None,
        None,
        (trigger,),
        (),
    )
    permission = MssqlR1PermissionRuleV3(
        MssqlR1SubjectRoleV3.RUNTIME,
        MssqlR1SubjectRoleV3.PROVISIONER,
        MssqlR1PermissionSourceV3.DIRECT,
        MssqlR1PermissionScopeV3.OBJECT,
        "dpone_authority",
        "consume_effect_v3",
        None,
        "EXECUTE",
        MssqlR1PermissionEffectV3.GRANT,
        False,
    )
    profiles = (
        MssqlR1SignerProfileV3(MssqlR1SignerProfileKindV3.ATTESTOR, "attestor_cert", "attestor_user"),
        MssqlR1SignerProfileV3(MssqlR1SignerProfileKindV3.STAGE_OWNER, "stage_owner_cert", "stage_owner_user"),
    )
    return MssqlR1SchemaContractV3(
        "dpone_authority",
        "dpone_stage",
        _digest(43),
        (procedure, table),
        (permission,),
        profiles,
        (MssqlR1SupportedCodecEntryV3("dpone.mssql-effect-request", "v3", _digest(44)),),
    )


def _principals() -> tuple[MssqlR1ObservedPrincipalV3, ...]:
    bindings = _bindings()
    observed = [
        MssqlR1ObservedPrincipalV3(
            MssqlR1ObservedPrincipalAuthorityKindV3.ENVIRONMENT,
            binding.subject_role,
            binding.database_principal_name,
            10 + index,
            binding.database_principal_sid_digest,
            binding.server_principal_sid_digest,
            binding.principal_type,
            binding.authentication_type,
            None,
        )
        for index, binding in enumerate(bindings)
    ]
    observed.extend(
        (
            MssqlR1ObservedPrincipalV3(
                MssqlR1ObservedPrincipalAuthorityKindV3.SIGNER,
                MssqlR1SubjectRoleV3.ATTESTATION_MODULE,
                "attestor_user",
                20,
                _digest(24),
                None,
                MssqlR1PrincipalTypeV3.CERTIFICATE,
                MssqlR1AuthenticationTypeV3.NONE,
                None,
            ),
            MssqlR1ObservedPrincipalV3(
                MssqlR1ObservedPrincipalAuthorityKindV3.SIGNER,
                MssqlR1SubjectRoleV3.STAGE_OWNER_MODULE,
                "stage_owner_user",
                21,
                _digest(25),
                None,
                MssqlR1PrincipalTypeV3.CERTIFICATE,
                MssqlR1AuthenticationTypeV3.NONE,
                None,
            ),
        )
    )
    return tuple(sorted(observed, key=lambda item: item.canonical_bytes))


def _attestation_parts():
    verification = _verification()
    authority = MssqlR1PrincipalAuthoritySetV3.create(verification, _bindings())
    contract = _portable_contract()
    principals = _principals()
    by_role = {item.subject_role: item for item in principals}
    provisioner = by_role[MssqlR1SubjectRoleV3.PROVISIONER]
    schemas = (
        MssqlR1ObservedSchemaV3(
            "dpone_authority", 100, provisioner.principal_id, provisioner.principal_id, provisioner.database_sid_digest
        ),
        MssqlR1ObservedSchemaV3(
            "dpone_stage", 101, provisioner.principal_id, provisioner.principal_id, provisioner.database_sid_digest
        ),
    )
    procedure, table = contract.ordered_objects
    signer = by_role[MssqlR1SubjectRoleV3.ATTESTATION_MODULE]
    signature = MssqlR1ModuleSignatureObservationV3(
        MssqlR1SignerProfileKindV3.ATTESTOR,
        200,
        "attestor_cert",
        300,
        _digest(50),
        signer.principal_id,
        signer.database_sid_digest,
        _digest(51),
        False,
    )
    objects = (
        MssqlR1ObservedSchemaObjectV3(
            procedure, 100, 200, None, provisioner.principal_id, provisioner.database_sid_digest, (), (signature,)
        ),
        MssqlR1ObservedSchemaObjectV3(
            table,
            100,
            201,
            None,
            provisioner.principal_id,
            provisioner.database_sid_digest,
            (MssqlR1ObservedTriggerIdentityV3(table.ordered_triggers[0].digest, 100, 202, 201),),
            (),
        ),
    )
    runtime = by_role[MssqlR1SubjectRoleV3.RUNTIME]
    permission = MssqlR1ObservedPermissionV3(
        MssqlR1PermissionSourceV3.DIRECT,
        None,
        runtime.principal_id,
        runtime.database_sid_digest,
        provisioner.principal_id,
        provisioner.database_sid_digest,
        MssqlR1PermissionScopeV3.OBJECT,
        "dpone_authority",
        "consume_effect_v3",
        None,
        "EXECUTE",
        MssqlR1PermissionEffectV3.GRANT,
        False,
    )
    return verification, authority, contract, schemas, objects, principals, (), (permission,)


def _attestation(**overrides) -> MssqlR1SchemaAttestationV3:
    verification, authority, contract, schemas, objects, principals, memberships, permissions = _attestation_parts()
    values = {
        "registration_verification": verification,
        "expected_contract": contract,
        "principal_authority_set": authority,
        "observed_server_instance_identity_sha256": _registration().server_instance_identity_sha256,
        "database_id": 7,
        "observed_database_guid": _registration().database_guid,
        "observed_database_family_guid": _registration().database_family_guid,
        "observed_recovery_fork_guid": _registration().recovery_fork_guid,
        "ordered_observed_schemas": schemas,
        "ordered_observed_objects": objects,
        "ordered_observed_principals": principals,
        "ordered_observed_role_memberships": memberships,
        "ordered_observed_permissions": permissions,
        "projection_revision": 1,
        "observed_at": NOW,
    }
    values.update(overrides)
    return MssqlR1SchemaAttestationV3.create(**values)


def test_all_portable_leaf_union_and_aggregate_types_round_trip() -> None:
    contract = _portable_contract()
    procedure, table = contract.ordered_objects
    models = [
        table.ordered_columns[0],
        table.ordered_constraints[0],
        MssqlR1ReferencedObjectV3("dbo", "parent", ("id",)),
        table.ordered_indexes[0],
        table.ordered_indexes[0].ordered_keys[0],
        procedure.ordered_parameters[0],
        procedure.result_contract.ordered_columns[0],  # type: ignore[union-attr]
        MssqlR1NoResultV3(),
        procedure.result_contract,
        MssqlR1StageScanTemplateV3(
            MssqlR1ResultCardinalityV3.ZERO_OR_MANY,
            _digest(60),
            (MssqlR1ResultColumnV3(1, "suffix", "binary", 32, 0, 0, False, None),),
        ),
        procedure.module_options,
        table.ordered_triggers[0],
        procedure.ordered_extended_properties[0],
        procedure,
        contract.ordered_supported_codecs[0],
        contract.ordered_permission_rules[0],
        contract.ordered_signer_profiles[0],
        contract,
    ]
    for model in models:
        assert type(model).from_canonical_bytes(model.canonical_bytes) == model
    assert MSSQL_R1_SCHEMA_CONTRACT_VERSION == "dpone-mssql-r1-v3-schema-2"
    assert "object_id" not in MssqlR1PortableSchemaObjectV3.__dataclass_fields__
    assert "principal_id" not in MssqlR1PermissionRuleV3.__dataclass_fields__


@pytest.mark.parametrize(
    "value", ["", " padded", "padded ", "e\u0301", "a\x00b", "a\x1fb", "a\x80b", "a" * 129, "😀" * 65]
)
def test_schema_2_identifier_rejects_noncanonical_or_overlong_text(value: str) -> None:
    with pytest.raises(MssqlR1V3ContractError, match="schema-2 identifier"):
        require_schema_identifier(value, "identifier")


def test_schema_2_identifier_accepts_exact_utf16_boundaries() -> None:
    assert require_schema_identifier("a" * 128, "identifier") == "a" * 128
    assert require_schema_identifier("😀" * 64, "identifier") == "😀" * 64


def test_module_and_raw_identity_digests_are_domain_separated_and_canonical() -> None:
    assert module_definition_digest("SELECT 1") == module_definition_digest("SELECT 1\r\n")
    assert module_definition_digest("SELECT 1\r") == module_definition_digest("SELECT 1\n")
    assert len({raw_sid_digest(b"x"), raw_thumbprint_digest(b"x"), raw_crypt_property_digest(b"x")}) == 3
    with pytest.raises(MssqlR1V3ContractError, match="final line feed"):
        module_definition_digest("SELECT 1\n\n")
    with pytest.raises(MssqlR1V3ContractError, match="NUL"):
        module_definition_digest("SELECT\0 1")


def test_stage_scan_template_instantiation_binds_plan_and_rejects_name_collisions() -> None:
    suffix = MssqlR1ResultColumnV3(1, "row_hash", "binary", 32, 0, 0, False, None)
    template = MssqlR1StageScanTemplateV3(MssqlR1ResultCardinalityV3.ZERO_OR_MANY, _digest(61), (suffix,))
    business = (MssqlR1ResultColumnV3(1, "id", "bigint", 8, 19, 0, False, None),)
    assert template.instantiated_digest(business, _digest(62)) == template.instantiated_digest(business, _digest(62))
    assert template.instantiated_digest(business, _digest(62)) != template.instantiated_digest(business, _digest(63))
    collision = (replace(business[0], name="row_hash"),)
    with pytest.raises(MssqlR1V3ContractError, match="names contain duplicates"):
        template.instantiated_digest(collision, _digest(62))


def test_portable_contract_rejects_order_kind_and_literal_drift() -> None:
    contract = _portable_contract()
    with pytest.raises(MssqlR1V3ContractError, match="coordinate order"):
        replace(contract, ordered_objects=tuple(reversed(contract.ordered_objects)))
    with pytest.raises(MssqlR1V3ContractError, match="procedure members"):
        replace(contract.ordered_objects[1], module_definition_digest=_digest(70))
    with pytest.raises(MssqlR1V3ContractError, match="contiguous"):
        replace(
            contract.ordered_objects[1],
            ordered_columns=(replace(contract.ordered_objects[1].ordered_columns[0], ordinal=2),),
        )
    with pytest.raises(MssqlR1V3ContractError, match="referenced object"):
        MssqlR1SchemaConstraintV3("fk", MssqlR1ConstraintKindV3.FOREIGN_KEY, ("id",), None, _digest(71), True, True)
    with pytest.raises(MssqlR1V3ContractError, match="uppercase ASCII"):
        replace(contract.ordered_permission_rules[0], permission="execute")
    with pytest.raises(MssqlR1V3ContractError, match="uppercase ASCII"):
        replace(contract.ordered_permission_rules[0], permission="EXECUTE_1")
    with pytest.raises(MssqlR1V3ContractError, match="nonempty"):
        replace(contract, ordered_supported_codecs=())
    with pytest.raises(MssqlR1V3ContractError, match="closed R1"):
        replace(contract.ordered_objects[0].module_options, execute_as="owner")


def test_principal_authority_is_derived_from_exact_verified_registration() -> None:
    verification = _verification()
    authority = MssqlR1PrincipalAuthoritySetV3.create(verification, _bindings())
    registration = _registration()
    assert authority.registration_payload_digest == verification.registration_payload_digest
    assert authority.resolved_profile_digest == registration.resolved_profile_digest
    assert MssqlR1PrincipalAuthoritySetV3.from_canonical_bytes(authority.canonical_bytes) == authority
    with pytest.raises(MssqlR1V3ContractError, match="pairwise distinct"):
        replace(
            authority,
            ordered_bindings=_sorted(
                _bindings()[0], replace(_bindings()[1], database_principal_name="loader_user"), *_bindings()[2:]
            ),
        )
    with pytest.raises(MssqlR1V3ContractError, match="server principal SID"):
        replace(_bindings()[-1], server_principal_sid_digest=None)


def test_attestation_has_exact_fields_and_is_deterministic_and_self_consistent() -> None:
    attestation = _attestation()
    assert tuple(item.name for item in fields(attestation)) == (
        "schema_contract_version",
        "target_binding_uuid",
        "registration_payload_digest",
        "registration_verification_receipt_digest",
        "registered_resolved_profile_digest",
        "registered_physical_authority_digest",
        "server_instance_identity_sha256",
        "database_id",
        "database_guid",
        "database_family_guid",
        "recovery_fork_guid",
        "expected_contract_bytes",
        "principal_authority_set_bytes",
        "ordered_observed_schemas",
        "ordered_observed_objects",
        "ordered_observed_principals",
        "ordered_observed_role_memberships",
        "ordered_observed_permissions",
        "expected_schema_contract_digest",
        "principal_authority_set_digest",
        "observed_schema_inventory_digest",
        "observed_security_inventory_digest",
        "live_identity_digest",
        "projection_revision",
        "observed_at",
        "attestation_digest",
    )
    assert _attestation().canonical_bytes == attestation.canonical_bytes
    assert MssqlR1SchemaAttestationV3.from_canonical_bytes(attestation.canonical_bytes) == attestation
    assert attestation.expected_schema_contract_digest == hashlib.sha256(attestation.expected_contract_bytes).digest()
    assert (
        attestation.principal_authority_set_digest == hashlib.sha256(attestation.principal_authority_set_bytes).digest()
    )
    with pytest.raises(MssqlR1V3ContractError, match="differs from exact canonical fields"):
        replace(attestation, live_identity_digest=_digest(90))


@pytest.mark.parametrize("suffix", [b"", b"x"])
def test_schema_2_decoders_reject_truncated_trailing_and_schema_1_bytes(suffix: bytes) -> None:
    contract = _portable_contract()
    payload = contract.canonical_bytes[:-1] if not suffix else contract.canonical_bytes + suffix
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1SchemaContractV3.from_canonical_bytes(payload)
    with pytest.raises(MssqlR1V3ContractError, match="unknown or legacy domain"):
        MssqlR1SchemaContractV3.from_canonical_bytes(
            load_fixture(FIXTURES / "postgres_mssql_r1_v3_schema_1_contract.json")
        )
    with pytest.raises(MssqlR1V3ContractError, match="unknown or legacy domain"):
        MssqlR1SchemaAttestationV3.from_canonical_bytes(
            load_fixture(FIXTURES / "postgres_mssql_r1_v3_schema_1_attestation.json")
        )


def test_attestation_rejects_owner_object_trigger_and_portable_projection_tamper() -> None:
    _, _, _, schemas, objects, principals, _, _ = _attestation_parts()
    with pytest.raises(MssqlR1V3ContractError, match="schema owner"):
        _attestation(ordered_observed_schemas=(replace(schemas[0], effective_owner_principal_id=99), schemas[1]))
    with pytest.raises(MssqlR1V3ContractError, match="object owner"):
        _attestation(ordered_observed_objects=(replace(objects[0], effective_owner_principal_id=99), objects[1]))
    drifted = replace(objects[0].portable_object, module_definition_digest=_digest(91))
    with pytest.raises(MssqlR1V3ContractError, match="reproduce portable"):
        _attestation(ordered_observed_objects=(replace(objects[0], portable_object=drifted), objects[1]))
    trigger = objects[1].ordered_trigger_identities[0]
    changed = _attestation(
        ordered_observed_objects=(
            objects[0],
            replace(objects[1], ordered_trigger_identities=(replace(trigger, object_id=203),)),
        )
    )
    assert changed.live_identity_digest != _attestation().live_identity_digest
    assert principals


def test_attestation_rejects_principal_permission_and_membership_substitution() -> None:
    _, _, contract, schemas, objects, principals, _, permissions = _attestation_parts()
    runtime_index = next(
        index for index, item in enumerate(principals) if item.subject_role is MssqlR1SubjectRoleV3.RUNTIME
    )
    changed = list(principals)
    changed[runtime_index] = replace(changed[runtime_index], principal_name="replacement_user")
    changed_principals = tuple(sorted(changed, key=lambda item: item.canonical_bytes))
    with pytest.raises(MssqlR1V3ContractError, match="authority binding"):
        _attestation(ordered_observed_principals=changed_principals)
    inherited = replace(permissions[0], source=MssqlR1PermissionSourceV3.PUBLIC)
    with pytest.raises(MssqlR1V3ContractError, match="inherited or public"):
        _attestation(ordered_observed_permissions=(inherited,))
    with pytest.raises(MssqlR1V3ContractError, match="permission principal"):
        _attestation(ordered_observed_permissions=(replace(permissions[0], grantor_principal_id=999),))
    with pytest.raises(MssqlR1V3ContractError, match="portable rules"):
        _attestation(ordered_observed_permissions=())
    provisioner = next(item for item in principals if item.subject_role is MssqlR1SubjectRoleV3.PROVISIONER)
    fake_nested = MssqlR1ObservedRoleMembershipV3(
        provisioner.principal_id,
        provisioner.database_sid_digest,
        provisioner.principal_id,
        provisioner.database_sid_digest,
        "nested",
    )
    with pytest.raises(MssqlR1V3ContractError, match="memberships differ"):
        _attestation(ordered_observed_role_memberships=(fake_nested,))
    assert contract and schemas and objects


def test_attestation_rejects_missing_wrong_or_extra_signature_and_private_key() -> None:
    _, _, _, _, objects, _, _, _ = _attestation_parts()
    signature = objects[0].ordered_signatures[0]
    with pytest.raises(MssqlR1V3ContractError, match="exactly one signature"):
        _attestation(ordered_observed_objects=(replace(objects[0], ordered_signatures=()), objects[1]))
    wrong = replace(signature, certificate_name="wrong_cert")
    with pytest.raises(MssqlR1V3ContractError, match="signer profile"):
        _attestation(ordered_observed_objects=(replace(objects[0], ordered_signatures=(wrong,)), objects[1]))
    with pytest.raises(MssqlR1V3ContractError, match="private key"):
        replace(signature, private_key_present=True)
    other = replace(signature, certificate_id=301)
    ordered = tuple(sorted((signature, other), key=lambda item: item.canonical_bytes))
    with pytest.raises(MssqlR1V3ContractError, match="exactly one signature"):
        _attestation(ordered_observed_objects=(replace(objects[0], ordered_signatures=ordered), objects[1]))


def test_create_and_decoded_admission_reject_foreign_registration_identity() -> None:
    registration = _registration()
    with pytest.raises(MssqlR1V3ContractError, match="schema_attestation_registration_mismatch"):
        _attestation(observed_database_guid=UUID(int=999))
    attestation = _attestation()
    foreign = _verification(replace(registration, target_binding_uuid=UUID(int=999)))
    with pytest.raises(MssqlR1V3ContractError, match="schema_attestation_registration_mismatch"):
        MssqlR1SchemaAttestationV3.from_canonical_bytes(attestation.canonical_bytes).assert_for_registration(foreign)


def test_observation_domains_and_port_signatures_are_schema_2_bound() -> None:
    from dpone.ports.mssql_r1_v3 import MssqlR1SchemaAuthorityV3Port

    attestation = _attestation()
    assert all(
        b"schema-2\0" in item.canonical_bytes.split(b"\0", 1)[0] + b"\0"
        for item in attestation.ordered_observed_schemas
    )
    assert tuple(inspect.signature(MssqlR1SchemaAuthorityV3Port.install_and_attest).parameters) == (
        "self",
        "transaction",
        "expected",
    )
    assert tuple(inspect.signature(MssqlR1SchemaAuthorityV3Port.attest_fresh).parameters) == ("self", "registration_id")


def test_all_observation_and_authority_aggregates_round_trip() -> None:
    _, authority, _, schemas, objects, principals, _, permissions = _attestation_parts()
    signature = objects[0].ordered_signatures[0]
    trigger = objects[1].ordered_trigger_identities[0]
    membership = MssqlR1ObservedRoleMembershipV3(11, _digest(120), 12, _digest(121), "loader_role")
    models = (*schemas, *objects, *principals, *permissions, signature, trigger, membership, authority)
    for model in models:
        assert type(model).from_canonical_bytes(model.canonical_bytes) == model


def test_contract_rejects_missing_or_aliased_required_members() -> None:
    contract = _portable_contract()
    with pytest.raises(MssqlR1V3ContractError, match="ordered objects must be nonempty"):
        replace(contract, ordered_objects=())
    with pytest.raises(MssqlR1V3ContractError, match="permission rules must be nonempty"):
        replace(contract, ordered_permission_rules=())
    with pytest.raises(MssqlR1V3ContractError, match="supported codecs must be nonempty"):
        replace(contract, ordered_supported_codecs=())
    with pytest.raises(MssqlR1V3ContractError, match="exactly attestor and stage_owner"):
        replace(contract, ordered_signer_profiles=contract.ordered_signer_profiles[:1])
    with pytest.raises(MssqlR1V3ContractError, match="one-to-one"):
        replace(
            contract,
            ordered_signer_profiles=(
                contract.ordered_signer_profiles[0],
                replace(contract.ordered_signer_profiles[1], certificate_name="attestor_cert"),
            ),
        )
    with pytest.raises(MssqlR1V3ContractError, match="outside the managed schemas"):
        replace(
            contract,
            ordered_objects=(replace(contract.ordered_objects[0], schema_name="dbo"), contract.ordered_objects[1]),
        )


def test_catalog_bounds_and_template_suffix_are_exact() -> None:
    contract = _portable_contract()
    column = contract.ordered_objects[1].ordered_columns[0]
    with pytest.raises(MssqlR1V3ContractError, match="maximum_length"):
        replace(column, maximum_length=-2)
    with pytest.raises(MssqlR1V3ContractError, match="precision"):
        replace(column, precision=2**31)
    with pytest.raises(MssqlR1V3ContractError, match="nonempty"):
        MssqlR1StageScanTemplateV3(MssqlR1ResultCardinalityV3.ZERO_OR_MANY, _digest(122), ())
    with pytest.raises(MssqlR1V3ContractError, match="schema_id"):
        MssqlR1ObservedTriggerIdentityV3(_digest(123), 0, 1, 1)


def test_frozen_schema_1_fixtures_are_static_and_hash_pinned() -> None:
    fixtures = {
        "postgres_mssql_r1_v3_schema_1_contract.json": "ae2b4facf7462d31cd9abf24e3c051516f68e978785ae405ee83d5d9e98dd3b2",
        "postgres_mssql_r1_v3_schema_1_attestation.json": "cff26e4ff4f0872e55a3a69a710b803ae62cce2cdde196dfc5666e93a1d75e06",
    }
    for name, expected in fixtures.items():
        assert hashlib.sha256(load_fixture(FIXTURES / name)).hexdigest() == expected
    assert not hasattr(MssqlR1SchemaContractV3, "from_schema_1_bytes")
    assert not hasattr(MssqlR1SchemaAttestationV3, "from_schema_1_bytes")


def test_principal_authority_rejects_missing_reordered_and_sid_aliases() -> None:
    verification = _verification()
    bindings = _bindings()
    with pytest.raises(MssqlR1V3ContractError, match="every environment role"):
        MssqlR1PrincipalAuthoritySetV3.create(verification, bindings[:-1])
    with pytest.raises(MssqlR1V3ContractError, match="canonical order"):
        MssqlR1PrincipalAuthoritySetV3.create(verification, tuple(reversed(bindings)))
    with pytest.raises(MssqlR1V3ContractError, match="pairwise distinct"):
        changed = replace(bindings[1], database_principal_sid_digest=bindings[0].database_principal_sid_digest)
        MssqlR1PrincipalAuthoritySetV3.create(verification, _sorted(bindings[0], changed, *bindings[2:]))


def test_permission_substitution_rejects_deny_extra_role_and_public_rows() -> None:
    _, _, contract, _, _, _, _, permissions = _attestation_parts()
    permission = permissions[0]
    with pytest.raises(MssqlR1V3ContractError, match="portable rules"):
        _attestation(ordered_observed_permissions=(replace(permission, effect=MssqlR1PermissionEffectV3.DENY),))
    extra = replace(permission, permission="CONTROL")
    with pytest.raises(MssqlR1V3ContractError, match="portable rules"):
        _attestation(ordered_observed_permissions=_sorted(permission, extra))
    with pytest.raises(MssqlR1V3ContractError, match="inherited or public"):
        _attestation(
            ordered_observed_permissions=(
                replace(permission, source=MssqlR1PermissionSourceV3.DATABASE_ROLE, source_role_name="operator"),
            )
        )
    with pytest.raises(MssqlR1V3ContractError, match="inherited or public"):
        _attestation(ordered_observed_permissions=(replace(permission, source=MssqlR1PermissionSourceV3.PUBLIC),))
    assert contract.ordered_permission_rules[0].effect is MssqlR1PermissionEffectV3.GRANT


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("target_binding_uuid", UUID(int=900)),
        ("target_object_uuid", UUID(int=901)),
        ("server_instance_identity_sha256", _digest(902)),
        ("database_guid", UUID(int=903)),
        ("database_family_guid", UUID(int=904)),
        ("recovery_fork_guid", UUID(int=905)),
    ),
)
def test_decoded_attestation_admission_rejects_each_registration_substitution(field: str, replacement: object) -> None:
    attestation = MssqlR1SchemaAttestationV3.from_canonical_bytes(_attestation().canonical_bytes)
    foreign_registration = replace(_registration(), **{field: replacement})
    with pytest.raises(MssqlR1V3ContractError, match="schema_attestation_registration_mismatch"):
        attestation.assert_for_registration(_verification(foreign_registration))


def test_factory_rejects_foreign_principal_authority_set() -> None:
    foreign_verification = _verification(replace(_registration(), target_binding_uuid=UUID(int=999)))
    foreign_authority = MssqlR1PrincipalAuthoritySetV3.create(foreign_verification, _bindings())
    with pytest.raises(MssqlR1V3ContractError, match="schema_attestation_registration_mismatch"):
        _attestation(principal_authority_set=foreign_authority)


def test_schema_and_object_numeric_ids_have_separate_namespaces() -> None:
    _, _, _, _, objects, _, _, _ = _attestation_parts()
    signature = replace(objects[0].ordered_signatures[0], module_object_id=100)
    changed_object = replace(objects[0], object_id=100, ordered_signatures=(signature,))
    assert (
        _attestation(ordered_observed_objects=(changed_object, objects[1])).ordered_observed_objects[0].object_id == 100
    )


def test_coherent_certificate_replacement_changes_live_identity_without_self_adoption() -> None:
    _, _, _, _, objects, principals, _, _ = _attestation_parts()
    signer_index = next(
        index for index, item in enumerate(principals) if item.subject_role is MssqlR1SubjectRoleV3.ATTESTATION_MODULE
    )
    changed_principals = list(principals)
    changed_signer = replace(changed_principals[signer_index], principal_id=220, database_sid_digest=_digest(906))
    changed_principals[signer_index] = changed_signer
    ordered_principals = tuple(sorted(changed_principals, key=lambda item: item.canonical_bytes))
    changed_signature = replace(
        objects[0].ordered_signatures[0],
        certificate_id=330,
        certificate_thumbprint_digest=_digest(907),
        certificate_user_principal_id=changed_signer.principal_id,
        certificate_user_sid_digest=changed_signer.database_sid_digest,
        crypt_property_digest=_digest(908),
    )
    replacement = _attestation(
        ordered_observed_principals=ordered_principals,
        ordered_observed_objects=(replace(objects[0], ordered_signatures=(changed_signature,)), objects[1]),
    )
    assert replacement.live_identity_digest != _attestation().live_identity_digest


def test_multi_permission_projection_is_keyed_by_portable_rules_not_live_ids() -> None:
    _, _, contract, _, _, principals, _, permissions = _attestation_parts()
    by_role = {item.subject_role: item for item in principals}
    runtime = replace(by_role[MssqlR1SubjectRoleV3.RUNTIME], principal_id=8)
    loader = replace(by_role[MssqlR1SubjectRoleV3.LOADER], principal_id=90)
    changed_principals = tuple(
        sorted(
            (
                runtime,
                loader,
                *(item for item in principals if item.subject_role not in {runtime.subject_role, loader.subject_role}),
            ),
            key=lambda item: item.canonical_bytes,
        )
    )
    base_rule = contract.ordered_permission_rules[0]
    loader_rule = replace(base_rule, subject_role=MssqlR1SubjectRoleV3.LOADER, permission="SELECT")
    changed_contract = replace(contract, ordered_permission_rules=_sorted(base_rule, loader_rule))
    runtime_permission = replace(permissions[0], grantee_principal_id=runtime.principal_id)
    loader_permission = replace(
        permissions[0],
        grantee_principal_id=loader.principal_id,
        grantee_sid_digest=loader.database_sid_digest,
        permission="SELECT",
    )
    observed_permissions = _sorted(runtime_permission, loader_permission)
    assert observed_permissions[0].grantee_principal_id == runtime.principal_id
    assert changed_contract.ordered_permission_rules[0].subject_role is MssqlR1SubjectRoleV3.LOADER
    assert (
        _attestation(
            expected_contract=changed_contract,
            ordered_observed_principals=changed_principals,
            ordered_observed_permissions=observed_permissions,
        ).expected_contract
        == changed_contract
    )


def test_attestation_rejects_unexpected_public_principal_inventory() -> None:
    _, _, _, _, _, principals, _, _ = _attestation_parts()
    public = MssqlR1ObservedPrincipalV3(
        MssqlR1ObservedPrincipalAuthorityKindV3.PUBLIC,
        None,
        "public",
        31,
        _digest(920),
        None,
        MssqlR1PrincipalTypeV3.DATABASE_ROLE,
        MssqlR1AuthenticationTypeV3.NONE,
        None,
    )
    with pytest.raises(MssqlR1V3ContractError, match="principal inventory"):
        _attestation(ordered_observed_principals=_sorted(*principals, public))


def test_permission_and_membership_identity_swaps_are_rejected() -> None:
    verification, _, _, _, _, principals, _, permissions = _attestation_parts()
    by_role = {item.subject_role: item for item in principals}
    runtime = by_role[MssqlR1SubjectRoleV3.RUNTIME]
    provisioner = by_role[MssqlR1SubjectRoleV3.PROVISIONER]
    swapped_permission = replace(
        permissions[0],
        grantee_principal_id=provisioner.principal_id,
        grantee_sid_digest=provisioner.database_sid_digest,
        grantor_principal_id=runtime.principal_id,
        grantor_sid_digest=runtime.database_sid_digest,
    )
    with pytest.raises(MssqlR1V3ContractError, match="permission principal"):
        _attestation(ordered_observed_permissions=(swapped_permission,))

    bindings = list(_bindings())
    loader_index = next(
        index for index, item in enumerate(bindings) if item.subject_role is MssqlR1SubjectRoleV3.LOADER
    )
    bindings[loader_index] = replace(bindings[loader_index], ordered_allowed_database_roles=("loader_role",))
    authority = MssqlR1PrincipalAuthoritySetV3.create(verification, _sorted(*bindings))
    loader = by_role[MssqlR1SubjectRoleV3.LOADER]
    role = MssqlR1ObservedPrincipalV3(
        MssqlR1ObservedPrincipalAuthorityKindV3.DATABASE_ROLE,
        None,
        "loader_role",
        31,
        _digest(921),
        None,
        MssqlR1PrincipalTypeV3.DATABASE_ROLE,
        MssqlR1AuthenticationTypeV3.NONE,
        None,
    )
    membership = MssqlR1ObservedRoleMembershipV3(
        loader.principal_id,
        loader.database_sid_digest,
        role.principal_id,
        role.database_sid_digest,
        role.principal_name,
    )
    role_principals = _sorted(*principals, role)
    assert _attestation(
        principal_authority_set=authority,
        ordered_observed_principals=role_principals,
        ordered_observed_role_memberships=(membership,),
    )
    swapped_membership = replace(
        membership,
        member_principal_id=role.principal_id,
        member_sid_digest=role.database_sid_digest,
        role_principal_id=loader.principal_id,
        role_sid_digest=loader.database_sid_digest,
    )
    with pytest.raises(MssqlR1V3ContractError, match="memberships differ"):
        _attestation(
            principal_authority_set=authority,
            ordered_observed_principals=role_principals,
            ordered_observed_role_memberships=(swapped_membership,),
        )


def test_principal_id_sid_and_owner_substitutions_are_rejected() -> None:
    _, _, _, schemas, objects, principals, _, permissions = _attestation_parts()
    runtime_index = next(
        index for index, item in enumerate(principals) if item.subject_role is MssqlR1SubjectRoleV3.RUNTIME
    )
    same_name_new_id = list(principals)
    same_name_new_id[runtime_index] = replace(same_name_new_id[runtime_index], principal_id=99)
    with pytest.raises(MssqlR1V3ContractError, match="permission principal"):
        _attestation(ordered_observed_principals=_sorted(*same_name_new_id))
    same_id_new_sid = list(principals)
    changed_runtime = replace(same_id_new_sid[runtime_index], database_sid_digest=_digest(922))
    same_id_new_sid[runtime_index] = changed_runtime
    with pytest.raises(MssqlR1V3ContractError, match="authority binding"):
        _attestation(
            ordered_observed_principals=_sorted(*same_id_new_sid),
            ordered_observed_permissions=(
                replace(permissions[0], grantee_sid_digest=changed_runtime.database_sid_digest),
            ),
        )
    with pytest.raises(MssqlR1V3ContractError, match="explicit per-object owners"):
        replace(objects[0], declared_owner_principal_id=objects[0].effective_owner_principal_id)
    with pytest.raises(MssqlR1V3ContractError, match="schema owner"):
        _attestation(ordered_observed_schemas=(replace(schemas[0], declared_owner_principal_id=99), schemas[1]))


def test_wrong_and_inconsistent_signer_profiles_are_rejected() -> None:
    _, _, contract, _, objects, _, _, _ = _attestation_parts()
    signature = objects[0].ordered_signatures[0]
    with pytest.raises(MssqlR1V3ContractError, match="signer profile"):
        _attestation(
            ordered_observed_objects=(
                replace(
                    objects[0],
                    ordered_signatures=(replace(signature, signer_profile=MssqlR1SignerProfileKindV3.STAGE_OWNER),),
                ),
                objects[1],
            )
        )
    second_portable = replace(
        contract.ordered_objects[0],
        object_name="consume_other_v3",
        module_definition_digest=_digest(923),
    )
    changed_contract = replace(
        contract,
        ordered_objects=(contract.ordered_objects[0], second_portable, contract.ordered_objects[1]),
    )
    second_signature = replace(
        signature,
        module_object_id=203,
        certificate_id=301,
        certificate_thumbprint_digest=_digest(924),
    )
    second_observation = replace(
        objects[0],
        portable_object=second_portable,
        object_id=203,
        ordered_signatures=(second_signature,),
    )
    with pytest.raises(MssqlR1V3ContractError, match="certificate identity is inconsistent"):
        _attestation(
            expected_contract=changed_contract,
            ordered_observed_objects=(objects[0], second_observation, objects[1]),
        )


def test_retained_baseline_rejects_coherent_certificate_replacement() -> None:
    baseline = _attestation()
    _, _, _, _, objects, principals, _, _ = _attestation_parts()
    signer_index = next(
        index for index, item in enumerate(principals) if item.subject_role is MssqlR1SubjectRoleV3.ATTESTATION_MODULE
    )
    changed = list(principals)
    changed_signer = replace(changed[signer_index], principal_id=220, database_sid_digest=_digest(925))
    changed[signer_index] = changed_signer
    signature = replace(
        objects[0].ordered_signatures[0],
        certificate_id=330,
        certificate_thumbprint_digest=_digest(926),
        certificate_user_principal_id=changed_signer.principal_id,
        certificate_user_sid_digest=changed_signer.database_sid_digest,
        crypt_property_digest=_digest(927),
    )
    fresh = _attestation(
        ordered_observed_principals=_sorted(*changed),
        ordered_observed_objects=(replace(objects[0], ordered_signatures=(signature,)), objects[1]),
    )

    def assert_retained_live_identity(candidate: MssqlR1SchemaAttestationV3) -> None:
        if candidate.live_identity_digest != baseline.live_identity_digest:
            raise MssqlR1V3ContractError("schema live identity differs from retained baseline")

    with pytest.raises(MssqlR1V3ContractError, match="retained baseline"):
        assert_retained_live_identity(fresh)


def test_two_trigger_projection_is_matched_by_digest_not_name_position() -> None:
    _, _, contract, _, objects, _, _, _ = _attestation_parts()
    first = contract.ordered_objects[1].ordered_triggers[0]
    second = replace(first, trigger_name="z_protect_receipt", definition_digest=_digest(1001))
    assert first.canonical_bytes < second.canonical_bytes
    assert second.digest < first.digest
    table = replace(contract.ordered_objects[1], ordered_triggers=(first, second))
    changed_contract = replace(contract, ordered_objects=(contract.ordered_objects[0], table))
    observed_triggers = tuple(
        sorted(
            (
                MssqlR1ObservedTriggerIdentityV3(first.digest, 100, 202, 201),
                MssqlR1ObservedTriggerIdentityV3(second.digest, 100, 204, 201),
            ),
            key=lambda item: (item.portable_trigger_digest, item.object_id),
        )
    )
    table_observation = replace(objects[1], portable_object=table, ordered_trigger_identities=observed_triggers)
    assert _attestation(
        expected_contract=changed_contract,
        ordered_observed_objects=(objects[0], table_observation),
    )


def test_raw_strings_are_rejected_for_every_security_and_observation_enum() -> None:
    _, _, contract, _, objects, principals, _, permissions = _attestation_parts()
    permission_rule = contract.ordered_permission_rules[0]
    signer_profile = contract.ordered_signer_profiles[0]
    binding = _bindings()[0]
    signature = objects[0].ordered_signatures[0]
    principal = principals[0]
    permission = permissions[0]
    cases = (
        (permission_rule, {"subject_role": permission_rule.subject_role.value}),
        (permission_rule, {"grantor_role": permission_rule.grantor_role.value}),
        (permission_rule, {"source": permission_rule.source.value}),
        (permission_rule, {"scope": permission_rule.scope.value}),
        (permission_rule, {"effect": permission_rule.effect.value}),
        (signer_profile, {"signer_profile": signer_profile.signer_profile.value}),
        (binding, {"subject_role": binding.subject_role.value}),
        (binding, {"principal_type": binding.principal_type.value}),
        (binding, {"authentication_type": binding.authentication_type.value}),
        (signature, {"signer_profile": signature.signer_profile.value}),
        (principal, {"authority_kind": principal.authority_kind.value}),
        (principal, {"subject_role": principal.subject_role.value}),
        (principal, {"principal_type": principal.principal_type.value}),
        (principal, {"authentication_type": principal.authentication_type.value}),
        (permission, {"source": permission.source.value}),
        (permission, {"scope": permission.scope.value}),
        (permission, {"effect": permission.effect.value}),
    )
    for model, substitution in cases:
        with pytest.raises(MssqlR1V3ContractError):
            replace(model, **substitution)


@pytest.mark.parametrize(
    "field",
    (
        "ordered_observed_schemas",
        "ordered_observed_objects",
        "ordered_observed_principals",
        "ordered_observed_role_memberships",
        "ordered_observed_permissions",
    ),
)
def test_factory_rejects_wrong_nested_tuple_items_with_contract_error(field: str) -> None:
    with pytest.raises(MssqlR1V3ContractError, match="must be a typed tuple"):
        _attestation(**{field: ("not-a-contract",)})


@pytest.mark.parametrize(
    ("kind", "referenced_object"),
    (
        (MssqlR1ConstraintKindV3.CHECK, MssqlR1ReferencedObjectV3("dbo", "parent", ("id",))),
        (MssqlR1ConstraintKindV3.UNIQUE, MssqlR1ReferencedObjectV3("dbo", "parent", ("id",))),
        (MssqlR1ConstraintKindV3.PRIMARY_KEY, MssqlR1ReferencedObjectV3("dbo", "parent", ("id",))),
        (MssqlR1ConstraintKindV3.FOREIGN_KEY, None),
        (MssqlR1ConstraintKindV3.FOREIGN_KEY, object()),
    ),
)
def test_constraint_rejects_reference_shape_outside_exact_kind_union(
    kind: MssqlR1ConstraintKindV3,
    referenced_object: object,
) -> None:
    with pytest.raises(MssqlR1V3ContractError, match="referenced object is inconsistent"):
        MssqlR1SchemaConstraintV3(
            "constraint_name",
            kind,
            ("id",),
            referenced_object,  # type: ignore[arg-type]
            _digest(940),
            True,
            True,
        )


@pytest.mark.parametrize(
    ("field", "error"),
    (
        ("expected_contract", "expected contract must be MssqlR1SchemaContractV3"),
        ("principal_authority_set", "principal authority set must be MssqlR1PrincipalAuthoritySetV3"),
    ),
)
def test_attestation_factory_type_gates_root_contracts(field: str, error: str) -> None:
    with pytest.raises(MssqlR1V3ContractError, match=error):
        _attestation(**{field: object()})


@pytest.mark.parametrize(
    ("field", "integer"),
    (
        ("uses_ansi_nulls", 1),
        ("uses_quoted_identifier", 1),
        ("schema_bound", 0),
        ("native_compilation", 0),
    ),
)
def test_module_options_reject_integer_boolean_substitution(field: str, integer: int) -> None:
    options = _portable_contract().ordered_objects[0].module_options
    assert isinstance(options, MssqlR1ModuleOptionsV3)
    with pytest.raises(MssqlR1V3ContractError, match="closed R1 profile"):
        replace(options, **{field: integer})


@pytest.mark.parametrize("index", (1, 2, 3, 4))
def test_module_options_decoder_rejects_integer_boolean_substitution(index: int) -> None:
    values: list[object] = [
        "caller",
        True,
        True,
        False,
        False,
        MssqlR1SignerProfileKindV3.NONE,
    ]
    values[index] = int(values[index])
    payload = schema_canonical_bytes(b"dpone-r1-schema-module-options-v3-schema-2\0", tuple(values))
    with pytest.raises(MssqlR1V3ContractError, match="closed R1 profile"):
        MssqlR1ModuleOptionsV3.from_canonical_bytes(payload)


def test_direct_attestation_constructor_rejects_wrong_nested_tuple_items() -> None:
    with pytest.raises(MssqlR1V3ContractError, match="must be a typed tuple"):
        replace(_attestation(), ordered_observed_objects=("not-an-observation",))
