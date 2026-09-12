from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass, fields, replace
from enum import Enum
from typing import Any, Literal
from uuid import UUID

import pytest

from dpone.contracts.mssql_r1_v3_codec import MssqlR1V3ContractError, canonical_bytes
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ResourceKindV1,
)
from dpone.contracts.mssql_r1_v3_provider_security_certificate import (
    MssqlR1CertificateCatalogObservationV1,
    MssqlR1CertificateInstallIdentityRefV2,
    MssqlR1SecretSqlTemplateV1,
    MssqlR1SharedCertificateMetadataV1,
)
from dpone.contracts.mssql_r1_v3_provider_security_enums import (
    MSSQL_R1_SECURITY_ENUM_REGISTRY,
    MssqlR1BindingPermissionProfileV1,
    MssqlR1BindingSignerNamingProfileV1,
    MssqlR1CertificateCreationProfileV1,
    MssqlR1CertificateLifecycleActionV1,
    MssqlR1CertificateLifecycleStateV1,
    MssqlR1CertificateSignatureAlgorithmV1,
    MssqlR1EnvironmentPrincipalProfileV1,
    MssqlR1ForbiddenPermissionEffectV1,
    MssqlR1PermissionTargetScopeV1,
    MssqlR1PrivateKeyDispositionV1,
    MssqlR1ReinstallPolicyV1,
    MssqlR1SecretGeneratorProfileV1,
    MssqlR1SecretLifetimePolicyV1,
    MssqlR1SecretLoggingPolicyV1,
    MssqlR1SecretPersistencePolicyV1,
    MssqlR1SecretTemplateKindV1,
    exact_lifecycle_transitions,
    lifecycle_transition_set_digest,
)
from dpone.contracts.mssql_r1_v3_provider_security_permissions import (
    MssqlR1BindingSignerLifecyclePolicyV2,
    MssqlR1PermissionClosurePolicyV1,
    MssqlR1PermissionObservationPolicyV2,
    MssqlR1ResolvedPermissionPathV2,
    project_schema_permission_rule,
)
from dpone.contracts.mssql_r1_v3_provider_security_principals import (
    SECRET_PLACEHOLDER,
    MssqlR1AnyDatabaseRoleRefV1,
    MssqlR1BindingSignerClassPrincipalRefV1,
    MssqlR1BindingSignerInstancePrincipalRefV1,
    MssqlR1DirectPermissionOriginV1,
    MssqlR1EnvironmentPrincipalRefV1,
    MssqlR1EphemeralSecretPolicyV1,
    MssqlR1ForbiddenRoleMembershipV1,
    MssqlR1NamedDatabaseRoleRefV1,
    MssqlR1PermissionTargetV1,
    MssqlR1PublicPermissionOriginV1,
    MssqlR1PublicPrincipalRefV1,
    MssqlR1RolePermissionOriginV1,
    MssqlR1SharedSignerPrincipalRefV1,
    MssqlR1SharedSignerProfileRefV1,
    decode_permission_origin,
    decode_security_principal,
)
from dpone.contracts.mssql_r1_v3_provider_security_profile import (
    MssqlR1CertificateReplayEvidenceV2,
    MssqlR1SharedInstallSecurityProfileV2,
    MssqlR1SharedSignerInstallAuthorityV1,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1SignerProfileKindV3
from dpone.contracts.mssql_r1_v3_schema_security import (
    MssqlR1AuthenticationTypeV3,
    MssqlR1PermissionEffectV3,
    MssqlR1PermissionScopeV3,
    MssqlR1PermissionSourceV3,
    MssqlR1PrincipalTypeV3,
    MssqlR1SubjectRoleV3,
)
from tests.test_postgres_mssql_r1_v3_physical_descriptor_contract import descriptor

DIGEST_A = hashlib.sha256(b"security-v2-a").digest()
DIGEST_B = hashlib.sha256(b"security-v2-b").digest()


def _descriptor():
    physical = descriptor()
    policy_digest = hashlib.sha256(MssqlR1PermissionObservationPolicyV2.exact().canonical_bytes).digest()
    schema = replace(physical.expected_schema_contract, permission_projection_policy_digest=policy_digest)
    return replace(physical, expected_schema_contract=schema)


def security_descriptor():
    """Build the exact physical descriptor bound by the security profile."""

    return _descriptor()


def _resource(name: str = "dpone_provider_install_receipt_v3") -> MssqlR1PhysicalResourceRefV1:
    return MssqlR1PhysicalResourceRefV1(MssqlR1ResourceKindV1.STATIC_OBJECT, "dpone_authority", name, None)


def _owner() -> MssqlR1EnvironmentPrincipalRefV1:
    return MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.PROVISIONER)


def _metadata(profile: MssqlR1SignerProfileKindV3) -> MssqlR1SharedCertificateMetadataV1:
    subject = "dpone R1 V3 attestor" if profile is MssqlR1SignerProfileKindV3.ATTESTOR else "dpone R1 V3 stage owner"
    return MssqlR1SharedCertificateMetadataV1(
        MssqlR1CertificateCreationProfileV1.SQLSERVER_SELF_SIGNED_SHA2_256_V1,
        MssqlR1CertificateSignatureAlgorithmV1.SHA2_256,
        subject,
        "20000101",
        "99991231",
        _owner(),
    )


def _template(kind: MssqlR1SecretTemplateKindV1, text: str) -> MssqlR1SecretSqlTemplateV1:
    policy = MssqlR1EphemeralSecretPolicyV1.exact()
    return MssqlR1SecretSqlTemplateV1(kind, text.encode(), policy.digest)


def _shared_signers(descriptor) -> tuple[MssqlR1SharedSignerInstallAuthorityV1, ...]:
    result = []
    for ordinal, kind in enumerate((MssqlR1SignerProfileKindV3.ATTESTOR, MssqlR1SignerProfileKindV3.STAGE_OWNER), 1):
        signer = next(
            item for item in descriptor.expected_schema_contract.ordered_signer_profiles if item.signer_profile is kind
        )
        procedures = tuple(
            item
            for item in descriptor.ordered_procedures
            if item.portable_object.module_options is not None
            and item.portable_object.module_options.signer_profile is kind
        )
        refs = tuple(_resource(item.portable_object.object_name) for item in procedures)
        role = (
            MssqlR1SubjectRoleV3.ATTESTATION_MODULE
            if kind is MssqlR1SignerProfileKindV3.ATTESTOR
            else MssqlR1SubjectRoleV3.STAGE_OWNER_MODULE
        )
        rule_digests = tuple(
            hashlib.sha256(item.canonical_bytes).digest()
            for item in descriptor.expected_schema_contract.ordered_permission_rules
            if item.subject_role is role
        )
        create = _template(
            MssqlR1SecretTemplateKindV1.CREATE_CERTIFICATE,
            f"CREATE CERTIFICATE [{signer.certificate_name}] ENCRYPTION BY PASSWORD = '{SECRET_PLACEHOLDER}';\n",
        )
        signatures = tuple(
            _template(
                MssqlR1SecretTemplateKindV1.ADD_SIGNATURE,
                f"ADD SIGNATURE TO [{ref.schema_name}].[{ref.object_name}] BY CERTIFICATE "
                f"[{signer.certificate_name}] WITH PASSWORD = '{SECRET_PLACEHOLDER}';\n",
            )
            for ref in refs
        )
        result.append(
            MssqlR1SharedSignerInstallAuthorityV1(
                ordinal,
                MssqlR1SharedSignerProfileRefV1(kind, hashlib.sha256(signer.canonical_bytes).digest()),
                _metadata(kind),
                create,
                signatures,
                refs,
                rule_digests,
            )
        )
    return tuple(result)


def _memberships() -> tuple[MssqlR1ForbiddenRoleMembershipV1, ...]:
    members: tuple[Any, ...] = (
        *(
            MssqlR1EnvironmentPrincipalRefV1(role)
            for role in (
                MssqlR1SubjectRoleV3.PROVISIONER,
                MssqlR1SubjectRoleV3.RUNTIME,
                MssqlR1SubjectRoleV3.LOADER,
                MssqlR1SubjectRoleV3.OBSERVER,
            )
        ),
        MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR),
        MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.STAGE_OWNER),
        MssqlR1BindingSignerClassPrincipalRefV1(),
    )
    return tuple(
        MssqlR1ForbiddenRoleMembershipV1(index, member, MssqlR1AnyDatabaseRoleRefV1())
        for index, member in enumerate(members, 1)
    )


def _profile() -> MssqlR1SharedInstallSecurityProfileV2:
    descriptor = _descriptor()
    return MssqlR1SharedInstallSecurityProfileV2(
        "dpone-mssql-r1-shared-security-2",
        descriptor.digest,
        MssqlR1EnvironmentPrincipalProfileV1.CONTAINED_SQL_USERS_V1,
        tuple(
            MssqlR1EnvironmentPrincipalRefV1(role)
            for role in (
                MssqlR1SubjectRoleV3.PROVISIONER,
                MssqlR1SubjectRoleV3.RUNTIME,
                MssqlR1SubjectRoleV3.LOADER,
                MssqlR1SubjectRoleV3.OBSERVER,
            )
        ),
        _shared_signers(descriptor),
        MssqlR1EphemeralSecretPolicyV1.exact(),
        exact_lifecycle_transitions(),
        MssqlR1PermissionClosurePolicyV1.create(descriptor),
        _memberships(),
        MssqlR1PrivateKeyDispositionV1.REMOVE_AFTER_ALL_SIGNATURES,
        MssqlR1ReinstallPolicyV1.OBSERVE_EXACT_OR_BLOCK,
    )


def security_profile() -> MssqlR1SharedInstallSecurityProfileV2:
    """Build the exact shared-install security profile used by cross-contract tests."""

    return _profile()


def _binding_policy(profile: MssqlR1SharedInstallSecurityProfileV2) -> MssqlR1BindingSignerLifecyclePolicyV2:
    return MssqlR1BindingSignerLifecyclePolicyV2(
        "dpone-mssql-r1-binding-signer-policy-2",
        profile.physical_schema_descriptor_digest,
        profile.digest,
        MssqlR1BindingSignerNamingProfileV1.LOWERCASE_BINDING_UUID_V1,
        "dpone_b_{binding_uuid_hex}_cert_v3",
        "dpone_b_{binding_uuid_hex}_cert_user_v3",
        "dpone R1 V3 binding {binding_uuid_hex}",
        _owner(),
        "20000101",
        "99991231",
        MssqlR1CertificateCreationProfileV1.SQLSERVER_SELF_SIGNED_SHA2_256_V1,
        MssqlR1CertificateSignatureAlgorithmV1.SHA2_256,
        profile.ephemeral_secret_policy.digest,
        "uuid_resolved_literal_identifiers_v1",
        tuple(MssqlR1BindingModuleKindV1),
        MssqlR1BindingPermissionProfileV1.EXACT_TARGET_AND_ROW_HASH_V1,
        profile.lifecycle_transition_set_digest,
    )


def _observation() -> MssqlR1CertificateCatalogObservationV1:
    return MssqlR1CertificateCatalogObservationV1(
        "attestor_cert",
        True,
        "dpone R1 V3 attestor",
        "20000101",
        "99991231",
        _owner(),
        b"thumbprint",
        False,
        "attestor_user",
        True,
        MssqlR1PrincipalTypeV3.CERTIFICATE,
        MssqlR1AuthenticationTypeV3.NONE,
        b"sid",
        b"thumbprint",
        (DIGEST_A,),
        (DIGEST_B,),
    )


def _identity(signer, lifecycle, observation) -> MssqlR1CertificateInstallIdentityRefV2:
    return MssqlR1CertificateInstallIdentityRefV2(
        _resource(),
        DIGEST_A,
        DIGEST_B,
        hashlib.sha256(b"stable catalog").digest(),
        signer,
        hashlib.sha256(lifecycle.canonical_bytes).digest(),
        hashlib.sha256(observation.canonical_bytes).digest(),
    )


def _profile_payload(
    profile: MssqlR1SharedInstallSecurityProfileV2,
    *,
    signer_payloads: tuple[bytes, ...] | None = None,
    extra_fields: tuple[Any, ...] = (),
) -> bytes:
    return canonical_bytes(
        b"dpone-r1-shared-install-security-profile-v2\0",
        (
            profile.contract_version,
            profile.physical_schema_descriptor_digest,
            profile.environment_principal_profile,
            tuple(item.canonical_bytes for item in profile.ordered_required_environment_principals),
            signer_payloads or tuple(item.canonical_bytes for item in profile.ordered_shared_signers),
            profile.ephemeral_secret_policy.canonical_bytes,
            tuple(item.canonical_bytes for item in profile.ordered_lifecycle_transitions),
            profile.permission_closure_policy.canonical_bytes,
            tuple(item.canonical_bytes for item in profile.ordered_forbidden_role_memberships),
            profile.private_key_disposition,
            profile.reinstall_policy,
            *extra_fields,
        ),
    )


def test_closed_registry_and_every_principal_origin_arm_round_trip() -> None:
    assert MSSQL_R1_SECURITY_ENUM_REGISTRY[-1] is MssqlR1SecretTemplateKindV1
    assert tuple(item.value for item in MssqlR1CertificateLifecycleStateV1) == (
        "absent",
        "private_key_present",
        "certificate_user_ready",
        "permissions_ready",
        "signatures_complete",
        "installed_public_key_only",
        "conflict",
    )
    principals = (
        _owner(),
        MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR),
        MssqlR1BindingSignerClassPrincipalRefV1(),
        MssqlR1BindingSignerInstancePrincipalRefV1(UUID(int=1)),
        MssqlR1PublicPrincipalRefV1(),
        MssqlR1NamedDatabaseRoleRefV1("etl_reader"),
        MssqlR1AnyDatabaseRoleRefV1(),
    )
    origins = (
        MssqlR1DirectPermissionOriginV1(),
        MssqlR1RolePermissionOriginV1(MssqlR1NamedDatabaseRoleRefV1("etl_reader")),
        MssqlR1RolePermissionOriginV1(MssqlR1AnyDatabaseRoleRefV1()),
        MssqlR1PublicPermissionOriginV1(),
    )
    assert tuple(decode_security_principal(item.canonical_bytes) for item in principals) == principals
    assert tuple(decode_permission_origin(item.canonical_bytes) for item in origins) == origins
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.ATTESTATION_MODULE)


@pytest.mark.parametrize(
    "target",
    (
        MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.DATABASE, None, None, None, False),
        MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.SCHEMA, "dbo", None, None, True),
        MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.OBJECT, "dbo", "orders", None, True),
        MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.COLUMN, "dbo", "orders", "id", False),
    ),
)
def test_permission_targets_round_trip(target: MssqlR1PermissionTargetV1) -> None:
    assert MssqlR1PermissionTargetV1.from_canonical_bytes(target.canonical_bytes) == target


def test_observation_policy_is_exact_and_descriptor_bound() -> None:
    descriptor = _descriptor()
    observation = MssqlR1PermissionObservationPolicyV2.exact()
    policy = MssqlR1PermissionClosurePolicyV1.create(descriptor)
    assert policy.observation_policy == observation
    assert policy.physical_schema_descriptor_digest == descriptor.digest
    assert (
        hashlib.sha256(observation.canonical_bytes).digest()
        == descriptor.expected_schema_contract.permission_projection_policy_digest
    )
    policy.validate_against(descriptor)
    with pytest.raises(MssqlR1V3ContractError):
        replace(policy, physical_schema_descriptor_digest=DIGEST_A).validate_against(descriptor)
    foreign_schema = replace(descriptor.expected_schema_contract, permission_projection_policy_digest=DIGEST_A)
    with pytest.raises(MssqlR1V3ContractError):
        policy.validate_against(replace(descriptor, expected_schema_contract=foreign_schema))


def test_shared_projection_is_total_typed_and_complete_closure_is_exact_set_equality() -> None:
    descriptor = _descriptor()
    policy = MssqlR1PermissionClosurePolicyV1.create(descriptor)
    shared = tuple(
        project_schema_permission_rule(item) for item in descriptor.expected_schema_contract.ordered_permission_rules
    )
    binding = MssqlR1ResolvedPermissionPathV2(
        MssqlR1BindingSignerInstancePrincipalRefV1(UUID(int=1)),
        _owner(),
        MssqlR1DirectPermissionOriginV1(),
        MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.OBJECT, "dbo", "orders", None, True),
        "SELECT",
        MssqlR1PermissionEffectV3.GRANT,
        False,
    )
    expected = tuple(sorted((*shared, binding), key=lambda item: item.canonical_bytes))
    assert policy.validate_complete(descriptor, (binding,), expected) == expected
    for observed in (expected[:-1], (*expected, binding), tuple(reversed(expected))):
        with pytest.raises(MssqlR1V3ContractError):
            policy.validate_complete(descriptor, (binding,), observed)
    role_path = replace(expected[0], origin=MssqlR1RolePermissionOriginV1(MssqlR1NamedDatabaseRoleRefV1("etl")))
    public_path = replace(expected[0], origin=MssqlR1PublicPermissionOriginV1())
    deny_path = replace(expected[0], effect=MssqlR1PermissionEffectV3.DENY)
    widened = replace(expected[0], grant_option=True)
    for mutation in (role_path, public_path, deny_path, widened):
        with pytest.raises(MssqlR1V3ContractError):
            policy.validate_complete(
                descriptor, (binding,), tuple(sorted((*expected[1:], mutation), key=lambda x: x.canonical_bytes))
            )


def test_resolved_permission_path_union_is_closed() -> None:
    target = MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.DATABASE, None, None, None, False)
    base = MssqlR1ResolvedPermissionPathV2(
        _owner(), _owner(), MssqlR1DirectPermissionOriginV1(), target, "CONNECT", MssqlR1PermissionEffectV3.GRANT, False
    )
    assert MssqlR1ResolvedPermissionPathV2.from_canonical_bytes(base.canonical_bytes) == base
    for field, value in (
        ("beneficiary", MssqlR1PublicPrincipalRefV1()),
        ("beneficiary", MssqlR1BindingSignerClassPrincipalRefV1()),
        ("grantor", MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.RUNTIME)),
        ("origin", MssqlR1RolePermissionOriginV1(MssqlR1AnyDatabaseRoleRefV1())),
    ):
        with pytest.raises(MssqlR1V3ContractError):
            replace(base, **{field: value})  # type: ignore[arg-type]
    with pytest.raises(MssqlR1V3ContractError):
        replace(base, effect=MssqlR1PermissionEffectV3.DENY, grant_option=True)


def test_shared_profile_v2_and_binding_policy_v2_are_exact_and_descriptor_bound() -> None:
    profile = _profile()
    descriptor = _descriptor()
    decoded = MssqlR1SharedInstallSecurityProfileV2.from_canonical_bytes(profile.canonical_bytes)
    assert decoded == profile
    profile.validate_against(descriptor)
    assert profile.lifecycle_transition_set_digest == lifecycle_transition_set_digest(exact_lifecycle_transitions())
    binding = _binding_policy(profile)
    assert MssqlR1BindingSignerLifecyclePolicyV2.from_canonical_bytes(binding.canonical_bytes) == binding
    assert binding.lifecycle_transition_set_digest == profile.lifecycle_transition_set_digest
    for transitions in (exact_lifecycle_transitions()[:-1], tuple(reversed(exact_lifecycle_transitions()))):
        with pytest.raises(MssqlR1V3ContractError):
            replace(profile, ordered_lifecycle_transitions=transitions)


def test_secret_template_and_exact_seven_memberships_fail_closed() -> None:
    profile = _profile()
    template = profile.ordered_shared_signers[0].create_certificate_template
    assert MssqlR1SecretSqlTemplateV1.from_canonical_bytes(template.canonical_bytes) == template
    for payload in (
        template.sql_template_utf8_bytes.replace(b"PASSWORD", b"SECRET"),
        template.sql_template_utf8_bytes.replace(b"}}", b"}}}}"),
        template.sql_template_utf8_bytes.replace(b"\n", b"\r\n"),
    ):
        with pytest.raises(MssqlR1V3ContractError):
            replace(template, sql_template_utf8_bytes=payload)
    assert len(profile.ordered_forbidden_role_memberships) == 7
    for memberships in (
        profile.ordered_forbidden_role_memberships[:-1],
        tuple(reversed(profile.ordered_forbidden_role_memberships)),
    ):
        with pytest.raises(MssqlR1V3ContractError):
            replace(profile, ordered_forbidden_role_memberships=memberships)


def test_replay_evidence_selects_exact_lifecycle_arm_and_receipt_resource() -> None:
    profile = _profile()
    binding = _binding_policy(profile)
    observation = _observation()
    shared_ref = MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR)
    shared_identity = _identity(shared_ref, profile, observation)
    evidence = MssqlR1CertificateReplayEvidenceV2(
        shared_identity, observation.canonical_bytes, profile.canonical_bytes, observation
    )
    assert evidence.state is MssqlR1CertificateLifecycleStateV1.INSTALLED_PUBLIC_KEY_ONLY
    assert MssqlR1CertificateReplayEvidenceV2.from_canonical_bytes(evidence.canonical_bytes) == evidence
    binding_ref = MssqlR1BindingSignerInstancePrincipalRefV1(UUID(int=1))
    binding_identity = _identity(binding_ref, binding, observation)
    assert (
        MssqlR1CertificateReplayEvidenceV2(
            binding_identity, observation.canonical_bytes, binding.canonical_bytes, observation
        ).state
        is MssqlR1CertificateLifecycleStateV1.INSTALLED_PUBLIC_KEY_ONLY
    )
    with pytest.raises(MssqlR1V3ContractError):
        replace(shared_identity, receipt_resource_ref=_resource("dpone_control_receipt_v3"))
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1CertificateReplayEvidenceV2(
            shared_identity, observation.canonical_bytes, binding.canonical_bytes, observation
        )
    conflict = replace(evidence, current_catalog_observation=replace(observation, private_key_present=True))
    assert conflict.state is MssqlR1CertificateLifecycleStateV1.CONFLICT


class _TextSubtype(str):
    pass


class _BytesSubtype(bytes):
    pass


class _IntSubtype(int):
    pass


@dataclass(frozen=True)
class _MutationCase:
    case_id: str
    disposition: Literal["must_reject", "valid_distinct"]
    action: Callable[[], Any]


def _replacement_action(instance: Any, **changes: Any) -> Callable[[], Any]:
    return lambda: replace(instance, **changes)


_ENUM_INVENTORY = (
    (MssqlR1EnvironmentPrincipalProfileV1, ("contained_sql_users_v1",)),
    (MssqlR1CertificateCreationProfileV1, ("sqlserver_self_signed_sha2_256_v1",)),
    (MssqlR1CertificateSignatureAlgorithmV1, ("sha2_256",)),
    (MssqlR1SecretGeneratorProfileV1, ("cryptographic_random_ascii_256bit_v1",)),
    (MssqlR1SecretPersistencePolicyV1, ("memory_only",)),
    (MssqlR1SecretLoggingPolicyV1, ("dpone_logging_forbidden",)),
    (MssqlR1SecretLifetimePolicyV1, ("installer_transaction_only",)),
    (MssqlR1PrivateKeyDispositionV1, ("remove_after_all_signatures",)),
    (MssqlR1ReinstallPolicyV1, ("observe_exact_or_block",)),
    (MssqlR1BindingSignerNamingProfileV1, ("lowercase_binding_uuid_v1",)),
    (MssqlR1BindingPermissionProfileV1, ("exact_target_and_row_hash_v1",)),
    (
        MssqlR1CertificateLifecycleStateV1,
        (
            "absent",
            "private_key_present",
            "certificate_user_ready",
            "permissions_ready",
            "signatures_complete",
            "installed_public_key_only",
            "conflict",
        ),
    ),
    (
        MssqlR1CertificateLifecycleActionV1,
        (
            "create_certificate",
            "create_certificate_user",
            "grant_exact_permissions",
            "sign_exact_modules",
            "remove_private_key",
            "observe_replay",
        ),
    ),
    (MssqlR1PermissionTargetScopeV1, ("database", "schema", "object", "column")),
    (MssqlR1ForbiddenPermissionEffectV1, ("grant", "deny", "grant_with_grant_option")),
    (MssqlR1SecretTemplateKindV1, ("create_certificate", "add_signature")),
)


_FIELD_INVENTORY = {
    "MssqlR1EnvironmentPrincipalRefV1": ("subject_role",),
    "MssqlR1SharedSignerPrincipalRefV1": ("signer_profile",),
    "MssqlR1SharedSignerProfileRefV1": ("signer_profile", "schema2_signer_profile_digest"),
    "MssqlR1BindingSignerClassPrincipalRefV1": (),
    "MssqlR1BindingSignerInstancePrincipalRefV1": ("target_binding_uuid",),
    "MssqlR1PublicPrincipalRefV1": (),
    "MssqlR1NamedDatabaseRoleRefV1": ("role_name",),
    "MssqlR1AnyDatabaseRoleRefV1": (),
    "MssqlR1DirectPermissionOriginV1": (),
    "MssqlR1RolePermissionOriginV1": ("role",),
    "MssqlR1PublicPermissionOriginV1": (),
    "MssqlR1PermissionTargetV1": ("scope", "schema_name", "object_name", "column_name", "include_descendants"),
    "MssqlR1ForbiddenRoleMembershipV1": ("ordinal", "member", "forbidden_role"),
    "MssqlR1EphemeralSecretPolicyV1": (
        "generator_profile",
        "minimum_entropy_bits",
        "output_length",
        "alphabet_ascii",
        "required_character_classes",
        "placeholder_ascii",
        "placeholder_occurrences_per_template",
        "persistence_policy",
        "logging_policy",
        "lifetime_policy",
    ),
    "MssqlR1CertificateLifecycleTransitionV1": ("ordinal", "from_state", "action", "to_state"),
    "MssqlR1SharedCertificateMetadataV1": (
        "creation_profile",
        "signature_algorithm",
        "subject",
        "start_date_yyyymmdd",
        "expiry_date_yyyymmdd",
        "owner",
    ),
    "MssqlR1SecretSqlTemplateV1": ("template_kind", "sql_template_utf8_bytes", "secret_policy_digest"),
    "MssqlR1CertificateCatalogObservationV1": (
        "certificate_name",
        "certificate_exists",
        "certificate_subject",
        "start_date_yyyymmdd",
        "expiry_date_yyyymmdd",
        "certificate_owner",
        "certificate_thumbprint",
        "private_key_present",
        "certificate_user_name",
        "certificate_user_exists",
        "certificate_user_type",
        "authentication_type",
        "certificate_user_sid_bytes",
        "certificate_user_bound_thumbprint",
        "ordered_module_signature_digests",
        "ordered_permission_edge_digests",
    ),
    "MssqlR1CertificateInstallIdentityRefV2": (
        "receipt_resource_ref",
        "installation_effect_key",
        "receipt_payload_digest",
        "stable_catalog_attestation_digest",
        "signer_ref",
        "lifecycle_policy_digest",
        "pinned_certificate_observation_digest",
    ),
    "MssqlR1PermissionObservationPolicyV2": (
        "contract_version",
        "managed_schema_source",
        "managed_object_source",
        "managed_principal_source",
        "securable_scope",
        "origin_coverage",
        "effect_coverage",
        "unknown_row_disposition",
    ),
    "MssqlR1ResolvedPermissionPathV2": (
        "beneficiary",
        "grantor",
        "origin",
        "target",
        "permission",
        "effect",
        "grant_option",
    ),
    "MssqlR1PermissionClosurePolicyV1": (
        "contract_version",
        "physical_schema_descriptor_digest",
        "observation_policy",
        "expected_shared_source",
        "expected_binding_source",
        "observation_scope",
        "unexpected_permission_disposition",
        "ordering",
    ),
    "MssqlR1BindingSignerLifecyclePolicyV2": (
        "contract_version",
        "physical_schema_descriptor_digest",
        "shared_install_security_profile_digest",
        "naming_profile",
        "certificate_name_template",
        "certificate_user_name_template",
        "certificate_subject_template",
        "certificate_owner",
        "start_date_yyyymmdd",
        "expiry_date_yyyymmdd",
        "certificate_creation_profile",
        "signature_algorithm",
        "secret_policy_digest",
        "template_instantiation_profile",
        "ordered_module_kinds",
        "permission_profile",
        "lifecycle_transition_set_digest",
    ),
    "MssqlR1SharedSignerInstallAuthorityV1": (
        "ordinal",
        "signer_ref",
        "certificate_metadata",
        "create_certificate_template",
        "ordered_add_signature_templates",
        "ordered_module_refs",
        "ordered_permission_rule_digests",
    ),
    "MssqlR1SharedInstallSecurityProfileV2": (
        "contract_version",
        "physical_schema_descriptor_digest",
        "environment_principal_profile",
        "ordered_required_environment_principals",
        "ordered_shared_signers",
        "ephemeral_secret_policy",
        "ordered_lifecycle_transitions",
        "permission_closure_policy",
        "ordered_forbidden_role_memberships",
        "private_key_disposition",
        "reinstall_policy",
    ),
    "MssqlR1CertificateReplayEvidenceV2": (
        "installed_identity_ref",
        "pinned_certificate_observation_payload",
        "active_lifecycle_authority_payload",
        "current_catalog_observation",
    ),
}


def _instances() -> dict[str, Any]:
    profile = _profile()
    observation = _observation()
    path = project_schema_permission_rule(_descriptor().expected_schema_contract.ordered_permission_rules[0])
    signer = MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR)
    identity = _identity(signer, profile, observation)
    values = (
        _owner(),
        MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR),
        MssqlR1SharedSignerProfileRefV1(MssqlR1SignerProfileKindV3.ATTESTOR, DIGEST_A),
        MssqlR1BindingSignerClassPrincipalRefV1(),
        MssqlR1BindingSignerInstancePrincipalRefV1(UUID(int=1)),
        MssqlR1PublicPrincipalRefV1(),
        MssqlR1NamedDatabaseRoleRefV1("role"),
        MssqlR1AnyDatabaseRoleRefV1(),
        MssqlR1DirectPermissionOriginV1(),
        MssqlR1RolePermissionOriginV1(MssqlR1NamedDatabaseRoleRefV1("role")),
        MssqlR1PublicPermissionOriginV1(),
        path.target,
        profile.ordered_forbidden_role_memberships[0],
        MssqlR1EphemeralSecretPolicyV1.exact(),
        profile.ordered_lifecycle_transitions[0],
        profile.ordered_shared_signers[0].certificate_metadata,
        profile.ordered_shared_signers[0].create_certificate_template,
        observation,
        identity,
        profile.permission_closure_policy.observation_policy,
        path,
        profile.permission_closure_policy,
        _binding_policy(profile),
        profile.ordered_shared_signers[0],
        profile,
        MssqlR1CertificateReplayEvidenceV2(identity, observation.canonical_bytes, profile.canonical_bytes, observation),
    )
    return {type(value).__name__: value for value in values}


def _wrong_exact_type(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if type(value) is bool:
        return int(value)
    if type(value) is int:
        return _IntSubtype(value)
    if type(value) is str:
        return _TextSubtype(value)
    if type(value) is bytes:
        return _BytesSubtype(value)
    if type(value) is UUID:
        return str(value)
    if type(value) is tuple:
        return list(value)
    return object()


def _field_mutation_cases() -> tuple[_MutationCase, ...]:
    instances = _instances()
    assert tuple(instances) == tuple(_FIELD_INVENTORY)
    cases = []
    for class_name, field_names in _FIELD_INVENTORY.items():
        instance = instances[class_name]
        assert tuple(field.name for field in fields(instance)) == field_names
        for field_name in field_names:
            replacement = _wrong_exact_type(getattr(instance, field_name))
            cases.append(
                _MutationCase(
                    f"{class_name}.{field_name}.wrong_exact_type",
                    "must_reject",
                    _replacement_action(instance, **{field_name: replacement}),
                )
            )
    return tuple(cases)


def _must_reject_semantic_cases() -> tuple[_MutationCase, ...]:
    descriptor = _descriptor()
    profile = _profile()
    binding = _binding_policy(profile)
    observation = _observation()
    signer = profile.ordered_shared_signers[0]
    subject_swapped_metadata = replace(signer.certificate_metadata, subject="dpone R1 V3 stage owner")
    subject_swapped_signer_payload = canonical_bytes(
        b"dpone-r1-security-shared-signer-install-authority-v1\0",
        (
            signer.ordinal,
            signer.signer_ref.canonical_bytes,
            subject_swapped_metadata.canonical_bytes,
            signer.create_certificate_template.canonical_bytes,
            tuple(item.canonical_bytes for item in signer.ordered_add_signature_templates),
            tuple(item.canonical_bytes for item in signer.ordered_module_refs),
            signer.ordered_permission_rule_digests,
        ),
    )
    subject_swapped_profile_payload = _profile_payload(
        profile,
        signer_payloads=(subject_swapped_signer_payload, profile.ordered_shared_signers[1].canonical_bytes),
    )
    transitions = exact_lifecycle_transitions()
    memberships = profile.ordered_forbidden_role_memberships
    bad_signature = replace(
        signer.ordered_add_signature_templates[0],
        sql_template_utf8_bytes=signer.ordered_add_signature_templates[0].sql_template_utf8_bytes.replace(
            b"BY CERTIFICATE [attestor_cert]", b"BY CERTIFICATE [foreign_cert]"
        ),
    )
    bad_signer = replace(
        signer, ordered_add_signature_templates=(bad_signature, *signer.ordered_add_signature_templates[1:])
    )
    bad_profile = replace(profile, ordered_shared_signers=(bad_signer, *profile.ordered_shared_signers[1:]))
    identity = _identity(MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR), profile, observation)
    evidence = MssqlR1CertificateReplayEvidenceV2(
        identity, observation.canonical_bytes, profile.canonical_bytes, observation
    )
    resource_cases = tuple(
        MssqlR1PhysicalResourceRefV1(
            MssqlR1ResourceKindV1.STATIC_OBJECT,
            item.portable_object.schema_name,
            item.portable_object.object_name,
            None,
        )
        for item in (*descriptor.ordered_tables, *descriptor.ordered_procedures)
        if (item.portable_object.schema_name, item.portable_object.object_name)
        != ("dpone_authority", "dpone_provider_install_receipt_v3")
    ) + tuple(
        MssqlR1PhysicalResourceRefV1(kind, None, None, f"{kind.value}_coordinate")
        for kind in tuple(MssqlR1ResourceKindV1)[1:]
    )
    closure = profile.permission_closure_policy
    shared_paths = tuple(
        project_schema_permission_rule(item) for item in descriptor.expected_schema_contract.ordered_permission_rules
    )
    binding_path = MssqlR1ResolvedPermissionPathV2(
        MssqlR1BindingSignerInstancePrincipalRefV1(UUID(int=1)),
        _owner(),
        MssqlR1DirectPermissionOriginV1(),
        MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.OBJECT, "dbo", "orders", None, True),
        "SELECT",
        MssqlR1PermissionEffectV3.GRANT,
        False,
    )
    expected_paths = tuple(sorted((*shared_paths, binding_path), key=lambda item: item.canonical_bytes))
    changed_path = expected_paths[0]

    def closure_with(
        original: MssqlR1ResolvedPermissionPathV2,
        replacement: MssqlR1ResolvedPermissionPathV2,
    ) -> None:
        observed = tuple(
            sorted(
                (*(item for item in expected_paths if item != original), replacement),
                key=lambda item: item.canonical_bytes,
            )
        )
        closure.validate_complete(descriptor, (binding_path,), observed)

    cases: list[_MutationCase] = [
        _MutationCase(
            "metadata.alternate_subject", "must_reject", lambda: replace(signer.certificate_metadata, subject="other")
        ),
        _MutationCase(
            "metadata.alternate_start",
            "must_reject",
            lambda: replace(signer.certificate_metadata, start_date_yyyymmdd="20010101"),
        ),
        _MutationCase(
            "metadata.alternate_expiry",
            "must_reject",
            lambda: replace(signer.certificate_metadata, expiry_date_yyyymmdd="20991231"),
        ),
        _MutationCase(
            "shared_profile.valid_subject_swap",
            "must_reject",
            lambda: replace(signer, certificate_metadata=subject_swapped_metadata),
        ),
        _MutationCase(
            "replay.valid_subject_swap",
            "must_reject",
            lambda: MssqlR1CertificateReplayEvidenceV2(
                replace(
                    identity,
                    lifecycle_policy_digest=hashlib.sha256(subject_swapped_profile_payload).digest(),
                ),
                observation.canonical_bytes,
                subject_swapped_profile_payload,
                observation,
            ),
        ),
        _MutationCase(
            "template.appended_statement",
            "must_reject",
            lambda: replace(
                signer.create_certificate_template,
                sql_template_utf8_bytes=signer.create_certificate_template.sql_template_utf8_bytes
                + b"DROP CERTIFICATE [x];\n",
            ),
        ),
        _MutationCase(
            "template.no_terminal_lf",
            "must_reject",
            lambda: replace(
                signer.create_certificate_template,
                sql_template_utf8_bytes=signer.create_certificate_template.sql_template_utf8_bytes[:-1],
            ),
        ),
        _MutationCase(
            "template.crlf",
            "must_reject",
            lambda: replace(
                signer.create_certificate_template,
                sql_template_utf8_bytes=signer.create_certificate_template.sql_template_utf8_bytes.replace(
                    b"\n", b"\r\n"
                ),
            ),
        ),
        _MutationCase(
            "template.nul",
            "must_reject",
            lambda: replace(
                signer.create_certificate_template,
                sql_template_utf8_bytes=signer.create_certificate_template.sql_template_utf8_bytes[:-1] + b"\0\n",
            ),
        ),
        _MutationCase(
            "template.extra_interpolation",
            "must_reject",
            lambda: replace(
                signer.create_certificate_template,
                sql_template_utf8_bytes=signer.create_certificate_template.sql_template_utf8_bytes.replace(
                    b"CREATE CERTIFICATE", b"CREATE CERTIFICATE {{OTHER}}"
                ),
            ),
        ),
        _MutationCase(
            "shared_signer.foreign_certificate", "must_reject", lambda: bad_profile.validate_against(descriptor)
        ),
        _MutationCase(
            "binding.foreign_descriptor",
            "must_reject",
            lambda: profile.validate_binding_policy(
                descriptor, replace(binding, physical_schema_descriptor_digest=DIGEST_A)
            ),
        ),
        _MutationCase(
            "binding.foreign_shared_profile",
            "must_reject",
            lambda: profile.validate_binding_policy(
                descriptor, replace(binding, shared_install_security_profile_digest=DIGEST_A)
            ),
        ),
        _MutationCase(
            "binding.foreign_secret_policy",
            "must_reject",
            lambda: profile.validate_binding_policy(descriptor, replace(binding, secret_policy_digest=DIGEST_A)),
        ),
        _MutationCase(
            "binding.foreign_transition_set",
            "must_reject",
            lambda: replace(binding, lifecycle_transition_set_digest=DIGEST_A),
        ),
        _MutationCase(
            "lifecycle.omission",
            "must_reject",
            lambda: replace(profile, ordered_lifecycle_transitions=transitions[:-1]),
        ),
        _MutationCase(
            "lifecycle.extra",
            "must_reject",
            lambda: replace(profile, ordered_lifecycle_transitions=(*transitions, transitions[-1])),
        ),
        _MutationCase(
            "lifecycle.duplicate",
            "must_reject",
            lambda: replace(profile, ordered_lifecycle_transitions=(*transitions[:-1], transitions[-2])),
        ),
        _MutationCase(
            "lifecycle.reorder",
            "must_reject",
            lambda: replace(profile, ordered_lifecycle_transitions=tuple(reversed(transitions))),
        ),
        _MutationCase(
            "lifecycle.gap",
            "must_reject",
            lambda: replace(
                profile,
                ordered_lifecycle_transitions=(transitions[0], replace(transitions[1], ordinal=3), *transitions[2:]),
            ),
        ),
        _MutationCase(
            "lifecycle.branch",
            "must_reject",
            lambda: replace(
                profile,
                ordered_lifecycle_transitions=(
                    replace(transitions[0], to_state=MssqlR1CertificateLifecycleStateV1.CONFLICT),
                    *transitions[1:],
                ),
            ),
        ),
        _MutationCase(
            "membership.omission",
            "must_reject",
            lambda: replace(profile, ordered_forbidden_role_memberships=memberships[:-1]),
        ),
        _MutationCase(
            "membership.extra",
            "must_reject",
            lambda: replace(profile, ordered_forbidden_role_memberships=(*memberships, memberships[-1])),
        ),
        _MutationCase(
            "membership.reorder",
            "must_reject",
            lambda: replace(profile, ordered_forbidden_role_memberships=tuple(reversed(memberships))),
        ),
        _MutationCase(
            "membership.wrong_member",
            "must_reject",
            lambda: replace(
                profile,
                ordered_forbidden_role_memberships=(
                    replace(memberships[0], member=memberships[1].member),
                    *memberships[1:],
                ),
            ),
        ),
        _MutationCase(
            "membership.named_role",
            "must_reject",
            lambda: replace(
                profile,
                ordered_forbidden_role_memberships=(
                    replace(memberships[0], forbidden_role=MssqlR1NamedDatabaseRoleRefV1("role")),
                    *memberships[1:],
                ),
            ),
        ),
        _MutationCase(
            "replay.pinned_digest",
            "must_reject",
            lambda: replace(
                evidence,
                pinned_certificate_observation_payload=replace(
                    observation, certificate_name="other_cert"
                ).canonical_bytes,
            ),
        ),
        _MutationCase(
            "replay.lifecycle_digest",
            "must_reject",
            lambda: replace(
                evidence,
                active_lifecycle_authority_payload=replace(
                    profile,
                    physical_schema_descriptor_digest=DIGEST_A,
                    permission_closure_policy=replace(
                        profile.permission_closure_policy, physical_schema_descriptor_digest=DIGEST_A
                    ),
                ).canonical_bytes,
            ),
        ),
        _MutationCase(
            "replay.cross_arm",
            "must_reject",
            lambda: replace(evidence, active_lifecycle_authority_payload=binding.canonical_bytes),
        ),
        _MutationCase(
            "closure.missing",
            "must_reject",
            lambda: closure.validate_complete(descriptor, (binding_path,), expected_paths[:-1]),
        ),
        _MutationCase(
            "closure.extra",
            "must_reject",
            lambda: closure.validate_complete(descriptor, (binding_path,), (*expected_paths, binding_path)),
        ),
        _MutationCase(
            "closure.reorder",
            "must_reject",
            lambda: closure.validate_complete(descriptor, (binding_path,), tuple(reversed(expected_paths))),
        ),
        _MutationCase(
            "closure.role_origin",
            "must_reject",
            lambda: closure_with(
                changed_path,
                replace(changed_path, origin=MssqlR1RolePermissionOriginV1(MssqlR1NamedDatabaseRoleRefV1("role"))),
            ),
        ),
        _MutationCase(
            "closure.public_origin",
            "must_reject",
            lambda: closure_with(changed_path, replace(changed_path, origin=MssqlR1PublicPermissionOriginV1())),
        ),
        _MutationCase(
            "closure.deny",
            "must_reject",
            lambda: closure_with(changed_path, replace(changed_path, effect=MssqlR1PermissionEffectV3.DENY)),
        ),
        _MutationCase(
            "closure.grant_option",
            "must_reject",
            lambda: closure_with(changed_path, replace(changed_path, grant_option=not changed_path.grant_option)),
        ),
        _MutationCase(
            "closure.scope_widening",
            "must_reject",
            lambda: closure_with(
                binding_path,
                replace(
                    binding_path,
                    target=MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.DATABASE, None, None, None, False),
                ),
            ),
        ),
        _MutationCase(
            "closure.transient_loader_row",
            "must_reject",
            lambda: closure.validate_complete(
                descriptor,
                (binding_path,),
                tuple(
                    sorted(
                        (
                            *expected_paths,
                            MssqlR1ResolvedPermissionPathV2(
                                MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.LOADER),
                                _owner(),
                                MssqlR1DirectPermissionOriginV1(),
                                binding_path.target,
                                "INSERT",
                                MssqlR1PermissionEffectV3.GRANT,
                                False,
                            ),
                        ),
                        key=lambda item: item.canonical_bytes,
                    )
                ),
            ),
        ),
    ]
    cases.extend(
        _MutationCase(
            f"identity.receipt_resource.{resource.resource_kind.value}.{resource.schema_name or resource.coordinate_name}.{resource.object_name or index}",
            "must_reject",
            _replacement_action(identity, receipt_resource_ref=resource),
        )
        for index, resource in enumerate(resource_cases, 1)
    )
    path_beneficiary_arms = {
        "public": MssqlR1PublicPrincipalRefV1(),
        "binding_class": MssqlR1BindingSignerClassPrincipalRefV1(),
        "named_role": MssqlR1NamedDatabaseRoleRefV1("role"),
        "any_role": MssqlR1AnyDatabaseRoleRefV1(),
    }
    cases.extend(
        _MutationCase(
            f"resolved_path.forbidden_beneficiary.{name}",
            "must_reject",
            _replacement_action(changed_path, beneficiary=beneficiary),
        )
        for name, beneficiary in path_beneficiary_arms.items()
    )
    identity_signer_arms = {
        "environment": MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.RUNTIME),
        "binding_class": MssqlR1BindingSignerClassPrincipalRefV1(),
        "public": MssqlR1PublicPrincipalRefV1(),
        "named_role": MssqlR1NamedDatabaseRoleRefV1("role"),
        "any_role": MssqlR1AnyDatabaseRoleRefV1(),
    }
    cases.extend(
        _MutationCase(
            f"install_identity.forbidden_signer.{name}",
            "must_reject",
            _replacement_action(identity, signer_ref=principal),
        )
        for name, principal in identity_signer_arms.items()
    )
    invalid_target_arguments = {
        "database_coordinate": (MssqlR1PermissionTargetScopeV1.DATABASE, "dbo", None, None, False),
        "schema_missing": (MssqlR1PermissionTargetScopeV1.SCHEMA, None, None, None, False),
        "schema_object": (MssqlR1PermissionTargetScopeV1.SCHEMA, "dbo", "orders", None, False),
        "object_missing": (MssqlR1PermissionTargetScopeV1.OBJECT, "dbo", None, None, False),
        "object_column": (MssqlR1PermissionTargetScopeV1.OBJECT, "dbo", "orders", "id", False),
        "column_missing": (MssqlR1PermissionTargetScopeV1.COLUMN, "dbo", "orders", None, False),
        "database_descendants": (MssqlR1PermissionTargetScopeV1.DATABASE, None, None, None, True),
        "column_descendants": (MssqlR1PermissionTargetScopeV1.COLUMN, "dbo", "orders", "id", True),
    }
    cases.extend(
        _MutationCase(
            f"permission_target.invalid_shape.{name}",
            "must_reject",
            lambda arguments=arguments: MssqlR1PermissionTargetV1(*arguments),
        )
        for name, arguments in invalid_target_arguments.items()
    )
    certificate_only = replace(
        observation,
        certificate_user_name=None,
        certificate_user_exists=False,
        certificate_user_type=None,
        authentication_type=None,
        certificate_user_sid_bytes=None,
        certificate_user_bound_thumbprint=None,
        ordered_module_signature_digests=(),
        ordered_permission_edge_digests=(),
    )
    user_only = replace(
        observation,
        certificate_exists=False,
        certificate_subject=None,
        start_date_yyyymmdd=None,
        expiry_date_yyyymmdd=None,
        certificate_owner=None,
        certificate_thumbprint=None,
        private_key_present=None,
        ordered_module_signature_digests=(),
        ordered_permission_edge_digests=(),
    )
    cases.extend(
        (
            _MutationCase(
                "catalog.certificate_only_with_edges",
                "must_reject",
                _replacement_action(certificate_only, ordered_module_signature_digests=(DIGEST_A,)),
            ),
            _MutationCase(
                "catalog.user_only_with_edges",
                "must_reject",
                _replacement_action(user_only, ordered_permission_edge_digests=(DIGEST_A,)),
            ),
        )
    )
    return tuple(cases)


def _valid_distinct_cases() -> tuple[_MutationCase, ...]:
    profile = _profile()
    source_rule = _descriptor().expected_schema_contract.ordered_permission_rules[0]
    observation = _observation()
    binding = _binding_policy(profile)
    signer = MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR)
    identity = _identity(signer, profile, observation)
    evidence = MssqlR1CertificateReplayEvidenceV2(
        identity, observation.canonical_bytes, profile.canonical_bytes, observation
    )

    def digests(before: Any, after: Any) -> tuple[bytes, bytes]:
        return before.digest, after.digest

    return (
        _MutationCase(
            "principal.binding_uuid",
            "valid_distinct",
            lambda: digests(
                MssqlR1BindingSignerInstancePrincipalRefV1(UUID(int=1)),
                MssqlR1BindingSignerInstancePrincipalRefV1(UUID(int=2)),
            ),
        ),
        _MutationCase(
            "principal.named_role",
            "valid_distinct",
            lambda: digests(MssqlR1NamedDatabaseRoleRefV1("role_a"), MssqlR1NamedDatabaseRoleRefV1("role_b")),
        ),
        _MutationCase(
            "template.resolved_certificate",
            "valid_distinct",
            lambda: digests(
                profile.ordered_shared_signers[0].create_certificate_template,
                replace(
                    profile.ordered_shared_signers[0].create_certificate_template,
                    sql_template_utf8_bytes=profile.ordered_shared_signers[
                        0
                    ].create_certificate_template.sql_template_utf8_bytes.replace(b"attestor_cert", b"other_cert"),
                ),
            ),
        ),
        _MutationCase(
            "observation.unexpected_owner",
            "valid_distinct",
            lambda: digests(
                observation,
                replace(observation, certificate_owner=MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.RUNTIME)),
            ),
        ),
        _MutationCase(
            "identity.effect_key",
            "valid_distinct",
            lambda: digests(identity, replace(identity, installation_effect_key=DIGEST_B)),
        ),
        _MutationCase(
            "binding.descriptor_reference",
            "valid_distinct",
            lambda: digests(binding, replace(binding, physical_schema_descriptor_digest=DIGEST_A)),
        ),
        _MutationCase(
            "replay.current_conflict",
            "valid_distinct",
            lambda: digests(
                evidence, replace(evidence, current_catalog_observation=replace(observation, private_key_present=True))
            ),
        ),
        _MutationCase(
            "projection.permission_copy",
            "valid_distinct",
            lambda: digests(
                project_schema_permission_rule(source_rule),
                project_schema_permission_rule(replace(source_rule, permission="SELECT")),
            ),
        ),
    )


def _v1_payloads() -> tuple[bytes, bytes, bytes]:
    profile = _profile()
    observation = _observation()
    forbidden_edge = canonical_bytes(
        b"dpone-r1-security-forbidden-permission-edge-v1\0",
        (
            1,
            _owner().canonical_bytes,
            MssqlR1DirectPermissionOriginV1().canonical_bytes,
            MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.DATABASE, None, None, None, False).canonical_bytes,
            "CONNECT",
            (MssqlR1ForbiddenPermissionEffectV1.GRANT,),
        ),
    )
    old_profile = canonical_bytes(
        b"dpone-r1-shared-install-security-profile-v1\0",
        (
            "dpone-mssql-r1-shared-security-1",
            profile.physical_schema_descriptor_digest,
            profile.environment_principal_profile,
            tuple(item.canonical_bytes for item in profile.ordered_required_environment_principals),
            tuple(item.canonical_bytes for item in profile.ordered_shared_signers),
            profile.ephemeral_secret_policy.canonical_bytes,
            tuple(item.canonical_bytes for item in profile.ordered_lifecycle_transitions),
            (forbidden_edge,),
            tuple(item.canonical_bytes for item in profile.ordered_forbidden_role_memberships),
            profile.private_key_disposition,
            profile.reinstall_policy,
        ),
    )
    old_identity = canonical_bytes(
        b"dpone-r1-security-certificate-install-identity-ref-v1\0",
        (
            _resource().canonical_bytes,
            DIGEST_A,
            profile.digest,
            observation.certificate_name,
            observation.certificate_user_name,
            observation.certificate_thumbprint,
            observation.certificate_user_sid_bytes,
            observation.certificate_user_type,
            observation.authentication_type,
            observation.ordered_module_signature_digests,
            observation.ordered_permission_edge_digests,
            hashlib.sha256(observation.canonical_bytes).digest(),
        ),
    )
    old_replay = canonical_bytes(
        b"dpone-r1-security-certificate-replay-admission-v1\0",
        (old_identity, observation.canonical_bytes, observation.canonical_bytes),
    )
    return old_profile, old_identity, old_replay


def test_exact_enum_and_field_mutation_registries_are_complete() -> None:
    assert tuple(item[0] for item in _ENUM_INVENTORY) == MSSQL_R1_SECURITY_ENUM_REGISTRY
    for enum_type, expected_values in _ENUM_INVENTORY:
        assert tuple(item.value for item in enum_type) == expected_values
        assert all(type(item) is enum_type and type(item.value) is str for item in enum_type)
    cases = _field_mutation_cases()
    assert len(cases) == 123
    assert len(cases) == sum(len(value) for value in _FIELD_INVENTORY.values())
    assert len({case.case_id for case in cases}) == len(cases)
    for case in cases:
        assert case.disposition == "must_reject"
        with pytest.raises(MssqlR1V3ContractError, match=".+"):
            case.action()


def test_semantic_must_reject_mutation_registry() -> None:
    cases = _must_reject_semantic_cases()
    assert len(cases) == 112
    assert len({case.case_id for case in cases}) == len(cases)
    for case in cases:
        assert case.disposition == "must_reject"
        with pytest.raises(MssqlR1V3ContractError, match=".+"):
            case.action()


def test_valid_distinct_mutations_change_owner_digest() -> None:
    cases = _valid_distinct_cases()
    assert len(cases) == 8
    assert len({case.case_id for case in cases}) == len(cases)
    for case in cases:
        before, after = case.action()
        assert case.disposition == "valid_distinct"
        assert type(before) is bytes and type(after) is bytes and before != after


def test_every_contract_round_trips_and_rejects_malformed_canonical_frames() -> None:
    for class_name, instance in _instances().items():
        payload = instance.canonical_bytes
        decoder = type(instance).from_canonical_bytes
        assert decoder(payload).canonical_bytes == payload, class_name
        for malformed in (payload[:-1], payload + b"\0"):
            with pytest.raises(MssqlR1V3ContractError):
                decoder(malformed)


def test_real_nonempty_v1_aggregate_identity_and_replay_frames_cannot_enter_v2() -> None:
    old_profile, old_identity, old_replay = _v1_payloads()
    assert all(len(payload) > 256 for payload in (old_profile, old_identity, old_replay))
    for decoder, payload in (
        (MssqlR1SharedInstallSecurityProfileV2.from_canonical_bytes, old_profile),
        (MssqlR1CertificateInstallIdentityRefV2.from_canonical_bytes, old_identity),
        (MssqlR1CertificateReplayEvidenceV2.from_canonical_bytes, old_replay),
    ):
        with pytest.raises(MssqlR1V3ContractError):
            decoder(payload)


def test_decoded_r2_descriptor_remains_the_only_active_descriptor_boundary() -> None:
    descriptor = _descriptor()
    decoded = type(descriptor).from_canonical_bytes(descriptor.canonical_bytes)
    assert decoded.canonical_bytes == descriptor.canonical_bytes
    _profile().validate_against(decoded)


def test_schema_permission_projection_is_total_typed_and_copies_effect_flags() -> None:
    base = _descriptor().expected_schema_contract.ordered_permission_rules[0]
    role_expectations = (
        (
            MssqlR1SubjectRoleV3.PROVISIONER,
            MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.PROVISIONER),
        ),
        (MssqlR1SubjectRoleV3.RUNTIME, MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.RUNTIME)),
        (MssqlR1SubjectRoleV3.LOADER, MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.LOADER)),
        (MssqlR1SubjectRoleV3.OBSERVER, MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.OBSERVER)),
        (
            MssqlR1SubjectRoleV3.ATTESTATION_MODULE,
            MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR),
        ),
        (
            MssqlR1SubjectRoleV3.STAGE_OWNER_MODULE,
            MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.STAGE_OWNER),
        ),
    )
    for role, expected in role_expectations:
        projected = project_schema_permission_rule(replace(base, subject_role=role))
        assert projected.beneficiary == expected
        assert projected.grantor == MssqlR1EnvironmentPrincipalRefV1(base.grantor_role)
        assert projected.permission == base.permission

    scope_expectations = (
        (MssqlR1PermissionScopeV3.DATABASE, None, None, None, False),
        (MssqlR1PermissionScopeV3.SCHEMA, "dbo", None, None, True),
        (MssqlR1PermissionScopeV3.OBJECT, "dbo", "orders", None, True),
        (MssqlR1PermissionScopeV3.COLUMN, "dbo", "orders", "id", False),
    )
    for scope, schema, object_name, column, descendants in scope_expectations:
        projected = project_schema_permission_rule(
            replace(base, scope=scope, schema_name=schema, object_name=object_name, column_name=column)
        )
        assert projected.target == MssqlR1PermissionTargetV1(
            MssqlR1PermissionTargetScopeV1(scope.value), schema, object_name, column, descendants
        )

    denied = project_schema_permission_rule(replace(base, effect=MssqlR1PermissionEffectV3.DENY))
    delegated = project_schema_permission_rule(replace(base, grant_option=True))
    selected = project_schema_permission_rule(replace(base, permission="SELECT"))
    assert denied.effect is MssqlR1PermissionEffectV3.DENY and denied.grant_option is False
    assert delegated.effect is MssqlR1PermissionEffectV3.GRANT and delegated.grant_option is True
    assert selected.permission == "SELECT" and selected.digest != project_schema_permission_rule(base).digest
    with pytest.raises(MssqlR1V3ContractError):
        project_schema_permission_rule(object())  # type: ignore[arg-type]
    with pytest.raises(MssqlR1V3ContractError):
        project_schema_permission_rule(replace(base, source=MssqlR1PermissionSourceV3.DATABASE_ROLE))


def test_union_optional_and_target_coordinate_branches_are_closed() -> None:
    target = MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.OBJECT, "dbo", "orders", None, True)
    base_path = MssqlR1ResolvedPermissionPathV2(
        _owner(),
        _owner(),
        MssqlR1DirectPermissionOriginV1(),
        target,
        "SELECT",
        MssqlR1PermissionEffectV3.GRANT,
        False,
    )
    for beneficiary in (
        _owner(),
        MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR),
        MssqlR1BindingSignerInstancePrincipalRefV1(UUID(int=1)),
    ):
        assert (
            MssqlR1ResolvedPermissionPathV2.from_canonical_bytes(
                replace(base_path, beneficiary=beneficiary).canonical_bytes
            ).beneficiary
            == beneficiary
        )
    for beneficiary in (
        MssqlR1PublicPrincipalRefV1(),
        MssqlR1BindingSignerClassPrincipalRefV1(),
        MssqlR1NamedDatabaseRoleRefV1("role"),
        MssqlR1AnyDatabaseRoleRefV1(),
    ):
        with pytest.raises(MssqlR1V3ContractError):
            replace(base_path, beneficiary=beneficiary)

    profile, observation = _profile(), _observation()
    identity = _identity(MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR), profile, observation)
    for signer in (
        MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.RUNTIME),
        MssqlR1BindingSignerClassPrincipalRefV1(),
        MssqlR1PublicPrincipalRefV1(),
        MssqlR1NamedDatabaseRoleRefV1("role"),
        MssqlR1AnyDatabaseRoleRefV1(),
    ):
        with pytest.raises(MssqlR1V3ContractError):
            replace(identity, signer_ref=signer)

    owners = (
        _owner(),
        MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR),
        MssqlR1BindingSignerClassPrincipalRefV1(),
        MssqlR1BindingSignerInstancePrincipalRefV1(UUID(int=1)),
        MssqlR1PublicPrincipalRefV1(),
        MssqlR1NamedDatabaseRoleRefV1("role"),
        MssqlR1AnyDatabaseRoleRefV1(),
    )
    for owner in owners:
        assert (
            MssqlR1CertificateCatalogObservationV1.from_canonical_bytes(
                replace(observation, certificate_owner=owner).canonical_bytes
            ).certificate_owner
            == owner
        )

    absent = MssqlR1CertificateCatalogObservationV1(
        "cert", False, None, None, None, None, None, None, None, False, None, None, None, None, (), ()
    )
    certificate_only = replace(
        observation,
        certificate_user_name=None,
        certificate_user_exists=False,
        certificate_user_type=None,
        authentication_type=None,
        certificate_user_sid_bytes=None,
        certificate_user_bound_thumbprint=None,
        ordered_module_signature_digests=(),
        ordered_permission_edge_digests=(),
    )
    user_only = replace(
        observation,
        certificate_exists=False,
        certificate_subject=None,
        start_date_yyyymmdd=None,
        expiry_date_yyyymmdd=None,
        certificate_owner=None,
        certificate_thumbprint=None,
        private_key_present=None,
        ordered_module_signature_digests=(),
        ordered_permission_edge_digests=(),
    )
    for state in (absent, certificate_only, user_only, observation):
        assert type(state).from_canonical_bytes(state.canonical_bytes) == state
    for partial in (certificate_only, user_only):
        with pytest.raises(MssqlR1V3ContractError):
            replace(partial, ordered_module_signature_digests=(DIGEST_A,))

    assert MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.SCHEMA, "dbo", None, None, False)
    assert MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.OBJECT, "dbo", "orders", None, False)
    invalid_targets = (
        (MssqlR1PermissionTargetScopeV1.DATABASE, "dbo", None, None, False),
        (MssqlR1PermissionTargetScopeV1.SCHEMA, None, None, None, False),
        (MssqlR1PermissionTargetScopeV1.SCHEMA, "dbo", "orders", None, False),
        (MssqlR1PermissionTargetScopeV1.OBJECT, "dbo", None, None, False),
        (MssqlR1PermissionTargetScopeV1.OBJECT, "dbo", "orders", "id", False),
        (MssqlR1PermissionTargetScopeV1.COLUMN, "dbo", "orders", None, False),
        (MssqlR1PermissionTargetScopeV1.DATABASE, None, None, None, True),
        (MssqlR1PermissionTargetScopeV1.COLUMN, "dbo", "orders", "id", True),
    )
    for arguments in invalid_targets:
        with pytest.raises(MssqlR1V3ContractError):
            MssqlR1PermissionTargetV1(*arguments)


def test_shared_signer_aggregate_mutations_and_positive_binding_are_certified() -> None:
    descriptor, profile = _descriptor(), _profile()
    profile.validate_binding_policy(descriptor, _binding_policy(profile))
    attestor, stage_owner = profile.ordered_shared_signers
    alternate_ref = _resource("alternate_signed_module_v3")
    alternate_template = _template(
        MssqlR1SecretTemplateKindV1.ADD_SIGNATURE,
        "ADD SIGNATURE TO [dpone_authority].[alternate_signed_module_v3] BY CERTIFICATE "
        f"[stage_owner_cert] WITH PASSWORD = '{SECRET_PLACEHOLDER}';\n",
    )

    action_ids = (
        "shared_signer.signer_ref_swap",
        "shared_signer.aggregate_order_swap",
        "shared_signer.module_missing",
        "shared_signer.module_extra",
        "shared_signer.module_duplicate",
        "shared_signer.module_reorder",
        "shared_signer.template_count_mismatch",
        "shared_signer.template_reorder",
        "shared_signer.permission_digest_extra",
        "shared_signer.create_secret_policy_swap",
        "shared_signer.signature_secret_policy_swap",
    )
    actions: tuple[Callable[[], Any], ...] = (
        lambda: replace(attestor, signer_ref=stage_owner.signer_ref),
        lambda: replace(profile, ordered_shared_signers=tuple(reversed(profile.ordered_shared_signers))),
        lambda: replace(
            profile,
            ordered_shared_signers=(
                attestor,
                replace(
                    stage_owner,
                    ordered_module_refs=stage_owner.ordered_module_refs[:-1],
                    ordered_add_signature_templates=stage_owner.ordered_add_signature_templates[:-1],
                ),
            ),
        ).validate_against(descriptor),
        lambda: replace(
            profile,
            ordered_shared_signers=(
                attestor,
                replace(
                    stage_owner,
                    ordered_module_refs=(*stage_owner.ordered_module_refs, alternate_ref),
                    ordered_add_signature_templates=(*stage_owner.ordered_add_signature_templates, alternate_template),
                ),
            ),
        ).validate_against(descriptor),
        lambda: replace(
            stage_owner, ordered_module_refs=(*stage_owner.ordered_module_refs, stage_owner.ordered_module_refs[0])
        ),
        lambda: replace(
            profile,
            ordered_shared_signers=(
                attestor,
                replace(stage_owner, ordered_module_refs=tuple(reversed(stage_owner.ordered_module_refs))),
            ),
        ).validate_against(descriptor),
        lambda: replace(attestor, ordered_add_signature_templates=()),
        lambda: replace(
            profile,
            ordered_shared_signers=(
                attestor,
                replace(
                    stage_owner,
                    ordered_add_signature_templates=tuple(reversed(stage_owner.ordered_add_signature_templates)),
                ),
            ),
        ).validate_against(descriptor),
        lambda: replace(
            profile,
            ordered_shared_signers=(replace(attestor, ordered_permission_rule_digests=(DIGEST_A,)), stage_owner),
        ).validate_against(descriptor),
        lambda: replace(
            profile,
            ordered_shared_signers=(
                replace(
                    attestor,
                    create_certificate_template=replace(
                        attestor.create_certificate_template, secret_policy_digest=DIGEST_A
                    ),
                ),
                stage_owner,
            ),
        ),
        lambda: replace(
            profile,
            ordered_shared_signers=(
                attestor,
                replace(
                    stage_owner,
                    ordered_add_signature_templates=(
                        replace(stage_owner.ordered_add_signature_templates[0], secret_policy_digest=DIGEST_A),
                        *stage_owner.ordered_add_signature_templates[1:],
                    ),
                ),
            ),
        ),
    )
    assert len(set(action_ids)) == len(action_ids) == len(actions)
    for _case_id, action in zip(action_ids, actions, strict=True):
        with pytest.raises(MssqlR1V3ContractError):
            action()


def test_receipt_identity_rejects_every_non_static_resource_shape() -> None:
    profile, observation = _profile(), _observation()
    identity = _identity(MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR), profile, observation)
    for kind in tuple(MssqlR1ResourceKindV1)[1:]:
        resource = MssqlR1PhysicalResourceRefV1(kind, None, None, f"{kind.value}_coordinate")
        with pytest.raises(MssqlR1V3ContractError):
            replace(identity, receipt_resource_ref=resource)


def test_valid_distinct_leaf_changes_propagate_to_owning_aggregates() -> None:
    profile, observation = _profile(), _observation()
    attestor, stage_owner = profile.ordered_shared_signers
    changed_template = _template(
        MssqlR1SecretTemplateKindV1.ADD_SIGNATURE,
        "ADD SIGNATURE TO [dpone_authority].[alternate] BY CERTIFICATE [stage_owner_cert] "
        f"WITH PASSWORD = '{SECRET_PLACEHOLDER}';\n",
    )
    changed_stage = replace(
        stage_owner,
        ordered_add_signature_templates=(changed_template, *stage_owner.ordered_add_signature_templates[1:]),
    )
    changed_profile = replace(profile, ordered_shared_signers=(attestor, changed_stage))
    assert changed_template.digest != stage_owner.ordered_add_signature_templates[0].digest
    assert changed_stage.digest != stage_owner.digest and changed_profile.digest != profile.digest

    base_path = project_schema_permission_rule(_descriptor().expected_schema_contract.ordered_permission_rules[0])
    for changes in (
        {"beneficiary": MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR)},
        {
            "target": MssqlR1PermissionTargetV1(
                MssqlR1PermissionTargetScopeV1.OBJECT,
                base_path.target.schema_name,
                base_path.target.object_name,
                None,
                False,
            )
        },
        {"origin": MssqlR1PublicPermissionOriginV1()},
    ):
        assert replace(base_path, **changes).digest != base_path.digest

    signer = MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR)
    identity = _identity(signer, profile, observation)
    evidence = MssqlR1CertificateReplayEvidenceV2(
        identity, observation.canonical_bytes, profile.canonical_bytes, observation
    )
    changed_observation = replace(observation, private_key_present=True)
    changed_evidence = replace(evidence, current_catalog_observation=changed_observation)
    assert changed_observation.digest != observation.digest and changed_evidence.digest != evidence.digest

    changed_ref = replace(stage_owner.signer_ref, schema2_signer_profile_digest=DIGEST_A)
    changed_stage_ref = replace(stage_owner, signer_ref=changed_ref)
    changed_profile_ref = replace(profile, ordered_shared_signers=(attestor, changed_stage_ref))
    assert changed_ref.digest != stage_owner.signer_ref.digest
    assert changed_stage_ref.digest != stage_owner.digest and changed_profile_ref.digest != profile.digest


def test_domains_golden_aggregates_extra_fields_and_unknown_unions_are_exact() -> None:
    domains = {
        "MssqlR1EnvironmentPrincipalRefV1": b"dpone-r1-security-environment-principal-ref-v1\0",
        "MssqlR1SharedSignerPrincipalRefV1": b"dpone-r1-security-shared-signer-principal-ref-v1\0",
        "MssqlR1SharedSignerProfileRefV1": b"dpone-r1-security-shared-signer-profile-ref-v1\0",
        "MssqlR1BindingSignerClassPrincipalRefV1": b"dpone-r1-security-binding-signer-class-ref-v1\0",
        "MssqlR1BindingSignerInstancePrincipalRefV1": b"dpone-r1-security-binding-signer-instance-ref-v1\0",
        "MssqlR1PublicPrincipalRefV1": b"dpone-r1-security-public-principal-ref-v1\0",
        "MssqlR1NamedDatabaseRoleRefV1": b"dpone-r1-security-named-database-role-ref-v1\0",
        "MssqlR1AnyDatabaseRoleRefV1": b"dpone-r1-security-any-database-role-ref-v1\0",
        "MssqlR1DirectPermissionOriginV1": b"dpone-r1-security-direct-permission-origin-v1\0",
        "MssqlR1RolePermissionOriginV1": b"dpone-r1-security-role-permission-origin-v1\0",
        "MssqlR1PublicPermissionOriginV1": b"dpone-r1-security-public-permission-origin-v1\0",
        "MssqlR1PermissionTargetV1": b"dpone-r1-security-permission-target-v1\0",
        "MssqlR1ForbiddenRoleMembershipV1": b"dpone-r1-security-forbidden-role-membership-v1\0",
        "MssqlR1EphemeralSecretPolicyV1": b"dpone-r1-security-ephemeral-secret-policy-v1\0",
        "MssqlR1CertificateLifecycleTransitionV1": b"dpone-r1-security-certificate-lifecycle-transition-v1\0",
        "MssqlR1SharedCertificateMetadataV1": b"dpone-r1-security-shared-certificate-metadata-v1\0",
        "MssqlR1SecretSqlTemplateV1": b"dpone-r1-security-secret-sql-template-v1\0",
        "MssqlR1CertificateCatalogObservationV1": b"dpone-r1-security-certificate-catalog-observation-v1\0",
        "MssqlR1CertificateInstallIdentityRefV2": b"dpone-r1-security-certificate-install-identity-ref-v2\0",
        "MssqlR1PermissionObservationPolicyV2": b"dpone-r1-security-permission-observation-policy-v2\0",
        "MssqlR1ResolvedPermissionPathV2": b"dpone-r1-security-resolved-permission-path-v2\0",
        "MssqlR1PermissionClosurePolicyV1": b"dpone-r1-security-permission-closure-policy-v1\0",
        "MssqlR1BindingSignerLifecyclePolicyV2": b"dpone-r1-binding-signer-lifecycle-policy-v2\0",
        "MssqlR1SharedSignerInstallAuthorityV1": b"dpone-r1-security-shared-signer-install-authority-v1\0",
        "MssqlR1SharedInstallSecurityProfileV2": b"dpone-r1-shared-install-security-profile-v2\0",
        "MssqlR1CertificateReplayEvidenceV2": b"dpone-r1-security-certificate-replay-evidence-v2\0",
    }
    assert tuple(domains) == tuple(_FIELD_INVENTORY)
    for name, instance in _instances().items():
        assert instance.canonical_bytes.startswith(domains[name])

    profile, observation = _profile(), _observation()
    binding = _binding_policy(profile)
    identity = _identity(MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR), profile, observation)
    replay = MssqlR1CertificateReplayEvidenceV2(
        identity, observation.canonical_bytes, profile.canonical_bytes, observation
    )
    assert tuple(hashlib.sha256(item.canonical_bytes).hexdigest() for item in (profile, binding, replay)) == (
        "af6a902063c5227c04ee924d6a3c1fdfeefbe7f65cb996c58b41f1dce826dd3b",
        "f0236a66454b645596650655bb4e68c7bdc6883534f194f686ddc454f338a760",
        "dd0243ff335cc6fee23ce39f1d6b8fc830829be1eb6edff78aae63a4c8d49420",
    )
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1SharedInstallSecurityProfileV2.from_canonical_bytes(
            _profile_payload(profile, extra_fields=(b"unknown-field",))
        )
    for decoder in (decode_security_principal, decode_permission_origin):
        with pytest.raises(MssqlR1V3ContractError):
            decoder(canonical_bytes(b"dpone-r1-security-unknown-union-arm-v1\0", ()))


def test_exact_numeric_digest_text_and_ordering_boundaries() -> None:
    profile, observation = _profile(), _observation()
    for instance in (
        profile.ordered_shared_signers[0],
        profile.ordered_lifecycle_transitions[0],
        profile.ordered_forbidden_role_memberships[0],
    ):
        for value in (1.0, True, _IntSubtype(1)):
            with pytest.raises(MssqlR1V3ContractError):
                replace(instance, ordinal=value)

    identity = _identity(MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR), profile, observation)
    for length in (31, 33):
        with pytest.raises(MssqlR1V3ContractError):
            replace(identity, installation_effect_key=b"x" * length)
    with pytest.raises(MssqlR1V3ContractError):
        replace(observation, certificate_subject="e\u0301")
    with pytest.raises(MssqlR1V3ContractError):
        replace(observation, certificate_subject="x" * 65)

    with pytest.raises(MssqlR1V3ContractError):
        replace(observation, ordered_module_signature_digests=(DIGEST_A, DIGEST_A))
    ordered = replace(observation, ordered_module_signature_digests=(DIGEST_A, DIGEST_B))
    reversed_order = replace(observation, ordered_module_signature_digests=(DIGEST_B, DIGEST_A))
    assert ordered.digest != reversed_order.digest
    with pytest.raises(MssqlR1V3ContractError):
        replace(
            profile.ordered_shared_signers[1],
            ordered_module_refs=(
                profile.ordered_shared_signers[1].ordered_module_refs[0],
                profile.ordered_shared_signers[1].ordered_module_refs[0],
            ),
            ordered_add_signature_templates=(
                profile.ordered_shared_signers[1].ordered_add_signature_templates[0],
                profile.ordered_shared_signers[1].ordered_add_signature_templates[0],
            ),
        )
    for decoder in (
        MssqlR1PermissionClosurePolicyV1.from_canonical_bytes,
        MssqlR1BindingSignerLifecyclePolicyV2.from_canonical_bytes,
        MssqlR1SharedInstallSecurityProfileV2.from_canonical_bytes,
    ):
        with pytest.raises(MssqlR1V3ContractError):
            decoder(DIGEST_A)
