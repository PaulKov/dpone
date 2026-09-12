"""Shared security aggregate and certificate replay evidence for MSSQL R1."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ResourceKindV1,
)
from dpone.contracts.mssql_r1_v3_physical_schema_descriptor import VERSION, MssqlR1PhysicalSchemaDescriptorV1
from dpone.contracts.mssql_r1_v3_provider_security_certificate import (
    MssqlR1CertificateCatalogObservationV1,
    MssqlR1CertificateInstallIdentityRefV2,
    MssqlR1SecretSqlTemplateV1,
    MssqlR1SharedCertificateMetadataV1,
)
from dpone.contracts.mssql_r1_v3_provider_security_enums import (
    SHARED_SIGNER_SUBJECTS,
    MssqlR1CertificateLifecycleStateV1,
    MssqlR1CertificateLifecycleTransitionV1,
    MssqlR1EnvironmentPrincipalProfileV1,
    MssqlR1PrivateKeyDispositionV1,
    MssqlR1ReinstallPolicyV1,
    MssqlR1SecretTemplateKindV1,
    MssqlR1SecurityCanonicalModel,
    decode_exact_members,
    lifecycle_transition_set_digest,
    require_exact_digest,
    require_exact_positive,
)
from dpone.contracts.mssql_r1_v3_provider_security_permissions import (
    MssqlR1BindingSignerLifecyclePolicyV2,
    MssqlR1PermissionClosurePolicyV1,
)
from dpone.contracts.mssql_r1_v3_provider_security_principals import (
    ENVIRONMENT_SUBJECT_ROLES,
    SHARED_SIGNER_PROFILES,
    MssqlR1AnyDatabaseRoleRefV1,
    MssqlR1BindingSignerClassPrincipalRefV1,
    MssqlR1BindingSignerInstancePrincipalRefV1,
    MssqlR1EnvironmentPrincipalRefV1,
    MssqlR1EphemeralSecretPolicyV1,
    MssqlR1ForbiddenRoleMembershipV1,
    MssqlR1SharedSignerPrincipalRefV1,
    MssqlR1SharedSignerProfileRefV1,
)
from dpone.contracts.mssql_r1_v3_schema_security import MssqlR1SubjectRoleV3

_SIGNER = b"dpone-r1-security-shared-signer-install-authority-v1\0"
_SHARED = b"dpone-r1-shared-install-security-profile-v2\0"
_REPLAY = b"dpone-r1-security-certificate-replay-evidence-v2\0"


@dataclass(frozen=True, slots=True)
class MssqlR1SharedSignerInstallAuthorityV1(MssqlR1SecurityCanonicalModel):
    ordinal: int
    signer_ref: MssqlR1SharedSignerProfileRefV1
    certificate_metadata: MssqlR1SharedCertificateMetadataV1
    create_certificate_template: MssqlR1SecretSqlTemplateV1
    ordered_add_signature_templates: tuple[MssqlR1SecretSqlTemplateV1, ...]
    ordered_module_refs: tuple[MssqlR1PhysicalResourceRefV1, ...]
    ordered_permission_rule_digests: tuple[bytes, ...]

    def __post_init__(self) -> None:
        require_exact_positive(self.ordinal, "shared signer ordinal")
        if (
            type(self.signer_ref) is not MssqlR1SharedSignerProfileRefV1
            or (type(self.certificate_metadata) is not MssqlR1SharedCertificateMetadataV1)
            or self.certificate_metadata.subject != SHARED_SIGNER_SUBJECTS[self.signer_ref.signer_profile.value]
            or type(self.create_certificate_template) is not MssqlR1SecretSqlTemplateV1
            or (self.create_certificate_template.template_kind is not MssqlR1SecretTemplateKindV1.CREATE_CERTIFICATE)
        ):
            raise MssqlR1V3ContractError("shared signer identity or create template is inexact")
        templates = self.ordered_add_signature_templates
        modules = self.ordered_module_refs
        digests = self.ordered_permission_rule_digests
        if (
            type(templates) is not tuple
            or type(modules) is not tuple
            or type(digests) is not tuple
            or not modules
            or (
                len(templates) != len(modules)
                or any(
                    type(item) is not MssqlR1SecretSqlTemplateV1
                    or item.template_kind is not MssqlR1SecretTemplateKindV1.ADD_SIGNATURE
                    for item in templates
                )
            )
            or any(type(item) is not MssqlR1PhysicalResourceRefV1 for item in modules)
            or (len({item.canonical_bytes for item in modules}) != len(modules))
            or any(require_exact_digest(item, "permission rule digest") != item for item in digests)
            or (len(set(digests)) != len(digests))
        ):
            raise MssqlR1V3ContractError("shared signer module or permission authority is incomplete")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _SIGNER,
            (
                self.ordinal,
                self.signer_ref.canonical_bytes,
                self.certificate_metadata.canonical_bytes,
                self.create_certificate_template.canonical_bytes,
                tuple(item.canonical_bytes for item in self.ordered_add_signature_templates),
                tuple(item.canonical_bytes for item in self.ordered_module_refs),
                self.ordered_permission_rule_digests,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SharedSignerInstallAuthorityV1:
        values = list(decode_canonical_bytes(payload, _SIGNER, field_count=7))
        values[1] = MssqlR1SharedSignerProfileRefV1.from_canonical_bytes(expect_bytes(values[1], "signer ref"))
        values[2] = MssqlR1SharedCertificateMetadataV1.from_canonical_bytes(expect_bytes(values[2], "metadata"))
        values[3] = MssqlR1SecretSqlTemplateV1.from_canonical_bytes(expect_bytes(values[3], "create template"))
        values[4] = decode_exact_members(values[4], MssqlR1SecretSqlTemplateV1, "signature templates")
        values[5] = decode_exact_members(values[5], MssqlR1PhysicalResourceRefV1, "module refs")
        values[6] = tuple(expect_bytes(item, "permission digest") for item in expect_tuple(values[6], "permissions"))
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1SharedInstallSecurityProfileV2(MssqlR1SecurityCanonicalModel):
    contract_version: Literal["dpone-mssql-r1-shared-security-2"]
    physical_schema_descriptor_digest: bytes
    environment_principal_profile: MssqlR1EnvironmentPrincipalProfileV1
    ordered_required_environment_principals: tuple[MssqlR1EnvironmentPrincipalRefV1, ...]
    ordered_shared_signers: tuple[MssqlR1SharedSignerInstallAuthorityV1, ...]
    ephemeral_secret_policy: MssqlR1EphemeralSecretPolicyV1
    ordered_lifecycle_transitions: tuple[MssqlR1CertificateLifecycleTransitionV1, ...]
    permission_closure_policy: MssqlR1PermissionClosurePolicyV1
    ordered_forbidden_role_memberships: tuple[MssqlR1ForbiddenRoleMembershipV1, ...]
    private_key_disposition: MssqlR1PrivateKeyDispositionV1
    reinstall_policy: MssqlR1ReinstallPolicyV1

    def __post_init__(self) -> None:
        require_exact_digest(self.physical_schema_descriptor_digest, "physical descriptor digest")
        principals, signers, memberships = (
            self.ordered_required_environment_principals,
            self.ordered_shared_signers,
            self.ordered_forbidden_role_memberships,
        )
        if (
            type(self.contract_version) is not str
            or self.contract_version != "dpone-mssql-r1-shared-security-2"
            or type(self.environment_principal_profile) is not MssqlR1EnvironmentPrincipalProfileV1
            or self.environment_principal_profile is not MssqlR1EnvironmentPrincipalProfileV1.CONTAINED_SQL_USERS_V1
            or type(principals) is not tuple
            or any(type(item) is not MssqlR1EnvironmentPrincipalRefV1 for item in principals)
            or (tuple(item.subject_role for item in principals) != ENVIRONMENT_SUBJECT_ROLES)
            or type(signers) is not tuple
            or any(type(item) is not MssqlR1SharedSignerInstallAuthorityV1 for item in signers)
            or (
                tuple((item.ordinal, item.signer_ref.signer_profile) for item in signers)
                != tuple(enumerate(SHARED_SIGNER_PROFILES, 1))
            )
        ):
            raise MssqlR1V3ContractError("shared security identity is incomplete or reordered")
        if (
            type(self.ephemeral_secret_policy) is not MssqlR1EphemeralSecretPolicyV1
            or (type(self.permission_closure_policy) is not MssqlR1PermissionClosurePolicyV1)
            or self.permission_closure_policy.physical_schema_descriptor_digest
            != self.physical_schema_descriptor_digest
        ):
            raise MssqlR1V3ContractError("shared security policy binding is inexact")
        lifecycle_transition_set_digest(self.ordered_lifecycle_transitions)
        expected_members = (
            *principals,
            *(MssqlR1SharedSignerPrincipalRefV1(item) for item in SHARED_SIGNER_PROFILES),
            MssqlR1BindingSignerClassPrincipalRefV1(),
        )
        if (
            type(memberships) is not tuple
            or len(memberships) != 7
            or (tuple(item.ordinal for item in memberships) != tuple(range(1, 8)))
            or tuple(item.member for item in memberships) != expected_members
            or any(type(item.forbidden_role) is not MssqlR1AnyDatabaseRoleRefV1 for item in memberships)
            or self.private_key_disposition is not MssqlR1PrivateKeyDispositionV1.REMOVE_AFTER_ALL_SIGNATURES
            or (self.reinstall_policy is not MssqlR1ReinstallPolicyV1.OBSERVE_EXACT_OR_BLOCK)
        ):
            raise MssqlR1V3ContractError("shared security negative or lifecycle policy is incomplete")
        for signer in signers:
            if any(
                item.secret_policy_digest != self.ephemeral_secret_policy.digest
                for item in (signer.create_certificate_template, *signer.ordered_add_signature_templates)
            ):
                raise MssqlR1V3ContractError("signer template references a foreign secret policy")

    @property
    def lifecycle_transition_set_digest(self) -> bytes:
        return lifecycle_transition_set_digest(self.ordered_lifecycle_transitions)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _SHARED,
            (
                self.contract_version,
                self.physical_schema_descriptor_digest,
                self.environment_principal_profile,
                tuple(item.canonical_bytes for item in self.ordered_required_environment_principals),
                tuple(item.canonical_bytes for item in self.ordered_shared_signers),
                self.ephemeral_secret_policy.canonical_bytes,
                tuple(item.canonical_bytes for item in self.ordered_lifecycle_transitions),
                self.permission_closure_policy.canonical_bytes,
                tuple(item.canonical_bytes for item in self.ordered_forbidden_role_memberships),
                self.private_key_disposition,
                self.reinstall_policy,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SharedInstallSecurityProfileV2:
        values = list(decode_canonical_bytes(payload, _SHARED, field_count=11))
        values[2] = expect_enum(MssqlR1EnvironmentPrincipalProfileV1, values[2], "principal profile")
        values[3] = decode_exact_members(values[3], MssqlR1EnvironmentPrincipalRefV1, "principals")
        values[4] = decode_exact_members(values[4], MssqlR1SharedSignerInstallAuthorityV1, "signers")
        values[5] = MssqlR1EphemeralSecretPolicyV1.from_canonical_bytes(expect_bytes(values[5], "secret policy"))
        values[6] = decode_exact_members(values[6], MssqlR1CertificateLifecycleTransitionV1, "transitions")
        values[7] = MssqlR1PermissionClosurePolicyV1.from_canonical_bytes(expect_bytes(values[7], "closure policy"))
        values[8] = decode_exact_members(values[8], MssqlR1ForbiddenRoleMembershipV1, "memberships")
        values[9] = expect_enum(MssqlR1PrivateKeyDispositionV1, values[9], "private key policy")
        values[10] = expect_enum(MssqlR1ReinstallPolicyV1, values[10], "reinstall policy")
        return cls(*values)  # type: ignore[arg-type]

    def validate_against(self, descriptor: MssqlR1PhysicalSchemaDescriptorV1) -> None:
        if (
            type(descriptor) is not MssqlR1PhysicalSchemaDescriptorV1
            or descriptor.descriptor_version != VERSION
            or (descriptor.digest != self.physical_schema_descriptor_digest)
        ):
            raise MssqlR1V3ContractError("shared profile references a foreign physical descriptor")
        self.permission_closure_policy.validate_against(descriptor)
        profiles = {item.signer_profile: item for item in descriptor.expected_schema_contract.ordered_signer_profiles}
        for authority in self.ordered_shared_signers:
            signer = profiles.get(authority.signer_ref.signer_profile)
            if signer is None:
                raise MssqlR1V3ContractError("shared signer is absent from descriptor")
            authority.signer_ref.resolve(signer)
            refs = tuple(
                MssqlR1PhysicalResourceRefV1(
                    MssqlR1ResourceKindV1.STATIC_OBJECT,
                    item.portable_object.schema_name,
                    item.portable_object.object_name,
                    None,
                )
                for item in descriptor.ordered_procedures
                if item.portable_object.module_options is not None
                and item.portable_object.module_options.signer_profile is signer.signer_profile
            )
            role = (
                MssqlR1SubjectRoleV3.ATTESTATION_MODULE
                if signer.signer_profile.value == "attestor"
                else MssqlR1SubjectRoleV3.STAGE_OWNER_MODULE
            )
            permissions = tuple(
                hashlib.sha256(item.canonical_bytes).digest()
                for item in descriptor.expected_schema_contract.ordered_permission_rules
                if item.subject_role is role
            )
            if authority.ordered_module_refs != refs or authority.ordered_permission_rule_digests != permissions:
                raise MssqlR1V3ContractError("shared signer authority differs from descriptor")
            create_ids = authority.create_certificate_template.resolved_identifiers
            signature_ids = tuple(item.resolved_identifiers for item in authority.ordered_add_signature_templates)
            expected_signature_ids = tuple((ref.schema_name, ref.object_name, signer.certificate_name) for ref in refs)
            if create_ids != (signer.certificate_name,) or signature_ids != expected_signature_ids:
                raise MssqlR1V3ContractError("shared signer template does not seal descriptor identity")

    def validate_binding_policy(
        self,
        descriptor: MssqlR1PhysicalSchemaDescriptorV1,
        policy: MssqlR1BindingSignerLifecyclePolicyV2,
    ) -> None:
        self.validate_against(descriptor)
        if type(policy) is not MssqlR1BindingSignerLifecyclePolicyV2 or (
            policy.physical_schema_descriptor_digest != descriptor.digest
            or policy.shared_install_security_profile_digest != self.digest
            or policy.secret_policy_digest != self.ephemeral_secret_policy.digest
            or policy.lifecycle_transition_set_digest != self.lifecycle_transition_set_digest
        ):
            raise MssqlR1V3ContractError("binding lifecycle policy references foreign authority")


@dataclass(frozen=True, slots=True)
class MssqlR1CertificateReplayEvidenceV2(MssqlR1SecurityCanonicalModel):
    installed_identity_ref: MssqlR1CertificateInstallIdentityRefV2
    pinned_certificate_observation_payload: bytes
    active_lifecycle_authority_payload: bytes
    current_catalog_observation: MssqlR1CertificateCatalogObservationV1

    def __post_init__(self) -> None:
        if (
            type(self.installed_identity_ref) is not MssqlR1CertificateInstallIdentityRefV2
            or type(self.current_catalog_observation) is not MssqlR1CertificateCatalogObservationV1
            or type(self.pinned_certificate_observation_payload) is not bytes
            or type(self.active_lifecycle_authority_payload) is not bytes
        ):
            raise MssqlR1V3ContractError("certificate replay evidence has an inexact type")
        pinned = MssqlR1CertificateCatalogObservationV1.from_canonical_bytes(
            self.pinned_certificate_observation_payload
        )
        if pinned.canonical_bytes != self.pinned_certificate_observation_payload or (
            hashlib.sha256(pinned.canonical_bytes).digest()
            != self.installed_identity_ref.pinned_certificate_observation_digest
        ):
            raise MssqlR1V3ContractError("pinned certificate observation digest differs")
        payload = self.active_lifecycle_authority_payload
        if type(self.installed_identity_ref.signer_ref) is MssqlR1SharedSignerPrincipalRefV1:
            authority = MssqlR1SharedInstallSecurityProfileV2.from_canonical_bytes(payload)
            if (
                sum(
                    item.signer_ref.signer_profile is self.installed_identity_ref.signer_ref.signer_profile
                    for item in authority.ordered_shared_signers
                )
                != 1
            ):
                raise MssqlR1V3ContractError("shared signer does not occur exactly once")
        elif type(self.installed_identity_ref.signer_ref) is MssqlR1BindingSignerInstancePrincipalRefV1:
            authority = MssqlR1BindingSignerLifecyclePolicyV2.from_canonical_bytes(payload)  # type: ignore[assignment]
        else:
            raise MssqlR1V3ContractError("replay signer arm is unsupported")
        if (
            authority.canonical_bytes != payload
            or hashlib.sha256(payload).digest() != self.installed_identity_ref.lifecycle_policy_digest
        ):
            raise MssqlR1V3ContractError("active lifecycle authority differs from receipt identity")

    @property
    def state(self) -> MssqlR1CertificateLifecycleStateV1:
        pinned = MssqlR1CertificateCatalogObservationV1.from_canonical_bytes(
            self.pinned_certificate_observation_payload
        )
        return (
            MssqlR1CertificateLifecycleStateV1.INSTALLED_PUBLIC_KEY_ONLY
            if pinned == self.current_catalog_observation and pinned.is_installed_public_key_only
            else MssqlR1CertificateLifecycleStateV1.CONFLICT
        )

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _REPLAY,
            (
                self.installed_identity_ref.canonical_bytes,
                self.pinned_certificate_observation_payload,
                self.active_lifecycle_authority_payload,
                self.current_catalog_observation.canonical_bytes,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1CertificateReplayEvidenceV2:
        identity, pinned, lifecycle, current = decode_canonical_bytes(payload, _REPLAY, field_count=4)
        return cls(
            MssqlR1CertificateInstallIdentityRefV2.from_canonical_bytes(expect_bytes(identity, "identity")),
            expect_bytes(pinned, "pinned observation"),
            expect_bytes(lifecycle, "lifecycle authority"),
            MssqlR1CertificateCatalogObservationV1.from_canonical_bytes(expect_bytes(current, "current observation")),
        )
