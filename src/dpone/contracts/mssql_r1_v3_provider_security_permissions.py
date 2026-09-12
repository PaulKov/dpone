"""Total permission closure and binding lifecycle authority for MSSQL R1."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

from dpone.contracts import mssql_r1_v3_provider_security_principals as security
from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1
from dpone.contracts.mssql_r1_v3_physical_schema_descriptor import VERSION, MssqlR1PhysicalSchemaDescriptorV1
from dpone.contracts.mssql_r1_v3_provider_security_binding_paths import is_exact_binding_path
from dpone.contracts.mssql_r1_v3_provider_security_enums import (
    MssqlR1BindingPermissionProfileV1,
    MssqlR1BindingSignerNamingProfileV1,
    MssqlR1CertificateCreationProfileV1,
    MssqlR1CertificateSignatureAlgorithmV1,
    MssqlR1PermissionTargetScopeV1,
    MssqlR1SecurityCanonicalModel,
    exact_lifecycle_transition_set_digest,
    require_exact_digest,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1SignerProfileKindV3
from dpone.contracts.mssql_r1_v3_schema_security import (
    MssqlR1PermissionEffectV3,
    MssqlR1PermissionRuleV3,
    MssqlR1PermissionScopeV3,
    MssqlR1PermissionSourceV3,
    MssqlR1SubjectRoleV3,
    require_permission_name,
)

_OBSERVATION = b"dpone-r1-security-permission-observation-policy-v2\0"
_PATH = b"dpone-r1-security-resolved-permission-path-v2\0"
_CLOSURE = b"dpone-r1-security-permission-closure-policy-v1\0"
_BINDING = b"dpone-r1-binding-signer-lifecycle-policy-v2\0"
_SCOPE_MAP = {
    MssqlR1PermissionScopeV3.DATABASE: MssqlR1PermissionTargetScopeV1.DATABASE,
    MssqlR1PermissionScopeV3.SCHEMA: MssqlR1PermissionTargetScopeV1.SCHEMA,
    MssqlR1PermissionScopeV3.OBJECT: MssqlR1PermissionTargetScopeV1.OBJECT,
    MssqlR1PermissionScopeV3.COLUMN: MssqlR1PermissionTargetScopeV1.COLUMN,
}
_OBSERVATION_VALUES = tuple(
    (
        "dpone-mssql-r1-permission-observation-2|exact_schema_contract_names|exact_schema_and_binding_objects|"
        "exact_resolved_principal_authority|database_and_all_descendants|direct_role_and_public|"
        "grant_grant_option_and_deny|block"
    ).split("|")
)
_CLOSURE_VALUES = tuple(
    (
        "dpone-mssql-r1-permission-closure-1|schema2_exact_permission_rules|"
        "resolved_binding_pack_exact_permission_rules|complete_managed_principal_database_permissions|"
        "block|canonical_observation_bytes"
    ).split("|")
)
_BINDING_VALUES = (
    ("dpone-mssql-r1-binding-signer-policy-2", MssqlR1BindingSignerNamingProfileV1.LOWERCASE_BINDING_UUID_V1)
    + ("dpone_b_{binding_uuid_hex}_cert_v3", "dpone_b_{binding_uuid_hex}_cert_user_v3")
    + ("dpone R1 V3 binding {binding_uuid_hex}", MssqlR1SubjectRoleV3.PROVISIONER, "20000101", "99991231")
    + (
        MssqlR1CertificateCreationProfileV1.SQLSERVER_SELF_SIGNED_SHA2_256_V1,
        MssqlR1CertificateSignatureAlgorithmV1.SHA2_256,
        "uuid_resolved_literal_identifiers_v1",
    )
    + (tuple(MssqlR1BindingModuleKindV1), MssqlR1BindingPermissionProfileV1.EXACT_TARGET_AND_ROW_HASH_V1)
)


@dataclass(frozen=True, slots=True)
class MssqlR1PermissionObservationPolicyV2(MssqlR1SecurityCanonicalModel):
    contract_version: Literal["dpone-mssql-r1-permission-observation-2"]
    managed_schema_source: Literal["exact_schema_contract_names"]
    managed_object_source: Literal["exact_schema_and_binding_objects"]
    managed_principal_source: Literal["exact_resolved_principal_authority"]
    securable_scope: Literal["database_and_all_descendants"]
    origin_coverage: Literal["direct_role_and_public"]
    effect_coverage: Literal["grant_grant_option_and_deny"]
    unknown_row_disposition: Literal["block"]

    def __post_init__(self) -> None:
        values = tuple(getattr(self, name) for name in self.__dataclass_fields__)
        if any(type(value) is not str for value in values) or values != _OBSERVATION_VALUES:
            raise MssqlR1V3ContractError("permission observation policy is not exact")

    @classmethod
    def exact(cls) -> MssqlR1PermissionObservationPolicyV2:
        return cls(*_OBSERVATION_VALUES)  # type: ignore[arg-type]

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_OBSERVATION, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PermissionObservationPolicyV2:
        return cls(*decode_canonical_bytes(payload, _OBSERVATION, field_count=8))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ResolvedPermissionPathV2(MssqlR1SecurityCanonicalModel):
    beneficiary: security.MssqlR1SecurityPrincipalRefV1
    grantor: security.MssqlR1SecurityPrincipalRefV1
    origin: security.MssqlR1PermissionOriginV1
    target: security.MssqlR1PermissionTargetV1
    permission: str
    effect: MssqlR1PermissionEffectV3
    grant_option: bool

    def __post_init__(self) -> None:
        if type(self.beneficiary) not in {
            security.MssqlR1EnvironmentPrincipalRefV1,
            security.MssqlR1SharedSignerPrincipalRefV1,
            security.MssqlR1BindingSignerInstancePrincipalRefV1,
        }:
            raise MssqlR1V3ContractError("permission beneficiary arm is forbidden")
        if type(self.grantor) is not security.MssqlR1EnvironmentPrincipalRefV1 or (
            self.grantor.subject_role is not MssqlR1SubjectRoleV3.PROVISIONER
        ):
            raise MssqlR1V3ContractError("permission grantor must be the provisioner")
        if type(self.origin) not in {
            security.MssqlR1DirectPermissionOriginV1,
            security.MssqlR1RolePermissionOriginV1,
            security.MssqlR1PublicPermissionOriginV1,
        } or (
            type(self.origin) is security.MssqlR1RolePermissionOriginV1
            and type(self.origin.role) is not security.MssqlR1NamedDatabaseRoleRefV1
        ):
            raise MssqlR1V3ContractError("permission origin arm is forbidden")
        if (
            type(self.target) is not security.MssqlR1PermissionTargetV1
            or type(self.effect) is not MssqlR1PermissionEffectV3
            or (type(self.grant_option) is not bool)
        ):
            raise MssqlR1V3ContractError("permission path discriminator is inexact")
        if type(self.permission) is not str:
            raise MssqlR1V3ContractError("permission name has an inexact text type")
        require_permission_name(self.permission)
        if self.effect is MssqlR1PermissionEffectV3.DENY and self.grant_option:
            raise MssqlR1V3ContractError("DENY cannot have grant option")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _PATH,
            (
                self.beneficiary.canonical_bytes,
                self.grantor.canonical_bytes,
                self.origin.canonical_bytes,
                self.target.canonical_bytes,
                self.permission,
                self.effect,
                self.grant_option,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ResolvedPermissionPathV2:
        values = list(decode_canonical_bytes(payload, _PATH, field_count=7))
        values[0] = security.decode_security_principal(expect_bytes(values[0], "beneficiary"))
        values[1] = security.decode_security_principal(expect_bytes(values[1], "grantor"))
        values[2] = security.decode_permission_origin(expect_bytes(values[2], "origin"))
        values[3] = security.MssqlR1PermissionTargetV1.from_canonical_bytes(expect_bytes(values[3], "target"))
        values[5] = expect_enum(MssqlR1PermissionEffectV3, values[5], "effect")
        return cls(*values)  # type: ignore[arg-type]


def project_schema_permission_rule(rule: MssqlR1PermissionRuleV3) -> MssqlR1ResolvedPermissionPathV2:
    if type(rule) is not MssqlR1PermissionRuleV3:
        raise MssqlR1V3ContractError("schema permission rule type is inexact")
    if (
        type(rule.subject_role) is not MssqlR1SubjectRoleV3
        or type(rule.grantor_role) is not MssqlR1SubjectRoleV3
        or type(rule.source) is not MssqlR1PermissionSourceV3
        or rule.source is not MssqlR1PermissionSourceV3.DIRECT
        or type(rule.scope) is not MssqlR1PermissionScopeV3
        or type(rule.effect) is not MssqlR1PermissionEffectV3
        or type(rule.grant_option) is not bool
    ):
        raise MssqlR1V3ContractError("schema permission rule fields are inexact")
    beneficiary: security.MssqlR1SecurityPrincipalRefV1
    if rule.subject_role in {MssqlR1SubjectRoleV3.ATTESTATION_MODULE, MssqlR1SubjectRoleV3.STAGE_OWNER_MODULE}:
        kind = (
            MssqlR1SignerProfileKindV3.ATTESTOR
            if rule.subject_role is MssqlR1SubjectRoleV3.ATTESTATION_MODULE
            else MssqlR1SignerProfileKindV3.STAGE_OWNER
        )
        beneficiary = security.MssqlR1SharedSignerPrincipalRefV1(kind)
    else:
        beneficiary = security.MssqlR1EnvironmentPrincipalRefV1(rule.subject_role)
    scope = _SCOPE_MAP[rule.scope]
    target = security.MssqlR1PermissionTargetV1(
        scope,
        rule.schema_name,
        rule.object_name,
        rule.column_name,
        scope in {MssqlR1PermissionTargetScopeV1.SCHEMA, MssqlR1PermissionTargetScopeV1.OBJECT},
    )
    return MssqlR1ResolvedPermissionPathV2(
        beneficiary,
        security.MssqlR1EnvironmentPrincipalRefV1(rule.grantor_role),
        security.MssqlR1DirectPermissionOriginV1(),
        target,
        rule.permission,
        rule.effect,
        rule.grant_option,
    )


@dataclass(frozen=True, slots=True)
class MssqlR1PermissionClosurePolicyV1(MssqlR1SecurityCanonicalModel):
    contract_version: Literal["dpone-mssql-r1-permission-closure-1"]
    physical_schema_descriptor_digest: bytes
    observation_policy: MssqlR1PermissionObservationPolicyV2
    expected_shared_source: Literal["schema2_exact_permission_rules"]
    expected_binding_source: Literal["resolved_binding_pack_exact_permission_rules"]
    observation_scope: Literal["complete_managed_principal_database_permissions"]
    unexpected_permission_disposition: Literal["block"]
    ordering: Literal["canonical_observation_bytes"]

    def __post_init__(self) -> None:
        require_exact_digest(self.physical_schema_descriptor_digest, "physical descriptor digest")
        literals = (
            self.contract_version,
            self.expected_shared_source,
            self.expected_binding_source,
            self.observation_scope,
            self.unexpected_permission_disposition,
            self.ordering,
        )
        if (
            type(self.observation_policy) is not MssqlR1PermissionObservationPolicyV2
            or any(type(value) is not str for value in literals)
            or literals != _CLOSURE_VALUES
        ):
            raise MssqlR1V3ContractError("permission closure policy is not exact")

    @classmethod
    def create(cls, descriptor: MssqlR1PhysicalSchemaDescriptorV1) -> MssqlR1PermissionClosurePolicyV1:
        if type(descriptor) is not MssqlR1PhysicalSchemaDescriptorV1 or descriptor.descriptor_version != VERSION:
            raise MssqlR1V3ContractError("permission closure requires the active R2 descriptor")
        result = cls(
            _CLOSURE_VALUES[0],  # type: ignore[arg-type]
            descriptor.digest,
            MssqlR1PermissionObservationPolicyV2.exact(),
            *_CLOSURE_VALUES[1:],  # type: ignore[arg-type]
        )
        result.validate_against(descriptor)
        return result

    def validate_against(self, descriptor: MssqlR1PhysicalSchemaDescriptorV1) -> None:
        if (
            type(descriptor) is not MssqlR1PhysicalSchemaDescriptorV1
            or descriptor.descriptor_version != VERSION
            or (descriptor.digest != self.physical_schema_descriptor_digest)
            or self.observation_policy.digest != descriptor.expected_schema_contract.permission_projection_policy_digest
        ):
            raise MssqlR1V3ContractError("permission closure authority differs from active descriptor")

    def validate_complete(
        self,
        descriptor: MssqlR1PhysicalSchemaDescriptorV1,
        binding_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
        observed_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
    ) -> tuple[MssqlR1ResolvedPermissionPathV2, ...]:
        self.validate_against(descriptor)
        for values, field in ((binding_paths, "binding paths"), (observed_paths, "observed paths")):
            if type(values) is not tuple or any(type(item) is not MssqlR1ResolvedPermissionPathV2 for item in values):
                raise MssqlR1V3ContractError(f"{field} are not exact")
            encoded = tuple(item.canonical_bytes for item in values)
            if encoded != tuple(sorted(encoded)) or len(set(encoded)) != len(encoded):
                raise MssqlR1V3ContractError(f"{field} are not canonical and unique")
        if any(not is_exact_binding_path(item) for item in binding_paths):
            raise MssqlR1V3ContractError("binding paths are not pre-resolved direct authority")
        shared = tuple(
            project_schema_permission_rule(item)
            for item in descriptor.expected_schema_contract.ordered_permission_rules
        )
        expected = tuple(sorted((*shared, *binding_paths), key=lambda item: item.canonical_bytes))
        if len({item.canonical_bytes for item in expected}) != len(expected) or observed_paths != expected:
            raise MssqlR1V3ContractError("permission complement is missing or contains forbidden paths")
        return expected

    @property
    def canonical_bytes(self) -> bytes:
        values = tuple(getattr(self, name) for name in self.__dataclass_fields__)
        return canonical_bytes(_CLOSURE, (*values[:2], self.observation_policy.canonical_bytes, *values[3:]))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PermissionClosurePolicyV1:
        values = list(decode_canonical_bytes(payload, _CLOSURE, field_count=8))
        values[2] = MssqlR1PermissionObservationPolicyV2.from_canonical_bytes(
            expect_bytes(values[2], "observation policy")
        )
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1BindingSignerLifecyclePolicyV2(MssqlR1SecurityCanonicalModel):
    contract_version: Literal["dpone-mssql-r1-binding-signer-policy-2"]
    physical_schema_descriptor_digest: bytes
    shared_install_security_profile_digest: bytes
    naming_profile: MssqlR1BindingSignerNamingProfileV1
    certificate_name_template: str
    certificate_user_name_template: str
    certificate_subject_template: str
    certificate_owner: security.MssqlR1EnvironmentPrincipalRefV1
    start_date_yyyymmdd: str
    expiry_date_yyyymmdd: str
    certificate_creation_profile: MssqlR1CertificateCreationProfileV1
    signature_algorithm: MssqlR1CertificateSignatureAlgorithmV1
    secret_policy_digest: bytes
    template_instantiation_profile: Literal["uuid_resolved_literal_identifiers_v1"]
    ordered_module_kinds: tuple[MssqlR1BindingModuleKindV1, ...]
    permission_profile: MssqlR1BindingPermissionProfileV1
    lifecycle_transition_set_digest: bytes

    def __post_init__(self) -> None:
        for name in (
            "physical_schema_descriptor_digest",
            "shared_install_security_profile_digest",
            "secret_policy_digest",
            "lifecycle_transition_set_digest",
        ):
            require_exact_digest(getattr(self, name), name)
        observed = (
            self.contract_version,
            self.naming_profile,
            self.certificate_name_template,
            self.certificate_user_name_template,
            self.certificate_subject_template,
            self.certificate_owner.subject_role
            if type(self.certificate_owner) is security.MssqlR1EnvironmentPrincipalRefV1
            else None,
            self.start_date_yyyymmdd,
            self.expiry_date_yyyymmdd,
            self.certificate_creation_profile,
            self.signature_algorithm,
            self.template_instantiation_profile,
            self.ordered_module_kinds,
            self.permission_profile,
        )
        if (
            tuple(type(value) for value in observed) != tuple(type(value) for value in _BINDING_VALUES)
            or observed != _BINDING_VALUES
            or any(type(item) is not MssqlR1BindingModuleKindV1 for item in self.ordered_module_kinds)
            or self.lifecycle_transition_set_digest != exact_lifecycle_transition_set_digest()
        ):
            raise MssqlR1V3ContractError("binding signer lifecycle policy is not exact")

    @property
    def canonical_bytes(self) -> bytes:
        values = tuple(getattr(self, name) for name in self.__dataclass_fields__)
        return canonical_bytes(_BINDING, (*values[:7], self.certificate_owner.canonical_bytes, *values[8:]))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1BindingSignerLifecyclePolicyV2:
        values = list(decode_canonical_bytes(payload, _BINDING, field_count=17))
        values[3] = expect_enum(MssqlR1BindingSignerNamingProfileV1, values[3], "naming profile")
        values[7] = cast(
            security.MssqlR1EnvironmentPrincipalRefV1,
            security.decode_security_principal(expect_bytes(values[7], "owner")),
        )
        values[10] = expect_enum(MssqlR1CertificateCreationProfileV1, values[10], "creation profile")
        values[11] = expect_enum(MssqlR1CertificateSignatureAlgorithmV1, values[11], "signature algorithm")
        values[14] = tuple(
            expect_enum(MssqlR1BindingModuleKindV1, item, "module kind")
            for item in expect_tuple(values[14], "module kinds")
        )
        values[15] = expect_enum(MssqlR1BindingPermissionProfileV1, values[15], "permission profile")
        return cls(*values)  # type: ignore[arg-type]
