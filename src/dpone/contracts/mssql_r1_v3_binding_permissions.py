"""Exact permission closure carried by a Binding V2 pack."""

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Literal
from uuid import UUID

from dpone.contracts.mssql_r1_v3_binding_modules import (
    MssqlR1BindingTargetMappingV2,
    MssqlR1InstantiatedBindingModuleV2,
    MssqlR1StageScanProjectionV2,
)
from dpone.contracts.mssql_r1_v3_codec import canonical_bytes
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import MssqlR1PhysicalResourceAccessV1
from dpone.contracts.mssql_r1_v3_provider_security_enums import (
    MssqlR1CertificateCreationProfileV1,
    MssqlR1CertificateSignatureAlgorithmV1,
    MssqlR1PermissionTargetScopeV1,
)
from dpone.contracts.mssql_r1_v3_provider_security_permissions import (
    MssqlR1BindingSignerLifecyclePolicyV2,
    MssqlR1ResolvedPermissionPathV2,
)
from dpone.contracts.mssql_r1_v3_provider_security_principals import (
    MssqlR1BindingSignerInstancePrincipalRefV1,
    MssqlR1DirectPermissionOriginV1,
    MssqlR1EnvironmentPrincipalRefV1,
    MssqlR1PermissionTargetV1,
)
from dpone.contracts.mssql_r1_v3_schema_security import MssqlR1PermissionEffectV3, MssqlR1SubjectRoleV3


@dataclass(frozen=True, slots=True)
class MssqlR1BindingSignerIdentityV2:
    contract_version: Literal["dpone-mssql-r1-binding-signer-identity-2"]
    target_binding_uuid: UUID
    lifecycle_policy: MssqlR1BindingSignerLifecyclePolicyV2
    certificate_name: str
    certificate_user_name: str
    certificate_subject: str
    certificate_owner: MssqlR1EnvironmentPrincipalRefV1
    start_date_yyyymmdd: str
    expiry_date_yyyymmdd: str
    certificate_creation_profile: MssqlR1CertificateCreationProfileV1
    signature_algorithm: MssqlR1CertificateSignatureAlgorithmV1
    secret_policy_digest: bytes

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-mssql-r1-binding-signer-identity-v2\0",
            (
                self.contract_version,
                self.target_binding_uuid,
                self.lifecycle_policy.canonical_bytes,
                self.certificate_name,
                self.certificate_user_name,
                self.certificate_subject,
                self.certificate_owner.canonical_bytes,
                self.start_date_yyyymmdd,
                self.expiry_date_yyyymmdd,
                self.certificate_creation_profile,
                self.signature_algorithm,
                self.secret_policy_digest,
            ),
        )

    @property
    def digest(self) -> bytes:
        return sha256(self.canonical_bytes).digest()


@dataclass(frozen=True, slots=True)
class MssqlR1BindingSignatureIntentV2:
    contract_version: Literal["dpone-mssql-r1-binding-signature-intent-2"]
    ordinal: int
    module_kind: MssqlR1BindingModuleKindV1
    semantic_module_digest: bytes
    signer_identity_digest: bytes

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-mssql-r1-binding-signature-intent-v2\0",
            (
                self.contract_version,
                self.ordinal,
                self.module_kind.value,
                self.semantic_module_digest,
                self.signer_identity_digest,
            ),
        )

    @property
    def digest(self) -> bytes:
        return sha256(self.canonical_bytes).digest()


@dataclass(frozen=True, slots=True)
class MssqlR1BindingAccessPermissionProjectionV2:
    contract_version: Literal["dpone-mssql-r1-binding-access-permission-projection-2"]
    source_access: MssqlR1PhysicalResourceAccessV1
    permission_path: MssqlR1ResolvedPermissionPathV2

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-mssql-r1-binding-access-permission-projection-v2\0",
            (self.contract_version, self.source_access.canonical_bytes, self.permission_path.canonical_bytes),
        )

    @property
    def digest(self) -> bytes:
        return sha256(self.canonical_bytes).digest()


@dataclass(frozen=True, slots=True)
class MssqlR1BindingPermissionContractV2:
    contract_version: Literal["dpone-mssql-r1-binding-permission-2"]
    permission_profile: Literal["exact_target_and_row_hash_v1"]
    ordered_access_projections: tuple[MssqlR1BindingAccessPermissionProjectionV2, ...]
    ordered_runtime_execute_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...]
    ordered_signature_intents: tuple[MssqlR1BindingSignatureIntentV2, ...]

    @property
    def ordered_permission_paths(self) -> tuple[Any, ...]:
        return tuple(
            sorted(
                (*(x.permission_path for x in self.ordered_access_projections), *self.ordered_runtime_execute_paths),
                key=lambda x: x.canonical_bytes,
            )
        )

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-mssql-r1-binding-permission-v2\0",
            (
                self.contract_version,
                self.permission_profile,
                tuple(x.canonical_bytes for x in self.ordered_access_projections),
                tuple(x.canonical_bytes for x in self.ordered_runtime_execute_paths),
                tuple(x.canonical_bytes for x in self.ordered_signature_intents),
            ),
        )

    @property
    def digest(self) -> bytes:
        return sha256(self.canonical_bytes).digest()


@dataclass(frozen=True, slots=True)
class MssqlR1ExpectedBindingInventoryV2:
    contract_version: Literal["dpone-mssql-r1-expected-binding-inventory-2"]
    target_binding_uuid: UUID
    ordered_module_names: tuple[str, ...]
    certificate_name: str
    certificate_user_name: str
    ordered_semantic_module_digests: tuple[bytes, ...]
    ordered_permission_path_digests: tuple[bytes, ...]
    ordered_signature_intent_digests: tuple[bytes, ...]
    permission_contract_digest: bytes

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-mssql-r1-expected-binding-inventory-v2\0",
            tuple(getattr(self, name) for name in self.__dataclass_fields__),
        )

    @property
    def digest(self) -> bytes:
        return sha256(self.canonical_bytes).digest()


@dataclass(frozen=True, slots=True)
class MssqlR1BindingInstantiationContractV2:
    contract_version: Literal["dpone-mssql-r1-binding-instantiation-2"]
    physical_descriptor_digest: bytes
    shared_security_profile_digest: bytes
    source_schema_authority_digest: bytes
    type_policy_authority_digest: bytes
    stable_target_authority_digest: bytes
    target_catalog_digest: bytes
    ordered_binding_template_digests: tuple[bytes, ...]
    stage_scan_template_digest: bytes
    stage_scan_request_authority_digest: bytes
    stage_scan_suffix_digest: bytes
    buffer_matrix_digest: bytes
    signer_lifecycle_policy: MssqlR1BindingSignerLifecyclePolicyV2
    permission_projection_profile: Literal["exact_target_and_row_hash_v1"]
    compiler_policy: Literal["pure_fail_closed_v2"]

    @property
    def canonical_bytes(self) -> bytes:
        values = tuple(getattr(self, name) for name in self.__dataclass_fields__)
        return canonical_bytes(
            b"dpone-mssql-r1-binding-instantiation-contract-v2\0",
            (*values[:12], self.signer_lifecycle_policy.canonical_bytes, *values[13:]),
        )

    @property
    def digest(self) -> bytes:
        return sha256(self.canonical_bytes).digest()


@dataclass(frozen=True, slots=True)
class MssqlR1BindingPortableIdentityV2:
    contract_version: Literal["dpone-mssql-r1-binding-portable-identity-2"]
    target_mapping: MssqlR1BindingTargetMappingV2
    ordered_stage_projections: tuple[MssqlR1StageScanProjectionV2, ...]
    ordered_modules: tuple[MssqlR1InstantiatedBindingModuleV2, ...]
    signer_identity: MssqlR1BindingSignerIdentityV2
    permission_contract: MssqlR1BindingPermissionContractV2
    expected_inventory: MssqlR1ExpectedBindingInventoryV2

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-mssql-r1-binding-portable-identity-v2\0",
            (
                self.contract_version,
                self.target_mapping.canonical_bytes,
                tuple(item.canonical_bytes for item in self.ordered_stage_projections),
                tuple(item.canonical_bytes for item in self.ordered_modules),
                self.signer_identity.canonical_bytes,
                self.permission_contract.canonical_bytes,
                self.expected_inventory.canonical_bytes,
            ),
        )

    @property
    def digest(self) -> bytes:
        return sha256(self.canonical_bytes).digest()


def derive_permissions(
    catalog: Any, modules: tuple[Any, ...], signatures: tuple[Any, ...]
) -> MssqlR1BindingPermissionContractV2:
    beneficiary = MssqlR1BindingSignerInstancePrincipalRefV1(catalog.target_binding_uuid)
    runtime = MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.RUNTIME)
    grantor = MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.PROVISIONER)
    origin = MssqlR1DirectPermissionOriginV1()

    def path(who: Any, schema: str, obj: str, permission: str) -> Any:
        target = MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.OBJECT, schema, obj, None, True)
        return MssqlR1ResolvedPermissionPathV2(
            who, grantor, origin, target, permission, MssqlR1PermissionEffectV3.GRANT, False
        )

    pairs = []
    for schema, obj, accesses in (
        (catalog.schema_name, catalog.object_name, modules[0].ordered_target_access_intents),
        ("dpone_authority", "dpone_target_row_hash_v3", modules[1].ordered_target_access_intents),
    ):
        for access in accesses:
            permission = {"read": "SELECT", "insert": "INSERT", "update": "UPDATE", "delete": "DELETE"}[
                access.access_kind.value
            ]
            pairs.append((access, path(beneficiary, schema, obj, permission)))
    projections = tuple(
        MssqlR1BindingAccessPermissionProjectionV2(
            "dpone-mssql-r1-binding-access-permission-projection-2", access, permission
        )
        for access, permission in sorted(pairs, key=lambda x: x[0].canonical_bytes)
    )
    runtime_paths = tuple(path(runtime, module.schema_name, module.object_name, "EXECUTE") for module in modules)
    return MssqlR1BindingPermissionContractV2(
        "dpone-mssql-r1-binding-permission-2", "exact_target_and_row_hash_v1", projections, runtime_paths, signatures
    )
