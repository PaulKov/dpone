"""Stable catalog, binding, security and permission-closure aggregates."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Literal

from dpone.contracts.mssql_r1_v3_binding_pack import MssqlR1BindingModulePackV2
from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import (
    QUERY_KIND_ORDER,
    RESULT_TYPES,
    MssqlR1BindingPrefixInventoryQueryResultV1,
    MssqlR1CertificateQueryResultV2,
    MssqlR1CertifiedServerBuildProfileV1,
    MssqlR1MigrationTargetRefV1,
    MssqlR1ModuleQueryResultV1,
    MssqlR1PermissionQueryResultV2,
    MssqlR1PostInstallStatementResultV2,
    MssqlR1ProviderAttestationStatementRegistryV2,
    MssqlR1ResolvedPermissionPathV2,
    MssqlR1SchemaQueryResultV1,
    MssqlR1SignatureQueryResultV1,
    MssqlR1TableQueryResultV1,
    project_schema_permission_rule,
)
from dpone.contracts.mssql_r1_v3_provider_attestation_stable_schema import (  # noqa: F401
    MssqlR1BindingLiveCatalogIdentityV2,
    MssqlR1PhysicalSchemaDescriptorV1,
    MssqlR1SchemaAttestationV3,
    MssqlR1StableSchemaAttestationV2,
    MssqlR1StableSharedSecurityAttestationV2,
    Reason,
    attestation_fail,
    canonical_encoded_size_v1,
    decode_attestation_model,
    decode_nested_bytes,
    decode_nested_tuple,
    emit_canonical,
    preflight_provider_attestation_canonical_v2,
    require_canonical_unique,
    require_digest32,
)
from dpone.contracts.mssql_r1_v3_provider_security_profile import MssqlR1SharedInstallSecurityProfileV2

CAP_QUERY = 3145728
CAP_BINDING = 4194304
CAP_CATALOG = 12582912
_CLOSURE = b"dpone-r1-security-permission-closure-result-v2\0"
_BINDING = b"dpone-r1-stable-binding-attestation-v2\0"
_CATALOG = b"dpone-r1-stable-catalog-attestation-v2\0"
_RESULT_ORDER = (
    MssqlR1SchemaQueryResultV1,
    MssqlR1TableQueryResultV1,
    MssqlR1ModuleQueryResultV1,
    MssqlR1CertificateQueryResultV2,
    MssqlR1PermissionQueryResultV2,
    MssqlR1SignatureQueryResultV1,
    MssqlR1BindingPrefixInventoryQueryResultV1,
)


def _unique_paths(paths: tuple[MssqlR1ResolvedPermissionPathV2, ...]) -> tuple[MssqlR1ResolvedPermissionPathV2, ...]:
    require_canonical_unique(paths, lambda item: item.canonical_bytes, "paths")
    return paths


@dataclass(frozen=True, slots=True)
class MssqlR1PermissionClosureResultV2:
    observation_policy_digest: bytes
    ordered_expected_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...]
    ordered_observed_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...]
    ordered_missing_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...]
    ordered_forbidden_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...]

    def __post_init__(self) -> None:
        require_digest32(self.observation_policy_digest, "observation_policy_digest")
        for name in (
            "ordered_expected_paths",
            "ordered_observed_paths",
            "ordered_missing_paths",
            "ordered_forbidden_paths",
        ):
            values = getattr(self, name)
            if len(values) > 256:
                attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
            _unique_paths(values)
        expected = {item.canonical_bytes for item in self.ordered_expected_paths}
        observed = {item.canonical_bytes for item in self.ordered_observed_paths}
        missing = tuple(item for item in self.ordered_expected_paths if item.canonical_bytes not in observed)
        forbidden = tuple(item for item in self.ordered_observed_paths if item.canonical_bytes not in expected)
        if missing != self.ordered_missing_paths or forbidden != self.ordered_forbidden_paths:
            attestation_fail(Reason.ATTESTATION_PERMISSION_CLOSURE_MISMATCH)
        emit_canonical(_CLOSURE, self._fields(), CAP_QUERY)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.observation_policy_digest,
            tuple(item.canonical_bytes for item in self.ordered_expected_paths),
            tuple(item.canonical_bytes for item in self.ordered_observed_paths),
            tuple(item.canonical_bytes for item in self.ordered_missing_paths),
            tuple(item.canonical_bytes for item in self.ordered_forbidden_paths),
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_CLOSURE, self._fields(), CAP_QUERY)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1PermissionClosureResultV2:
        values = list(decode_attestation_model(payload, _CLOSURE, 5, CAP_QUERY))
        for index in range(1, 5):
            values[index] = decode_nested_tuple(
                values[index], MssqlR1ResolvedPermissionPathV2.from_canonical_bytes, "paths", maximum=256
            )
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1StableBindingAttestationV2:
    binding_pack_digest: bytes
    live_catalog_identity: MssqlR1BindingLiveCatalogIdentityV2

    def __post_init__(self) -> None:
        require_digest32(self.binding_pack_digest, "binding_pack_digest")
        if type(self.live_catalog_identity) is not MssqlR1BindingLiveCatalogIdentityV2:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        emit_canonical(_BINDING, (self.binding_pack_digest, self.live_catalog_identity.canonical_bytes), CAP_BINDING)

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(
            _BINDING, (self.binding_pack_digest, self.live_catalog_identity.canonical_bytes), CAP_BINDING
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1StableBindingAttestationV2:
        digest, live = decode_attestation_model(payload, _BINDING, 2, CAP_BINDING)
        return cls(digest, decode_nested_bytes(live, MssqlR1BindingLiveCatalogIdentityV2.from_canonical_bytes, "live"))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1StableCatalogAttestationV2:
    attestation_version: Literal["dpone-r1-stable-catalog-attestation-2"]
    target_ref: MssqlR1MigrationTargetRefV1
    provider_contract_digest: bytes
    physical_schema_descriptor_digest: bytes
    renderer_registry_digest: bytes
    ordered_statement_results: tuple[MssqlR1PostInstallStatementResultV2, ...]
    stable_schema_attestation: MssqlR1StableSchemaAttestationV2
    stable_shared_security_attestation: MssqlR1StableSharedSecurityAttestationV2
    stable_binding_attestation: MssqlR1StableBindingAttestationV2
    permission_closure_result: MssqlR1PermissionClosureResultV2

    def __post_init__(self) -> None:
        if self.attestation_version != "dpone-r1-stable-catalog-attestation-2":
            attestation_fail(Reason.ATTESTATION_PROJECTION_MISMATCH)
        require_digest32(self.provider_contract_digest, "provider_contract_digest")
        require_digest32(self.physical_schema_descriptor_digest, "physical_schema_descriptor_digest")
        require_digest32(self.renderer_registry_digest, "renderer_registry_digest")
        if len(self.ordered_statement_results) != 7:
            attestation_fail(Reason.ATTESTATION_REGISTRY_MISMATCH)
        if tuple(item.attestation_kind for item in self.ordered_statement_results) != QUERY_KIND_ORDER:
            attestation_fail(Reason.ATTESTATION_RESULT_ARM_MISMATCH)
        if tuple(type(item.typed_result) for item in self.ordered_statement_results) != _RESULT_ORDER:
            attestation_fail(Reason.ATTESTATION_RESULT_ARM_MISMATCH)
        emit_canonical(_CATALOG, self._fields(), CAP_CATALOG)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.attestation_version,
            self.target_ref.canonical_bytes,
            self.provider_contract_digest,
            self.physical_schema_descriptor_digest,
            self.renderer_registry_digest,
            tuple(item.canonical_bytes for item in self.ordered_statement_results),
            self.stable_schema_attestation.canonical_bytes,
            self.stable_shared_security_attestation.canonical_bytes,
            self.stable_binding_attestation.canonical_bytes,
            self.permission_closure_result.canonical_bytes,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_CATALOG, self._fields(), CAP_CATALOG)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1StableCatalogAttestationV2:
        values = list(decode_attestation_model(payload, _CATALOG, 10, CAP_CATALOG))
        values[1] = decode_nested_bytes(values[1], MssqlR1MigrationTargetRefV1.from_canonical_bytes, "target")
        values[5] = decode_nested_tuple(
            values[5], MssqlR1PostInstallStatementResultV2.from_canonical_bytes, "results", maximum=7, exact=7
        )
        values[6] = decode_nested_bytes(values[6], MssqlR1StableSchemaAttestationV2.from_canonical_bytes, "schema")
        values[7] = decode_nested_bytes(
            values[7], MssqlR1StableSharedSecurityAttestationV2.from_canonical_bytes, "shared"
        )
        values[8] = decode_nested_bytes(values[8], MssqlR1StableBindingAttestationV2.from_canonical_bytes, "binding")
        values[9] = decode_nested_bytes(values[9], MssqlR1PermissionClosureResultV2.from_canonical_bytes, "closure")
        return cls(*values)  # type: ignore[arg-type]

    @classmethod
    def create(
        cls,
        *,
        expected_target_ref: MssqlR1MigrationTargetRefV1,
        provider_contract_digest: bytes,
        physical_schema_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
        security_profile: MssqlR1SharedInstallSecurityProfileV2,
        binding_pack: MssqlR1BindingModulePackV2,
        expected_build_profile: MssqlR1CertifiedServerBuildProfileV1,
        statement_registry: MssqlR1ProviderAttestationStatementRegistryV2,
        ordered_statement_results: tuple[MssqlR1PostInstallStatementResultV2, ...],
        source_schema_attestation: MssqlR1SchemaAttestationV3,
    ) -> MssqlR1StableCatalogAttestationV2:
        qs, qt, qm, qc, qp, qg, qb = _typed_results(ordered_statement_results, statement_registry)
        schema = MssqlR1StableSchemaAttestationV2.create(
            source_schema_attestation=source_schema_attestation,
            schema_query_result=qs,
            table_query_result=qt,
            module_query_result=qm,
            signature_query_result=qg,
            active_physical_descriptor=physical_schema_descriptor,
        )
        forbidden = qs.ordered_role_memberships
        shared = MssqlR1StableSharedSecurityAttestationV2(qc.ordered_shared_certificates, forbidden)
        live = MssqlR1BindingLiveCatalogIdentityV2(
            qs.database_identity,
            qm.ordered_binding_modules,
            qc.binding_signer,
            qg.ordered_binding_signatures,
            qb.ordered_binding_prefix_inventory,
        )
        binding = MssqlR1StableBindingAttestationV2(binding_pack.digest, live)
        expected_paths = _expected_paths(physical_schema_descriptor, binding_pack)
        observed_paths = qp.ordered_observed_permission_paths
        observed_keys = {path.canonical_bytes for path in observed_paths}
        expected_keys = {path.canonical_bytes for path in expected_paths}
        policy = hashlib.sha256(security_profile.permission_closure_policy.observation_policy.canonical_bytes).digest()
        closure = MssqlR1PermissionClosureResultV2(
            policy,
            expected_paths,
            observed_paths,
            tuple(item for item in expected_paths if item.canonical_bytes not in observed_keys),
            tuple(item for item in observed_paths if item.canonical_bytes not in expected_keys),
        )
        catalog = cls(
            "dpone-r1-stable-catalog-attestation-2",
            expected_target_ref,
            provider_contract_digest,
            physical_schema_descriptor.digest,
            statement_registry.renderer_registry_digest,
            ordered_statement_results,
            schema,
            shared,
            binding,
            closure,
        )
        validate_provider_attestation_against_authorities_v2(
            catalog=catalog,
            active_physical_descriptor=physical_schema_descriptor,
            active_security_profile=security_profile,
            active_binding_pack=binding_pack,
            expected_target_ref=expected_target_ref,
            expected_build_profile=expected_build_profile,
            source_schema_attestation=source_schema_attestation,
            expected_statement_registry=statement_registry,
        )
        return catalog


def _typed_results(
    results: tuple[MssqlR1PostInstallStatementResultV2, ...],
    registry: MssqlR1ProviderAttestationStatementRegistryV2,
) -> tuple:
    if len(results) != 7 or len(registry.ordered_statements) != 7:
        attestation_fail(Reason.ATTESTATION_REGISTRY_MISMATCH)
    typed = []
    for result, statement, expected_type in zip(results, registry.ordered_statements, _RESULT_ORDER, strict=True):
        if (
            result.statement_ref != statement.statement_ref
            or result.attestation_kind is not statement.attestation_kind
            or result.result_authority_kind is not statement.result_authority_kind
            or result.query_definition_digest != statement.query_definition_digest()
            or type(result.typed_result) is not expected_type
            or type(result.typed_result) is not RESULT_TYPES[statement.attestation_kind]
        ):
            attestation_fail(Reason.ATTESTATION_RESULT_ARM_MISMATCH)
        typed.append(result.typed_result)
    return tuple(typed)


def _expected_paths(descriptor: MssqlR1PhysicalSchemaDescriptorV1, pack: MssqlR1BindingModulePackV2):
    merged = sorted(
        (
            *(
                project_schema_permission_rule(rule)
                for rule in descriptor.expected_schema_contract.ordered_permission_rules
            ),
            *pack.portable_identity.permission_contract.ordered_permission_paths,
        ),
        key=lambda item: item.canonical_bytes,
    )
    seen: set[bytes] = set()
    unique: list[MssqlR1ResolvedPermissionPathV2] = []
    for item in merged:
        digest = item.canonical_bytes
        if digest not in seen:
            seen.add(digest)
            unique.append(item)
    return tuple(unique)


def validate_provider_attestation_against_authorities_v2(
    *,
    catalog: MssqlR1StableCatalogAttestationV2 | None = None,
    active_physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
    active_security_profile: MssqlR1SharedInstallSecurityProfileV2,
    active_binding_pack: MssqlR1BindingModulePackV2,
    expected_target_ref: MssqlR1MigrationTargetRefV1,
    expected_build_profile: MssqlR1CertifiedServerBuildProfileV1,
    source_schema_attestation: MssqlR1SchemaAttestationV3,
    expected_statement_registry: MssqlR1ProviderAttestationStatementRegistryV2,
) -> None:
    """Prove one catalog against the exact typed descriptor/Security/Binding authorities."""

    if catalog is None:
        attestation_fail(Reason.ATTESTATION_AUTHORITY_SPLICE)
    assert catalog is not None
    parts: tuple[Any, ...] = tuple(item.typed_result for item in catalog.ordered_statement_results)
    qs, _qt, qm, qc, qp, qg, qb = parts
    if (
        type(active_physical_descriptor) is not MssqlR1PhysicalSchemaDescriptorV1
        or type(active_security_profile) is not MssqlR1SharedInstallSecurityProfileV2
        or type(active_binding_pack) is not MssqlR1BindingModulePackV2
    ):
        attestation_fail(Reason.ATTESTATION_AUTHORITY_SPLICE)
    if (
        catalog.physical_schema_descriptor_digest != active_physical_descriptor.digest
        or active_security_profile.physical_schema_descriptor_digest != active_physical_descriptor.digest
        or catalog.stable_binding_attestation.binding_pack_digest != active_binding_pack.digest
        or catalog.stable_schema_attestation.expected_contract_bytes
        != active_physical_descriptor.expected_schema_contract.canonical_bytes
        or catalog.renderer_registry_digest != expected_statement_registry.renderer_registry_digest
        or catalog.target_ref != expected_target_ref
    ):
        attestation_fail(Reason.ATTESTATION_AUTHORITY_SPLICE)
    identity = qs.database_identity
    if identity.certified_server_build_profile != expected_build_profile or not identity.matches_target_ref(
        expected_target_ref
    ):
        attestation_fail(Reason.ATTESTATION_TARGET_IDENTITY_MISMATCH)
    if catalog.stable_schema_attestation.expected_contract_bytes != source_schema_attestation.expected_contract_bytes:
        attestation_fail(Reason.ATTESTATION_AUTHORITY_SPLICE)
    policy = hashlib.sha256(
        active_security_profile.permission_closure_policy.observation_policy.canonical_bytes
    ).digest()
    closure = catalog.permission_closure_result
    shared = catalog.stable_shared_security_attestation
    if (
        policy != closure.observation_policy_digest
        or policy != active_physical_descriptor.expected_schema_contract.permission_projection_policy_digest
        or closure.ordered_observed_paths != qp.ordered_observed_permission_paths
        or closure.ordered_expected_paths != _expected_paths(active_physical_descriptor, active_binding_pack)
        or closure.ordered_missing_paths
        or closure.ordered_forbidden_paths
    ):
        attestation_fail(Reason.ATTESTATION_PERMISSION_CLOSURE_MISMATCH)
    if shared.ordered_forbidden_membership_observations != qs.ordered_role_memberships:
        attestation_fail(Reason.ATTESTATION_PROJECTION_MISMATCH)
    if (
        shared.ordered_forbidden_membership_observations
        or shared.ordered_certificate_observations != qc.ordered_shared_certificates
    ):
        attestation_fail(Reason.ATTESTATION_CERTIFICATE_MISMATCH)
    inventory = active_binding_pack.portable_identity.expected_inventory
    modules = qm.ordered_binding_modules
    kinds = tuple(item.module_kind for item in active_binding_pack.portable_identity.ordered_modules)
    if (
        tuple(item.module_kind for item in modules) != kinds
        or tuple(item.object_name for item in modules) != inventory.ordered_module_names
        or tuple(item.signature_intent_digest for item in qg.ordered_binding_signatures)
        != inventory.ordered_signature_intent_digests
        or qb.ordered_binding_prefix_inventory
        != catalog.stable_binding_attestation.live_catalog_identity.ordered_binding_prefix_inventory
    ):
        attestation_fail(Reason.ATTESTATION_BINDING_INVENTORY_MISMATCH)
    _typed_results(catalog.ordered_statement_results, expected_statement_registry)


__all__ = (
    "MssqlR1BindingLiveCatalogIdentityV2",
    "MssqlR1PermissionClosureResultV2",
    "MssqlR1StableSharedSecurityAttestationV2",
    "MssqlR1StableBindingAttestationV2",
    "MssqlR1StableCatalogAttestationV2",
    "canonical_encoded_size_v1",
    "preflight_provider_attestation_canonical_v2",
    "validate_provider_attestation_against_authorities_v2",
)
