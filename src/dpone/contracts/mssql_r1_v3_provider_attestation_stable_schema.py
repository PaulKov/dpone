"""Stable schema projection and two-way core-object merge for Attestation V2."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from dpone.contracts.mssql_r1_v3_physical_schema_descriptor import MssqlR1PhysicalSchemaDescriptorV1
from dpone.contracts.mssql_r1_v3_provider_attestation_query_results import (  # noqa: F401
    MssqlR1CertificateCatalogObservationV1,
    MssqlR1ModuleQueryResultV1,
    MssqlR1ModuleSignatureObservationV3,
    MssqlR1ObservedBindingModuleV1,
    MssqlR1ObservedBindingPrefixObjectV1,
    MssqlR1ObservedBindingSignatureV1,
    MssqlR1ObservedBindingSignerV2,
    MssqlR1ObservedDatabaseIdentityV1,
    MssqlR1ObservedPrincipalV3,
    MssqlR1ObservedRoleMembershipV3,
    MssqlR1ObservedSchemaObjectV3,
    MssqlR1ObservedSchemaV3,
    MssqlR1SchemaQueryResultV1,
    MssqlR1SignatureQueryResultV1,
    MssqlR1TableQueryResultV1,
    Reason,
    attestation_fail,
    canonical_encoded_size_v1,
    decode_attestation_model,
    decode_nested_bytes,
    decode_nested_tuple,
    emit_canonical,
    object_coordinate,
    preflight_provider_attestation_canonical_v2,
    require_canonical_unique,
    require_digest32,
    require_nonzero_uuid,
    require_sql_positive,
)
from dpone.contracts.mssql_r1_v3_schema_attestation import MssqlR1SchemaAttestationV3
from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1SchemaObjectKindV3

CAP = 4194304
_CAP_QUERY = 3145728
_LIVE = b"dpone-mssql-r1-binding-live-catalog-identity-v2\0"
_SHARED = b"dpone-r1-stable-shared-security-attestation-v2\0"
_DOMAIN = b"dpone-r1-stable-schema-attestation-v2\0"
_INVENTORY = b"dpone-r1-stable-schema-observed-inventory-v2\0"
_MEMBERSHIP = b"dpone-r1-stable-schema-principal-membership-inventory-v2\0"
_LIVE = b"dpone-r1-stable-schema-live-identity-v2\0"


def stable_merge_provider_schema_v2(
    table_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...],
    core_module_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...],
    expected_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...],
) -> tuple[MssqlR1ObservedSchemaObjectV3, ...]:
    """Merge independently canonical table and core-module observations."""

    def checked(items: tuple[MssqlR1ObservedSchemaObjectV3, ...], kind: MssqlR1SchemaObjectKindV3) -> dict:
        mapping: dict[tuple[bytes, bytes], MssqlR1ObservedSchemaObjectV3] = {}
        for item in items:
            if item.portable_object.kind is not kind:
                attestation_fail(Reason.ATTESTATION_PROJECTION_MISMATCH)
            coordinate = object_coordinate(item)
            if coordinate in mapping:
                attestation_fail(Reason.ATTESTATION_PROJECTION_MISMATCH)
            mapping[coordinate] = item
        return mapping

    tables = checked(table_objects, MssqlR1SchemaObjectKindV3.TABLE)
    modules = checked(core_module_objects, MssqlR1SchemaObjectKindV3.PROCEDURE)
    if set(tables) & set(modules):
        attestation_fail(Reason.ATTESTATION_PROJECTION_MISMATCH)
    merged_keys = tuple(sorted((*tables, *modules)))
    merged = tuple({**tables, **modules}[key] for key in merged_keys)
    expected_keys = tuple(sorted(object_coordinate(item) for item in expected_objects))
    if merged_keys != expected_keys:
        attestation_fail(Reason.ATTESTATION_SCHEMA_INVENTORY_MISMATCH)
    expected_map = {object_coordinate(item): item for item in expected_objects}
    if any(item.canonical_bytes != expected_map[object_coordinate(item)].canonical_bytes for item in merged):
        attestation_fail(Reason.ATTESTATION_SCHEMA_INVENTORY_MISMATCH)
    return merged


def derive_stable_schema_digests(
    *,
    expected_contract_bytes: bytes,
    principal_authority_set_bytes: bytes,
    ordered_observed_schemas: tuple[MssqlR1ObservedSchemaV3, ...],
    ordered_observed_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...],
    ordered_observed_principals: tuple[MssqlR1ObservedPrincipalV3, ...],
    ordered_observed_role_memberships: tuple[MssqlR1ObservedRoleMembershipV3, ...],
    server_instance_identity_sha256: bytes,
    database_id: int,
    database_guid: UUID,
    database_family_guid: UUID,
    recovery_fork_guid: UUID,
) -> tuple[bytes, bytes, bytes, bytes, bytes]:
    inventory = hashlib.sha256(
        emit_canonical(
            _INVENTORY,
            (
                tuple(item.canonical_bytes for item in ordered_observed_schemas),
                tuple(item.canonical_bytes for item in ordered_observed_objects),
            ),
            CAP,
        )
    ).digest()
    membership = hashlib.sha256(
        emit_canonical(
            _MEMBERSHIP,
            (
                tuple(item.canonical_bytes for item in ordered_observed_principals),
                tuple(item.canonical_bytes for item in ordered_observed_role_memberships),
            ),
            CAP,
        )
    ).digest()
    live = hashlib.sha256(
        emit_canonical(
            _LIVE,
            (
                server_instance_identity_sha256,
                database_id,
                database_guid,
                database_family_guid,
                recovery_fork_guid,
                inventory,
                membership,
            ),
            CAP,
        )
    ).digest()
    return (
        hashlib.sha256(expected_contract_bytes).digest(),
        hashlib.sha256(principal_authority_set_bytes).digest(),
        inventory,
        membership,
        live,
    )


@dataclass(frozen=True, slots=True)
class MssqlR1StableSchemaAttestationV2:
    schema_contract_version: str
    target_binding_uuid: UUID
    registration_payload_digest: bytes
    registration_verification_receipt_digest: bytes
    registered_resolved_profile_digest: bytes
    registered_physical_authority_digest: bytes
    server_instance_identity_sha256: bytes
    database_id: int
    database_guid: UUID
    database_family_guid: UUID
    recovery_fork_guid: UUID
    expected_contract_bytes: bytes
    principal_authority_set_bytes: bytes
    ordered_observed_schemas: tuple[MssqlR1ObservedSchemaV3, ...]
    ordered_observed_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...]
    ordered_observed_principals: tuple[MssqlR1ObservedPrincipalV3, ...]
    ordered_observed_role_memberships: tuple[MssqlR1ObservedRoleMembershipV3, ...]
    expected_schema_contract_digest: bytes
    principal_authority_set_digest: bytes
    observed_schema_inventory_digest: bytes
    observed_principal_membership_inventory_digest: bytes
    live_identity_digest: bytes
    projection_revision: Literal[1]

    def __post_init__(self) -> None:
        require_nonzero_uuid(self.target_binding_uuid, "target_binding_uuid")
        for name in (
            "registration_payload_digest",
            "registration_verification_receipt_digest",
            "registered_resolved_profile_digest",
            "registered_physical_authority_digest",
            "server_instance_identity_sha256",
        ):
            require_digest32(getattr(self, name), name)
        require_sql_positive(self.database_id, "database_id")
        require_nonzero_uuid(self.database_guid, "database_guid")
        require_nonzero_uuid(self.database_family_guid, "database_family_guid")
        require_nonzero_uuid(self.recovery_fork_guid, "recovery_fork_guid")
        if self.projection_revision != 1:
            attestation_fail(Reason.ATTESTATION_PROJECTION_MISMATCH)
        if len(self.expected_contract_bytes) > 524288 or len(self.principal_authority_set_bytes) > 524288:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        derived = derive_stable_schema_digests(
            expected_contract_bytes=self.expected_contract_bytes,
            principal_authority_set_bytes=self.principal_authority_set_bytes,
            ordered_observed_schemas=self.ordered_observed_schemas,
            ordered_observed_objects=self.ordered_observed_objects,
            ordered_observed_principals=self.ordered_observed_principals,
            ordered_observed_role_memberships=self.ordered_observed_role_memberships,
            server_instance_identity_sha256=self.server_instance_identity_sha256,
            database_id=self.database_id,
            database_guid=self.database_guid,
            database_family_guid=self.database_family_guid,
            recovery_fork_guid=self.recovery_fork_guid,
        )
        actual = (
            self.expected_schema_contract_digest,
            self.principal_authority_set_digest,
            self.observed_schema_inventory_digest,
            self.observed_principal_membership_inventory_digest,
            self.live_identity_digest,
        )
        if actual != derived:
            attestation_fail(Reason.ATTESTATION_PROJECTION_MISMATCH)
        emit_canonical(_DOMAIN, self._fields(), CAP)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.schema_contract_version,
            self.target_binding_uuid,
            self.registration_payload_digest,
            self.registration_verification_receipt_digest,
            self.registered_resolved_profile_digest,
            self.registered_physical_authority_digest,
            self.server_instance_identity_sha256,
            self.database_id,
            self.database_guid,
            self.database_family_guid,
            self.recovery_fork_guid,
            self.expected_contract_bytes,
            self.principal_authority_set_bytes,
            tuple(item.canonical_bytes for item in self.ordered_observed_schemas),
            tuple(item.canonical_bytes for item in self.ordered_observed_objects),
            tuple(item.canonical_bytes for item in self.ordered_observed_principals),
            tuple(item.canonical_bytes for item in self.ordered_observed_role_memberships),
            self.expected_schema_contract_digest,
            self.principal_authority_set_digest,
            self.observed_schema_inventory_digest,
            self.observed_principal_membership_inventory_digest,
            self.live_identity_digest,
            self.projection_revision,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_DOMAIN, self._fields(), CAP)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1StableSchemaAttestationV2:
        values = list(decode_attestation_model(payload, _DOMAIN, 23, CAP))
        values[13] = decode_nested_tuple(values[13], MssqlR1ObservedSchemaV3.from_canonical_bytes, "schemas", maximum=8)
        values[14] = decode_nested_tuple(
            values[14], MssqlR1ObservedSchemaObjectV3.from_canonical_bytes, "objects", maximum=64
        )
        values[15] = decode_nested_tuple(
            values[15], MssqlR1ObservedPrincipalV3.from_canonical_bytes, "principals", maximum=32
        )
        values[16] = decode_nested_tuple(
            values[16], MssqlR1ObservedRoleMembershipV3.from_canonical_bytes, "memberships", maximum=32
        )
        return cls(*values)  # type: ignore[arg-type]

    @classmethod
    def create(
        cls,
        *,
        source_schema_attestation: MssqlR1SchemaAttestationV3,
        schema_query_result: MssqlR1SchemaQueryResultV1,
        table_query_result: MssqlR1TableQueryResultV1,
        module_query_result: MssqlR1ModuleQueryResultV1,
        signature_query_result: MssqlR1SignatureQueryResultV1,
        active_physical_descriptor: MssqlR1PhysicalSchemaDescriptorV1,
    ) -> MssqlR1StableSchemaAttestationV2:
        if type(source_schema_attestation) is not MssqlR1SchemaAttestationV3:
            attestation_fail(Reason.ATTESTATION_AUTHORITY_SPLICE)
        if source_schema_attestation.projection_revision != 1:
            attestation_fail(Reason.ATTESTATION_PROJECTION_MISMATCH)
        identity = schema_query_result.database_identity
        if (
            identity.server_instance_identity_sha256 != source_schema_attestation.server_instance_identity_sha256
            or identity.database_id != source_schema_attestation.database_id
            or identity.database_guid != source_schema_attestation.database_guid
            or identity.database_family_guid != source_schema_attestation.database_family_guid
            or identity.recovery_fork_guid != source_schema_attestation.recovery_fork_guid
        ):
            attestation_fail(Reason.ATTESTATION_TARGET_IDENTITY_MISMATCH)
        if (
            schema_query_result.ordered_schemas != source_schema_attestation.ordered_observed_schemas
            or schema_query_result.ordered_principals != source_schema_attestation.ordered_observed_principals
            or schema_query_result.ordered_role_memberships
            != source_schema_attestation.ordered_observed_role_memberships
        ):
            attestation_fail(Reason.ATTESTATION_SCHEMA_INVENTORY_MISMATCH)
        merged = stable_merge_provider_schema_v2(
            table_query_result.ordered_table_objects,
            module_query_result.ordered_core_module_objects,
            source_schema_attestation.ordered_observed_objects,
        )
        expected = tuple(
            item.canonical_bytes for item in active_physical_descriptor.expected_schema_contract.ordered_objects
        )
        if tuple(item.portable_object.canonical_bytes for item in merged) != expected:
            attestation_fail(Reason.ATTESTATION_SCHEMA_INVENTORY_MISMATCH)
        flatten = tuple(signature for item in merged for signature in item.ordered_signatures)
        if flatten != signature_query_result.ordered_core_signatures:
            attestation_fail(Reason.ATTESTATION_PROJECTION_MISMATCH)
        derived = derive_stable_schema_digests(
            expected_contract_bytes=source_schema_attestation.expected_contract_bytes,
            principal_authority_set_bytes=source_schema_attestation.principal_authority_set_bytes,
            ordered_observed_schemas=source_schema_attestation.ordered_observed_schemas,
            ordered_observed_objects=merged,
            ordered_observed_principals=source_schema_attestation.ordered_observed_principals,
            ordered_observed_role_memberships=source_schema_attestation.ordered_observed_role_memberships,
            server_instance_identity_sha256=source_schema_attestation.server_instance_identity_sha256,
            database_id=source_schema_attestation.database_id,
            database_guid=source_schema_attestation.database_guid,
            database_family_guid=source_schema_attestation.database_family_guid,
            recovery_fork_guid=source_schema_attestation.recovery_fork_guid,
        )
        return cls(
            source_schema_attestation.schema_contract_version,
            source_schema_attestation.target_binding_uuid,
            source_schema_attestation.registration_payload_digest,
            source_schema_attestation.registration_verification_receipt_digest,
            source_schema_attestation.registered_resolved_profile_digest,
            source_schema_attestation.registered_physical_authority_digest,
            source_schema_attestation.server_instance_identity_sha256,
            source_schema_attestation.database_id,
            source_schema_attestation.database_guid,
            source_schema_attestation.database_family_guid,
            source_schema_attestation.recovery_fork_guid,
            source_schema_attestation.expected_contract_bytes,
            source_schema_attestation.principal_authority_set_bytes,
            source_schema_attestation.ordered_observed_schemas,
            merged,
            source_schema_attestation.ordered_observed_principals,
            source_schema_attestation.ordered_observed_role_memberships,
            *derived,
            1,
        )


@dataclass(frozen=True, slots=True)
class MssqlR1BindingLiveCatalogIdentityV2:
    database_identity: object
    ordered_modules: tuple[MssqlR1ObservedBindingModuleV1, ...]
    signer: MssqlR1ObservedBindingSignerV2
    ordered_signatures: tuple[MssqlR1ObservedBindingSignatureV1, ...]
    ordered_binding_prefix_inventory: tuple[MssqlR1ObservedBindingPrefixObjectV1, ...]

    def __post_init__(self) -> None:
        if (
            len(self.ordered_modules) != 6
            or len(self.ordered_signatures) != 6
            or len(self.ordered_binding_prefix_inventory) > 64
        ):
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        emit_canonical(_LIVE, self._fields(), _CAP_QUERY)

    def _fields(self) -> tuple[object, ...]:
        return (
            self.database_identity.canonical_bytes,  # type: ignore[attr-defined]
            tuple(item.canonical_bytes for item in self.ordered_modules),
            self.signer.canonical_bytes,
            tuple(item.canonical_bytes for item in self.ordered_signatures),
            tuple(item.canonical_bytes for item in self.ordered_binding_prefix_inventory),
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_LIVE, self._fields(), _CAP_QUERY)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1BindingLiveCatalogIdentityV2:
        values = list(decode_attestation_model(payload, _LIVE, 5, _CAP_QUERY))
        values[0] = decode_nested_bytes(values[0], MssqlR1ObservedDatabaseIdentityV1.from_canonical_bytes, "identity")
        values[1] = decode_nested_tuple(
            values[1], MssqlR1ObservedBindingModuleV1.from_canonical_bytes, "modules", maximum=6, exact=6
        )
        values[2] = decode_nested_bytes(values[2], MssqlR1ObservedBindingSignerV2.from_canonical_bytes, "signer")
        values[3] = decode_nested_tuple(
            values[3], MssqlR1ObservedBindingSignatureV1.from_canonical_bytes, "signatures", maximum=6, exact=6
        )
        values[4] = decode_nested_tuple(
            values[4], MssqlR1ObservedBindingPrefixObjectV1.from_canonical_bytes, "prefix", maximum=64
        )
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1StableSharedSecurityAttestationV2:
    ordered_certificate_observations: tuple[MssqlR1CertificateCatalogObservationV1, ...]
    ordered_forbidden_membership_observations: tuple[MssqlR1ObservedRoleMembershipV3, ...]

    def __post_init__(self) -> None:
        if len(self.ordered_certificate_observations) != 2 or len(self.ordered_forbidden_membership_observations) > 32:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        emit_canonical(_SHARED, self._fields(), _CAP_QUERY)

    def _fields(self) -> tuple[object, ...]:
        return (
            tuple(item.canonical_bytes for item in self.ordered_certificate_observations),
            tuple(item.canonical_bytes for item in self.ordered_forbidden_membership_observations),
        )

    @property
    def canonical_bytes(self) -> bytes:
        return emit_canonical(_SHARED, self._fields(), _CAP_QUERY)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1StableSharedSecurityAttestationV2:
        certs, memberships = decode_attestation_model(payload, _SHARED, 2, _CAP_QUERY)
        return cls(
            decode_nested_tuple(
                certs, MssqlR1CertificateCatalogObservationV1.from_canonical_bytes, "certs", maximum=2, exact=2
            ),
            decode_nested_tuple(
                memberships, MssqlR1ObservedRoleMembershipV3.from_canonical_bytes, "memberships", maximum=32
            ),
        )


__all__ = (
    "MssqlR1BindingLiveCatalogIdentityV2",
    "MssqlR1PhysicalSchemaDescriptorV1",
    "MssqlR1SchemaAttestationV3",
    "MssqlR1StableSchemaAttestationV2",
    "MssqlR1StableSharedSecurityAttestationV2",
    "canonical_encoded_size_v1",
    "derive_stable_schema_digests",
    "preflight_provider_attestation_canonical_v2",
    "stable_merge_provider_schema_v2",
)
