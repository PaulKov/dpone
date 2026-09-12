"""Portable schema contract and registration-bound live attestation."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, fields
from dataclasses import field as dataclass_field
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from dpone.contracts.mssql_r1_v3_schema_attestation_identity import (
    derive_attestation_digests,
    with_derived_attestation_fields,
)
from dpone.contracts.mssql_r1_v3_schema_attestation_validation import (
    MssqlR1ObservedPermissionV3,
    MssqlR1ObservedPrincipalV3,
    MssqlR1ObservedRoleMembershipV3,
    MssqlR1ObservedSchemaObjectV3,
    MssqlR1ObservedSchemaV3,
    assert_attestation_for_registration,
    registration_from_verification,
    validate_attestation_projection,
    validate_observation_types,
)
from dpone.contracts.mssql_r1_v3_schema_modules import MssqlR1PortableSchemaObjectV3
from dpone.contracts.mssql_r1_v3_schema_primitives import (
    MssqlR1SignerProfileKindV3,
    MssqlR1SupportedCodecEntryV3,
    MssqlR1V3ContractError,
    canonical_bytes,
    canonical_utc_text,
    decode_canonical_bytes,
    decode_members,
    expect_text,
    parse_canonical_utc_text,
    require_canonical_set,
    require_digest,
    require_positive,
    require_schema_identifier,
    require_sql_int,
    require_uuid,
)
from dpone.contracts.mssql_r1_v3_schema_security import (
    MssqlR1PermissionRuleV3,
    MssqlR1PrincipalAuthoritySetV3,
    MssqlR1SignerProfileV3,
)

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_registration import MssqlTargetRegistrationVerificationV1


MSSQL_R1_SCHEMA_CONTRACT_VERSION = "dpone-mssql-r1-v3-schema-2"
_CONTRACT = b"dpone-r1-schema-contract-v3-schema-2\0"
_ATTESTATION = b"dpone-r1-schema-attestation-v3-schema-2\0"
_ATTESTATION_IDENTITY = b"dpone-r1-schema-attestation-identity-v3-schema-2\0"


@dataclass(frozen=True, slots=True)
class MssqlR1SchemaContractV3:
    schema_contract_version: str = dataclass_field(default=MSSQL_R1_SCHEMA_CONTRACT_VERSION, init=False)
    authority_schema_name: str
    stage_schema_name: str
    permission_projection_policy_digest: bytes
    ordered_objects: tuple[MssqlR1PortableSchemaObjectV3, ...]
    ordered_permission_rules: tuple[MssqlR1PermissionRuleV3, ...]
    ordered_signer_profiles: tuple[MssqlR1SignerProfileV3, ...]
    ordered_supported_codecs: tuple[MssqlR1SupportedCodecEntryV3, ...]

    def __post_init__(self) -> None:
        if self.schema_contract_version != MSSQL_R1_SCHEMA_CONTRACT_VERSION:
            raise MssqlR1V3ContractError("schema contract version is unsupported")
        require_schema_identifier(self.authority_schema_name, "authority schema name")
        require_schema_identifier(self.stage_schema_name, "stage schema name")
        if self.authority_schema_name == self.stage_schema_name:
            raise MssqlR1V3ContractError("managed schema names must be distinct")
        require_digest(self.permission_projection_policy_digest, "permission projection policy digest")
        for values, contract, field in (
            (self.ordered_permission_rules, MssqlR1PermissionRuleV3, "permission rules"),
            (self.ordered_signer_profiles, MssqlR1SignerProfileV3, "signer profiles"),
            (self.ordered_supported_codecs, MssqlR1SupportedCodecEntryV3, "supported codecs"),
        ):
            require_canonical_set(values, contract, field)
            if field != "signer profiles" and not values:
                raise MssqlR1V3ContractError(f"{field} must be nonempty")
        if not isinstance(self.ordered_objects, tuple) or not all(
            isinstance(item, MssqlR1PortableSchemaObjectV3) for item in self.ordered_objects
        ):
            raise MssqlR1V3ContractError("ordered objects must be a typed tuple")
        if not self.ordered_objects:
            raise MssqlR1V3ContractError("ordered objects must be nonempty")
        managed_schemas = {self.authority_schema_name, self.stage_schema_name}
        if any(item.schema_name not in managed_schemas for item in self.ordered_objects):
            raise MssqlR1V3ContractError("portable object is outside the managed schemas")
        coordinates = tuple((item.schema_name.encode(), item.object_name.encode()) for item in self.ordered_objects)
        if coordinates != tuple(sorted(coordinates)) or len(set(coordinates)) != len(coordinates):
            raise MssqlR1V3ContractError("portable objects must use strict coordinate order")
        if tuple(profile.signer_profile for profile in self.ordered_signer_profiles) != (
            MssqlR1SignerProfileKindV3.ATTESTOR,
            MssqlR1SignerProfileKindV3.STAGE_OWNER,
        ):
            raise MssqlR1V3ContractError("signer profiles must be exactly attestor and stage_owner")
        if (
            len({item.certificate_name for item in self.ordered_signer_profiles}) != 2
            or len({item.certificate_user_name for item in self.ordered_signer_profiles}) != 2
        ):
            raise MssqlR1V3ContractError("signer profiles must map one-to-one to certificate identities")
        codec_keys = tuple((item.codec_id, item.codec_version) for item in self.ordered_supported_codecs)
        if len(set(codec_keys)) != len(codec_keys):
            raise MssqlR1V3ContractError("supported codec identities contain duplicates")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _CONTRACT,
            (
                self.schema_contract_version,
                self.authority_schema_name,
                self.stage_schema_name,
                self.permission_projection_policy_digest,
                tuple(item.canonical_bytes for item in self.ordered_objects),
                tuple(item.canonical_bytes for item in self.ordered_permission_rules),
                tuple(item.canonical_bytes for item in self.ordered_signer_profiles),
                tuple(item.canonical_bytes for item in self.ordered_supported_codecs),
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SchemaContractV3:
        values = list(decode_canonical_bytes(payload, _CONTRACT, field_count=8))
        values[4] = decode_members(values[4], MssqlR1PortableSchemaObjectV3, "portable object")
        values[5] = decode_members(values[5], MssqlR1PermissionRuleV3, "permission rule")
        values[6] = decode_members(values[6], MssqlR1SignerProfileV3, "signer profile")
        values[7] = decode_members(values[7], MssqlR1SupportedCodecEntryV3, "supported codec")
        if expect_text(values[0], "schema contract version") != MSSQL_R1_SCHEMA_CONTRACT_VERSION:
            raise MssqlR1V3ContractError("schema contract version is unsupported")
        return cls(*values[1:])  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1SchemaAttestationV3:
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
    ordered_observed_permissions: tuple[MssqlR1ObservedPermissionV3, ...]
    expected_schema_contract_digest: bytes
    principal_authority_set_digest: bytes
    observed_schema_inventory_digest: bytes
    observed_security_inventory_digest: bytes
    live_identity_digest: bytes
    projection_revision: int
    observed_at: datetime
    attestation_digest: bytes

    def __post_init__(self) -> None:
        if self.schema_contract_version != MSSQL_R1_SCHEMA_CONTRACT_VERSION:
            raise MssqlR1V3ContractError("schema attestation version is unsupported")
        require_uuid(self.target_binding_uuid, "target binding UUID")
        for name in (
            "registration_payload_digest",
            "registration_verification_receipt_digest",
            "registered_resolved_profile_digest",
            "registered_physical_authority_digest",
            "server_instance_identity_sha256",
        ):
            require_digest(getattr(self, name), name)
        require_sql_int(self.database_id, "database_id")
        for name in ("database_guid", "database_family_guid", "recovery_fork_guid"):
            require_uuid(getattr(self, name), name)
        require_positive(self.projection_revision, "projection_revision")
        canonical_utc_text(self.observed_at, "observed_at")
        validate_observation_types(
            self.ordered_observed_schemas,
            self.ordered_observed_objects,
            self.ordered_observed_principals,
            self.ordered_observed_role_memberships,
            self.ordered_observed_permissions,
        )
        expected = MssqlR1SchemaContractV3.from_canonical_bytes(self.expected_contract_bytes)
        authority = MssqlR1PrincipalAuthoritySetV3.from_canonical_bytes(self.principal_authority_set_bytes)
        validate_attestation_projection(self, expected, authority)
        for name, value in derive_attestation_digests(self, canonical_bytes).items():
            if require_digest(getattr(self, name), name) != value:
                raise MssqlR1V3ContractError(f"{name} differs from exact canonical fields")

    @property
    def expected_contract(self) -> MssqlR1SchemaContractV3:
        return MssqlR1SchemaContractV3.from_canonical_bytes(self.expected_contract_bytes)

    @property
    def principal_authority_set(self) -> MssqlR1PrincipalAuthoritySetV3:
        return MssqlR1PrincipalAuthoritySetV3.from_canonical_bytes(self.principal_authority_set_bytes)

    def _canonical_values(self, include_digest: bool) -> tuple[object, ...]:
        nested = {
            "ordered_observed_schemas",
            "ordered_observed_objects",
            "ordered_observed_principals",
            "ordered_observed_role_memberships",
            "ordered_observed_permissions",
        }
        values = []
        for item in fields(self):
            if item.name == "attestation_digest" and not include_digest:
                continue
            value = getattr(self, item.name)
            if item.name in nested:
                value = tuple(member.canonical_bytes for member in value)
            values.append(value)
        return tuple(values)

    @property
    def identity_bytes(self) -> bytes:
        return canonical_bytes(_ATTESTATION_IDENTITY, self._canonical_values(False))

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_ATTESTATION, self._canonical_values(True))

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def create(
        cls,
        *,
        registration_verification: MssqlTargetRegistrationVerificationV1,
        expected_contract: MssqlR1SchemaContractV3,
        principal_authority_set: MssqlR1PrincipalAuthoritySetV3,
        observed_server_instance_identity_sha256: bytes,
        database_id: int,
        observed_database_guid: UUID,
        observed_database_family_guid: UUID,
        observed_recovery_fork_guid: UUID,
        ordered_observed_schemas: tuple[MssqlR1ObservedSchemaV3, ...],
        ordered_observed_objects: tuple[MssqlR1ObservedSchemaObjectV3, ...],
        ordered_observed_principals: tuple[MssqlR1ObservedPrincipalV3, ...],
        ordered_observed_role_memberships: tuple[MssqlR1ObservedRoleMembershipV3, ...],
        ordered_observed_permissions: tuple[MssqlR1ObservedPermissionV3, ...],
        projection_revision: int,
        observed_at: datetime,
    ) -> MssqlR1SchemaAttestationV3:
        if not isinstance(expected_contract, MssqlR1SchemaContractV3):
            raise MssqlR1V3ContractError("expected contract must be MssqlR1SchemaContractV3")
        if not isinstance(principal_authority_set, MssqlR1PrincipalAuthoritySetV3):
            raise MssqlR1V3ContractError("principal authority set must be MssqlR1PrincipalAuthoritySetV3")
        registration = registration_from_verification(registration_verification)
        observed_identity = (
            observed_server_instance_identity_sha256,
            observed_database_guid,
            observed_database_family_guid,
            observed_recovery_fork_guid,
        )
        registered_identity = (
            registration.server_instance_identity_sha256,
            registration.database_guid,
            registration.database_family_guid,
            registration.recovery_fork_guid,
        )
        if observed_identity != registered_identity:
            raise MssqlR1V3ContractError("schema_attestation_registration_mismatch")
        validate_observation_types(
            ordered_observed_schemas,
            ordered_observed_objects,
            ordered_observed_principals,
            ordered_observed_role_memberships,
            ordered_observed_permissions,
        )
        values: dict[str, object] = {
            "schema_contract_version": expected_contract.schema_contract_version,
            "target_binding_uuid": registration.target_binding_uuid,
            "registration_payload_digest": registration_verification.registration_payload_digest,
            "registration_verification_receipt_digest": registration_verification.receipt_digest,
            "registered_resolved_profile_digest": registration.resolved_profile_digest,
            "registered_physical_authority_digest": registration.physical_identity.registered_physical_authority_digest,
            "server_instance_identity_sha256": registration.server_instance_identity_sha256,
            "database_id": database_id,
            "database_guid": registration.database_guid,
            "database_family_guid": registration.database_family_guid,
            "recovery_fork_guid": registration.recovery_fork_guid,
            "expected_contract_bytes": expected_contract.canonical_bytes,
            "principal_authority_set_bytes": principal_authority_set.canonical_bytes,
            "ordered_observed_schemas": ordered_observed_schemas,
            "ordered_observed_objects": ordered_observed_objects,
            "ordered_observed_principals": ordered_observed_principals,
            "ordered_observed_role_memberships": ordered_observed_role_memberships,
            "ordered_observed_permissions": ordered_observed_permissions,
            "projection_revision": projection_revision,
            "observed_at": observed_at,
        }
        attestation = with_derived_attestation_fields(cls, values, canonical_bytes)
        attestation.assert_for_registration(registration_verification)
        return attestation

    def assert_for_registration(self, verification: MssqlTargetRegistrationVerificationV1) -> None:
        assert_attestation_for_registration(self, verification)

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SchemaAttestationV3:
        values = list(decode_canonical_bytes(payload, _ATTESTATION, field_count=26))
        contracts = (
            (13, MssqlR1ObservedSchemaV3, "observed schema"),
            (14, MssqlR1ObservedSchemaObjectV3, "observed object"),
            (15, MssqlR1ObservedPrincipalV3, "observed principal"),
            (16, MssqlR1ObservedRoleMembershipV3, "observed membership"),
            (17, MssqlR1ObservedPermissionV3, "observed permission"),
        )
        for index, contract, field in contracts:
            values[index] = decode_members(values[index], contract, field)
        values[24] = parse_canonical_utc_text(values[24], "observed_at")
        return cls(*values)  # type: ignore[arg-type]


__all__ = ["MSSQL_R1_SCHEMA_CONTRACT_VERSION", "MssqlR1SchemaAttestationV3", "MssqlR1SchemaContractV3"]
