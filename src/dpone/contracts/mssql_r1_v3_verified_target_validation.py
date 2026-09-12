"""Cross-authority validation for current R1 target registration."""

from __future__ import annotations

import hashlib

from dpone.contracts.mssql_r1_v3_errors import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_physical_schema_descriptor import (
    VERSION,
    MssqlR1PhysicalSchemaDescriptorV1,
)
from dpone.contracts.mssql_r1_v3_provider_security_profile import MssqlR1SharedInstallSecurityProfileV2
from dpone.contracts.mssql_r1_v3_registration import RegistrationActionV1
from dpone.contracts.postgres_mssql_type_target_enums import reject


def validate_registration_admission(
    registration,
    verification,
    head,
    observed_at,
    stable_target,
    catalog,
    descriptor,
    security,
    schema_lock_digest,
) -> None:
    """Repeat every persisted registration, probe, security and catalog equality."""
    if (
        type(descriptor) is not MssqlR1PhysicalSchemaDescriptorV1
        or descriptor.descriptor_version != VERSION
        or type(security) is not MssqlR1SharedInstallSecurityProfileV2
    ):
        reject("authority_splice", operator=True)
    if type(schema_lock_digest) is not bytes or len(schema_lock_digest) != 32:
        reject("authority_splice", operator=True)
    try:
        security.validate_against(descriptor)
    except MssqlR1V3ContractError:
        reject("authority_splice")
    if (
        verification.payload_bytes != registration.canonical_bytes
        or verification.registration_payload_digest != registration.payload_digest
        or head.active_registration_payload_digest != registration.payload_digest
        or head.registration_verification_policy_digest != verification.verification_policy_digest
        or head.registration_verification_receipt_digest != verification.receipt_digest
    ):
        reject("registration_verification_mismatch", operator=True)
    expected_revision = (
        1
        if registration.registration_action is RegistrationActionV1.INITIAL
        else registration.expected_active_registration_revision + 1
    )
    if registration.revocation_revision != 0 or head.revocation_revision != 0:
        reject("registration_revoked")
    if (
        head.active_registration_id != registration.registration_id
        or head.active_registration_revision != expected_revision
        or head.target_binding_uuid != registration.target_binding_uuid
        or head.target_object_uuid != registration.target_object_uuid
    ):
        reject("registration_not_active")
    if not (
        registration.issued_at <= verification.verified_at <= observed_at < registration.expires_at
        and head.observed_at == observed_at
        and head.updated_at <= observed_at
    ):
        reject("registration_expired")
    physical = registration.physical_identity
    if (
        head.physical_coordinate_digest != physical.physical_object_coordinate_digest
        or head.registered_physical_authority_digest != physical.registered_physical_authority_digest
    ):
        reject("physical_identity_mismatch", operator=True)
    expected_schema = hashlib.sha256(descriptor.expected_schema_contract.canonical_bytes).digest()
    expected_permission = hashlib.sha256(security.canonical_bytes).digest()
    if head.schema_contract_digest != expected_schema or head.permission_contract_digest != expected_permission:
        reject("authority_splice", operator=True)
    probes = tuple(
        item
        for item in descriptor.ordered_procedures
        if item.portable_object.object_name == "dpone_probe_registration_v3"
    )
    if len(probes) != 1 or head.observation_contract_digest != hashlib.sha256(probes[0].canonical_bytes).digest():
        reject("authority_splice", operator=True)
    if head.schema_lock_binding_digest != schema_lock_digest:
        reject("authority_splice", operator=True)
    if stable_target.canonical_bytes != type(stable_target).from_registration(registration).canonical_bytes:
        reject("authority_splice", operator=True)
    if registration.catalog_contract_digest != catalog.digest:
        reject("catalog_digest_mismatch", operator=True)
    coordinates = (
        catalog.target_binding_uuid,
        catalog.target_object_uuid,
        catalog.physical_generation_uuid,
        catalog.database_name,
        catalog.schema_name,
        catalog.object_name,
        catalog.object_id,
        catalog.target_contract_revision,
    )
    expected_coordinates = (
        registration.target_binding_uuid,
        registration.target_object_uuid,
        registration.physical_generation_uuid,
        registration.database_name,
        registration.schema_name,
        registration.object_name,
        registration.object_id,
        registration.target_contract_revision,
    )
    if coordinates != expected_coordinates:
        reject("catalog_coordinate_mismatch", operator=True)


__all__ = ["validate_registration_admission"]
