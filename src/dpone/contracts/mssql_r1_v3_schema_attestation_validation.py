"""Admission and cross-projection validation for schema-2 attestations."""

from __future__ import annotations

from typing import Any

from dpone.contracts.mssql_r1_v3_registration import (
    MssqlTargetRegistrationPayloadV1,
    MssqlTargetRegistrationVerificationV1,
)
from dpone.contracts.mssql_r1_v3_schema_observation import (
    MssqlR1ObservedPermissionV3,
    MssqlR1ObservedPrincipalAuthorityKindV3,
    MssqlR1ObservedPrincipalV3,
    MssqlR1ObservedRoleMembershipV3,
    MssqlR1ObservedSchemaObjectV3,
    MssqlR1ObservedSchemaV3,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import (
    MssqlR1SchemaObjectKindV3,
    MssqlR1SignerProfileKindV3,
    MssqlR1V3ContractError,
    require_canonical_set,
)
from dpone.contracts.mssql_r1_v3_schema_security import (
    ENVIRONMENT_ROLES,
    MODULE_ROLE_BY_PROFILE,
    MssqlR1PermissionRuleV3,
    MssqlR1PermissionSourceV3,
    MssqlR1PrincipalAuthoritySetV3,
    MssqlR1SubjectRoleV3,
)


def registration_from_verification(
    verification: MssqlTargetRegistrationVerificationV1,
) -> MssqlTargetRegistrationPayloadV1:
    """Reproduce the exact verified registration or fail with the admission error."""
    if not isinstance(verification, MssqlTargetRegistrationVerificationV1):
        raise MssqlR1V3ContractError("schema_attestation_registration_mismatch")
    registration = MssqlTargetRegistrationPayloadV1.from_canonical_bytes(verification.payload_bytes)
    if (
        registration.canonical_bytes != verification.payload_bytes
        or registration.payload_digest != verification.registration_payload_digest
    ):
        raise MssqlR1V3ContractError("schema_attestation_registration_mismatch")
    return registration


def validate_attestation_projection(
    attestation: Any,
    contract: Any,
    authority: MssqlR1PrincipalAuthoritySetV3,
) -> None:
    """Validate all typed observations against portable and independent authority."""
    if (
        authority.registration_payload_digest != attestation.registration_payload_digest
        or authority.resolved_profile_digest != attestation.registered_resolved_profile_digest
    ):
        raise MssqlR1V3ContractError("schema_attestation_registration_mismatch")
    require_canonical_set(
        attestation.ordered_observed_principals,
        MssqlR1ObservedPrincipalV3,
        "observed principals",
    )
    require_canonical_set(
        attestation.ordered_observed_role_memberships,
        MssqlR1ObservedRoleMembershipV3,
        "observed memberships",
    )
    require_canonical_set(
        attestation.ordered_observed_permissions,
        MssqlR1ObservedPermissionV3,
        "observed permissions",
    )
    _validate_security_projection(attestation, contract, authority)
    _validate_observed_projection(attestation, contract)


def _validate_security_projection(attestation: Any, contract: Any, authority: MssqlR1PrincipalAuthoritySetV3) -> None:
    """Reject principal, membership and permission substitution in one closed projection."""
    principals = attestation.ordered_observed_principals
    if len({item.principal_id for item in principals}) != len(principals) or len(
        {item.database_sid_digest for item in principals}
    ) != len(principals):
        raise MssqlR1V3ContractError("observed principal IDs and SIDs must be unique")
    by_role = {item.subject_role: item for item in principals if item.subject_role is not None}
    labeled = tuple(item.subject_role for item in principals if item.subject_role is not None)
    expected_labeled = set(ENVIRONMENT_ROLES) | set(MODULE_ROLE_BY_PROFILE.values())
    if len(labeled) != len(set(labeled)) or set(labeled) != expected_labeled:
        raise MssqlR1V3ContractError("observed authority-labeled principals must be exact and unique")
    for binding in authority.ordered_bindings:
        principal = by_role[binding.subject_role]
        if (
            principal.authority_kind is not MssqlR1ObservedPrincipalAuthorityKindV3.ENVIRONMENT
            or principal.principal_name != binding.database_principal_name
            or principal.database_sid_digest != binding.database_principal_sid_digest
            or principal.server_sid_digest != binding.server_principal_sid_digest
            or principal.principal_type is not binding.principal_type
            or principal.authentication_type is not binding.authentication_type
        ):
            raise MssqlR1V3ContractError("observed environment principal differs from authority binding")
    signer_profiles = {MODULE_ROLE_BY_PROFILE[item.signer_profile]: item for item in contract.ordered_signer_profiles}
    for role, profile in signer_profiles.items():
        principal = by_role.get(role)
        if (
            principal is None
            or principal.authority_kind is not MssqlR1ObservedPrincipalAuthorityKindV3.SIGNER
            or principal.principal_name != profile.certificate_user_name
        ):
            raise MssqlR1V3ContractError("observed signer principal differs from portable profile")
    roles = {
        item.principal_name: item
        for item in principals
        if item.authority_kind is MssqlR1ObservedPrincipalAuthorityKindV3.DATABASE_ROLE
    }
    allowed_role_names = {
        name for binding in authority.ordered_bindings for name in binding.ordered_allowed_database_roles
    }
    expected_kind_counts = {
        MssqlR1ObservedPrincipalAuthorityKindV3.ENVIRONMENT: len(ENVIRONMENT_ROLES),
        MssqlR1ObservedPrincipalAuthorityKindV3.SIGNER: len(MODULE_ROLE_BY_PROFILE),
        MssqlR1ObservedPrincipalAuthorityKindV3.DATABASE_ROLE: len(allowed_role_names),
        MssqlR1ObservedPrincipalAuthorityKindV3.PUBLIC: 0,
    }
    actual_kind_counts = {
        kind: sum(item.authority_kind is kind for item in principals) for kind in expected_kind_counts
    }
    if actual_kind_counts != expected_kind_counts:
        raise MssqlR1V3ContractError("observed principal inventory differs from independent authority")
    if set(roles) != allowed_role_names:
        raise MssqlR1V3ContractError("observed database roles differ from independent authority")
    _validate_memberships(attestation, authority, by_role, roles)
    _validate_permissions(attestation, contract, principals)


def _validate_memberships(
    attestation: Any, authority: MssqlR1PrincipalAuthoritySetV3, by_role: dict, roles: dict
) -> None:
    expected = set()
    for binding in authority.ordered_bindings:
        member = by_role[binding.subject_role]
        for name in binding.ordered_allowed_database_roles:
            role = roles[name]
            expected.add(
                (member.principal_id, member.database_sid_digest, role.principal_id, role.database_sid_digest, name)
            )
    actual = {
        (item.member_principal_id, item.member_sid_digest, item.role_principal_id, item.role_sid_digest, item.role_name)
        for item in attestation.ordered_observed_role_memberships
    }
    if actual != expected:
        raise MssqlR1V3ContractError("role memberships differ from independent authority")


def _validate_permissions(attestation: Any, contract: Any, principals: tuple) -> None:
    by_id = {item.principal_id: item for item in principals}
    observed_rules = []
    for item in attestation.ordered_observed_permissions:
        if item.source is not MssqlR1PermissionSourceV3.DIRECT:
            raise MssqlR1V3ContractError("inherited or public permission is forbidden")
        grantee, grantor = by_id.get(item.grantee_principal_id), by_id.get(item.grantor_principal_id)
        if (
            grantee is None
            or grantor is None
            or grantee.database_sid_digest != item.grantee_sid_digest
            or grantor.subject_role is not MssqlR1SubjectRoleV3.PROVISIONER
            or grantor.database_sid_digest != item.grantor_sid_digest
            or grantee.subject_role is None
        ):
            raise MssqlR1V3ContractError("permission principal binding is inconsistent")
        observed_rules.append(
            MssqlR1PermissionRuleV3(
                grantee.subject_role,
                MssqlR1SubjectRoleV3.PROVISIONER,
                item.source,
                item.scope,
                item.schema_name,
                item.object_name,
                item.column_name,
                item.permission,
                item.effect,
                item.grant_option,
            )
        )
    if tuple(sorted(rule.canonical_bytes for rule in observed_rules)) != tuple(
        rule.canonical_bytes for rule in contract.ordered_permission_rules
    ):
        raise MssqlR1V3ContractError("observed permissions differ from portable rules")


def _validate_observed_projection(attestation: Any, contract: Any) -> None:
    schemas = attestation.ordered_observed_schemas
    expected_names = tuple(
        sorted((contract.authority_schema_name, contract.stage_schema_name), key=lambda value: value.encode())
    )
    if not isinstance(schemas, tuple) or tuple(item.schema_name for item in schemas) != expected_names:
        raise MssqlR1V3ContractError("observed schemas do not match portable contract")
    objects = attestation.ordered_observed_objects
    expected_objects = contract.ordered_objects
    observed_coordinates = tuple(
        (item.portable_object.schema_name.encode(), item.portable_object.object_name.encode()) for item in objects
    )
    expected_coordinates = tuple((item.schema_name.encode(), item.object_name.encode()) for item in expected_objects)
    if (
        not isinstance(objects, tuple)
        or observed_coordinates != expected_coordinates
        or tuple(item.portable_object.canonical_bytes for item in objects)
        != tuple(item.canonical_bytes for item in expected_objects)
    ):
        raise MssqlR1V3ContractError("observed objects do not reproduce portable contract")
    principals = attestation.ordered_observed_principals
    by_role = {item.subject_role: item for item in principals if item.subject_role is not None}
    provisioner = by_role[MssqlR1SubjectRoleV3.PROVISIONER]
    if any(
        item.declared_owner_principal_id != provisioner.principal_id
        or item.effective_owner_principal_id != provisioner.principal_id
        or item.effective_owner_sid_digest != provisioner.database_sid_digest
        for item in schemas
    ):
        raise MssqlR1V3ContractError("managed schema owner differs from provisioner")
    schema_by_name = {item.schema_name: item for item in schemas}
    for item in objects:
        schema = schema_by_name.get(item.portable_object.schema_name)
        if (
            schema is None
            or item.schema_id != schema.schema_id
            or item.effective_owner_principal_id != provisioner.principal_id
            or item.effective_owner_sid_digest != provisioner.database_sid_digest
        ):
            raise MssqlR1V3ContractError("observed object owner or schema identity is inconsistent")
    schema_ids = tuple(item.schema_id for item in schemas)
    if len(set(schema_ids)) != len(schema_ids):
        raise MssqlR1V3ContractError("observed schema IDs contain duplicates")
    _validate_object_identities(attestation, contract, by_role, schema_by_name)


def _validate_object_identities(attestation: Any, contract: Any, principals: dict, schemas: dict) -> None:
    object_ids = [item.object_id for item in attestation.ordered_observed_objects]
    profiles = {item.signer_profile: item for item in contract.ordered_signer_profiles}
    profile_identities: dict[object, tuple[object, ...]] = {}
    for item in attestation.ordered_observed_objects:
        portable = item.portable_object
        expected_triggers = {trigger.digest for trigger in portable.ordered_triggers}
        observed_triggers = {trigger.portable_trigger_digest for trigger in item.ordered_trigger_identities}
        if (
            observed_triggers != expected_triggers
            or len(observed_triggers) != len(item.ordered_trigger_identities)
            or any(
                trigger.parent_object_id != item.object_id
                or trigger.schema_id != schemas[portable.schema_name].schema_id
                for trigger in item.ordered_trigger_identities
            )
        ):
            raise MssqlR1V3ContractError("observed trigger identities differ from portable triggers")
        object_ids.extend(trigger.object_id for trigger in item.ordered_trigger_identities)
        expected_profile = (
            MssqlR1SignerProfileKindV3.NONE
            if portable.kind is MssqlR1SchemaObjectKindV3.TABLE
            else portable.module_options.signer_profile
        )
        if expected_profile is MssqlR1SignerProfileKindV3.NONE:
            if item.ordered_signatures:
                raise MssqlR1V3ContractError("unsigned object has a signature")
            continue
        if len(item.ordered_signatures) != 1:
            raise MssqlR1V3ContractError("signed procedure requires exactly one signature")
        signature = item.ordered_signatures[0]
        profile = profiles[expected_profile]
        signer = principals[MODULE_ROLE_BY_PROFILE[expected_profile]]
        if (
            signature.signer_profile is not expected_profile
            or signature.module_object_id != item.object_id
            or signature.certificate_name != profile.certificate_name
            or signature.certificate_user_principal_id != signer.principal_id
            or signature.certificate_user_sid_digest != signer.database_sid_digest
        ):
            raise MssqlR1V3ContractError("module signature differs from signer profile")
        identity = (
            signature.certificate_id,
            signature.certificate_thumbprint_digest,
            signature.certificate_user_principal_id,
            signature.certificate_user_sid_digest,
        )
        if expected_profile in profile_identities and profile_identities[expected_profile] != identity:
            raise MssqlR1V3ContractError("signer profile certificate identity is inconsistent")
        profile_identities[expected_profile] = identity
    if len(set(object_ids)) != len(object_ids):
        raise MssqlR1V3ContractError("observed object IDs contain duplicates")
    if len(set(profile_identities.values())) != len(profile_identities):
        raise MssqlR1V3ContractError("signer profiles alias one certificate identity")
    certificate_ids = tuple(identity[0] for identity in profile_identities.values())
    thumbprints = tuple(identity[1] for identity in profile_identities.values())
    if len(set(certificate_ids)) != len(certificate_ids) or len(set(thumbprints)) != len(thumbprints):
        raise MssqlR1V3ContractError("signer profiles alias a certificate ID or thumbprint")


def validate_observation_types(*groups: object) -> None:
    """Reject malformed factory inputs before derived digests inspect nested members."""
    contracts = (
        MssqlR1ObservedSchemaV3,
        MssqlR1ObservedSchemaObjectV3,
        MssqlR1ObservedPrincipalV3,
        MssqlR1ObservedRoleMembershipV3,
        MssqlR1ObservedPermissionV3,
    )
    for values, contract in zip(groups, contracts, strict=True):
        if not isinstance(values, tuple) or not all(isinstance(item, contract) for item in values):
            raise MssqlR1V3ContractError(f"{contract.__name__} members must be a typed tuple")


def assert_attestation_for_registration(
    attestation: Any,
    verification: MssqlTargetRegistrationVerificationV1,
) -> None:
    """Repeat every registered target equality for decoded admission."""
    try:
        registration = registration_from_verification(verification)
        expected = (
            registration.target_binding_uuid,
            verification.registration_payload_digest,
            verification.receipt_digest,
            registration.resolved_profile_digest,
            registration.physical_identity.registered_physical_authority_digest,
            registration.server_instance_identity_sha256,
            registration.database_guid,
            registration.database_family_guid,
            registration.recovery_fork_guid,
        )
        actual = (
            attestation.target_binding_uuid,
            attestation.registration_payload_digest,
            attestation.registration_verification_receipt_digest,
            attestation.registered_resolved_profile_digest,
            attestation.registered_physical_authority_digest,
            attestation.server_instance_identity_sha256,
            attestation.database_guid,
            attestation.database_family_guid,
            attestation.recovery_fork_guid,
        )
        if actual != expected:
            raise MssqlR1V3ContractError("mismatch")
        principal = attestation.principal_authority_set
        if (
            principal.registration_payload_digest != verification.registration_payload_digest
            or principal.resolved_profile_digest != registration.resolved_profile_digest
        ):
            raise MssqlR1V3ContractError("foreign principal authority")
    except (MssqlR1V3ContractError, TypeError, ValueError) as exc:
        raise MssqlR1V3ContractError("schema_attestation_registration_mismatch") from exc


__all__ = [
    "assert_attestation_for_registration",
    "registration_from_verification",
    "validate_attestation_projection",
    "validate_observation_types",
]
