from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

from dpone.contracts.mssql_r1_v3_control import (
    MssqlR1ControlOperationV1,
    MssqlR1ControlReceiptV1,
    build_control_effect_key,
)
from dpone.contracts.mssql_r1_v3_identity import canonical_identifier_digest
from dpone.contracts.mssql_r1_v3_registration import (
    MssqlTargetRegistrationPayloadV1,
    MssqlTargetRegistrationVerificationV1,
    RegistrationActionV1,
)
from dpone.contracts.mssql_r1_v3_verified_target_authority import (
    MssqlR1ActiveRegistrationHeadObservationV1,
    MssqlR1RegistrationAdmissionEvidenceV1,
    MssqlR1RotationStableTargetAuthorityV1,
)
from dpone.contracts.postgres_mssql_type_target_enums import (
    PostgresMssqlLengthKindV1,
    PostgresMssqlSourceScalarFamilyV1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import PostgresMssqlSourceScalarShapeV1
from tests.test_postgres_mssql_r1_v3_provider_security_contract import security_descriptor, security_profile
from tests.test_postgres_mssql_r1_v3_registered_target_catalog_contract import registered_target_catalog

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def source_shape(
    family: PostgresMssqlSourceScalarFamilyV1,
    oid: int,
    typmod: int = -1,
    *,
    precision: int | None = None,
    scale: int | None = None,
    maximum_characters: int | None = None,
) -> PostgresMssqlSourceScalarShapeV1:
    """Build one exact source shape with the family-owned length discriminator."""

    length_kind = (
        PostgresMssqlLengthKindV1.BOUNDED
        if family is PostgresMssqlSourceScalarFamilyV1.VARCHAR
        else PostgresMssqlLengthKindV1.MAXIMUM
        if family is PostgresMssqlSourceScalarFamilyV1.TEXT
        else PostgresMssqlLengthKindV1.NOT_APPLICABLE
    )
    return PostgresMssqlSourceScalarShapeV1(
        family,
        oid,
        typmod,
        length_kind,
        precision,
        scale,
        maximum_characters,
    )


def registration(catalog) -> MssqlTargetRegistrationPayloadV1:
    return MssqlTargetRegistrationPayloadV1(
        UUID("40000000-0000-0000-0000-000000000004"),
        RegistrationActionV1.INITIAL,
        None,
        None,
        NOW - timedelta(minutes=5),
        NOW + timedelta(minutes=5),
        b"n" * 16,
        "r1-v3",
        b"c" * 32,
        b"r" * 32,
        b"s" * 32,
        "ordinary_disk_rowstore_v1",
        "dpone-mssql-target-catalog-v1",
        0,
        catalog.target_binding_uuid,
        catalog.target_object_uuid,
        UUID("50000000-0000-0000-0000-000000000005"),
        1,
        b"i" * 32,
        UUID("60000000-0000-0000-0000-000000000006"),
        UUID("70000000-0000-0000-0000-000000000007"),
        UUID("80000000-0000-0000-0000-000000000008"),
        canonical_identifier_digest(catalog.database_name),
        canonical_identifier_digest(catalog.schema_name),
        canonical_identifier_digest(catalog.object_name),
        catalog.database_name,
        catalog.schema_name,
        catalog.object_name,
        catalog.object_id,
        catalog.physical_generation_uuid,
        catalog.digest,
        catalog.target_contract_revision,
    )


def verification(payload: MssqlTargetRegistrationPayloadV1) -> MssqlTargetRegistrationVerificationV1:
    return MssqlTargetRegistrationVerificationV1(
        payload.payload_digest,
        hashlib.sha256(b"bundle").digest(),
        b"q" * 32,
        b"t" * 32,
        b"p" * 32,
        "cosign-v1",
        NOW - timedelta(minutes=1),
        b"e" * 32,
        b"j" * 32,
        payload.canonical_bytes,
    )


def authorities():
    """Build one fully cross-validated registration-admission authority set."""

    catalog = registered_target_catalog()
    payload = registration(catalog)
    verified = verification(payload)
    physical = security_descriptor()
    security = security_profile()
    probe = next(
        item
        for item in physical.ordered_procedures
        if item.portable_object.object_name == "dpone_probe_registration_v3"
    )
    stable = MssqlR1RotationStableTargetAuthorityV1.from_registration(payload)
    lock = b"l" * 32
    head = MssqlR1ActiveRegistrationHeadObservationV1(
        payload.target_binding_uuid,
        payload.physical_identity.physical_object_coordinate_digest,
        payload.target_object_uuid,
        payload.registration_id,
        1,
        payload.payload_digest,
        verified.verification_policy_digest,
        verified.receipt_digest,
        payload.revocation_revision,
        payload.physical_identity.registered_physical_authority_digest,
        hashlib.sha256(physical.expected_schema_contract.canonical_bytes).digest(),
        hashlib.sha256(security.canonical_bytes).digest(),
        UUID("90000000-0000-0000-0000-000000000009"),
        b"k" * 32,
        1,
        NOW,
        b"z" * 8,
        hashlib.sha256(probe.canonical_bytes).digest(),
        lock,
        NOW,
    )
    return catalog, payload, verified, physical, security, stable, head, lock


def admitted_authorities():
    """Build the authority set plus its fully validated admission evidence."""

    catalog, payload, verified, physical, security, stable, head, lock = authorities()
    admission = MssqlR1RegistrationAdmissionEvidenceV1.create(
        payload,
        verified,
        head,
        catalog,
        physical,
        security,
        lock,
    )
    return catalog, payload, verified, physical, security, stable, head, lock, admission


def rotation_case():
    """Build adjacent admitted registrations and their exact rotation receipt."""

    catalog, initial, initial_verified, physical, security, _stable, predecessor, lock = authorities()
    rotated = replace(
        initial,
        registration_id=UUID("a0000000-0000-0000-0000-00000000000a"),
        registration_action=RegistrationActionV1.ROTATE,
        predecessor_registration_id=initial.registration_id,
        expected_active_registration_revision=predecessor.active_registration_revision,
        nonce=b"o" * 16,
    )
    rotated_verified = verification(rotated)
    receipt_id = UUID("b0000000-0000-0000-0000-00000000000b")
    writer_digest = b"w" * 32
    receipt = MssqlR1ControlReceiptV1(
        receipt_id,
        build_control_effect_key(
            MssqlR1ControlOperationV1.REGISTRATION_ROTATE,
            predecessor.physical_coordinate_digest,
            rotated.payload_digest,
            predecessor.active_registration_revision,
        ),
        MssqlR1ControlOperationV1.REGISTRATION_ROTATE,
        predecessor.physical_coordinate_digest,
        predecessor.registered_physical_authority_digest,
        predecessor.target_binding_uuid,
        rotated.registration_id,
        rotated.payload_digest,
        rotated_verified.receipt_digest,
        predecessor.active_registration_revision,
        predecessor.active_registration_revision + 1,
        1,
        1,
        writer_digest,
        writer_digest,
        predecessor.schema_contract_digest,
        predecessor.permission_contract_digest,
        None,
        None,
        NOW,
    )
    successor = replace(
        predecessor,
        active_registration_id=rotated.registration_id,
        active_registration_revision=predecessor.active_registration_revision + 1,
        active_registration_payload_digest=rotated.payload_digest,
        registration_verification_policy_digest=rotated_verified.verification_policy_digest,
        registration_verification_receipt_digest=rotated_verified.receipt_digest,
        last_control_receipt_id=receipt_id,
        last_control_receipt_digest=receipt.receipt_digest,
    )
    predecessor_admission = MssqlR1RegistrationAdmissionEvidenceV1.create(
        initial, initial_verified, predecessor, catalog, physical, security, lock
    )
    successor_admission = MssqlR1RegistrationAdmissionEvidenceV1.create(
        rotated, rotated_verified, successor, catalog, physical, security, lock
    )
    return (
        catalog,
        initial,
        initial_verified,
        physical,
        security,
        predecessor,
        rotated,
        rotated_verified,
        receipt,
        successor,
        predecessor_admission,
        successor_admission,
        lock,
    )
