from __future__ import annotations

import inspect
from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_verified_target_authority import (
    MssqlR1RegistrationAdmissionEvidenceV1,
    MssqlR1RotationStableTargetAuthorityV1,
)
from tests.postgres_mssql_r1_v3_type_target_test_support import NOW, authorities, verification

LOCK = b"l" * 32


def _authorities():
    return authorities()[:-1]


def test_current_registration_admission_round_trips_without_signed_command() -> None:
    catalog, payload, verified, physical, security, stable, head = _authorities()

    evidence = MssqlR1RegistrationAdmissionEvidenceV1.create(
        payload,
        verified,
        head,
        catalog,
        physical,
        security,
        LOCK,
    )

    assert evidence.stable_target == stable
    assert evidence.observed_at == NOW
    assert MssqlR1RegistrationAdmissionEvidenceV1.from_canonical_bytes(evidence.canonical_bytes) == evidence
    evidence.validate_against_authorities(physical, security, LOCK)
    assert (
        MssqlR1RegistrationAdmissionEvidenceV1.create_from_persisted_bytes(
            payload.canonical_bytes,
            verified.canonical_bytes,
            head,
            catalog.canonical_bytes,
            physical,
            security,
            LOCK,
        )
        == evidence
    )


def test_stable_authority_enforces_semantic_id_identifier_and_bigint_domains() -> None:
    _catalog_value, _payload, _verified, _physical, _security, stable, head = _authorities()
    supplementary_128_units = "😀" * 64

    assert replace(stable, profile_id="a").profile_id == "a"
    assert replace(stable, profile_id="a" * 64).profile_id == "a" * 64
    with pytest.raises(MssqlR1V3ContractError):
        replace(stable, profile_id="é")
    with pytest.raises(MssqlR1V3ContractError):
        replace(stable, profile_id="a" * 65)
    assert replace(stable, object_name="a").object_name == "a"
    assert replace(stable, object_name=supplementary_128_units).object_name == supplementary_128_units
    with pytest.raises(MssqlR1V3ContractError):
        replace(stable, object_name=supplementary_128_units + "a")
    assert replace(head, active_registration_revision=2**63 - 1).active_registration_revision == 2**63 - 1
    with pytest.raises(MssqlR1V3ContractError):
        replace(head, active_registration_revision=2**63)


def test_registration_is_expired_when_projected_server_time_equals_expiry() -> None:
    catalog, payload, verified, physical, security, _stable, head = _authorities()
    expiry_head = replace(head, observed_at=payload.expires_at)

    with pytest.raises(MssqlR1V3ContractError) as caught:
        MssqlR1RegistrationAdmissionEvidenceV1.create(
            payload,
            verified,
            expiry_head,
            catalog,
            physical,
            security,
            LOCK,
        )

    assert caught.value.reason_id == "registration_expired"


@pytest.mark.parametrize(
    "mutation",
    (
        lambda values: replace(values[-1], active_registration_payload_digest=b"x" * 32),
        lambda values: replace(values[-1], registration_verification_policy_digest=b"x" * 32),
        lambda values: replace(values[-1], registration_verification_receipt_digest=b"x" * 32),
        lambda values: replace(values[-1], observation_contract_digest=b"x" * 32),
        lambda values: replace(values[-1], schema_lock_binding_digest=b"x" * 32),
    ),
    ids=(
        "must_reject__registration_payload_digest",
        "must_reject__verification_policy_digest",
        "must_reject__verification_receipt_digest",
        "must_reject__observation_contract_digest",
        "must_reject__schema_lock_binding_digest",
    ),
)
def test_admission_rejects_head_and_verification_splices(mutation) -> None:
    catalog, payload, verified, physical, security, _stable, head = _authorities()
    changed = mutation((catalog, payload, verified, physical, security, head))

    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1RegistrationAdmissionEvidenceV1.create(
            payload,
            verified,
            changed,
            catalog,
            physical,
            security,
            LOCK,
        )


def test_admission_classifies_revocation_mismatch_as_operator_intervention() -> None:
    catalog, payload, verified, physical, security, _stable, head = _authorities()

    with pytest.raises(MssqlR1V3ContractError) as caught:
        MssqlR1RegistrationAdmissionEvidenceV1.create(
            payload,
            verified,
            replace(head, revocation_revision=1),
            catalog,
            physical,
            security,
            LOCK,
        )

    assert caught.value.reason_id == "registration_revoked"


def test_admission_rejects_matching_nonzero_revocation_authority() -> None:
    catalog, payload, _verified, physical, security, _stable, head = _authorities()
    revoked = replace(payload, revocation_revision=1)
    revoked_verification = verification(revoked)
    revoked_head = replace(
        head,
        active_registration_payload_digest=revoked.payload_digest,
        registration_verification_policy_digest=revoked_verification.verification_policy_digest,
        registration_verification_receipt_digest=revoked_verification.receipt_digest,
        revocation_revision=1,
    )

    with pytest.raises(MssqlR1V3ContractError) as caught:
        MssqlR1RegistrationAdmissionEvidenceV1.create(
            revoked,
            revoked_verification,
            revoked_head,
            catalog,
            physical,
            security,
            LOCK,
        )

    assert caught.value.reason_id == "registration_revoked"


def test_stable_target_excludes_registration_attempt_fields() -> None:
    catalog, payload, _verified, _physical, _security, stable, _head = _authorities()
    rotated_attempt = replace(
        payload,
        registration_id=UUID("a0000000-0000-0000-0000-00000000000a"),
        issued_at=payload.issued_at.replace(microsecond=1),
        expires_at=payload.expires_at.replace(microsecond=1),
        nonce=b"o" * 16,
    )

    assert MssqlR1RotationStableTargetAuthorityV1.from_registration(rotated_attempt) == stable
    assert catalog.digest == rotated_attempt.catalog_contract_digest


def test_validation_module_has_no_reverse_import_cycle() -> None:
    import dpone.contracts.mssql_r1_v3_verified_target_validation as validation

    assert "mssql_r1_v3_verified_target_authority" not in inspect.getsource(validation)
