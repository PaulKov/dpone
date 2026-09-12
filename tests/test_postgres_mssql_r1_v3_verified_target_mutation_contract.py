from __future__ import annotations

from dataclasses import fields, replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_verified_target_authority import (
    MssqlR1ActiveRegistrationHeadObservationV1,
    MssqlR1RegistrationAdmissionEvidenceV1,
    MssqlR1RotationStableTargetAuthorityV1,
)
from tests.postgres_mssql_r1_v3_type_target_test_support import admitted_authorities, verification

_STRUCTURAL_CONTRACT_CLASSES = (
    MssqlR1RotationStableTargetAuthorityV1,
    MssqlR1ActiveRegistrationHeadObservationV1,
    MssqlR1RegistrationAdmissionEvidenceV1,
)
STRUCTURAL_WRONG_TYPE_CASES = tuple(
    (f"must_reject__structural_wrong_type__{contract.__name__}__{field.name}", index, field.name)
    for index, contract in enumerate(_STRUCTURAL_CONTRACT_CLASSES)
    for field in fields(contract)
)


def _contract_values():
    *authorities, admission = admitted_authorities()
    stable = authorities[5]
    head = authorities[6]
    return stable, head, admission


@pytest.mark.parametrize(
    ("case_id", "contract_index", "field_name"),
    STRUCTURAL_WRONG_TYPE_CASES,
    ids=[case[0] for case in STRUCTURAL_WRONG_TYPE_CASES],
)
def test_verified_contract_rejects_structural_wrong_types(case_id, contract_index, field_name) -> None:
    value = _contract_values()[contract_index]

    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1V3ContractError):
        replace(value, **{field_name: object()})


def test_valid_stable_and_head_mutations_change_canonical_bytes() -> None:
    *authorities, _admission = admitted_authorities()
    stable = authorities[5]
    head = authorities[6]

    changed_stable = replace(stable, profile_id="r1-v3-alternate")
    changed_head = replace(head, projection_revision=head.projection_revision + 1)

    assert changed_stable.canonical_bytes != stable.canonical_bytes
    assert changed_head.canonical_bytes != head.canonical_bytes


def test_registration_attempt_mutation_changes_admission_but_not_stable_target() -> None:
    catalog, payload, _verified, physical, security, stable, head, lock, admission = admitted_authorities()
    changed_payload = replace(
        payload,
        registration_id=UUID("d0000000-0000-0000-0000-00000000000d"),
        issued_at=payload.issued_at.replace(microsecond=1),
        expires_at=payload.expires_at.replace(microsecond=1),
        nonce=b"d" * 16,
    )
    changed_verification = verification(changed_payload)
    changed_head = replace(
        head,
        active_registration_id=changed_payload.registration_id,
        active_registration_payload_digest=changed_payload.payload_digest,
        registration_verification_policy_digest=changed_verification.verification_policy_digest,
        registration_verification_receipt_digest=changed_verification.receipt_digest,
        row_token=b"d" * 8,
    )
    changed_admission = MssqlR1RegistrationAdmissionEvidenceV1.create(
        changed_payload,
        changed_verification,
        changed_head,
        catalog,
        physical,
        security,
        lock,
    )

    assert changed_admission.stable_target == stable
    assert changed_admission.canonical_bytes != admission.canonical_bytes
