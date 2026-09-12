from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.mssql_r1_v3_codec import canonical_bytes, decode_canonical_bytes
from dpone.contracts.mssql_r1_v3_control import MssqlR1ControlOperationV1, build_control_effect_key
from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_registration_rotation import (
    MssqlR1RegistrationRotationTransitionEvidenceV1,
)
from dpone.contracts.mssql_r1_v3_verified_target_authority import MssqlR1RegistrationAdmissionEvidenceV1
from tests.postgres_mssql_r1_v3_type_target_test_support import NOW, rotation_case
from tests.test_postgres_mssql_r1_v3_physical_descriptor_contract import descriptor

_ROTATION_DOMAIN = b"dpone-mssql-r1-registration-rotation-transition-evidence-v1\0"


def test_rotation_transition_round_trips_and_revalidates_both_admissions() -> None:
    (
        _catalog,
        _initial,
        initial_verified,
        physical,
        security,
        predecessor,
        _rotated,
        _rotated_verified,
        receipt,
        successor,
        predecessor_admission,
        successor_admission,
        lock,
    ) = rotation_case()

    evidence = MssqlR1RegistrationRotationTransitionEvidenceV1.create(
        predecessor_admission,
        successor_admission,
        receipt,
        physical,
        security,
        lock,
    )

    assert MssqlR1RegistrationRotationTransitionEvidenceV1.from_canonical_bytes(evidence.canonical_bytes) == evidence
    evidence.validate_against_authorities(
        predecessor_admission,
        successor_admission,
        physical,
        security,
        lock,
    )
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1RegistrationRotationTransitionEvidenceV1(
            predecessor,
            successor,
            receipt.canonical_bytes,
            receipt.receipt_digest,
        )
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1RegistrationRotationTransitionEvidenceV1.create(
            predecessor_admission,
            replace(successor_admission, registration_verification=initial_verified),
            receipt,
            physical,
            security,
            lock,
        )
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1RegistrationRotationTransitionEvidenceV1.create(
            predecessor_admission,
            replace(
                successor_admission,
                stable_target=replace(successor_admission.stable_target, profile_id="other"),
            ),
            receipt,
            physical,
            security,
            lock,
        )


@pytest.mark.parametrize(
    "field_index",
    range(4),
    ids=(
        "must_reject__decoded_predecessor",
        "must_reject__decoded_successor",
        "must_reject__decoded_receipt",
        "must_reject__decoded_receipt_digest",
    ),
)
def test_rotation_decode_rejects_each_semantically_mutated_field(field_index) -> None:
    (
        _catalog,
        _initial,
        _initial_verified,
        physical,
        security,
        _predecessor,
        _rotated,
        _rotated_verified,
        receipt,
        _successor,
        predecessor_admission,
        successor_admission,
        lock,
    ) = rotation_case()
    evidence = MssqlR1RegistrationRotationTransitionEvidenceV1.create(
        predecessor_admission,
        successor_admission,
        receipt,
        physical,
        security,
        lock,
    )
    fields = list(decode_canonical_bytes(evidence.canonical_bytes, _ROTATION_DOMAIN, field_count=4))
    replacements = (
        successor_admission.active_head.canonical_bytes,
        predecessor_admission.active_head.canonical_bytes,
        b"not-a-control-receipt",
        b"x" * 32,
    )
    fields[field_index] = replacements[field_index]

    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1RegistrationRotationTransitionEvidenceV1.from_canonical_bytes(
            canonical_bytes(_ROTATION_DOMAIN, tuple(fields))
        )


def test_rotation_rejects_schema_lock_and_observation_contract_splices() -> None:
    (
        _catalog,
        _initial,
        _initial_verified,
        physical,
        security,
        _predecessor,
        _rotated,
        _rotated_verified,
        receipt,
        successor,
        predecessor_admission,
        successor_admission,
        lock,
    ) = rotation_case()

    for case_id, changed_head, changed_lock in (
        (
            "must_reject__schema_lock_binding",
            replace(successor, schema_lock_binding_digest=b"m" * 32),
            b"m" * 32,
        ),
        ("must_reject__observation_contract", replace(successor, observation_contract_digest=b"m" * 32), lock),
    ):
        changed_admission = replace(successor_admission, active_head=changed_head)
        assert case_id.startswith("must_reject__")
        with pytest.raises(MssqlR1V3ContractError):
            MssqlR1RegistrationRotationTransitionEvidenceV1.create(
                predecessor_admission,
                changed_admission,
                receipt,
                physical,
                security,
                lock,
            )


@pytest.mark.parametrize(
    ("case_id", "field_name"),
    (
        ("must_reject__decoded_schema_lock_binding", "schema_lock_binding_digest"),
        ("must_reject__decoded_observation_contract", "observation_contract_digest"),
    ),
)
def test_rotation_decode_rejects_head_authority_divergence(case_id, field_name) -> None:
    (
        _catalog,
        _initial,
        _initial_verified,
        physical,
        security,
        _predecessor,
        _rotated,
        _rotated_verified,
        receipt,
        _successor,
        predecessor_admission,
        successor_admission,
        lock,
    ) = rotation_case()
    evidence = MssqlR1RegistrationRotationTransitionEvidenceV1.create(
        predecessor_admission,
        successor_admission,
        receipt,
        physical,
        security,
        lock,
    )
    fields = list(decode_canonical_bytes(evidence.canonical_bytes, _ROTATION_DOMAIN, field_count=4))
    fields[1] = replace(successor_admission.active_head, **{field_name: b"m" * 32}).canonical_bytes

    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1RegistrationRotationTransitionEvidenceV1.from_canonical_bytes(
            canonical_bytes(_ROTATION_DOMAIN, tuple(fields))
        )


def test_rotation_replay_rejects_unadmitted_and_foreign_external_authorities() -> None:
    (
        _catalog,
        _initial,
        initial_verified,
        physical,
        security,
        _predecessor,
        _rotated,
        rotated_verified,
        receipt,
        _successor,
        predecessor_admission,
        successor_admission,
        lock,
    ) = rotation_case()
    evidence = MssqlR1RegistrationRotationTransitionEvidenceV1.create(
        predecessor_admission,
        successor_admission,
        receipt,
        physical,
        security,
        lock,
    )
    cases = (
        (
            "must_reject__replay_predecessor_verification_splice",
            replace(predecessor_admission, registration_verification=rotated_verified),
            successor_admission,
            physical,
            security,
            lock,
        ),
        (
            "must_reject__replay_successor_verification_splice",
            predecessor_admission,
            replace(successor_admission, registration_verification=initial_verified),
            physical,
            security,
            lock,
        ),
        (
            "must_reject__replay_foreign_descriptor",
            predecessor_admission,
            successor_admission,
            descriptor(),
            security,
            lock,
        ),
        (
            "must_reject__replay_foreign_security",
            predecessor_admission,
            successor_admission,
            physical,
            object(),
            lock,
        ),
        (
            "must_reject__replay_foreign_schema_lock",
            predecessor_admission,
            successor_admission,
            physical,
            security,
            b"m" * 32,
        ),
    )
    for case_id, predecessor, successor, candidate_descriptor, candidate_security, candidate_lock in cases:
        assert case_id.startswith("must_reject__")
        with pytest.raises(MssqlR1V3ContractError):
            evidence.validate_against_authorities(
                predecessor,
                successor,
                candidate_descriptor,
                candidate_security,
                candidate_lock,
            )


def test_rotation_rejects_receipt_outside_observation_interval() -> None:
    (
        catalog,
        _initial,
        _initial_verified,
        physical,
        security,
        _predecessor,
        rotated,
        rotated_verified,
        receipt,
        successor,
        predecessor_admission,
        _successor_admission,
        lock,
    ) = rotation_case()

    for case_id, committed_at in (
        ("must_reject__receipt_before_predecessor_observation", NOW.replace(year=2025)),
        ("must_reject__receipt_after_successor_observation", NOW.replace(year=2027)),
    ):
        changed_receipt = replace(receipt, committed_at=committed_at)
        changed_head = replace(successor, last_control_receipt_digest=changed_receipt.receipt_digest)
        changed_admission = MssqlR1RegistrationAdmissionEvidenceV1.create(
            rotated,
            rotated_verified,
            changed_head,
            catalog,
            physical,
            security,
            lock,
        )
        assert case_id.startswith("must_reject__")
        with pytest.raises(MssqlR1V3ContractError):
            MssqlR1RegistrationRotationTransitionEvidenceV1.create(
                predecessor_admission,
                changed_admission,
                changed_receipt,
                physical,
                security,
                lock,
            )


@pytest.mark.parametrize(
    ("case_id", "mutate"),
    (
        (
            "must_reject__physical_coordinate",
            lambda receipt: replace(
                receipt,
                physical_object_coordinate_digest=b"x" * 32,
                control_effect_key=build_control_effect_key(
                    MssqlR1ControlOperationV1.REGISTRATION_ROTATE,
                    b"x" * 32,
                    receipt.input_payload_digest,
                    receipt.expected_active_registration_revision,
                ),
            ),
        ),
        (
            "must_reject__physical_authority",
            lambda receipt: replace(receipt, registered_physical_authority_digest=b"x" * 32),
        ),
        (
            "must_reject__target_binding",
            lambda receipt: replace(receipt, target_binding_uuid=type(receipt.target_binding_uuid)(int=1)),
        ),
        (
            "must_reject__registration_id",
            lambda receipt: replace(receipt, registration_id=type(receipt.registration_id)(int=1)),
        ),
        (
            "must_reject__input_payload",
            lambda receipt: replace(
                receipt,
                input_payload_digest=b"x" * 32,
                control_effect_key=build_control_effect_key(
                    MssqlR1ControlOperationV1.REGISTRATION_ROTATE,
                    receipt.physical_object_coordinate_digest,
                    b"x" * 32,
                    receipt.expected_active_registration_revision,
                ),
            ),
        ),
        ("must_reject__effect_key", lambda receipt: replace(receipt, control_effect_key=b"x" * 32)),
        (
            "must_reject__registration_revisions",
            lambda receipt: replace(
                receipt,
                expected_active_registration_revision=2,
                committed_active_registration_revision=3,
                control_effect_key=build_control_effect_key(
                    MssqlR1ControlOperationV1.REGISTRATION_ROTATE,
                    receipt.physical_object_coordinate_digest,
                    receipt.input_payload_digest,
                    2,
                ),
            ),
        ),
        (
            "must_reject__control_receipt_id",
            lambda receipt: replace(receipt, control_receipt_id=type(receipt.control_receipt_id)(int=1)),
        ),
        ("must_reject__verification_receipt", lambda receipt: replace(receipt, verification_receipt_digest=b"x" * 32)),
        ("must_reject__schema_contract", lambda receipt: replace(receipt, schema_contract_digest=b"x" * 32)),
        ("must_reject__permission_contract", lambda receipt: replace(receipt, permission_contract_digest=b"x" * 32)),
    ),
)
def test_rotation_rejects_semantic_receipt_splices(case_id, mutate) -> None:
    (
        catalog,
        _initial,
        _initial_verified,
        physical,
        security,
        _predecessor,
        rotated,
        rotated_verified,
        receipt,
        successor,
        predecessor_admission,
        _successor_admission,
        lock,
    ) = rotation_case()

    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1V3ContractError):
        changed_receipt = mutate(receipt)
        changed_head = replace(successor, last_control_receipt_digest=changed_receipt.receipt_digest)
        changed_admission = MssqlR1RegistrationAdmissionEvidenceV1.create(
            rotated,
            rotated_verified,
            changed_head,
            catalog,
            physical,
            security,
            lock,
        )
        MssqlR1RegistrationRotationTransitionEvidenceV1.create(
            predecessor_admission,
            changed_admission,
            changed_receipt,
            physical,
            security,
            lock,
        )
