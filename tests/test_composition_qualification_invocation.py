"""Exact replay projection preserves complete qualification operation originals."""

import hashlib
from dataclasses import fields, replace
from typing import Any

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_qualification_operation import CompositionQualificationOperation
from tests.test_composition_qualification_operation import NEXT_RUN, OTHER, operation, owner


def test_independent_invocation_preimage_and_hash_vector() -> None:
    preimage = (
        '{"owner_key":"sha256:3244c4163f7c0f980f28e58753b4ed700378894478b6a8c2b22b21f9aa730e84",'
        '"runner_invocation_id":"11111111-1111-4111-8111-111111111111",'
        '"schema":"dpone.composition-qualification-invocation-key.v1",'
        '"try_number":1,"work_item_id":"route/α\\\\fixture"}'
    ).encode()
    expected = "sha256:8cdfc6d9455e5ee9ab0879d49856d8b2e1820e231fb8290d84e98efa5cfa3ca8"
    assert "sha256:" + hashlib.sha256(preimage).hexdigest() == expected
    assert operation().invocation_key == expected


def test_invocation_property_preserves_pre_amendment_operation_bytes_and_hash() -> None:
    original = (
        '{"action":"route_qualification",'
        '"fixture_plan_sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        '"grant_sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        '"guard_epochs":[["sha256:8a512f69acacb46ad3493ae97071842523403ebe077bdc08025018777ee79b94",1],'
        '["sha256:91879df8135377225de21ebf67da11d07affdb9f9771222b4ccf43355efa4092",1],'
        '["sha256:c08752823ff7ac2b6983a2e0f2bc028a3e82d9661551e8d6a73fd7a17f07f2f3",1]],'
        '"owner_key":"sha256:3244c4163f7c0f980f28e58753b4ed700378894478b6a8c2b22b21f9aa730e84",'
        '"owner_subject_sha256":"sha256:67b569cd9eddda76cb72f47677d7d176dea4fb24c78ca57b126b099a18df2b25",'
        '"qualification_plan_sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        '"qualification_run_id":"11111111-1111-4111-8111-111111111111",'
        '"runner_invocation_id":"11111111-1111-4111-8111-111111111111",'
        '"schema":"dpone.composition-qualification-operation.v1","try_number":1,'
        '"work_item_id":"route/α\\\\fixture",'
        '"work_item_sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'
    ).encode()
    expected = "sha256:2a7213b01ce44aee667c73c0fe84570c32b490401008e41e7c3a77150f8d9389"
    value = operation()
    assert value.to_bytes() == original
    invocation = value.invocation_key
    assert value.to_bytes() == original
    assert value.operation_key == expected
    reopened = CompositionQualificationOperation.from_bytes(original, expected_sha256=expected)
    assert reopened.invocation_key == invocation
    assert reopened.to_bytes() == original
    assert "invocation_key" not in {field.name for field in fields(value)}
    assert "invocation_key" not in value.to_dict()
    descriptor = vars(CompositionQualificationOperation)["invocation_key"]
    assert isinstance(descriptor, property) and descriptor.fset is None


@pytest.mark.parametrize(
    "changes",
    [
        {"action": "fixture_seed"},
        {"action": "source_seal"},
        {"work_item_sha256": OTHER},
        {"owner_subject_sha256": OTHER},
        {"grant_sha256": OTHER},
        {"fixture_plan_sha256": OTHER},
        {"qualification_plan_sha256": OTHER},
        {"guard_epochs": ((OTHER, 2),)},
    ],
)
def test_changed_original_subjects_cannot_create_a_new_invocation(changes: dict[str, Any]) -> None:
    original = operation()
    changed = replace(original, **changes)
    assert changed.to_bytes() != original.to_bytes()
    assert changed.operation_key != original.operation_key
    assert changed.invocation_key == original.invocation_key


@pytest.mark.parametrize(
    "changes",
    [
        {"owner_key": owner(qualification_run_id=NEXT_RUN).owner_key, "qualification_run_id": NEXT_RUN},
        {"work_item_id": "another/work-item"},
        {"runner_invocation_id": NEXT_RUN},
        {"try_number": 2},
    ],
)
def test_each_actual_invocation_coordinate_changes_its_key(changes: dict[str, Any]) -> None:
    assert operation(**changes).invocation_key != operation().invocation_key


@pytest.mark.parametrize(
    ("left", "right"),
    [("route/a", "route\\a"), ("route/é", "route/e\u0301"), ("route//a", "route/a"), ("route/α", "route/Α")],
)
def test_invocation_preserves_exact_text_without_normalization(left: str, right: str) -> None:
    assert operation(work_item_id=left).invocation_key != operation(work_item_id=right).invocation_key


def test_512_supplementary_code_points_roundtrip_without_truncation() -> None:
    identifier = "\U0001f642" * 512
    value = operation(work_item_id=identifier)
    raw = value.to_bytes()
    assert identifier.encode("utf-8") in raw
    reopened = CompositionQualificationOperation.from_bytes(raw, expected_sha256=value.operation_key)
    assert reopened.work_item_id == identifier
    assert reopened.to_bytes() == raw
    assert reopened.invocation_key == value.invocation_key
    assert replace(value, work_item_id=identifier[:-1]).invocation_key != value.invocation_key
    assert replace(value, work_item_id=identifier[:-1] + "\U0001f643").invocation_key != value.invocation_key


@pytest.mark.parametrize(
    ("field", "invalid", "reason"),
    [
        ("owner_key", OTHER, "qualification_operation_owner"),
        ("qualification_run_id", NEXT_RUN, "qualification_operation_owner"),
        ("work_item_id", "\ud800-secret", "qualification_operation"),
        ("work_item_id", " secret ", "qualification_operation"),
        ("work_item_id", "\U0001f642" * 513, "qualification_operation"),
        ("runner_invocation_id", "secret", "qualification_operation"),
        ("try_number", True, "qualification_operation_positive_bigint"),
        ("try_number", 2**63, "qualification_operation_positive_bigint"),
        ("action", "secret", "qualification_operation_action"),
        ("work_item_sha256", "secret", "qualification_operation"),
        ("guard_epochs", (), "qualification_operation_epochs"),
    ],
)
def test_invocation_revalidates_complete_defensive_state_with_fixed_errors(
    field: str, invalid: Any, reason: str
) -> None:
    value = operation()
    object.__setattr__(value, field, invalid)
    with pytest.raises(CompositionAdmissionError) as caught:
        _ = value.invocation_key
    assert caught.value.reason == reason
    assert str(caught.value) == f"DPONE_COMPOSITION_ADMISSION_UNAVAILABLE: {reason}"
    assert caught.value.__suppress_context__ or caught.value.__context__ is None
