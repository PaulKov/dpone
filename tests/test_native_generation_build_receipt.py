"""A successful bridge result is not a custody or dispatch grant."""

from dataclasses import FrozenInstanceError, replace
from uuid import UUID

import pytest

from dpone.contracts.native_delivery import GenerationBuildReceipt, NativeGenerationContractError
from dpone.contracts.native_identity import OriginalRef


def receipt():
    reference = OriginalRef("generation/original", "sha256:" + "a" * 64)
    return GenerationBuildReceipt(UUID(int=1), 1, reference, UUID(int=2), reference, reference, reference, "SUCCEEDED")


def test_positive_build_receipt_is_frozen_and_keeps_exact_references():
    value = receipt()
    assert value.outcome == "SUCCEEDED"
    assert value.build_evidence is value.artifact_inventory is value.termination
    with pytest.raises(FrozenInstanceError):
        value.guard_epoch = 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("generation_id", "00000000-0000-0000-0000-000000000001"),
        ("executor_invocation_id", 2),
        ("guard_epoch", True),
        ("guard_epoch", 0),
        ("guard_epoch", -1),
        ("guard_epoch", 9223372036854775808),
        ("guard_epoch", 1.0),
        ("outcome", "FAILED"),
        ("outcome", "UNKNOWN"),
        ("outcome", True),
        ("reservation", None),
        ("build_evidence", None),
        ("artifact_inventory", None),
        ("termination", None),
    ],
)
def test_receipt_cannot_represent_partial_or_failed_completion(field, value):
    with pytest.raises(NativeGenerationContractError):
        replace(receipt(), **{field: value})


def test_receipt_accepts_maximum_sql_epoch():
    assert replace(receipt(), guard_epoch=9223372036854775807).guard_epoch == 9223372036854775807
