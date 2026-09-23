"""V4 parent authority is immutable, ordered and safe to replay."""

from dataclasses import replace

import pytest

from dpone.contracts.mssql_native_parent_journal import (
    NativeCheckpointReceipt,
    NativeChunkRetirementReceipt,
    NativeParentAuthority,
    NativeParentRetirementReceipt,
)

H = "a" * 64


def chunk(ordinal: int) -> NativeChunkRetirementReceipt:
    return NativeChunkRetirementReceipt(
        ordinal=ordinal,
        attempt_id=f"run-{ordinal}-0",
        parent_authority_digest=H,
        object_incarnation_sha256="b" * 64,
        verification_receipt_sha256="c" * 64,
        implementation_sha256="d" * 64,
        drop_operation_sha256="e" * 64,
        absence_sha256="f" * 64,
        terminal_sha256="1" * 64,
        directory_sha256="2" * 64,
        capacity_sha256="3" * 64,
    )


def test_parent_authority_accepts_only_settled_outcomes():
    assert NativeParentAuthority("published", H, 1).kind == "published"
    assert NativeParentAuthority("aborted", H, 1).kind == "aborted"
    with pytest.raises(ValueError, match="unsettled"):
        NativeParentAuthority("unknown", H, 1)


def test_parent_retirement_receipt_requires_exact_order_and_authority():
    receipt = NativeParentRetirementReceipt(H, (chunk(0), chunk(1)))
    assert receipt.digest == receipt.digest
    for changed in (
        (chunk(1), chunk(0)),
        (chunk(0), replace(chunk(1), parent_authority_digest="4" * 64)),
    ):
        with pytest.raises(ValueError, match="retirement"):
            NativeParentRetirementReceipt(H, changed)


def test_nested_receipt_mutation_changes_parent_digest():
    original = NativeParentRetirementReceipt(H, (chunk(0),))
    changed = NativeParentRetirementReceipt(H, (replace(chunk(0), absence_sha256="4" * 64),))
    assert changed.digest != original.digest


def test_checkpoint_receipt_is_closed_and_requires_cas_identity():
    receipt = NativeCheckpointReceipt(H, "target", "window", 2, 7, "b" * 64)
    assert NativeCheckpointReceipt.from_dict(receipt.to_dict()) == receipt
    with pytest.raises(ValueError, match="checkpoint"):
        NativeCheckpointReceipt.from_dict({**receipt.to_dict(), "extra": True})
