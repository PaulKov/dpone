"""Closed immutable dispatch identity; no fixture confers writer authority."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import (
    ClickHouseDispatchColumn,
    CreateGenerationDispatch,
    ExchangeSnapshotDispatch,
    InsertGenerationDispatch,
    decode_clickhouse_dispatch,
)
from tests.composition_snapshot_helpers import intent


def create():
    value = intent()
    return CreateGenerationDispatch(
        value.attempt, value.target, value.generation.new_generation_uuid, (ClickHouseDispatchColumn("id", "Int32"),)
    )


def insert(payload=b"native", ordinal=0):
    value = create()
    return InsertGenerationDispatch(
        value.attempt,
        value.target,
        value.generation_uuid,
        value.columns,
        "sha256:" + sha256(payload).hexdigest(),
        len(payload),
        ordinal,
    )


@pytest.mark.parametrize("dispatch", [create(), insert(), ExchangeSnapshotDispatch(intent())])
def test_dispatch_canonical_roundtrip_and_stable_identity(dispatch):
    assert decode_clickhouse_dispatch(dispatch.to_bytes(), dispatch.dispatch_sha256) == dispatch
    assert len(dispatch.claim_key) == 71
    assert dispatch.query_id


def test_insert_ordinal_claim_cannot_be_reset_by_payload_or_generation_refingerprint():
    original = insert()
    changed = replace(original, payload_sha256="sha256:" + "f" * 64)
    assert changed.dispatch_sha256 != original.dispatch_sha256
    assert changed.claim_key == original.claim_key
    assert replace(original, ordinal=1).claim_key != original.claim_key
    assert replace(original, generation_uuid=intent().generation.old_target_uuid).claim_key == original.claim_key


@pytest.mark.parametrize(
    "name,type_name",
    [
        ("id;DROP", "Int32"),
        ("id", "Int32 DEFAULT 1"),
        ("id", "Int32) ENGINE=URL('evil')"),
        ("id", "Decimal(77,0)"),
        ("id", "Decimal(10,11)"),
        ("id", "Nullable(Nullable(Int32))"),
        ("id", "DateTime64(10)"),
        ("id", "Array(Int32)"),
    ],
)
def test_reject_non_closed_schema(name, type_name):
    with pytest.raises(CompositionAdmissionError):
        ClickHouseDispatchColumn(name, type_name)


@pytest.mark.parametrize(
    "type_name",
    [
        "Int32",
        "UInt64",
        "UUID",
        "Nullable(String)",
        "Decimal(38, 9)",
        "Nullable(Decimal(18,4))",
        "DateTime64(7, 'UTC')",
        "Date",
        "Date32",
        "FixedString(16)",
    ],
)
def test_admit_bounded_native_scalar_types(type_name):
    assert ClickHouseDispatchColumn("value", type_name).type_name == type_name


def test_target_guard_and_column_uniqueness_are_mandatory():
    value = create()
    with pytest.raises(CompositionAdmissionError):
        replace(value, attempt=replace(value.attempt, guard_epochs=(("sha256:" + "f" * 64, 1),)))
    with pytest.raises(CompositionAdmissionError):
        replace(value, columns=value.columns * 2)


def test_unknown_fields_and_hash_drift_are_rejected():
    value = create()
    with pytest.raises(CompositionAdmissionError):
        decode_clickhouse_dispatch(value.to_bytes() + b" ", value.dispatch_sha256)
    with pytest.raises(CompositionAdmissionError):
        decode_clickhouse_dispatch(value.to_bytes(), "sha256:" + "f" * 64)
