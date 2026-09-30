"""Golden vectors are pinned against the pre-refactor legacy implementation."""

from __future__ import annotations

import importlib
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from dpone.runtime.clickhouse_binary_encoding import encode_clickhouse_value, var_uint
from dpone.runtime.clickhouse_temporal_encoding import encode_temporal_integer
from dpone.runtime.sinks.clickhouse_window_evidence import TypedMultiset
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy


def test_temporal_compatibility_import_does_not_load_optional_connector_sdks():
    code = """
import sys
from dpone.runtime.clickhouse_temporal_encoding import encode_temporal_integer
assert encode_temporal_integer(0, 'date') == bytes(2)
assert not {'pyodbc', 'clickhouse_driver', 'pyarrow', 'sqlalchemy'} & sys.modules.keys()
"""
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True, timeout=15)


def test_legacy_evidence_vectors_unchanged():
    policy = MssqlClickHouseTypePolicy()
    rows = ((7, None), (7, None), (0, ""))
    multiset = TypedMultiset(2)
    assert multiset.digest() == "a58899e3f336188f8006c192bd29dfe14abcd79d4169de1d9ca947aef101b4ce"
    for row, expected in zip(rows, ("0700000001", "0700000001", "000000000000"), strict=True):
        encoded = encode_clickhouse_value(row[0], "Int32", "int", policy) + encode_clickhouse_value(
            row[1], "Nullable(String)", "nvarchar", policy
        )
        assert encoded.hex() == expected
        multiset.add(row, encoded)
    assert multiset.digest() == "4f01125a68944af1cab7b2a502392fbad30104c06dc4c14b3a5dcb446184b98f"
    assert multiset.nulls == [0, 2]
    assert multiset.encoded_bytes == 16
    assert var_uint(128) == b"\x80\x01"
    assert encode_clickhouse_value(1.9, "Int32", "int", policy) == b"\x01\0\0\0"
    assert (
        encode_clickhouse_value(
            datetime(1970, 1, 1, microsecond=3333, tzinfo=UTC), "DateTime64(3)", "datetime2", policy
        )
        == b"\x03" + b"\0" * 7
    )
    assert encode_clickhouse_value(Decimal("1.23"), "Decimal256(2)", "decimal", policy) == b"\x7b" + b"\0" * 31
    with pytest.raises(ValueError, match="clickhouse_binary_numeric_out_of_range"):
        encode_clickhouse_value(256, "UInt8", "int", policy)
    with pytest.raises(ValueError, match="clickhouse_binary_temporal_out_of_range"):
        encode_temporal_integer(-1, "date")
    with pytest.raises(ValueError, match="clickhouse_binary_decimal_precision_loss"):
        encode_clickhouse_value(Decimal("1.001"), "Decimal32(2)", "decimal", policy)


def test_snapshot_and_restore_do_not_rehash_or_change_vectors():
    api = importlib.import_module("dpone.contracts.clickhouse_typed_multiset")
    value = TypedMultiset(2)
    value.add((1, None), b"x")
    state = api.snapshot_multiset(value)
    restored = api.restore_multiset(state)
    assert restored.digest() == value.digest()
    assert restored.encoded_bytes == 1
    assert api.TypedMultiset is TypedMultiset
    restored.add((1, 2), b"y")
    assert state.count == 1
    assert state.nulls == (0, 1)


def test_injected_hash_collision_is_not_mathematical_equality(monkeypatch):
    api = importlib.import_module("dpone.contracts.clickhouse_typed_multiset")

    class Collision:
        def digest(self):
            return bytes(32)

        def hexdigest(self):
            return "0" * 64

    monkeypatch.setattr(api.hashlib, "sha256", lambda _: Collision())
    first, second = api.TypedMultiset(1), api.TypedMultiset(1)
    first.add((None,), b"a")
    second.add((0,), b"b")
    second.add((0,), b"b")
    assert first.total == second.total == 0
    assert (first.count, first.nulls) != (second.count, second.nulls)


@pytest.mark.parametrize(
    ("count", "total", "size", "nulls"),
    [
        (-1, 0, 0, (0,)),
        (1, -1, 1, (0,)),
        (1, 2**256, 1, (0,)),
        (1, 0, -1, (0,)),
        (1, 0, 1, (2,)),
        (True, 0, 0, (0,)),
        (1, 0, 1, []),
    ],
)
def test_invalid_multiset_state_rejects(count, total, size, nulls):
    api = importlib.import_module("dpone.contracts.clickhouse_observation")
    with pytest.raises(ValueError):
        api.MultisetState(count, total, size, nulls)
