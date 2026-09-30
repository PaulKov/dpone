"""Strict, lossless evidence inputs; no SDK or server is needed for this layer."""

from __future__ import annotations

import copy
import importlib
import math
import pickle
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from uuid import UUID

import pytest


def _api():
    return importlib.import_module("dpone.contracts.clickhouse_observation")


def _limits(**changes):
    return _api().ObservationLimits(
        **(
            dict(
                max_columns=16,
                max_batch_rows=8,
                max_batch_bytes=1024,
                max_row_bytes=256,
                max_partitions=8,
                max_scan_rows=100,
                max_scan_bytes=4096,
                request_seconds=5.0,
            )
            | changes
        )
    )


def _profile(*types, limits=None):
    api = _api()
    design = api.CandidateDesign(tuple(api.CandidateColumn(f"c{i}", t) for i, t in enumerate(types)), (), (), ())
    module = importlib.import_module("dpone.adapters.clickhouse_observation_profile")
    return module.ProtectedObservationProfile(design, limits or _limits())


@pytest.mark.parametrize(
    ("dtype", "value", "hex_value"),
    [
        ("Bool", True, "01"),
        ("Int8", -128, "80"),
        ("UInt8", 255, "ff"),
        ("Int16", -32768, "0080"),
        ("UInt16", 65535, "ffff"),
        ("Int32", -2147483648, "00000080"),
        ("UInt32", 4294967295, "ffffffff"),
        ("Int64", -(2**63), "0000000000000080"),
        ("UInt64", 2**64 - 1, "ffffffffffffffff"),
        ("Float32", -0.0, "00000080"),
        ("Float64", 1.5, "000000000000f83f"),
        ("String", b"\x00\xff", "0200ff"),
        ("String", "я", "02d18f"),
        ("FixedString(4)", b"x\x00", "78000000"),
        ("UUID", UUID("00112233-4455-6677-8899-aabbccddeeff"), "7766554433221100ffeeddccbbaa9988"),
        ("Date", date(1970, 1, 1), "0000"),
        ("Date32", date(1969, 12, 31), "ffffffff"),
        ("DateTime('UTC')", datetime(1970, 1, 1, tzinfo=UTC), "00000000"),
        ("DateTime64(3, 'UTC')", datetime(1970, 1, 1, microsecond=1000, tzinfo=UTC), "0100000000000000"),
        ("Decimal32(2)", Decimal("-1.23"), "85ffffff"),
        ("Decimal64(2)", Decimal("1.23"), "7b00000000000000"),
        ("Decimal128(2)", Decimal("1.23"), "7b" + "00" * 15),
        ("Nullable(String)", None, "01"),
        ("Nullable(String)", b"", "0000"),
    ],
)
def test_exact_scalar_vectors(dtype, value, hex_value):
    assert _profile(dtype).encode_row((value,)).hex() == hex_value


@pytest.mark.parametrize(
    ("dtype", "value"),
    [
        ("Bool", 1),
        ("Int32", True),
        ("Int32", 1.0),
        ("Int8", 128),
        ("UInt8", -1),
        ("Float32", 0.1),
        ("Float32", 1e40),
        ("Float64", 1),
        ("Float64", math.inf),
        ("String", bytearray(b"x")),
        ("String", object()),
        ("String", ["x"]),
        ("FixedString(1)", b"xx"),
        ("UUID", "00000000-0000-0000-0000-000000000000"),
        ("Decimal32(2)", Decimal("1.001")),
        ("Decimal32(2)", Decimal("10000000")),
        ("Decimal32(2)", Decimal("NaN")),
        ("Decimal32(2)", 1),
        ("Date", date(1969, 12, 31)),
        ("Date32", date(1899, 12, 31)),
        ("Date32", date(2300, 1, 1)),
        ("Date", datetime(2020, 1, 1)),
        ("DateTime('UTC')", datetime(2020, 1, 1)),
        ("DateTime('UTC')", datetime(2020, 1, 1, microsecond=1, tzinfo=UTC)),
        ("DateTime('UTC')", datetime(2020, 1, 1, tzinfo=timezone(timedelta(hours=1)))),
        ("DateTime64(3, 'UTC')", datetime(2020, 1, 1, microsecond=1, tzinfo=UTC)),
        ("String", None),
    ],
)
def test_lossy_or_custom_values_reject(dtype, value):
    with pytest.raises(ValueError):
        _profile(dtype).encode_row((value,))


def test_float64_nonfinite_is_rejected():
    profile = _profile("Float64")
    with pytest.raises(ValueError):
        profile.encode_row((float("nan"),))
    assert profile.encode_row((-0.0,)) != profile.encode_row((0.0,))


@pytest.mark.parametrize(
    "dtype",
    [
        "DateTime64(7, 'UTC')",
        "DateTime64(3)",
        "DateTime",
        "Decimal256(2)",
        "Decimal(39, 2)",
        "Decimal(10, 2)",
        "Decimal32(10)",
        "Array(Int32)",
        "Tuple(Int32)",
        "Map(String, Int32)",
        "LowCardinality(String)",
        "Enum8('a'=1)",
        "AggregateFunction(sum, Int64)",
        "Nullable(Nullable(Int32))",
        "FixedString(0)",
    ],
)
def test_unsupported_declarations_reject(dtype):
    with pytest.raises(ValueError):
        _profile(dtype)


@pytest.mark.parametrize(
    "field",
    [
        "max_columns",
        "max_batch_rows",
        "max_batch_bytes",
        "max_row_bytes",
        "max_partitions",
        "max_scan_rows",
        "max_scan_bytes",
        "request_seconds",
    ],
)
@pytest.mark.parametrize("value", [True, 0, -1, math.inf, math.nan])
def test_limits_require_explicit_positive_finite_values(field, value):
    with pytest.raises(ValueError):
        _limits(**{field: value})


def test_batches_preserve_multiplicity_but_payload_preserves_order():
    profile = _profile("Nullable(String)")
    empty = profile.validate_batch(())
    assert empty.evidence.count == 0
    rows = ((None,), (b"",), (b"",))
    batch = profile.validate_batch(rows)
    reverse = profile.validate_batch(tuple(reversed(rows)))
    assert batch.evidence == reverse.evidence
    assert batch.payload_digest != reverse.payload_digest
    assert batch.evidence.nulls == (1,)
    assert batch.evidence.count == 3
    assert batch.evidence.encoded_bytes == 5
    assert profile.encode_row((b"x",)) == profile.encode_row(("x",))
    assert _profile("FixedString(4)").encode_row((b"x",)) == _profile("FixedString(4)").encode_row((b"x\0\0",))


def test_explicit_limits_reject_without_truncation():
    with pytest.raises(ValueError):
        _limits(max_row_bytes=2048)
    with pytest.raises(ValueError):
        _profile("String", "String", limits=_limits(max_columns=1))
    profile = _profile("String", limits=_limits(max_batch_bytes=4, max_row_bytes=4, max_batch_rows=2))
    assert profile.encode_row((b"abc",)) == b"\x03abc"
    for rows in [((b"abcd",),), ((b"aa",), (b"aa",)), ((b"",),) * 3]:
        with pytest.raises(ValueError):
            profile.validate_batch(rows)
    with pytest.raises(ValueError):
        _profile("FixedString(1000000000)").encode_row((b"x",))
    with pytest.raises(ValueError):
        profile.encode_row(("x", "y"))


def test_mutable_rows_and_subclasses_reject():
    class CustomInt(int):
        pass

    class Submicrosecond(datetime):
        submicrosecond_100ns = 1

    for dtype, value in [("Int64", CustomInt(1)), ("DateTime64(6, 'UTC')", Submicrosecond(2020, 1, 1, tzinfo=UTC))]:
        with pytest.raises(ValueError):
            _profile(dtype).encode_row((value,))
    with pytest.raises(ValueError):
        _profile("String").validate_batch((["x"],))


def test_aliases_and_profile_identity():
    alias, canonical = _profile("Decimal32(2)"), _profile("Decimal(9, 2)")
    assert alias.design == canonical.design
    assert alias.profile_digest == canonical.profile_digest
    assert alias.profile_id == "dpone.clickhouse.observation.v1"
    other = _profile("Decimal32(2)", limits=_limits(max_scan_rows=101))
    assert other.design_digest == alias.design_digest
    assert other.profile_digest != alias.profile_digest


def test_verified_enrollment_is_not_a_serializable_success_flag():
    from dpone.contracts.clickhouse_authority import AuthorityError, AuthoritySubject

    api = importlib.import_module("dpone.contracts.clickhouse_candidate")
    profile = _profile("Int32")
    request = api.ProtectedPublicationRequest(
        "deployment:one",
        AuthoritySubject("deployment", "server", "db", "target"),
        "candidate",
        profile.design,
        profile.limits,
    )
    with pytest.raises(TypeError):
        api.VerifiedEnrollment(True)
    # Trusted test issuer models verified readiness, never an operator input flag.
    capability = api.VerifiedEnrollment._issue(request, "inventory-1", "a" * 64, profile.profile_digest)
    capability.assert_current(request)
    with pytest.raises(AuthorityError):
        capability.assert_current(replace(request, candidate="other"))
    for copy_fn in [copy.copy, copy.deepcopy, pickle.dumps]:
        with pytest.raises(TypeError):
            copy_fn(capability)
    capability.close()
    with pytest.raises(AuthorityError):
        capability.assert_current(request)


def test_mutable_custom_timezone_cannot_change_accepted_value():
    class MutableTimezone(tzinfo):
        offset = timedelta(0)

        def utcoffset(self, dt):
            return self.offset

    with pytest.raises(ValueError):
        _profile("DateTime64(6, 'UTC')").encode_row((datetime(2020, 1, 1, tzinfo=MutableTimezone()),))


def test_request_seconds_must_be_representable_and_identity_is_canonical():
    with pytest.raises(ValueError):
        _limits(request_seconds=10**400)
    assert _profile("Int32", limits=_limits(request_seconds=5)).profile_digest == _profile("Int32").profile_digest


def test_byte_limit_precedes_wire_payload_allocation(monkeypatch):
    module = importlib.import_module("dpone.adapters.clickhouse_observation_profile")
    calls = []
    monkeypatch.setattr(module, "encode_string", lambda value: calls.append(value))
    monkeypatch.setattr(module, "encode_fixed_string", lambda value, width: calls.append((value, width)))
    for dtype, value in [("String", "雪" * 256), ("String", b"x" * 256), ("FixedString(1000000000)", b"x")]:
        with pytest.raises(ValueError):
            _profile(dtype).encode_row((value,))
    assert calls == []
