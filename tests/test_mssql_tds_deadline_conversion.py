"""Absolute deadlines may lose a fractional nanosecond, never gain time."""

import math
from fractions import Fraction

import pytest

from dpone.contracts.mssql_tds_validation import deadline_nanoseconds


@pytest.mark.parametrize(
    "deadline", [1e-9, 0.1, 1.0, 12345.6789, 10000000.000000002, math.nextafter((2**63 - 1) / 1e9, 0)]
)
def test_conversion_bounds_exact_binary64_value(deadline):
    observed = deadline_nanoseconds(deadline)
    original = Fraction(deadline)
    assert Fraction(observed, 10**9) <= original < Fraction(observed + 1, 10**9)
    assert type(observed) is int and 1 <= observed <= 2**63 - 1


def test_naive_float_multiplication_would_extend_deadline():
    deadline = 10000000.000000002
    assert int(deadline * 1e9) > deadline_nanoseconds(deadline)


@pytest.mark.parametrize(
    "deadline",
    [
        0,
        1,
        True,
        None,
        "1.0",
        0.0,
        -0.0,
        -1.0,
        float("nan"),
        float("inf"),
        -float("inf"),
        math.nextafter(1e-9, 0),
        (2**63 - 1) / 1e9,
    ],
)
def test_invalid_or_unrepresentable_deadline_rejected(deadline):
    with pytest.raises(ValueError, match="mssql_native.tds_deadline_invalid"):
        deadline_nanoseconds(deadline)


@pytest.mark.parametrize("nanoseconds", [1, 100, 100000000, 1281354761776374, 2**63 - 1])
def test_integer_deadline_uses_greatest_float_that_does_not_extend_it(nanoseconds):
    from dpone.contracts.mssql_tds_validation import deadline_seconds

    projected = deadline_seconds(nanoseconds)
    assert Fraction(projected) <= Fraction(nanoseconds, 10**9)
    assert Fraction(math.nextafter(projected, math.inf)) > Fraction(nanoseconds, 10**9)


def test_lossy_integer_roundtrip_is_bound_by_canonical_float_not_inverse():
    from dpone.contracts.mssql_tds_validation import deadline_seconds

    original = 1281354761776374
    assert deadline_nanoseconds(original / 1e9) != original
    for ns in range(original, original + 1000):
        projected = deadline_seconds(ns)
        assert Fraction(projected) <= Fraction(ns, 10**9) < Fraction(math.nextafter(projected, math.inf))


@pytest.mark.parametrize("nanoseconds", [True, False, None, 1.0, "1", 0, -1, 2**63])
def test_seconds_projection_rejects_noncanonical_integer(nanoseconds):
    from dpone.contracts.mssql_tds_validation import deadline_seconds

    with pytest.raises(ValueError, match="tds_deadline_invalid"):
        deadline_seconds(nanoseconds)
