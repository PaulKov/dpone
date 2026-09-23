"""Shared exact scalar validation for TDS identity and authority records."""

import math
import re
from typing import Any
from uuid import UUID


def _integer(value: Any, minimum: int = 0, maximum: int = 2**63 - 1) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("mssql_native.tds_invalid_integer")


def _text(value: Any, maximum: int = 256) -> None:
    if type(value) is not str or not value or len(value) > maximum or any(ord(c) < 32 for c in value):
        raise ValueError("mssql_native.tds_invalid_text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError:
        raise ValueError("mssql_native.tds_invalid_text") from None


def _hash(value: Any) -> None:
    if type(value) is not str or re.fullmatch("[0-9a-f]{64}", value) is None:
        raise ValueError("mssql_native.tds_invalid_sha256")


def _uuid(value: Any) -> None:
    _text(value, 36)
    try:
        valid = str(UUID(value)) == value and UUID(value).int != 0
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("mssql_native.tds_invalid_uuid")


def deadline_nanoseconds(deadline: float) -> int:
    """Convert original binary64 monotonic seconds downward without extending it.

    The caller retains the original seconds value for transport/connection APIs
    and authenticates its clock domain. This pure conversion reads no clock and
    grants no new budget. Sub-nanosecond and int64-overflow values are unusable.
    """
    if type(deadline) is not float or not math.isfinite(deadline) or deadline <= 0:
        raise ValueError("mssql_native.tds_deadline_invalid")
    numerator, denominator = deadline.as_integer_ratio()
    converted = numerator * 1_000_000_000 // denominator
    if not 1 <= converted <= 2**63 - 1:
        raise ValueError("mssql_native.tds_deadline_invalid")
    return converted


def deadline_seconds(nanoseconds: int) -> float:
    """Project an original integer deadline to binary64 without granting time.

    Keep the integer in its authenticated request. This scheduling projection
    is not invertible: even one nanosecond projects below the inverse helper's
    usable range. Exact clock/request identity must not depend on a round trip.
    """
    if type(nanoseconds) is not int or not 1 <= nanoseconds <= 2**63 - 1:
        raise ValueError("mssql_native.tds_deadline_invalid")
    seconds = nanoseconds / 1_000_000_000
    numerator, denominator = seconds.as_integer_ratio()
    if numerator * 1_000_000_000 > nanoseconds * denominator:
        seconds = math.nextafter(seconds, 0.0)
    return seconds
