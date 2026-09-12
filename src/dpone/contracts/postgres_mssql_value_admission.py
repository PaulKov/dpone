"""Exact projected-value admission for PostgreSQL→MSSQL R1 scalars."""

from __future__ import annotations

import math
import re
import struct
from collections.abc import Callable
from datetime import date, datetime, time
from uuid import UUID

from dpone.contracts.postgres_mssql_type_target_shapes import (
    MssqlR1CanonicalTargetScalarShapeV1,
    MssqlR1TargetScalarFamilyV1,
    PostgresMssqlCodecV1,
    PostgresMssqlEqualityPolicyV1,
    PostgresMssqlLengthKindV1,
    PostgresMssqlNormalizationV1,
    PostgresMssqlSourceScalarFamilyV1,
    PostgresMssqlSourceScalarShapeV1,
    PostgresMssqlValueAdmissionAuthorityV1,
    PostgresMssqlValueGuardV1,
    authority_decode,
    authority_validation,
    reject,
    require_identifier_v1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import (
    PostgresMssqlValueGuardV1 as Guard,
)


def admit_text_value(
    value: str,
    source: PostgresMssqlSourceScalarShapeV1,
    admission: PostgresMssqlValueAdmissionAuthorityV1,
    target: MssqlR1CanonicalTargetScalarShapeV1,
) -> str:
    """Require exact canonical projected text and its sealed size bounds."""

    if type(value) is not str:
        reject("value_grammar_invalid")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        reject("value_grammar_invalid")
    if len(encoded) > admission.maximum_input_bytes:
        reject("value_grammar_invalid")
    guard = admission.guard
    if guard is Guard.BOOLEAN_DOMAIN and value not in {"0", "1"}:
        reject("value_grammar_invalid")
    if guard in {Guard.INT2_RANGE, Guard.INT4_RANGE, Guard.INT8_RANGE}:
        _admit_integer(value, guard)
    if guard is Guard.DECIMAL_SHAPE_AND_VALUE:
        _admit_decimal(value, source.scale, target.precision, target.scale)
    if guard is Guard.FINITE_FLOAT4:
        if value != canonical_binary32_text(_float32_bits_from_text(value)):
            reject("value_grammar_invalid")
    if guard is Guard.FINITE_FLOAT8:
        if value != canonical_binary64_text(_float64_bits_from_text(value)):
            reject("value_grammar_invalid")
    if guard is Guard.UUID_DOMAIN:
        _admit_uuid(value)
    if guard is Guard.SQLSERVER_DATE_RANGE:
        _admit_date(value)
    if guard in {
        Guard.SQLSERVER_TIME_PRECISION,
        Guard.SQLSERVER_DATETIME2_RANGE_PRECISION,
        Guard.SQLSERVER_DATETIMEOFFSET_RANGE_PRECISION,
    }:
        _admit_temporal(value, source.precision, guard)
    if guard is Guard.UTF8_AND_UTF16_CAPACITY:
        try:
            units = len(value.encode("utf-16-le")) // 2
        except UnicodeEncodeError:
            reject("value_grammar_invalid")
        if admission.maximum_utf16_units is not None and units > admission.maximum_utf16_units:
            reject("value_grammar_invalid")
    return value


def canonical_binary32_text(bits: bytes) -> str:
    """Return the pinned shortest text for one big-endian IEEE binary32."""

    if type(bits) is not bytes or len(bits) != 4:
        reject("value_grammar_invalid")
    value = struct.unpack("!f", bits)[0]
    if not math.isfinite(value):
        reject("value_grammar_invalid")
    if value == 0.0:
        return "0"
    return _shortest_round_trip_text(value, bits, maximum_precision=9, parse_bits=_try_float32_bits)


def canonical_binary64_text(bits: bytes) -> str:
    """Return the pinned shortest text for one big-endian IEEE binary64."""

    if type(bits) is not bytes or len(bits) != 8:
        reject("value_grammar_invalid")
    value = struct.unpack("!d", bits)[0]
    if not math.isfinite(value):
        reject("value_grammar_invalid")
    if value == 0.0:
        return "0"
    return _shortest_round_trip_text(value, bits, maximum_precision=17, parse_bits=_try_float64_bits)


def _admit_integer(value: str, guard: Guard) -> None:
    if re.fullmatch(r"0|-?[1-9][0-9]*", value) is None:
        reject("value_grammar_invalid")
    bounds = {
        Guard.INT2_RANGE: (-32768, 32767),
        Guard.INT4_RANGE: (-(2**31), 2**31 - 1),
        Guard.INT8_RANGE: (-(2**63), 2**63 - 1),
    }
    digits = value.lstrip("-")
    if len(digits) > 19:
        reject("value_grammar_invalid")
    if not bounds[guard][0] <= int(value) <= bounds[guard][1]:
        reject("value_grammar_invalid")


def _admit_decimal(
    value: str,
    source_scale: int | None,
    precision: int | None,
    scale: int | None,
) -> None:
    assert source_scale is not None and precision is not None and scale is not None
    pattern = r"0|-?[1-9][0-9]*" if scale == 0 else rf"-?(?:0|[1-9][0-9]*)\.[0-9]{{{scale}}}"
    if re.fullmatch(pattern, value) is None or (value.startswith("-") and set(value[1:].replace(".", "")) == {"0"}):
        reject("value_grammar_invalid")
    integer = value.lstrip("-").split(".", 1)[0]
    if len(integer if integer != "0" else "") > precision - scale:
        reject("value_grammar_invalid")
    required_zeroes = -source_scale
    if source_scale < 0 and value != "0" and not value.endswith("0" * required_zeroes):
        reject("value_grammar_invalid")


def _shortest_round_trip_text(
    value: float,
    bits: bytes,
    maximum_precision: int,
    parse_bits: Callable[[str], bytes | None],
) -> str:
    """Choose the shortest canonical decimal spelling that round-trips to ``bits``."""

    for precision in range(1, maximum_precision + 1):
        candidates = tuple(
            candidate
            for candidate in _equivalent_decimal_spellings(format(value, f".{precision}g"))
            if parse_bits(candidate) == bits
        )
        if candidates:
            # Fixed notation wins an equal-length tie; lexical order pins the remaining tie.
            return min(candidates, key=lambda candidate: (len(candidate), "e" in candidate, candidate))
    reject("value_grammar_invalid")


def _equivalent_decimal_spellings(value: str) -> tuple[str, ...]:
    """Return minimal fixed and scientific spellings of one finite decimal value."""

    negative = value.startswith("-")
    unsigned = value.removeprefix("-").lower()
    mantissa, marker, exponent_text = unsigned.partition("e")
    exponent = int(exponent_text) if marker else 0
    integer, dot, fraction = mantissa.partition(".")
    digits = integer + (fraction if dot else "")
    decimal_position = len(integer) + exponent

    leading_zeroes = len(digits) - len(digits.lstrip("0"))
    digits = digits.lstrip("0") or "0"
    decimal_position -= leading_zeroes
    if digits != "0":
        digits = digits.rstrip("0")

    if decimal_position <= 0:
        fixed = "0." + ("0" * -decimal_position) + digits
    elif decimal_position >= len(digits):
        fixed = digits + ("0" * (decimal_position - len(digits)))
    else:
        fixed = digits[:decimal_position] + "." + digits[decimal_position:]

    scientific_exponent = decimal_position - 1
    scientific = digits[0]
    if len(digits) > 1:
        scientific += "." + digits[1:]
    if scientific_exponent:
        scientific += f"e{scientific_exponent}"

    sign = "-" if negative else ""
    return tuple(dict.fromkeys((sign + fixed, sign + scientific)))


def _float32_bits_from_text(value: str) -> bytes:
    bits = _try_float32_bits(value)
    if bits is None:
        reject("value_grammar_invalid")
    return bits


def _try_float32_bits(value: str) -> bytes | None:
    try:
        parsed = float(value)
        if not math.isfinite(parsed):
            return None
        return struct.pack("!f", parsed)
    except (OverflowError, ValueError):
        return None


def _float64_bits_from_text(value: str) -> bytes:
    bits = _try_float64_bits(value)
    if bits is None:
        reject("value_grammar_invalid")
    return bits


def _try_float64_bits(value: str) -> bytes | None:
    try:
        parsed = float(value)
        if not math.isfinite(parsed):
            return None
        return struct.pack("!d", parsed)
    except (OverflowError, ValueError):
        return None


def _admit_uuid(value: str) -> None:
    pattern = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    if re.fullmatch(pattern, value) is None:
        reject("value_grammar_invalid")
    try:
        parsed = UUID(value)
    except ValueError:
        reject("value_grammar_invalid")
    if str(parsed) != value:
        reject("value_grammar_invalid")


def _admit_date(value: str) -> None:
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None:
        reject("value_grammar_invalid")
    try:
        date.fromisoformat(value)
    except ValueError:
        reject("value_grammar_invalid")


def _parse_utc_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.removesuffix("Z") + "+00:00")


def _admit_temporal(value: str, precision: int | None, guard: Guard) -> None:
    assert precision is not None
    fraction = "" if precision == 0 else rf"\.[0-9]{{{precision}}}"
    clock = rf"[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}{fraction}"
    if guard is Guard.SQLSERVER_TIME_PRECISION:
        pattern = clock
        if re.fullmatch(pattern, value) is None:
            reject("value_grammar_invalid")
        try:
            time.fromisoformat(value)
        except ValueError:
            reject("value_grammar_invalid")
        return
    elif guard is Guard.SQLSERVER_DATETIME2_RANGE_PRECISION:
        pattern = rf"[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T{clock}"
        if re.fullmatch(pattern, value) is None:
            reject("value_grammar_invalid")
        try:
            datetime.fromisoformat(value)
        except ValueError:
            reject("value_grammar_invalid")
        return
    else:
        pattern = rf"[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T{clock}Z"
    if re.fullmatch(pattern, value) is None:
        reject("value_grammar_invalid")
    try:
        _parse_utc_datetime(value)
    except ValueError:
        reject("value_grammar_invalid")


def admit_binary_value(
    value: bytes,
    codec: PostgresMssqlCodecV1,
    maximum_input_bytes: int,
) -> bytes:
    """Admit an exact raw-binary scalar without copying or coercion."""

    if (
        codec is not PostgresMssqlCodecV1.RAW_BINARY_V1
        or type(value) is not bytes
        or type(maximum_input_bytes) is not int
        or len(value) > maximum_input_bytes
    ):
        reject("value_grammar_invalid")
    return value


__all__ = (
    "MssqlR1CanonicalTargetScalarShapeV1",
    "MssqlR1TargetScalarFamilyV1",
    "PostgresMssqlCodecV1",
    "PostgresMssqlEqualityPolicyV1",
    "PostgresMssqlLengthKindV1",
    "PostgresMssqlNormalizationV1",
    "PostgresMssqlSourceScalarFamilyV1",
    "PostgresMssqlSourceScalarShapeV1",
    "PostgresMssqlValueAdmissionAuthorityV1",
    "PostgresMssqlValueGuardV1",
    "admit_text_value",
    "authority_decode",
    "authority_validation",
    "canonical_binary32_text",
    "canonical_binary64_text",
    "reject",
    "require_identifier_v1",
)
