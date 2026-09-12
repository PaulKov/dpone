"""Closed discriminators and errors for PostgreSQL→MSSQL R1 type authority."""

from __future__ import annotations

import unicodedata
from collections.abc import Callable
from functools import wraps
from typing import NoReturn, ParamSpec, TypeVar

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_errors import MssqlR1V3ContractError


class PostgresMssqlSourceScalarFamilyV1(StrEnum):
    BOOL = "bool"
    INT2 = "int2"
    INT4 = "int4"
    INT8 = "int8"
    NUMERIC = "numeric"
    FLOAT4 = "float4"
    FLOAT8 = "float8"
    UUID = "uuid"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"
    TEXT = "text"
    VARCHAR = "varchar"
    BYTEA = "bytea"


class MssqlR1TargetScalarFamilyV1(StrEnum):
    BIT = "bit"
    SMALLINT = "smallint"
    INT = "int"
    BIGINT = "bigint"
    DECIMAL = "decimal"
    REAL = "real"
    FLOAT_53 = "float_53"
    UNIQUEIDENTIFIER = "uniqueidentifier"
    DATE = "date"
    TIME = "time"
    DATETIME2 = "datetime2"
    DATETIMEOFFSET = "datetimeoffset"
    NVARCHAR = "nvarchar"
    VARBINARY = "varbinary"


class PostgresMssqlLengthKindV1(StrEnum):
    NOT_APPLICABLE = "not_applicable"
    BOUNDED = "bounded"
    MAXIMUM = "maximum"


class PostgresMssqlCodecV1(StrEnum):
    BOOL_ASCII_V1 = "bool_ascii_v1"
    SIGNED_INTEGER_ASCII_V1 = "signed_integer_ascii_v1"
    DECIMAL_FIXED_ASCII_V1 = "decimal_fixed_ascii_v1"
    RYU_BINARY32_SHORTEST_ASCII_V1 = "ryu_binary32_shortest_ascii_v1"
    RYU_BINARY64_SHORTEST_ASCII_V1 = "ryu_binary64_shortest_ascii_v1"
    UUID_LOWER_ASCII_V1 = "uuid_lower_ascii_v1"
    ISO_DATE_ASCII_V1 = "iso_date_ascii_v1"
    ISO_TIME_ASCII_V1 = "iso_time_ascii_v1"
    ISO_TIMESTAMP_ASCII_V1 = "iso_timestamp_ascii_v1"
    ISO_UTC_TIMESTAMP_ASCII_V1 = "iso_utc_timestamp_ascii_v1"
    UTF8_TO_UTF16LE_V1 = "utf8_to_utf16le_v1"
    RAW_BINARY_V1 = "raw_binary_v1"


class PostgresMssqlNormalizationV1(StrEnum):
    IDENTITY = "identity"
    CANONICAL_POSITIVE_ZERO = "canonical_positive_zero"
    UTC_INSTANT = "utc_instant"
    UNICODE_SCALAR_IDENTITY = "unicode_scalar_identity"


class PostgresMssqlEqualityPolicyV1(StrEnum):
    BOOLEAN_EXACT = "boolean_exact"
    INTEGER_EXACT = "integer_exact"
    DECIMAL_EXACT = "decimal_exact"
    IEEE_VALUE_AFTER_POSITIVE_ZERO = "ieee_value_after_positive_zero"
    UUID_OCTETS = "uuid_octets"
    DATE_EXACT = "date_exact"
    TIME_EXACT = "time_exact"
    TIMESTAMP_EXACT = "timestamp_exact"
    UTC_INSTANT_EXACT = "utc_instant_exact"
    UNICODE_CODEPOINT_EXACT = "unicode_codepoint_exact"
    BINARY_OCTETS = "binary_octets"


class PostgresMssqlValueGuardV1(StrEnum):
    BOOLEAN_DOMAIN = "boolean_domain"
    INT2_RANGE = "int2_range"
    INT4_RANGE = "int4_range"
    INT8_RANGE = "int8_range"
    DECIMAL_SHAPE_AND_VALUE = "decimal_shape_and_value"
    FINITE_FLOAT4 = "finite_float4"
    FINITE_FLOAT8 = "finite_float8"
    UUID_DOMAIN = "uuid_domain"
    SQLSERVER_DATE_RANGE = "sqlserver_date_range"
    SQLSERVER_TIME_PRECISION = "sqlserver_time_precision"
    SQLSERVER_DATETIME2_RANGE_PRECISION = "sqlserver_datetime2_range_precision"
    SQLSERVER_DATETIMEOFFSET_RANGE_PRECISION = "sqlserver_datetimeoffset_range_precision"
    UTF8_AND_UTF16_CAPACITY = "utf8_and_utf16_capacity"
    BINARY_CAPACITY = "binary_capacity"


class MssqlR1TypeTargetRecoveryClass(StrEnum):
    PERMANENT_INPUT_ERROR = "permanent_input_error"
    REFRESH_AUTHORITY_AND_RETRY = "refresh_authority_and_retry"
    OPERATOR_INTERVENTION = "operator_intervention"


class MssqlR1TypeTargetAuthorityError(MssqlR1V3ContractError):
    """A stable fail-closed type/target authority rejection."""

    def __init__(self, reason_id: str, recovery_class: MssqlR1TypeTargetRecoveryClass) -> None:
        self.reason_id = reason_id
        self.recovery_class = recovery_class
        super().__init__(reason_id)


_PERMANENT_REASONS = frozenset(
    {
        "wrong_domain",
        "wrong_version",
        "malformed_canonical_bytes",
        "enum_unsupported",
        "invalid_facet",
        "value_grammar_invalid",
        "identifier_invalid",
        "ordinal_invalid",
        "decision_id_invalid",
        "decision_duplicate",
        "decision_order_invalid",
        "policy_coverage_invalid",
        "duplicate_column",
        "target_key_invalid",
        "catalog_order_invalid",
    }
)
_REFRESH_REASONS = frozenset({"registration_expired", "registration_not_active", "active_head_stale"})
_OPERATOR_REASONS = frozenset(
    {
        "registration_verification_mismatch",
        "registration_revoked",
        "rotation_transition_mismatch",
        "physical_identity_mismatch",
        "catalog_digest_mismatch",
        "catalog_coordinate_mismatch",
        "catalog_behavior_mismatch",
        "target_profile_unsupported",
        "target_feature_unsupported",
        "authority_splice",
        "lossy_mapping",
    }
)
_RECOVERY_BY_REASON = {
    **dict.fromkeys(_PERMANENT_REASONS, MssqlR1TypeTargetRecoveryClass.PERMANENT_INPUT_ERROR),
    **dict.fromkeys(_REFRESH_REASONS, MssqlR1TypeTargetRecoveryClass.REFRESH_AUTHORITY_AND_RETRY),
    **dict.fromkeys(_OPERATOR_REASONS, MssqlR1TypeTargetRecoveryClass.OPERATOR_INTERVENTION),
}

_P = ParamSpec("_P")
_R = TypeVar("_R")


def authority_decode(function: Callable[_P, _R]) -> Callable[_P, _R]:
    """Normalize structural decode failures into the closed error family."""

    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return function(*args, **kwargs)
        except MssqlR1TypeTargetAuthorityError:
            raise
        except (MssqlR1V3ContractError, KeyError, AttributeError, TypeError, ValueError) as exc:
            message = str(exc).lower()
            reason = (
                "wrong_domain"
                if "domain" in message
                else "enum_unsupported"
                if "unsupported" in message
                else "malformed_canonical_bytes"
            )
            try:
                reject(reason)
            except MssqlR1TypeTargetAuthorityError as wrapped_error:
                raise wrapped_error from exc

    return wrapped


def authority_validation(function: Callable[_P, _R]) -> Callable[_P, _R]:
    """Prevent upstream helpers or structural substitutes leaking raw errors."""

    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return function(*args, **kwargs)
        except MssqlR1TypeTargetAuthorityError:
            raise
        except (MssqlR1V3ContractError, KeyError, AttributeError, TypeError, ValueError) as exc:
            reason = "identifier_invalid" if "identifier" in str(exc).lower() else "invalid_facet"
            try:
                reject(reason)
            except MssqlR1TypeTargetAuthorityError as wrapped_error:
                raise wrapped_error from exc

    return wrapped


def reject(reason_id: str, *, operator: bool | None = None) -> NoReturn:
    """Raise one closed reason/recovery pair.

    ``operator`` is retained only to keep in-flight internal callers source
    compatible; it cannot override the specification-owned classification.
    """

    del operator
    try:
        recovery = _RECOVERY_BY_REASON[reason_id]
    except KeyError as exc:
        raise MssqlR1TypeTargetAuthorityError(
            "malformed_canonical_bytes",
            MssqlR1TypeTargetRecoveryClass.PERMANENT_INPUT_ERROR,
        ) from exc
    raise MssqlR1TypeTargetAuthorityError(reason_id, recovery)


def require_identifier_v1(value: object) -> str:
    """Require one exact SQL Server identifier domain from the approved child spec."""

    if type(value) is not str:
        reject("identifier_invalid")
    try:
        utf16_units = len(value.encode("utf-16-le")) // 2
    except UnicodeEncodeError:
        reject("identifier_invalid")
    if (
        not 1 <= utf16_units <= 128
        or value != value.strip()
        or unicodedata.normalize("NFC", value) != value
        or "\0" in value
    ):
        reject("identifier_invalid")
    return value


def require_semantic_id_v1(value: object) -> str:
    """Require one 1..64-byte ASCII semantic identifier."""

    if type(value) is not str:
        reject("invalid_facet")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        reject("invalid_facet")
    if not 1 <= len(encoded) <= 64 or value != value.strip() or "\0" in value:
        reject("invalid_facet")
    return value


__all__ = [
    "MssqlR1TargetScalarFamilyV1",
    "MssqlR1TypeTargetAuthorityError",
    "MssqlR1TypeTargetRecoveryClass",
    "PostgresMssqlCodecV1",
    "PostgresMssqlEqualityPolicyV1",
    "PostgresMssqlLengthKindV1",
    "PostgresMssqlNormalizationV1",
    "PostgresMssqlSourceScalarFamilyV1",
    "PostgresMssqlValueGuardV1",
    "authority_decode",
    "authority_validation",
    "require_identifier_v1",
    "require_semantic_id_v1",
]
