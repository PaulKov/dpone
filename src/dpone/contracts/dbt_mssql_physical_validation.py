"""Exact physical-plan wire primitives and deterministic owned-object names.

These checks establish representation only. They do not authenticate a profile,
database, predecessor receipt, qualification or permission to execute SQL.
"""

from __future__ import annotations

import re
from datetime import datetime
from hashlib import sha256
from unicodedata import category
from uuid import UUID

from dpone.contracts.native_delivery_json import MAX_NATIVE_JSON_STRING_BYTES, encode_native_delivery_json

_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{7}")
_NAME_PREFIXES = {"CANDIDATE": "dpone_c_", "HELPER": "dpone_h_", "BACKUP": "dpone_b_", "CCI": "dpone_i_"}


class PhysicalPlanError(ValueError):
    """Malformed physical plan; diagnostics identify fields, never supplied data."""


def require_physical_text(value: object, field: str) -> str:
    """Require nonempty exact Unicode text within the inherited JSON bound."""
    if type(value) is not str or not value or len(value) > MAX_NATIVE_JSON_STRING_BYTES:
        raise PhysicalPlanError(f"{field} requires bounded nonempty text")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError:
        raise PhysicalPlanError(f"{field} requires Unicode scalar text") from None
    if size > MAX_NATIVE_JSON_STRING_BYTES:
        raise PhysicalPlanError(f"{field} exceeds the UTF-8 string bound")
    return value


def require_physical_identifier(value: object, field: str) -> str:
    """Retain an exact SQL identifier of at most 128 UTF-16 code units.

    No SQL-collation equivalence is inferred. SQL consumers must quote the name.
    """
    name = require_physical_text(value, field)
    if len(name.encode("utf-16-le")) > 256 or any(category(character) == "Cc" for character in name):
        raise PhysicalPlanError(f"{field} exceeds SQL identifier width or contains controls")
    return name


def require_physical_uuid(value: object, field: str) -> str:
    """Require canonical lowercase hyphenated UUID text without coercion."""
    if type(value) is not str or len(value) != 36:
        raise PhysicalPlanError(f"{field} requires canonical UUID text")
    try:
        parsed = UUID(value)
    except ValueError:
        raise PhysicalPlanError(f"{field} requires canonical UUID text") from None
    if str(parsed) != value:
        raise PhysicalPlanError(f"{field} requires canonical UUID text")
    return value


def require_sql_positive_integer(value: object, field: str, *, bigint: bool = False) -> int:
    """Reject bool, coercion, nonpositive values and SQL integer overflow."""
    upper = 9223372036854775807 if bigint else 2147483647
    if type(value) is not int or not 1 <= value <= upper:
        raise PhysicalPlanError(f"{field} requires a positive SQL integer within bounds")
    return value


def require_physical_timestamp(value: object, field: str) -> str:
    """Validate Gregorian date/clock fields without losing the seventh digit."""
    if type(value) is not str or _TIMESTAMP.fullmatch(value) is None:
        raise PhysicalPlanError(f"{field} requires canonical seven-fraction timestamp text")
    try:
        datetime(
            int(value[:4]), int(value[5:7]), int(value[8:10]), int(value[11:13]), int(value[14:16]), int(value[17:19])
        )
    except ValueError:
        raise PhysicalPlanError(f"{field} has invalid Gregorian date or clock fields") from None
    return value


def physical_object_name(generation_id: str, model_unique_id: str, role: str) -> str:
    """Derive one exact helper identity; never search suffixes or repair collisions."""
    require_physical_uuid(generation_id, "generation_id")
    require_physical_text(model_unique_id, "model_unique_id")
    if type(role) is not str or role not in _NAME_PREFIXES:
        raise PhysicalPlanError("role requires an exact physical object role")
    payload = encode_native_delivery_json(
        {
            "schema": "dpone.mssql-physical-object-name.v1",
            "generation_id": generation_id,
            "model_unique_id": model_unique_id,
            "role": role,
        }
    )
    return _NAME_PREFIXES[role] + sha256(payload).hexdigest()
