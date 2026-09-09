from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from urllib.parse import urlparse


def matches_expected_type(value: object, expected_type: object) -> bool:
    if isinstance(expected_type, str):
        return _matches_type(value, expected_type)
    if isinstance(expected_type, list):
        allowed_types = [item for item in expected_type if isinstance(item, str)]
        if not allowed_types:
            return True
        return any(_matches_type(value, item) for item in allowed_types)
    return True


def format_expected_type(expected_type: object) -> str:
    if isinstance(expected_type, str):
        return expected_type
    if isinstance(expected_type, list):
        allowed_types = [item for item in expected_type if isinstance(item, str)]
        if allowed_types:
            return " or ".join(allowed_types)
    return "the declared type"


def matches_format(value: object, expected_format: object) -> bool:
    if not isinstance(expected_format, str) or not isinstance(value, str):
        return True
    if expected_format == "uri":
        return _matches_uri_format(value)
    if expected_format == "date-time":
        return _matches_date_time_format(value)
    return True


def format_name(expected_format: object) -> str:
    if isinstance(expected_format, str) and expected_format:
        return expected_format
    return "the declared format"


def _matches_type(value: object, expected_type: str) -> bool:
    if expected_type == "null":
        return value is None
    if expected_type == "object":
        return isinstance(value, Mapping)
    if expected_type == "array":
        return isinstance(value, list)
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "boolean":
        return isinstance(value, bool)
    if expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    return True


def _matches_uri_format(value: str) -> bool:
    parsed = urlparse(value)
    if not parsed.scheme:
        return False
    if parsed.scheme in {"http", "https"}:
        return bool(parsed.netloc)
    return bool(parsed.netloc or parsed.path)


def _matches_date_time_format(value: str) -> bool:
    normalized = value.removesuffix("Z")
    if normalized != value:
        normalized = f"{normalized}+00:00"
    try:
        datetime.fromisoformat(normalized)
    except ValueError:
        return False
    return "T" in value
