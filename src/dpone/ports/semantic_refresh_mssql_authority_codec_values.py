"""Closed scalar and collection decoding for MSSQL authority documents."""

from __future__ import annotations

from collections.abc import Mapping, Sequence


def mapping(value: Mapping[str, object], field_name: str) -> Mapping[str, object]:
    """Return one required mapping field."""

    raw = value[field_name]
    if not isinstance(raw, Mapping):
        raise ValueError(f"canonical authority {field_name} must be an object")
    return raw


def mapping_sequence(value: Mapping[str, object], field_name: str) -> tuple[Mapping[str, object], ...]:
    """Return a required sequence containing mappings only."""

    return tuple(mapping({"item": item}, "item") for item in sequence(value, field_name))


def mapping_bool(value: Mapping[str, object], field_name: str) -> bool:
    """Return one required boolean field without integer coercion."""

    raw = value[field_name]
    if not isinstance(raw, bool):
        raise ValueError(f"canonical authority {field_name} must be boolean")
    return raw


def closed_mapping(value: object, name: str, fields: set[str]) -> Mapping[str, object]:
    """Require an object with exactly the protected field closure."""

    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError(f"canonical authority {name} fields are not closed")
    return value


def sequence(value: Mapping[str, object], field_name: str) -> Sequence[object]:
    """Return one required non-text sequence field."""

    raw = value[field_name]
    if not isinstance(raw, Sequence) or isinstance(raw, str | bytes):
        raise ValueError(f"canonical authority {field_name} must be an array")
    return raw


def mapping_text(value: Mapping[str, object], field_name: str) -> str:
    """Return one required non-empty text field."""

    raw = value[field_name]
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"canonical authority {field_name} must be non-empty text")
    return raw


def optional_mapping_text(value: Mapping[str, object], field_name: str) -> str | None:
    """Return one optional non-empty text field."""

    raw = value[field_name]
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"canonical authority {field_name} must be non-empty text")
    return raw


def mapping_int(value: Mapping[str, object], field_name: str) -> int:
    """Return one required integer field without boolean coercion."""

    raw = value[field_name]
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError(f"canonical authority {field_name} must be an integer")
    return raw


__all__ = [
    "closed_mapping",
    "mapping",
    "mapping_bool",
    "mapping_int",
    "mapping_sequence",
    "mapping_text",
    "optional_mapping_text",
    "sequence",
]
