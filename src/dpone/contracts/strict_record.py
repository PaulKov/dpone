"""Dependency-free structural primitives shared by closed record codecs.

These helpers enforce exact field sets and scalar spellings. Domain codecs own
value constraints, nested decoding, byte limits and public error translation.
No operation state, session authority or vendor policy belongs at this boundary.
"""

from dataclasses import fields
from typing import Any
from uuid import UUID


def record_shape(cls: Any, value: Any) -> dict:
    """Require all declared dataclass fields, including fields with defaults."""
    if type(value) is not dict or set(value) != {field.name for field in fields(cls)}:
        raise ValueError("record_fields")
    return value


def construct_record(cls: Any, value: Any) -> Any:
    """Construct a record after exact shape admission; the class owns policy."""
    return cls(**record_shape(cls, value))


def string_enum(cls: Any, value: Any) -> Any:
    """Decode only a plain string; the enum supplies its allowed vocabulary."""
    if type(value) is not str:
        raise ValueError("enum_type")
    return cls(value)


def canonical_uuid(value: Any) -> UUID:
    """Reject alternate UUID spellings without adding nonzero identity policy."""
    if type(value) is not str:
        raise ValueError("uuid_type")
    result = UUID(value)
    if str(result) != value:
        raise ValueError("uuid_alias")
    return result
