"""Injectable scalar validation for the R1 physical descriptor."""

from __future__ import annotations

import re
from collections.abc import Callable
from enum import Enum
from typing import TypeVar

from dpone.contracts.mssql_r1_v3_identity import (
    MAX_SQL_BIGINT,
    MAX_SQL_INT,
    require_canonical_text,
)

EnumT = TypeVar("EnumT", bound=Enum)
ErrorFactory = Callable[[str], Exception]
LOWER_ASCII = re.compile(r"[a-z][a-z0-9_]{0,127}\Z")


class MssqlR1ScalarValidatorsV1:
    """Raise the caller's stable contract error for shared scalar violations."""

    __slots__ = ("_error_factory",)

    def __init__(self, error_factory: ErrorFactory) -> None:
        self._error_factory = error_factory

    def require_exact_enum(self, value: object, contract: type[EnumT], field: str) -> EnumT:
        if type(value) is not contract:
            raise self._error_factory(f"{field} must use the exact enum type")
        return value

    def require_bool(self, value: object, field: str) -> bool:
        if type(value) is not bool:
            raise self._error_factory(f"{field} must be boolean")
        return value

    def require_int(
        self,
        value: object,
        field: str,
        *,
        minimum: int = 0,
        maximum: int = MAX_SQL_BIGINT,
    ) -> int:
        if type(value) is not int or not minimum <= value <= maximum:
            raise self._error_factory(f"{field} is outside its exact integer bounds")
        return value

    def require_ordinal(self, value: object, field: str) -> int:
        return self.require_int(value, field, minimum=1, maximum=MAX_SQL_INT)

    def require_text(self, value: object, field: str) -> str:
        if type(value) is not str:
            raise self._error_factory(f"{field} must be exact text")
        return require_canonical_text(value, field, maximum_bytes=256)

    def require_lower_ascii(self, value: object, field: str) -> str:
        if type(value) is not str or LOWER_ASCII.fullmatch(value) is None:
            raise self._error_factory(f"{field} must be bounded lowercase ASCII")
        return value

    def require_tuple(
        self,
        value: object,
        contract: type[object],
        field: str,
        *,
        nonempty: bool = False,
    ) -> tuple[object, ...]:
        if type(value) is not tuple or (nonempty and not value) or any(type(item) is not contract for item in value):
            raise self._error_factory(f"{field} must be an exact typed tuple")
        return value

    def require_contiguous(self, values: tuple[object, ...], field: str) -> None:
        if tuple(getattr(item, "ordinal") for item in values) != tuple(range(1, len(values) + 1)):
            raise self._error_factory(f"{field} ordinals must be contiguous")

    def require_canonical(self, values: tuple[object, ...], field: str) -> None:
        encoded = tuple(getattr(item, "canonical_bytes") for item in values)
        if not values or encoded != tuple(sorted(encoded)) or len(set(encoded)) != len(encoded):
            raise self._error_factory(f"{field} must be a nonempty canonical set")


__all__ = ("MssqlR1ScalarValidatorsV1",)
