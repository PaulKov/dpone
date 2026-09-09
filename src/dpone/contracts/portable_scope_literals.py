"""Typed literal parsing and canonicalization for portable relation scopes."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, TypeAlias, cast
from uuid import UUID

PortableLiteralKind: TypeAlias = Literal[
    "integer",
    "decimal",
    "text",
    "boolean",
    "date",
    "timestamp",
    "timestamptz",
    "uuid",
]

RANGE_LITERAL_KINDS = frozenset({"integer", "decimal", "date", "timestamp", "timestamptz", "uuid"})
_LITERAL_KINDS = RANGE_LITERAL_KINDS | {"text", "boolean"}
_DECIMAL_LITERAL = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")


class PortableScopeContractError(ValueError):
    """Stable fail-closed error for an invalid portable scope contract."""

    code = "DPONE_PORTABLE_SCOPE_CONTRACT_BLOCKED"

    def __init__(self, blocker: str, detail: str) -> None:
        self.blocker = blocker
        super().__init__(f"{self.code}:{blocker}: {detail}")


@dataclass(frozen=True, slots=True)
class PortableLiteral:
    """One typed literal whose canonical value is independent of SQL syntax."""

    kind: PortableLiteralKind
    value: int | Decimal | str | bool | date | datetime | UUID

    def __post_init__(self) -> None:
        _validate_literal_instance(self)

    def to_contract(self) -> dict[str, object]:
        """Return the canonical JSON-safe literal representation."""

        return {"type": self.kind, "value": canonical_literal_value(self)}

    @property
    def parameter_value(self) -> object:
        """Return the typed DB-API parameter value used by both renderers."""

        if self.kind == "timestamptz":
            # pyodbc has no portable aware-datetime input binding.  Both
            # dialect renderers bind this canonical UTC text as a parameter.
            return canonical_literal_value(self)
        return self.value


def parse_portable_literal(raw: Any) -> PortableLiteral:
    """Parse a closed JSON literal object into one validated typed value."""

    mapping = require_mapping(raw, "portable_scope.literal")
    unknown = sorted(str(key) for key in mapping if str(key) not in {"type", "value"})
    if unknown:
        blocked("portable_scope.literal.fields", f"unsupported fields: {', '.join(unknown)}")
    raw_kind = mapping.get("type")
    if raw_kind not in _LITERAL_KINDS:
        blocked("portable_scope.literal.type", "unsupported literal type")
    kind = cast(PortableLiteralKind, raw_kind)
    if "value" not in mapping:
        blocked("portable_scope.literal.value", "literal value is required")
    value = mapping["value"]
    if kind == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            blocked("portable_scope.literal.integer", "integer value must be a JSON integer")
        return PortableLiteral(kind, value)
    if kind == "decimal":
        return PortableLiteral(kind, _parse_decimal(value))
    if kind == "text":
        if not isinstance(value, str):
            blocked("portable_scope.literal.text", "text value must be a string")
        _validate_text(value)
        return PortableLiteral(kind, value)
    if kind == "boolean":
        if not isinstance(value, bool):
            blocked("portable_scope.literal.boolean", "boolean value must be true or false")
        return PortableLiteral(kind, value)
    if kind == "uuid":
        return PortableLiteral(kind, _parse_uuid(value))
    return PortableLiteral(kind, _parse_temporal(kind, value))


def canonical_literal_value(literal: PortableLiteral) -> object:
    """Return one locale- and dialect-independent scalar representation."""

    if literal.kind == "decimal":
        assert isinstance(literal.value, Decimal)
        return _canonical_decimal_text(literal.value)
    if literal.kind == "date":
        assert isinstance(literal.value, date)
        return literal.value.isoformat()
    if literal.kind == "timestamp":
        assert isinstance(literal.value, datetime)
        return literal.value.isoformat(timespec="microseconds").rstrip("0").rstrip(".")
    if literal.kind == "timestamptz":
        assert isinstance(literal.value, datetime)
        utc = _as_utc(literal.value)
        body = utc.replace(tzinfo=None).isoformat(timespec="microseconds")
        if "." in body:
            body = body.rstrip("0").rstrip(".")
        return body + "Z"
    if literal.kind == "uuid":
        assert isinstance(literal.value, UUID)
        return str(literal.value)
    return literal.value


def require_mapping(raw: Any, blocker: str) -> Mapping[str, Any]:
    """Require one mapping while retaining the stable blocker taxonomy."""

    if not isinstance(raw, Mapping):
        blocked(blocker, "value must be an object")
    return raw


def blocked(blocker: str, detail: str) -> None:
    """Raise the public typed contract error."""

    raise PortableScopeContractError(blocker, detail)


def _parse_decimal(value: Any) -> Decimal:
    if not isinstance(value, str) or _DECIMAL_LITERAL.fullmatch(value) is None:
        blocked("portable_scope.literal.decimal", "decimal value must be a finite base-10 string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:  # pragma: no cover - regex is the primary gate.
        raise PortableScopeContractError("portable_scope.literal.decimal", "decimal value is invalid") from exc
    canonical = _canonical_decimal_text(parsed)
    if value != canonical:
        blocked(
            "portable_scope.literal.decimal_canonical",
            f"decimal value must use canonical form {canonical!r}",
        )
    return parsed


def _parse_temporal(kind: PortableLiteralKind, value: Any) -> date | datetime:
    if not isinstance(value, str):
        blocked(f"portable_scope.literal.{kind}", f"{kind} value must be an ISO-8601 string")
    try:
        if kind == "date":
            parsed_date = date.fromisoformat(value)
            if parsed_date.isoformat() != value:
                raise ValueError
            return parsed_date
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed_timestamp = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise PortableScopeContractError(
            f"portable_scope.literal.{kind}", f"{kind} value must be canonical ISO-8601"
        ) from exc
    aware = _datetime_is_aware(parsed_timestamp, kind=kind)
    if kind == "timestamp" and aware:
        blocked("portable_scope.literal.timestamp_timezone", "timestamp cannot carry a timezone")
    if kind == "timestamptz" and not aware:
        blocked(
            "portable_scope.literal.timestamptz_timezone",
            "timestamptz requires an explicit timezone",
        )
    return _as_utc(parsed_timestamp) if kind == "timestamptz" else parsed_timestamp


def _validate_literal_instance(literal: PortableLiteral) -> None:
    kind = literal.kind
    value = literal.value
    if kind == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            blocked("portable_scope.literal.integer", "integer value must be an integer")
        return
    if kind == "decimal":
        if not isinstance(value, Decimal) or not value.is_finite():
            blocked("portable_scope.literal.decimal", "decimal value must be finite Decimal")
        assert isinstance(value, Decimal)
        canonical = _canonical_decimal_text(value)
        if str(value) != canonical:
            blocked(
                "portable_scope.literal.decimal_canonical",
                f"decimal value must use canonical form {canonical!r}",
            )
        return
    if kind == "text":
        if not isinstance(value, str):
            blocked("portable_scope.literal.text", "text value must be a string")
        assert isinstance(value, str)
        _validate_text(value)
        return
    if kind == "boolean":
        if not isinstance(value, bool):
            blocked("portable_scope.literal.boolean", "boolean value must be true or false")
        return
    if kind == "date":
        if type(value) is not date:
            blocked("portable_scope.literal.date", "date value must be a date")
        return
    if kind == "uuid":
        if not isinstance(value, UUID):
            blocked("portable_scope.literal.uuid", "uuid value must be a UUID")
        return
    if kind in {"timestamp", "timestamptz"}:
        if not isinstance(value, datetime):
            blocked(f"portable_scope.literal.{kind}", f"{kind} value must be a datetime")
        assert isinstance(value, datetime)
        aware = _datetime_is_aware(value, kind=kind)
        if kind == "timestamp" and aware:
            blocked("portable_scope.literal.timestamp_timezone", "timestamp cannot carry a timezone")
        if kind == "timestamptz" and not aware:
            blocked("portable_scope.literal.timestamptz_timezone", "timestamptz requires a timezone")
        if kind == "timestamptz":
            object.__setattr__(literal, "value", _as_utc(value))
        return
    blocked("portable_scope.literal.type", f"unsupported literal type {kind!r}")


def _parse_uuid(value: Any) -> UUID:
    if not isinstance(value, str):
        blocked("portable_scope.literal.uuid", "uuid value must be a canonical string")
    try:
        parsed = UUID(value)
    except (AttributeError, ValueError) as exc:
        raise PortableScopeContractError(
            "portable_scope.literal.uuid",
            "uuid value must use canonical lowercase 8-4-4-4-12 form",
        ) from exc
    if str(parsed) != value:
        blocked(
            "portable_scope.literal.uuid_canonical",
            "uuid value must use canonical lowercase 8-4-4-4-12 form",
        )
    return parsed


def _validate_text(value: str) -> None:
    if "\x00" in value or any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        blocked("portable_scope.literal.text_encoding", "text must be valid PostgreSQL/MSSQL Unicode")


def _datetime_is_aware(value: datetime, *, kind: str) -> bool:
    if value.tzinfo is None:
        return False
    try:
        return value.utcoffset() is not None
    except (OverflowError, ValueError) as exc:
        raise PortableScopeContractError(
            f"portable_scope.literal.{kind}_range",
            f"{kind} local and UTC values must fit SQL Server years 0001 through 9999",
        ) from exc


def _as_utc(value: datetime) -> datetime:
    try:
        return value.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise PortableScopeContractError(
            "portable_scope.literal.timestamptz_range",
            "timestamptz local and UTC values must fit SQL Server years 0001 through 9999",
        ) from exc


def _canonical_decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


__all__ = [
    "PortableLiteral",
    "PortableLiteralKind",
    "PortableScopeContractError",
    "RANGE_LITERAL_KINDS",
    "blocked",
    "canonical_literal_value",
    "parse_portable_literal",
    "require_mapping",
]
