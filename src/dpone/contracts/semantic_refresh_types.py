"""Closed vocabularies and effective-key domains for semantic refresh."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum

from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_closed_mapping,
    require_digest,
    require_enum,
    require_text,
)

DATE_DOMAIN_MIN = "1970-01-01"
DATE_DOMAIN_MAX = "2149-06-06"
DATETIME_DOMAIN_MIN = "1900-01-01T00:00:00.000000Z"
DATETIME_DOMAIN_MAX = "2299-12-31T23:59:59.999999Z"


class WorkflowMode(str, Enum):  # noqa: UP042
    """Supported semantic-refresh workflow branches."""

    NORMAL = "normal"
    FAILED_PRECOMMIT_REPLACEMENT = "failed_precommit_replacement"
    COMPLETE_SCOPE_REPLAY = "complete_scope_replay"


class ClosureStatus(str, Enum):  # noqa: UP042
    """Fail-closed proof outcome; only ``PROVEN`` may execute."""

    PROVEN = "PROVEN"
    NONCONFORMANT = "NONCONFORMANT"
    UNVERIFIED = "UNVERIFIED"


class SqlServerModelOutcome(str, Enum):  # noqa: UP042
    """Durable SQL Server outcome vocabulary."""

    NOT_INVOKED = "NOT_INVOKED"
    ROLLED_BACK = "ROLLED_BACK"
    COMMITTED_WITH_IMAGES = "COMMITTED_WITH_IMAGES"
    COMMIT_UNKNOWN = "COMMIT_UNKNOWN"


class ReplacementAction(str, Enum):  # noqa: UP042
    """Allowed failed-precommit replacement decisions."""

    RESTORE_THEN_REBUILD = "RESTORE_THEN_REBUILD"
    BUILD_FRESH = "BUILD_FRESH"
    BLOCK = "BLOCK"


class EffectiveKeyMapping(str, Enum):  # noqa: UP042
    """Certified injective SQL Server-to-ClickHouse equality mappings."""

    BIT_BOOL = "bit_bool"
    INTEGER_WIDTH_PRESERVING = "integer_width_preserving"
    UUID_EQUALITY = "uuid_equality"
    DATE = "date"
    DATETIME2_UTC = "datetime2_utc"
    DECIMAL_EXACT = "decimal_exact"


_SCALAR_PAIRS = {
    ("bit", "Bool"): EffectiveKeyMapping.BIT_BOOL,
    ("tinyint", "UInt8"): EffectiveKeyMapping.INTEGER_WIDTH_PRESERVING,
    ("smallint", "Int16"): EffectiveKeyMapping.INTEGER_WIDTH_PRESERVING,
    ("int", "Int32"): EffectiveKeyMapping.INTEGER_WIDTH_PRESERVING,
    ("bigint", "Int64"): EffectiveKeyMapping.INTEGER_WIDTH_PRESERVING,
    ("uniqueidentifier", "UUID"): EffectiveKeyMapping.UUID_EQUALITY,
}
_DECIMAL_SOURCE = re.compile(r"decimal\(([1-9][0-9]?),([0-9]|[1-9][0-9]?)\)")
_DECIMAL_TARGET = re.compile(r"Decimal\(([1-9][0-9]?),([0-9]|[1-9][0-9]?)\)")
_EFFECTIVE_KEY_REQUIRED = frozenset({"name", "source_type", "target_type", "mapping", "nullable"})
_EFFECTIVE_KEY_OPTIONAL = frozenset({"domain_min", "domain_max", "utc_assurance_sha256"})


@dataclass(frozen=True, slots=True)
class EffectiveKeyColumn:
    """One non-null effective-key column with an exact injective mapping."""

    name: str
    source_type: str
    target_type: str
    domain_min: str | None = None
    domain_max: str | None = None
    utc_assurance_sha256: str | None = None
    mapping: EffectiveKeyMapping = field(init=False)
    nullable: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        require_text(self.name, "effective key name")
        require_text(self.source_type, "effective key source_type")
        require_text(self.target_type, "effective key target_type")
        mapping = _mapping_for(self.source_type, self.target_type)
        object.__setattr__(self, "mapping", mapping)
        _validate_domain(self, mapping)

    @classmethod
    def from_mapping(cls, value: object) -> EffectiveKeyColumn:
        """Parse one closed effective-key column mapping."""

        raw = require_closed_mapping(
            value,
            "effective_key_column",
            required=_EFFECTIVE_KEY_REQUIRED,
            optional=_EFFECTIVE_KEY_OPTIONAL,
        )
        if raw.get("nullable") is not False:
            raise SemanticRefreshContractError("effective keys must be non-null")
        column = cls(
            name=require_text(raw.get("name"), "effective key name"),
            source_type=require_text(raw.get("source_type"), "effective key source_type"),
            target_type=require_text(raw.get("target_type"), "effective key target_type"),
            domain_min=_optional_text(raw, "domain_min"),
            domain_max=_optional_text(raw, "domain_max"),
            utc_assurance_sha256=_optional_digest(raw, "utc_assurance_sha256"),
        )
        declared = require_enum(raw.get("mapping"), "effective key mapping", EffectiveKeyMapping)
        if declared is not column.mapping:
            raise SemanticRefreshContractError("effective key mapping label differs from its types")
        return column

    def to_dict(self) -> dict[str, object]:
        """Return the closed canonical public mapping."""

        result: dict[str, object] = {
            "mapping": self.mapping.value,
            "name": self.name,
            "nullable": self.nullable,
            "source_type": self.source_type,
            "target_type": self.target_type,
        }
        if self.domain_min is not None:
            result["domain_min"] = self.domain_min
        if self.domain_max is not None:
            result["domain_max"] = self.domain_max
        if self.utc_assurance_sha256 is not None:
            result["utc_assurance_sha256"] = self.utc_assurance_sha256
        return result


def replacement_action_for(outcome: SqlServerModelOutcome) -> ReplacementAction:
    """Map one proven SQL Server outcome to its only safe replacement action."""

    if outcome is SqlServerModelOutcome.COMMITTED_WITH_IMAGES:
        return ReplacementAction.RESTORE_THEN_REBUILD
    if outcome in {SqlServerModelOutcome.ROLLED_BACK, SqlServerModelOutcome.NOT_INVOKED}:
        return ReplacementAction.BUILD_FRESH
    return ReplacementAction.BLOCK


def _mapping_for(source_type: str, target_type: str) -> EffectiveKeyMapping:
    scalar = _SCALAR_PAIRS.get((source_type, target_type))
    if scalar is not None:
        return scalar
    if (source_type, target_type) == ("date", "Date"):
        return EffectiveKeyMapping.DATE
    if (source_type, target_type) == ("datetime2(6)", "DateTime64(6,'UTC')"):
        return EffectiveKeyMapping.DATETIME2_UTC
    source_decimal = _DECIMAL_SOURCE.fullmatch(source_type)
    target_decimal = _DECIMAL_TARGET.fullmatch(target_type)
    if source_decimal is not None and target_decimal is not None:
        source_precision, source_scale = (int(item) for item in source_decimal.groups())
        target_precision, target_scale = (int(item) for item in target_decimal.groups())
        if (
            1 <= source_precision <= 38
            and 0 <= source_scale <= source_precision
            and (source_precision, source_scale) == (target_precision, target_scale)
        ):
            return EffectiveKeyMapping.DECIMAL_EXACT
    raise SemanticRefreshContractError("effective-key type pair is not certified")


def _validate_domain(column: EffectiveKeyColumn, mapping: EffectiveKeyMapping) -> None:
    expected_domain: tuple[str | None, str | None]
    if mapping is EffectiveKeyMapping.DATE:
        expected_domain = (DATE_DOMAIN_MIN, DATE_DOMAIN_MAX)
    elif mapping is EffectiveKeyMapping.DATETIME2_UTC:
        expected_domain = (DATETIME_DOMAIN_MIN, DATETIME_DOMAIN_MAX)
    else:
        expected_domain = (None, None)
    if (column.domain_min, column.domain_max) != expected_domain:
        raise SemanticRefreshContractError("effective-key domain differs from the certified exact range")
    if mapping is EffectiveKeyMapping.DATETIME2_UTC:
        require_digest(column.utc_assurance_sha256, "effective key UTC assurance")
    elif column.utc_assurance_sha256 is not None:
        raise SemanticRefreshContractError("UTC assurance is only valid for datetime2(6)")


def _optional_text(raw: Mapping[str, object], field_name: str) -> str | None:
    if field_name not in raw:
        return None
    return require_text(raw[field_name], field_name)


def _optional_digest(raw: Mapping[str, object], field_name: str) -> str | None:
    if field_name not in raw:
        return None
    return require_digest(raw[field_name], field_name)


__all__ = [
    "DATE_DOMAIN_MAX",
    "DATE_DOMAIN_MIN",
    "DATETIME_DOMAIN_MAX",
    "DATETIME_DOMAIN_MIN",
    "ClosureStatus",
    "EffectiveKeyColumn",
    "EffectiveKeyMapping",
    "ReplacementAction",
    "SqlServerModelOutcome",
    "WorkflowMode",
    "replacement_action_for",
]
