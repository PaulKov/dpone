"""Acyclic template and final mapping identities for semantic-refresh keys."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_closed_mapping,
    require_enum,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_types import EffectiveKeyColumn, EffectiveKeyMapping

EFFECTIVE_KEY_TEMPLATE_SCHEMA = "dpone.semantic-refresh-effective-key-template.v1"
EFFECTIVE_KEY_MAPPING_SCHEMA = "dpone.semantic-refresh-effective-key-mapping.v1"
_REQUIRED = frozenset({"name", "source_type", "target_type", "mapping", "nullable", "utc_assurance_required"})
_OPTIONAL = frozenset({"domain_min", "domain_max"})
_VALIDATION_ASSURANCE = "sha256:" + "0" * 64


@dataclass(frozen=True, slots=True)
class EffectiveKeyTemplateColumn:
    """Pre-release key semantics with no deployment/runtime assurance identity."""

    name: str
    source_type: str
    target_type: str
    domain_min: str | None = None
    domain_max: str | None = None
    mapping: EffectiveKeyMapping = field(init=False)
    nullable: bool = field(default=False, init=False)
    utc_assurance_required: bool = field(init=False)

    def __post_init__(self) -> None:
        validated = EffectiveKeyColumn(
            name=self.name,
            source_type=self.source_type,
            target_type=self.target_type,
            domain_min=self.domain_min,
            domain_max=self.domain_max,
            utc_assurance_sha256=(_VALIDATION_ASSURANCE if self.source_type == "datetime2(6)" else None),
        )
        object.__setattr__(self, "mapping", validated.mapping)
        object.__setattr__(self, "utc_assurance_required", self.source_type == "datetime2(6)")

    @classmethod
    def from_effective_key(cls, value: EffectiveKeyColumn) -> EffectiveKeyTemplateColumn:
        """Remove the downstream assurance identity from one validated final column."""

        if not isinstance(value, EffectiveKeyColumn):
            raise SemanticRefreshContractError("effective key must be a typed final column")
        return cls(value.name, value.source_type, value.target_type, value.domain_min, value.domain_max)

    @classmethod
    def from_mapping(cls, value: object) -> EffectiveKeyTemplateColumn:
        """Parse one closed deployment-neutral key template column."""

        raw = require_closed_mapping(value, "effective_key_template_column", required=_REQUIRED, optional=_OPTIONAL)
        if raw.get("nullable") is not False:
            raise SemanticRefreshContractError("effective-key templates must be non-null")
        column = cls(
            name=require_text(raw.get("name"), "effective key template name"),
            source_type=require_text(raw.get("source_type"), "effective key template source_type"),
            target_type=require_text(raw.get("target_type"), "effective key template target_type"),
            domain_min=_optional_text(raw, "domain_min"),
            domain_max=_optional_text(raw, "domain_max"),
        )
        if (
            require_enum(raw.get("mapping"), "effective key template mapping", EffectiveKeyMapping)
            is not column.mapping
        ):
            raise SemanticRefreshContractError("effective-key template mapping label differs from its types")
        if raw.get("utc_assurance_required") is not column.utc_assurance_required:
            raise SemanticRefreshContractError("effective-key template UTC assurance requirement differs from its type")
        return column

    def to_dict(self) -> dict[str, object]:
        """Return semantic key fields only, deliberately excluding assurance receipts."""

        result: dict[str, object] = {
            "mapping": self.mapping.value,
            "name": self.name,
            "nullable": self.nullable,
            "source_type": self.source_type,
            "target_type": self.target_type,
            "utc_assurance_required": self.utc_assurance_required,
        }
        if self.domain_min is not None:
            result["domain_min"] = self.domain_min
        if self.domain_max is not None:
            result["domain_max"] = self.domain_max
        return result


def semantic_refresh_effective_key_template_sha256(
    columns: Sequence[EffectiveKeyTemplateColumn | EffectiveKeyColumn],
) -> str:
    """Digest ordered key semantics without any runtime assurance identity."""

    templates = tuple(
        item if isinstance(item, EffectiveKeyTemplateColumn) else EffectiveKeyTemplateColumn.from_effective_key(item)
        for item in _validate_columns(columns)
    )
    return semantic_refresh_sha256(
        {"columns": [item.to_dict() for item in templates], "schema": EFFECTIVE_KEY_TEMPLATE_SCHEMA}
    )


def semantic_refresh_effective_key_mapping_sha256(columns: Sequence[EffectiveKeyColumn]) -> str:
    """Digest the final ordered mapping, including any exact UTC receipt identity."""

    final = _validate_columns(columns)
    if any(not isinstance(item, EffectiveKeyColumn) for item in final):
        raise SemanticRefreshContractError("final effective-key mapping requires typed final columns")
    return semantic_refresh_sha256(
        {"columns": [item.to_dict() for item in final], "schema": EFFECTIVE_KEY_MAPPING_SCHEMA}
    )


def _validate_columns(
    columns: Sequence[EffectiveKeyTemplateColumn | EffectiveKeyColumn],
) -> tuple[EffectiveKeyTemplateColumn | EffectiveKeyColumn, ...]:
    if isinstance(columns, str | bytes) or not columns:
        raise SemanticRefreshContractError("effective-key identity requires ordered columns")
    result = tuple(columns)
    if any(not isinstance(item, EffectiveKeyTemplateColumn | EffectiveKeyColumn) for item in result):
        raise SemanticRefreshContractError("effective-key identity requires typed columns")
    names = tuple(item.name for item in result)
    if len(names) != len(set(names)):
        raise SemanticRefreshContractError("effective-key identity contains duplicate column names")
    return result


def _optional_text(raw: Mapping[str, Any], field_name: str) -> str | None:
    if field_name not in raw:
        return None
    return require_text(raw[field_name], field_name)


__all__ = [
    "EFFECTIVE_KEY_MAPPING_SCHEMA",
    "EFFECTIVE_KEY_TEMPLATE_SCHEMA",
    "EffectiveKeyTemplateColumn",
    "semantic_refresh_effective_key_mapping_sha256",
    "semantic_refresh_effective_key_template_sha256",
]
