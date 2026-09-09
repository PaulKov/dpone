"""Immutable schema identity config and evidence models."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Literal

from dpone.readiness.schema_evolution import ColumnDef

SCHEMA_IDENTITY_PLAN_SCHEMA = "dpone.schema_identity_plan.v1"
_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")


@dataclass(frozen=True, slots=True)
class SchemaAlias:
    name: str
    compatibility: Literal["read_alias", "dual_write"] = "read_alias"
    remove_after: date | None = None

    @classmethod
    def from_config(cls, raw: Mapping[str, Any]) -> SchemaAlias:
        name = str(raw.get("name", "")).strip()
        if not name:
            raise ValueError("schema_identity alias name is required")
        compatibility = _literal(
            raw.get("compatibility", "read_alias"),
            {"read_alias", "dual_write"},
            "alias.compatibility",
        )
        return cls(
            name=name,
            compatibility=compatibility,  # type: ignore[arg-type]
            remove_after=_optional_date(raw.get("remove_after")),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["remove_after"] = self.remove_after.isoformat() if self.remove_after else None
        return payload


@dataclass(frozen=True, slots=True)
class SchemaObjectIdentity:
    name: str
    identity_id: str
    aliases: tuple[SchemaAlias, ...] = ()

    @classmethod
    def from_config(cls, name: str, raw: Mapping[str, Any]) -> SchemaObjectIdentity:
        identity_id = str(raw.get("id", "")).strip()
        if not identity_id:
            raise ValueError(f"schema_identity.columns.{name}.id is required")
        if not _ID_RE.match(identity_id):
            raise ValueError(f"schema_identity id has unsafe characters: {identity_id}")
        aliases = tuple(SchemaAlias.from_config(item) for item in raw.get("aliases", []) if isinstance(item, Mapping))
        return cls(name=str(name), identity_id=identity_id, aliases=tuple(sorted(aliases, key=lambda item: item.name)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "id": self.identity_id,
            "aliases": [alias.to_dict() for alias in self.aliases],
        }


@dataclass(frozen=True, slots=True)
class SchemaIdentityOptions:
    enabled: bool = False
    mode: Literal["observe", "enforce"] = "enforce"
    rename_detection: Literal["explicit", "suggest"] = "explicit"
    expired_alias: Literal["block", "warn"] = "block"
    rename_strategy: Literal["expand_contract", "runtime_alias", "direct_rename"] = "expand_contract"
    columns: tuple[SchemaObjectIdentity, ...] = ()

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> SchemaIdentityOptions:
        if not raw:
            return cls()
        if not isinstance(raw, Mapping):
            raise ValueError("sink.options.schema_identity must be an object")
        columns_raw = raw.get("columns", {})
        if not isinstance(columns_raw, Mapping):
            raise ValueError("schema_identity.columns must be an object")
        rename_raw = raw.get("rename", {})
        rename = rename_raw if isinstance(rename_raw, Mapping) else {}
        options = cls(
            enabled=bool(raw.get("enabled", False)),
            mode=_literal(raw.get("mode", "enforce"), {"observe", "enforce"}, "mode"),  # type: ignore[arg-type]
            rename_detection=_literal(
                raw.get("rename_detection", "explicit"), {"explicit", "suggest"}, "rename_detection"
            ),  # type: ignore[arg-type]
            expired_alias=_literal(raw.get("expired_alias", "block"), {"block", "warn"}, "expired_alias"),  # type: ignore[arg-type]
            rename_strategy=_literal(
                rename.get("strategy", "expand_contract"),
                {"expand_contract", "runtime_alias", "direct_rename"},
                "rename.strategy",
            ),  # type: ignore[arg-type]
            columns=_parse_columns(columns_raw),
        )
        _validate_graph(options)
        return options

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "rename_detection": self.rename_detection,
            "expired_alias": self.expired_alias,
            "rename": {"strategy": self.rename_strategy},
            "columns": {
                item.name: {"id": item.identity_id, "aliases": [alias.to_dict() for alias in item.aliases]}
                for item in self.columns
            },
        }


@dataclass(frozen=True, slots=True)
class SchemaIdentityDecision:
    canonical_name: str
    observed_name: str
    action: str
    object_type: str = "column"
    risk: str = "metadata"
    blocker: str | None = None
    warning: str | None = None
    compatibility: str | None = None
    source_type: str | None = None
    target_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}


@dataclass(frozen=True, slots=True)
class SchemaIdentityResult:
    canonical_source: tuple[ColumnDef, ...]
    decisions: tuple[SchemaIdentityDecision, ...] = ()
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity_decisions": [item.to_dict() for item in self.decisions],
            "canonical_source_schema": [
                {"name": item.name, "dtype": item.dtype, "nullable": item.nullable} for item in self.canonical_source
            ],
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class AliasProjectionResult:
    rows: tuple[dict[str, Any], ...]
    schema: tuple[tuple[str, str], ...]
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


def alias_lookup(options: SchemaIdentityOptions) -> dict[str, tuple[SchemaObjectIdentity, SchemaAlias]]:
    return {alias.name.lower(): (identity, alias) for identity in options.columns for alias in identity.aliases}


def expiration_issue(identity_name: str, alias: SchemaAlias, today: date) -> str | None:
    if alias.remove_after and today > alias.remove_after:
        return f"schema_identity.alias_expired:{identity_name}:{alias.name}"
    return None


def actual_key(row: Mapping[str, Any], name: str) -> str | None:
    normalized = name.lower()
    for key in row:
        if str(key).lower() == normalized:
            return str(key)
    return None


def _parse_columns(columns_raw: Mapping[str, Any]) -> tuple[SchemaObjectIdentity, ...]:
    return tuple(
        sorted(
            (
                SchemaObjectIdentity.from_config(str(name), item)
                for name, item in columns_raw.items()
                if isinstance(item, Mapping)
            ),
            key=lambda item: item.name,
        )
    )


def _validate_graph(options: SchemaIdentityOptions) -> None:
    ids: dict[str, str] = {}
    aliases: dict[str, str] = {}
    canonical_names = {item.name.lower() for item in options.columns}
    for identity in options.columns:
        existing = ids.get(identity.identity_id)
        if existing and existing != identity.name:
            raise ValueError(f"duplicate identity id {identity.identity_id}: {existing}, {identity.name}")
        ids[identity.identity_id] = identity.name
        for alias in identity.aliases:
            normalized = alias.name.lower()
            if normalized in canonical_names and normalized != identity.name.lower():
                raise ValueError(f"alias {alias.name} collides with another canonical column")
            existing_alias = aliases.get(normalized)
            if existing_alias and existing_alias != identity.name:
                raise ValueError(f"duplicate alias {alias.name}: {existing_alias}, {identity.name}")
            aliases[normalized] = identity.name


def _optional_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _literal(value: Any, allowed: set[str], field_name: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in allowed:
        raise ValueError(f"schema_identity.{field_name} must be one of: {', '.join(sorted(allowed))}")
    return normalized


__all__ = [
    "AliasProjectionResult",
    "SCHEMA_IDENTITY_PLAN_SCHEMA",
    "SchemaAlias",
    "SchemaIdentityDecision",
    "SchemaIdentityOptions",
    "SchemaIdentityResult",
    "SchemaObjectIdentity",
    "actual_key",
    "alias_lookup",
    "expiration_issue",
]
