"""User-declared logical schema contracts.

Contracts are portable. They describe what a column means before a target
renderer decides whether it becomes ``decimal(18,4)``, ``Decimal(18,4)``,
``NUMERIC`` or another dialect-specific type.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any, Literal, cast

EnforcementMode = Literal["strict", "coerce", "quarantine", "warn"]
OpaqueNativeProof = Literal["clickhouse_staging"]


@dataclass(frozen=True, slots=True)
class ColumnContract:
    name: str
    logical_type: str
    precision: int | None = None
    scale: int | None = None
    nullable: bool = True
    timezone: bool | None = None

    @classmethod
    def from_config(cls, name: str, raw: Mapping[str, Any]) -> ColumnContract:
        supported = {"type", "logical_type", "precision", "scale", "nullable", "timezone"}
        unknown = sorted(str(key) for key in raw if str(key) not in supported)
        if unknown:
            raise ValueError(f"schema_contract.columns.{name} has unsupported options: {', '.join(unknown)}")
        return cls(
            name=str(name),
            logical_type=str(raw.get("type", raw.get("logical_type", "string"))),
            precision=_optional_int(raw.get("precision"), "precision"),
            scale=_optional_int(raw.get("scale"), "scale"),
            nullable=bool(raw.get("nullable", True)),
            timezone=bool(raw["timezone"]) if "timezone" in raw else None,
        )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SchemaContract:
    enforcement: EnforcementMode = "strict"
    columns: Mapping[str, ColumnContract] | None = None
    opaque_native_proof: OpaqueNativeProof | None = None

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> SchemaContract:
        values = dict(raw or {})
        enforcement = str(values.get("enforcement", "strict"))
        if enforcement not in {"strict", "coerce", "quarantine", "warn"}:
            raise ValueError("schema_contract.enforcement must be one of: strict, coerce, quarantine, warn")
        proof = values.get("opaque_native_proof")
        if proof is not None and proof != "clickhouse_staging":
            raise ValueError("schema_contract.opaque_native_proof must be clickhouse_staging")
        raw_columns = values.get("columns", {})
        if raw_columns is None:
            raw_columns = {}
        if not isinstance(raw_columns, Mapping):
            raise ValueError("schema_contract.columns must be an object")
        columns = {
            str(name): ColumnContract.from_config(str(name), _ensure_mapping(config, str(name)))
            for name, config in raw_columns.items()
        }
        return cls(
            enforcement=enforcement,  # type: ignore[arg-type]
            columns=columns,
            opaque_native_proof=cast(OpaqueNativeProof, proof) if proof is not None else None,
        )

    def column(self, name: str) -> ColumnContract | None:
        values = self.columns or {}
        return values.get(name) or values.get(name.lower())

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "enforcement": self.enforcement,
            "columns": {name: column.to_dict() for name, column in (self.columns or {}).items()},
        }
        if self.opaque_native_proof is not None:
            payload["opaque_native_proof"] = self.opaque_native_proof
        return payload


def _ensure_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"schema_contract.columns.{name} must be an object")
    return value


def _optional_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    result = int(value)
    if result < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return result
