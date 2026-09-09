"""Physical target design models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.readiness.physical_reconciliation_approval import (
    PhysicalReconciliationApproval,
)
from dpone.readiness.physical_reconciliation_approval import (
    approval_blockers as approval_blockers,
)

PhysicalApplyMode = Literal["online", "safe_window", "plan_only", "manual_approval"]
PhysicalMode = Literal["auto", "explicit", "off"]
LowCardinalityMode = Literal["off", "auto", "explicit", "force", "preserve"]
PhysicalReconciliationMode = Literal["block", "auto_safe", "plan_only", "safe_window"]
PhysicalMigrationStrategy = Literal["block", "online_safe", "shadow"]


@dataclass(frozen=True, slots=True)
class LowCardinalityOptions:
    mode: LowCardinalityMode = "auto"
    columns: tuple[str, ...] = ()
    max_distinct_values: int = 10000
    max_distinct_ratio: float = 0.05

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> LowCardinalityOptions:
        values = dict(raw or {})
        mode = str(values.get("mode", "auto"))
        if mode not in {"off", "auto", "explicit", "force", "preserve"}:
            raise ValueError("physical_design.storage.clickhouse.low_cardinality.mode is invalid")
        return cls(
            mode=mode,  # type: ignore[arg-type]
            columns=tuple(str(item) for item in values.get("columns", ())),
            max_distinct_values=int(values.get("max_distinct_values", 10000)),
            max_distinct_ratio=float(values.get("max_distinct_ratio", 0.05)),
        )


@dataclass(frozen=True, slots=True)
class PhysicalReconciliationOptions:
    mode: PhysicalReconciliationMode = "block"
    approval: PhysicalReconciliationApproval | None = None

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> PhysicalReconciliationOptions:
        values = dict(raw or {})
        mode = str(values.get("mode", "block"))
        if mode not in {"block", "auto_safe", "plan_only", "safe_window"}:
            raise ValueError(
                "physical_design.reconciliation.mode must be one of: block, auto_safe, plan_only, safe_window"
            )
        approval_raw = values.get("approval")
        approval = (
            PhysicalReconciliationApproval.from_config(approval_raw) if isinstance(approval_raw, Mapping) else None
        )
        return cls(mode=mode, approval=approval)  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {"mode": self.mode}
        if self.approval is not None:
            payload["approval"] = self.approval.to_dict()
        return payload


@dataclass(frozen=True, slots=True)
class PhysicalMigrationOptions:
    strategy: PhysicalMigrationStrategy = "block"
    shadow: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> PhysicalMigrationOptions:
        values = dict(raw or {})
        strategy = str(values.get("strategy", "block"))
        if strategy not in {"block", "online_safe", "shadow"}:
            raise ValueError("physical_design.migration.strategy must be one of: block, online_safe, shadow")
        shadow = values.get("shadow", {})
        if shadow is not None and not isinstance(shadow, Mapping):
            raise ValueError("physical_design.migration.shadow must be an object")
        return cls(strategy=strategy, shadow=dict(shadow or {}))  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, object]:
        return {"strategy": self.strategy, "shadow": dict(self.shadow)}


@dataclass(frozen=True, slots=True)
class PhysicalColumnOverride:
    target_type: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> PhysicalColumnOverride:
        values = dict(raw or {})
        raw_target = values.get("target_type", {})
        if raw_target and not isinstance(raw_target, Mapping):
            raise ValueError("physical_design.columns.*.target_type must be an object")
        target_types = {str(key): str(value) for key, value in dict(raw_target).items()}
        for sink_alias in ("mssql", "sqlserver", "sql_server"):
            if sink_alias in target_types:
                target_types[sink_alias] = normalize_mssql_physical_type(target_types[sink_alias])
        return cls(target_type=target_types)


@dataclass(frozen=True, slots=True)
class PhysicalDesignOptions:
    enabled: bool = True
    mode: PhysicalMode = "auto"
    apply: PhysicalApplyMode = "online"
    columns: Mapping[str, PhysicalColumnOverride] = field(default_factory=dict)
    partitioning: Mapping[str, Any] = field(default_factory=dict)
    indexes: Mapping[str, Any] = field(default_factory=dict)
    storage: Mapping[str, Any] = field(default_factory=dict)
    reconciliation: PhysicalReconciliationOptions = field(default_factory=PhysicalReconciliationOptions)
    migration: PhysicalMigrationOptions = field(default_factory=PhysicalMigrationOptions)

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> PhysicalDesignOptions:
        values = dict(raw or {})
        mode = str(values.get("mode", "auto"))
        if mode not in {"auto", "explicit", "off"}:
            raise ValueError("physical_design.mode must be one of: auto, explicit, off")
        apply = str(values.get("apply", "online"))
        if apply not in {"online", "safe_window", "plan_only", "manual_approval"}:
            raise ValueError("physical_design.apply must be one of: online, safe_window, plan_only, manual_approval")
        raw_columns = values.get("columns", {})
        if raw_columns and not isinstance(raw_columns, Mapping):
            raise ValueError("physical_design.columns must be an object")
        enabled = values.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("physical_design.enabled must be boolean")
        return cls(
            enabled=enabled,
            mode=mode,  # type: ignore[arg-type]
            apply=apply,  # type: ignore[arg-type]
            columns={
                str(name): PhysicalColumnOverride.from_config(_ensure_mapping(config, str(name)))
                for name, config in dict(raw_columns).items()
            },
            partitioning=dict(_ensure_mapping(values.get("partitioning", {}), "partitioning")),
            indexes=dict(_ensure_mapping(values.get("indexes", {}), "indexes")),
            storage=dict(_ensure_mapping(values.get("storage", {}), "storage")),
            reconciliation=PhysicalReconciliationOptions.from_config(
                _ensure_mapping(values.get("reconciliation", {}), "reconciliation")
            ),
            migration=PhysicalMigrationOptions.from_config(_ensure_mapping(values.get("migration", {}), "migration")),
        )

    @property
    def active(self) -> bool:
        """Whether physical design may influence target DDL or reconciliation."""

        return self.enabled and self.mode != "off"

    def low_cardinality(self) -> LowCardinalityOptions:
        clickhouse = self.storage.get("clickhouse", {})
        if not isinstance(clickhouse, Mapping):
            return LowCardinalityOptions()
        low_cardinality = clickhouse.get("low_cardinality", {})
        return LowCardinalityOptions.from_config(low_cardinality if isinstance(low_cardinality, Mapping) else {})

    def to_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "apply": self.apply,
            "columns": {name: override.target_type for name, override in self.columns.items()},
            "partitioning": dict(self.partitioning),
            "indexes": dict(self.indexes),
            "storage": dict(self.storage),
            "reconciliation": self.reconciliation.to_dict(),
            "migration": self.migration.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class ResolvedTargetColumn:
    name: str
    logical_type: str
    target_type: str
    nullable: bool
    decision_source: str
    reason: str
    decision_category: str = "auto_inferred"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PhysicalDesignPlan:
    sink_type: str
    table: str
    options: PhysicalDesignOptions
    columns: Mapping[str, ResolvedTargetColumn]
    ddl: Sequence[str]
    risks: Sequence[str]
    contract: Any
    resolved_table_settings: Sequence[Any] = ()
    warnings: Sequence[str] = ()
    resolved_target_design: Any = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "sink_type": self.sink_type,
            "table": self.table,
            "options": self.options.to_dict(),
            "columns": {name: column.to_dict() for name, column in self.columns.items()},
            "ddl": list(self.ddl),
            "risks": list(self.risks),
            "contract": self.contract.to_dict() if hasattr(self.contract, "to_dict") else {},
            "resolved_table_settings": [
                setting.to_dict() if hasattr(setting, "to_dict") else dict(setting)
                for setting in self.resolved_table_settings
            ],
            "warnings": list(self.warnings),
        }
        if self.resolved_target_design is not None:
            payload["resolved_target_design"] = (
                self.resolved_target_design.to_dict()
                if hasattr(self.resolved_target_design, "to_dict")
                else self.resolved_target_design
            )
        return payload


def _ensure_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"physical_design.{field_name} must be an object")
    return value
