"""DDL governance and online schema evolution planning.

This module is intentionally side-effect free except for the ledger writer. The
schema comparator detects drift; this layer decides whether the resulting DDL is
online-safe, needs a change window, must be deferred, or requires an
expand-contract migration.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.readiness.ddl_policy_decisions import (
    ColumnContractMode,
    DataTypeContractMode,
    DdlMode,
    Decision,
    RiskLevel,
    SchemaChangeBehavior,
    TableContractMode,
    blocker_code,
    decide_change,
    decision_blocks_load,
    governance_option_blockers,
    inline_ddl_budget_status,
)
from dpone.readiness.ddl_policy_decisions import (
    decide_missing_table as _decide_missing_table,
)
from dpone.readiness.schema_evolution import SchemaChange, SchemaPlan


@dataclass(frozen=True, slots=True)
class DdlGovernancePolicy:
    tables: TableContractMode = "evolve"
    columns: ColumnContractMode = "evolve"
    data_type: DataTypeContractMode = "widen"
    ddl_mode: DdlMode = "online"
    lock_timeout_seconds: int | None = None
    statement_timeout_seconds: int | None = None
    max_table_size_for_inline_ddl: int | None = None
    on_schema_change: SchemaChangeBehavior = "apply"
    ledger_path: str | None = None
    allow_blocking_online: bool = False

    @classmethod
    def from_schema_evolution_options(cls, raw: Mapping[str, Any] | None) -> DdlGovernancePolicy:
        values = dict(raw or {})
        return cls(
            tables=_literal(values.get("tables", "evolve"), {"evolve", "freeze", "ignore"}, "tables"),
            columns=_literal(values.get("columns", "evolve"), {"evolve", "freeze", "ignore", "quarantine"}, "columns"),
            data_type=_literal(
                values.get("data_type", "widen"), {"widen", "variant_column", "freeze", "quarantine"}, "data_type"
            ),
            ddl_mode=_literal(
                values.get("ddl_mode", "online"), {"online", "safe_window", "plan_only", "manual_approval"}, "ddl_mode"
            ),
            lock_timeout_seconds=_optional_positive_int(values.get("lock_timeout_seconds"), "lock_timeout_seconds"),
            statement_timeout_seconds=_optional_positive_int(
                values.get("statement_timeout_seconds"), "statement_timeout_seconds"
            ),
            max_table_size_for_inline_ddl=_optional_positive_int(
                values.get("max_table_size_for_inline_ddl"), "max_table_size_for_inline_ddl"
            ),
            on_schema_change=_literal(
                values.get("on_schema_change", "apply"),
                {"apply", "notify", "fail", "disable_pipeline"},
                "on_schema_change",
            ),
            ledger_path=str(values["ledger_path"]) if values.get("ledger_path") else None,
            allow_blocking_online=bool(values.get("allow_blocking_online", False)),
        )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def decide_missing_table(policy: DdlGovernancePolicy) -> Decision:
    """Evaluate missing-table policy through the stable governance facade."""

    return _decide_missing_table(policy)


@dataclass(frozen=True, slots=True)
class DdlCapability:
    dialect: str
    change_type: str
    risk_level: RiskLevel
    online_supported: bool
    reason: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class DdlCapabilityRegistry:
    """Classifies sink DDL capabilities without connector dependencies."""

    def classify(self, *, dialect: str, change: SchemaChange) -> DdlCapability:
        normalized = _normalize_dialect(dialect)
        if change.breaking:
            return DdlCapability(normalized, change.change_type, "breaking", False, "breaking schema change")
        if normalized == "kafka":
            return DdlCapability(normalized, change.change_type, "unsupported", False, "Kafka uses Schema Registry")
        if change.change_type in {"add_column", "add_generated_column"}:
            if change.source and change.source.nullable:
                return DdlCapability(
                    normalized,
                    change.change_type,
                    "metadata_only",
                    True,
                    "nullable ADD COLUMN is metadata-only or low-lock for supported sinks",
                )
            return DdlCapability(
                normalized, change.change_type, "blocking", False, "non-null ADD COLUMN can rewrite data"
            )
        if change.change_type == "map_to_generated_column":
            return DdlCapability(normalized, change.change_type, "metadata_only", True, "no target DDL required")
        if change.change_type == "type_widen":
            if normalized == "bigquery":
                return DdlCapability(normalized, change.change_type, "low_lock", True, "BigQuery schema API widening")
            return DdlCapability(
                normalized, change.change_type, "blocking", False, "type widening may lock or rewrite data"
            )
        if change.change_type == "nullability_relax":
            return DdlCapability(
                normalized,
                change.change_type,
                "blocking",
                False,
                "ALTER COLUMN nullability may scan or lock the target table",
            )
        return DdlCapability(normalized, change.change_type, "unsupported", False, "DDL capability is not supported")


@dataclass(frozen=True, slots=True)
class GovernedDdlAction:
    schema_change_id: str
    change_type: str
    column: str
    risk_level: RiskLevel
    online_supported: bool
    decision: Decision
    approval_status: str
    ddl: tuple[str, ...]
    reason: str

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["ddl"] = list(self.ddl)
        return data


@dataclass(frozen=True, slots=True)
class GovernedSchemaPlan:
    dialect: str
    table: str
    policy: DdlGovernancePolicy
    actions: tuple[GovernedDdlAction, ...]
    blockers: tuple[str, ...]
    expand_contract_plan: tuple[str, ...]

    @property
    def online_eligible(self) -> bool:
        return not self.blockers and all(action.online_supported for action in self.actions)

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_dict(self) -> dict[str, object]:
        return {
            "dialect": self.dialect,
            "table": self.table,
            "policy": self.policy.to_dict(),
            "online_eligible": self.online_eligible,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "expand_contract_plan": list(self.expand_contract_plan),
            "actions": [item.to_dict() for item in self.actions],
        }


class OnlineSchemaPlanner:
    """Adds online-safety and governance decisions to a schema plan."""

    def __init__(self, registry: DdlCapabilityRegistry | None = None) -> None:
        self.registry = registry or DdlCapabilityRegistry()

    def plan(
        self,
        *,
        schema_plan: SchemaPlan,
        dialect: str,
        table: str,
        policy: DdlGovernancePolicy,
        table_row_count: int | None = None,
    ) -> GovernedSchemaPlan:
        normalized = _normalize_dialect(dialect)
        actions = tuple(
            self._action(
                change=change,
                schema_plan=schema_plan,
                dialect=normalized,
                table=table,
                policy=policy,
                table_row_count=table_row_count,
            )
            for change in schema_plan.changes
        )
        action_blockers = tuple(
            blocker_code(
                change_type=action.change_type,
                column=action.column,
                risk_level=action.risk_level,
                decision=action.decision,
                reason=action.reason,
            )
            for action in actions
            if decision_blocks_load(action.decision)
        )
        blockers = (
            *governance_option_blockers(dialect=normalized, policy=policy, has_changes=bool(actions)),
            *action_blockers,
        )
        expand_contract = tuple(
            _expand_contract_step(action)
            for action in actions
            if action.risk_level in {"blocking", "breaking", "unsupported"}
        )
        return GovernedSchemaPlan(
            dialect=normalized,
            table=table,
            policy=policy,
            actions=actions,
            blockers=blockers,
            expand_contract_plan=expand_contract,
        )

    def _action(
        self,
        *,
        change: SchemaChange,
        schema_plan: SchemaPlan,
        dialect: str,
        table: str,
        policy: DdlGovernancePolicy,
        table_row_count: int | None,
    ) -> GovernedDdlAction:
        capability = self.registry.classify(dialect=dialect, change=change)
        raw_ddl = _ddl_for_change(schema_plan, change, dialect, table)
        decision = decide_change(
            change_type=change.change_type,
            risk_level=capability.risk_level,
            policy=policy,
            table_row_count=table_row_count,
        )
        budget_status = inline_ddl_budget_status(
            change_type=change.change_type,
            policy=policy,
            table_row_count=table_row_count,
        )
        risk_level = (
            "blocking"
            if budget_status is not None and capability.risk_level == "metadata_only"
            else capability.risk_level
        )
        reason = capability.reason if not change.reason else f"{change.reason}; {capability.reason}"
        if budget_status == "unknown":
            reason = f"{reason}; table row count is unavailable for inline DDL budget"
        elif budget_status == "exceeded":
            reason = f"{reason}; table size exceeds inline DDL budget"
        return GovernedDdlAction(
            schema_change_id=_schema_change_id(table, dialect, change),
            change_type=change.change_type,
            column=change.column,
            risk_level=risk_level,
            online_supported=capability.online_supported,
            decision=decision,
            approval_status="applied" if decision == "apply" else "pending",
            ddl=tuple(_decorate_ddl(dialect, raw_ddl, policy))
            if decision in {"apply", "notify", "defer"}
            else tuple(raw_ddl),
            reason=reason,
        )


class SchemaChangeLedger:
    """Writes tamper-evident-ish schema change evidence for run artifacts."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def record(self, *, run_id: str, table: str, dialect: str, plan: GovernedSchemaPlan) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        status = "applied" if plan.passed else "deferred"
        payload = {
            "run_id": run_id,
            "table": table,
            "dialect": dialect,
            "status": status,
            "blockers": list(plan.blockers),
            "online_eligible": plan.online_eligible,
            "actions": [action.to_dict() for action in plan.actions],
            "expand_contract_plan": list(plan.expand_contract_plan),
        }
        path = self.root / f"schema_change_{run_id}_{_payload_hash(payload)}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return path


def _expand_contract_step(action: GovernedDdlAction) -> str:
    return (
        f"expand-contract required for {action.change_type}:{action.column}: "
        "expand compatible schema, route new values safely, backfill if needed, then contract manually"
    )


def _ddl_for_change(schema_plan: SchemaPlan, change: SchemaChange, dialect: str, table: str) -> list[str]:
    for safe_change in schema_plan.safe_changes:
        if safe_change is change:
            single_plan = SchemaPlan([safe_change], schema_plan.policy)
            return single_plan.ddl_sql(dialect, table)
    return []


def _decorate_ddl(dialect: str, ddl: list[str], policy: DdlGovernancePolicy) -> list[str]:
    prefix: list[str] = []
    if dialect == "postgres":
        if policy.lock_timeout_seconds is not None:
            prefix.append(f"SET lock_timeout = '{policy.lock_timeout_seconds}s'")
        if policy.statement_timeout_seconds is not None:
            prefix.append(f"SET statement_timeout = '{policy.statement_timeout_seconds}s'")
    elif dialect == "mssql" and policy.lock_timeout_seconds is not None:
        prefix.append(f"SET LOCK_TIMEOUT {policy.lock_timeout_seconds * 1000}")
    return [*prefix, *ddl]


def _schema_change_id(table: str, dialect: str, change: SchemaChange) -> str:
    raw = f"{dialect}:{table}:{change.change_type}:{change.column}:{change.reason}".encode()
    return "sch_" + hashlib.sha256(raw).hexdigest()[:24]


def _payload_hash(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def _normalize_dialect(dialect: str) -> str:
    normalized = dialect.lower().replace("-", "_")
    aliases = {
        "sqlserver": "mssql",
        "sql_server": "mssql",
        "postgresql": "postgres",
        "pg": "postgres",
        "ch": "clickhouse",
        "bq": "bigquery",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in {"mssql", "postgres", "clickhouse", "bigquery", "kafka"}:
        raise ValueError(f"Unsupported DDL governance dialect: {dialect}")
    return normalized


def _literal(value: object, allowed: set[str], field_name: str):
    normalized = str(value).strip().lower()
    if normalized not in allowed:
        raise ValueError(f"schema_evolution.{field_name} must be one of: {', '.join(sorted(allowed))}")
    return normalized


def _optional_positive_int(value: object, field_name: str) -> int | None:
    if value is None or value == "":
        return None
    result = int(str(value))
    if result < 0:
        raise ValueError(f"schema_evolution.{field_name} must be >= 0")
    return result
