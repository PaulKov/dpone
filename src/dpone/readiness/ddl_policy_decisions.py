"""Finite, fail-closed policy decisions for governed schema evolution.

The functions in this module are connector-neutral and side-effect free.  A
runtime may continue only when the effective decision is ``apply``; every
other decision is evidence that the source payload and target schema are not
yet aligned for a safe load.
"""

from __future__ import annotations

from typing import Literal, Protocol

TableContractMode = Literal["evolve", "freeze", "ignore"]
ColumnContractMode = Literal["evolve", "freeze", "ignore", "quarantine"]
DataTypeContractMode = Literal["widen", "variant_column", "freeze", "quarantine"]
DdlMode = Literal["online", "safe_window", "plan_only", "manual_approval"]
SchemaChangeBehavior = Literal["apply", "notify", "fail", "disable_pipeline"]
RiskLevel = Literal["metadata_only", "low_lock", "blocking", "breaking", "unsupported"]
Decision = Literal["apply", "notify", "defer", "fail", "manual_approval", "quarantine"]

_PHYSICAL_COLUMN_CHANGES = frozenset(
    {"add_column", "add_generated_column", "collation_change", "nullability_relax", "nullability_tighten"}
)
_DATA_TYPE_CHANGES = frozenset({"add_generated_column", "map_to_generated_column", "type_widen", "type_change"})
_INLINE_DDL_CHANGES = frozenset({"add_column", "add_generated_column", "nullability_relax", "type_widen"})


class DdlDecisionPolicy(Protocol):
    """Structural subset required by the policy evaluator."""

    @property
    def tables(self) -> TableContractMode: ...

    @property
    def columns(self) -> ColumnContractMode: ...

    @property
    def data_type(self) -> DataTypeContractMode: ...

    @property
    def ddl_mode(self) -> DdlMode: ...

    @property
    def lock_timeout_seconds(self) -> int | None: ...

    @property
    def statement_timeout_seconds(self) -> int | None: ...

    @property
    def max_table_size_for_inline_ddl(self) -> int | None: ...

    @property
    def on_schema_change(self) -> SchemaChangeBehavior: ...

    @property
    def allow_blocking_online(self) -> bool: ...


def decide_change(
    *,
    change_type: str,
    risk_level: RiskLevel,
    policy: DdlDecisionPolicy,
    table_row_count: int | None = None,
) -> Decision:
    """Return the one effective action for a detected schema change.

    Precedence is deliberate: global stop behavior wins, then entity contract
    modes, operator approval modes, physical safety, and finally notification.
    This makes all enum combinations deterministic and prevents a metadata-only
    risk classification from overriding a non-apply policy.
    """

    if policy.on_schema_change in {"fail", "disable_pipeline"}:
        return "fail"
    if change_type in _PHYSICAL_COLUMN_CHANGES:
        if policy.columns == "quarantine":
            return "quarantine"
        if policy.columns in {"freeze", "ignore"}:
            return "defer"
    if change_type in _DATA_TYPE_CHANGES:
        if policy.data_type == "quarantine":
            return "quarantine"
        if policy.data_type == "freeze":
            return "defer"
    if policy.ddl_mode == "manual_approval":
        return "manual_approval"
    if policy.ddl_mode == "plan_only":
        return "defer"
    if (
        inline_ddl_budget_status(
            change_type=change_type,
            policy=policy,
            table_row_count=table_row_count,
        )
        is not None
    ):
        return "defer"
    if risk_level in {"breaking", "unsupported"}:
        return "defer"
    if policy.ddl_mode == "online" and risk_level == "blocking" and not policy.allow_blocking_online:
        return "defer"
    if policy.on_schema_change == "notify":
        return "notify"
    return "apply"


def decide_missing_table(policy: DdlDecisionPolicy) -> Decision:
    """Decide whether the downstream load may create an absent target table."""

    if policy.on_schema_change in {"fail", "disable_pipeline"}:
        return "fail"
    if policy.tables in {"freeze", "ignore"}:
        return "defer"
    if policy.ddl_mode == "manual_approval":
        return "manual_approval"
    if policy.ddl_mode == "plan_only":
        return "defer"
    if policy.on_schema_change == "notify":
        return "notify"
    return "apply"


def decision_blocks_load(decision: Decision) -> bool:
    """Return whether a decision forbids both DDL and target DML."""

    return decision != "apply"


def blocker_code(
    *,
    change_type: str,
    column: str,
    risk_level: RiskLevel,
    decision: Decision,
    reason: str,
) -> str:
    """Render the stable machine-readable blocker carried by plan evidence."""

    suffix = f"{change_type}:{column}"
    if "table row count is unavailable for inline DDL budget" in reason:
        return f"schema_evolution.table_size_unknown:{suffix}"
    if "table size exceeds inline DDL budget" in reason:
        return f"schema_evolution.table_size_budget:{suffix}"
    if risk_level == "breaking":
        return f"schema_evolution.breaking:{suffix}"
    if risk_level == "unsupported":
        return f"schema_evolution.unsupported:{suffix}"
    if decision == "manual_approval":
        return f"schema_evolution.manual_approval:{suffix}"
    if decision == "quarantine":
        return f"schema_evolution.quarantine:{suffix}"
    if decision == "notify":
        return f"schema_evolution.notify:{suffix}"
    if decision == "fail":
        return f"schema_evolution.fail:{suffix}"
    return f"schema_evolution.blocking:{suffix}"


def exceeds_table_budget(*, policy: DdlDecisionPolicy, table_row_count: int | None) -> bool:
    """Return whether metadata-only inline DDL exceeds its configured budget."""

    return (
        policy.max_table_size_for_inline_ddl is not None
        and table_row_count is not None
        and table_row_count > policy.max_table_size_for_inline_ddl
    )


def inline_ddl_budget_status(
    *,
    change_type: str,
    policy: DdlDecisionPolicy,
    table_row_count: int | None,
) -> Literal["unknown", "exceeded"] | None:
    """Return the fail-closed budget status for a physical inline DDL action."""

    if policy.max_table_size_for_inline_ddl is None or change_type not in _INLINE_DDL_CHANGES:
        return None
    if table_row_count is None:
        return "unknown"
    if table_row_count > policy.max_table_size_for_inline_ddl:
        return "exceeded"
    return None


def governance_option_blockers(
    *,
    dialect: str,
    policy: DdlDecisionPolicy,
    has_changes: bool,
) -> tuple[str, ...]:
    """Reject timeout controls that the selected sink cannot enforce."""

    if not has_changes:
        return ()
    blockers: list[str] = []
    if policy.lock_timeout_seconds is not None and dialect not in {"postgres", "mssql"}:
        blockers.append(f"schema_evolution.unsupported_option:lock_timeout_seconds:{dialect}")
    if policy.statement_timeout_seconds is not None and dialect != "postgres":
        blockers.append(f"schema_evolution.unsupported_option:statement_timeout_seconds:{dialect}")
    return tuple(blockers)
