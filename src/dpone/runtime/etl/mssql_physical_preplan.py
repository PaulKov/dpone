"""Catalog-first physical-design planning for governed SQL Server loads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

from dpone.readiness.physical_design import (
    PhysicalDesignOptions,
    PhysicalDesignPlanner,
    ResolvedTargetColumn,
)
from dpone.readiness.physical_reconciliation import PhysicalDesignDriftDetector, PhysicalDesignReconciler
from dpone.readiness.physical_state import PhysicalColumnState, PhysicalTableState
from dpone.runtime.sinks.mssql_physical_introspection import MssqlPhysicalMigrationDialect
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import physical_catalog_expectation
from dpone.runtime.sinks.mssql_target_mutation_plan import (
    MssqlTargetMutationPlan,
    append_target_mutations,
)


@dataclass(frozen=True, slots=True)
class MssqlPhysicalPreplan:
    """Immutable physical transition and operator-facing decision evidence."""

    mutation_plan: MssqlTargetMutationPlan
    report: Mapping[str, Any] | None


def plan_existing_mssql_physical_design(
    load_config: Any,
    *,
    sink: Any,
    admission: Any,
    mutation_plan: MssqlTargetMutationPlan,
    final_columns: tuple[Any, ...],
) -> MssqlPhysicalPreplan:
    """Plan and fingerprint existing-target physical changes before extraction."""

    raw = _physical_options(load_config)
    if raw is None:
        return MssqlPhysicalPreplan(mutation_plan, None)
    physical = PhysicalDesignOptions.from_config(raw)
    apply_runtime = raw.get("apply_runtime", True)
    if not isinstance(apply_runtime, bool):
        raise ValueError("physical_design.apply_runtime must be boolean")
    # Parse the target-specific finite contract even for external provisioning;
    # unsupported settings must never survive until a later target mutation.
    plan = _physical_plan(load_config, physical, final_columns)
    inspect = getattr(sink, "inspect_physical_design", None)
    if not callable(inspect):
        raise RuntimeError("physical_design.existing_target_introspection_required")
    actual = inspect(load_config)
    if not isinstance(actual, PhysicalTableState):
        actual = PhysicalTableState.from_mapping(actual if isinstance(actual, Mapping) else {})
    if actual.sink_type.casefold() != "mssql" or actual.table != plan.table:
        raise RuntimeError("physical_design.existing_target_introspection_identity_mismatch")
    schema_after = _replace_columns(actual, final_columns)
    if not apply_runtime:
        desired = _desired_state(plan, actual, final_columns)
        drift = PhysicalDesignDriftDetector().detect(desired, actual)
        if drift:
            paths = ",".join(dict.fromkeys(change.path for change in drift))
            raise RuntimeError(f"physical_design.external_contract_drift:{paths}")
        expectation = physical_catalog_expectation(actual, expected_after=schema_after)
        frozen = append_target_mutations(
            mutation_plan,
            admission,
            kind="physical_design",
            statements=(),
            expectation=expectation,
        )
        return MssqlPhysicalPreplan(
            frozen,
            {
                "applied": False,
                "external_provisioning": True,
                "table": plan.table,
                "blockers": [],
                "executed": [],
                "preplanned": True,
            },
        )
    if not physical.active:
        expectation = physical_catalog_expectation(actual, expected_after=schema_after)
        frozen = append_target_mutations(
            mutation_plan,
            admission,
            kind="physical_design",
            statements=(),
            expectation=expectation,
        )
        return MssqlPhysicalPreplan(
            frozen,
            {
                "applied": False,
                "table": plan.table,
                "blockers": [],
                "executed": [],
                "preplanned": True,
            },
        )
    desired = _desired_state(plan, actual, final_columns)
    reconciliation = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=physical.reconciliation,
        dialect=MssqlPhysicalMigrationDialect(),
        execution_apply_mode=physical.apply,
    )
    blockers = list(reconciliation.blockers)
    if physical.apply == "plan_only":
        blockers.append("physical_design.plan_only")
    elif physical.apply == "manual_approval":
        blockers.append("physical_design.manual_approval_required")
    if blockers:
        raise RuntimeError("physical DDL apply blocked: " + ", ".join(dict.fromkeys(blockers)))
    expected_after = _apply_authorized_changes(schema_after, reconciliation)
    expectation = physical_catalog_expectation(actual, expected_after=expected_after)
    frozen = append_target_mutations(
        mutation_plan,
        admission,
        kind="physical_design",
        statements=list(reconciliation.ddl),
        expectation=expectation,
    )
    report = reconciliation.to_dict()
    report.update(
        {
            "applied": False,
            "deferred_to_target_transaction": bool(reconciliation.ddl),
            "executed": [],
            "preplanned": True,
        }
    )
    return MssqlPhysicalPreplan(frozen, report)


def _physical_options(load_config: Any) -> Mapping[str, Any] | None:
    options = getattr(load_config, "options", {}) or {}
    if not isinstance(options, Mapping) or "physical_design" not in options:
        return None
    raw = options.get("physical_design")
    if not isinstance(raw, Mapping):
        raise ValueError("physical_design must be an object")
    return raw


def _physical_plan(
    load_config: Any,
    options: PhysicalDesignOptions,
    columns: tuple[Any, ...],
) -> Any:
    resolved = {
        column.name: ResolvedTargetColumn(
            name=column.name,
            logical_type=column.dtype,
            target_type=column.dtype,
            nullable=column.nullable,
            decision_source="canonical_target_projection",
            reason="catalog-first governed MSSQL physical preplan",
        )
        for column in columns
    }
    return PhysicalDesignPlanner().plan(
        sink_type="mssql",
        table=_table_name(load_config),
        options=options,
        resolved_columns=resolved,
    )


def _desired_state(plan: Any, actual: PhysicalTableState, columns: tuple[Any, ...]) -> PhysicalTableState:
    desired = PhysicalTableState.from_physical_plan(plan)
    settings = dict(desired.table_settings)
    for key in ("filegroup", "textimage_filegroup", "index_fillfactor"):
        if key not in settings and key in actual.table_settings:
            settings[key] = actual.table_settings[key]
    return replace(
        desired,
        columns=_column_states(columns),
        table_settings=settings,
    )


def _replace_columns(state: PhysicalTableState, columns: tuple[Any, ...]) -> PhysicalTableState:
    return replace(state, columns=_column_states(columns))


def _column_states(columns: tuple[Any, ...]) -> dict[str, PhysicalColumnState]:
    return {
        column.name: PhysicalColumnState(
            name=column.name,
            target_type=column.dtype,
            nullable=column.nullable,
            position=index,
        )
        for index, column in enumerate(columns, start=1)
    }


def _apply_authorized_changes(
    state: PhysicalTableState,
    reconciliation: Any,
) -> PhysicalTableState:
    settings = dict(state.table_settings)
    for action in reconciliation.actions:
        if not action.ddl:
            continue
        if action.change.path != "table_settings.compression":
            raise RuntimeError("physical_design.unmodeled_expected_after_transition")
        settings["compression"] = str(action.change.desired).upper()
    return replace(state, table_settings=settings)


def _table_name(load_config: Any) -> str:
    database = str(getattr(load_config, "target_database", "") or "").strip()
    schema = str(load_config.target_schema)
    table = str(load_config.target_table)
    if not database:
        raise RuntimeError("mssql_transaction.target_database_required")
    return f"[{database.replace(']', ']]')}].[{schema.replace(']', ']]')}].[{table.replace(']', ']]')}]"


__all__ = ["MssqlPhysicalPreplan", "plan_existing_mssql_physical_design"]
