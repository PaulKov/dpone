"""Plan physical target design and defer governed MSSQL DDL to finalization."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.contracts.technical_columns import TechnicalColumnCatalog
from dpone.readiness.physical_apply import DdlExecutionRequest, PhysicalDdlApplyService
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_design_models import ResolvedTargetColumn
from dpone.readiness.physical_reconciliation import PhysicalDesignReconciler, PhysicalReconciliationApplyService
from dpone.readiness.physical_state import PhysicalTableState, TargetPhysicalMigrationDialect
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import (
    MssqlTargetCatalogExpectation,
    physical_catalog_expectation,
)
from dpone.runtime.sinks.mssql_target_mutation_plan import append_target_mutations
from dpone.runtime.support.mssql_native_projection import resolve_native_column_types


@dataclass(frozen=True, slots=True)
class PhysicalDesignLifecycleOutcome:
    payload: LoadPayload
    report: Mapping[str, Any] | None


class RuntimePhysicalDesignService:
    """Keep planner decisions pure until a target transaction owns MSSQL DDL."""

    def prepare(self, load_config: Any, sink: Any, payload: LoadPayload) -> PhysicalDesignLifecycleOutcome:
        frozen = (getattr(load_config, "options", {}) or {}).get("__dpone_mssql_schema_preplan")
        frozen_report = getattr(frozen, "physical_report", None)
        if _is_governed_mssql(payload, _sink_type(load_config)) and frozen_report is not None:
            return PhysicalDesignLifecycleOutcome(payload, frozen_report)
        planned = self._planned(load_config, payload)
        if planned is None:
            return PhysicalDesignLifecycleOutcome(payload, None)
        options, plan, table_exists = planned
        if not _is_governed_mssql(payload, plan.sink_type):
            return PhysicalDesignLifecycleOutcome(
                payload,
                self._apply(load_config, sink, options, plan, table_exists),
            )
        report, statements, expectation = self._defer_mssql(load_config, sink, options, plan, table_exists)
        if not statements:
            return PhysicalDesignLifecycleOutcome(payload, report)
        admission = payload.mssql_transaction_admission
        if not isinstance(admission, MssqlTransactionAdmission):
            raise RuntimeError("mssql_transaction.physical_plan_admission_required")
        mutation_plan = append_target_mutations(
            payload.mssql_target_mutation_plan,
            admission,
            kind="physical_design",
            statements=list(statements),
            expectation=expectation,
        )
        return PhysicalDesignLifecycleOutcome(
            payload.rebind(mssql_target_mutation_plan=mutation_plan),
            report,
        )

    def apply_legacy(self, load_config: Any, sink: Any, payload: LoadPayload) -> Mapping[str, Any] | None:
        """Compatibility adapter for direct non-governed lifecycle tests."""

        planned = self._planned(load_config, payload)
        if planned is None:
            return None
        options, plan, table_exists = planned
        return self._apply(load_config, sink, options, plan, table_exists)

    def _planned(self, load_config: Any, payload: LoadPayload) -> tuple[Any, Any, bool] | None:
        options = _options(load_config)
        if "physical_design" not in options:
            return None
        raw = options.get("physical_design")
        if not isinstance(raw, Mapping):
            raise ValueError("physical_design must be an object")
        apply_runtime = raw.get("apply_runtime", True)
        if not isinstance(apply_runtime, bool):
            raise ValueError("physical_design.apply_runtime must be boolean")
        if not apply_runtime:
            return None
        physical = PhysicalDesignOptions.from_config(raw)
        sink_type = _sink_type(load_config)
        table = (
            MSSQLObjectName.from_parts(
                database=getattr(load_config, "target_database", None),
                schema=load_config.target_schema,
                table=load_config.target_table,
                strict=True,
            ).dataset
            if _is_mssql(sink_type)
            else f"{load_config.target_schema}.{load_config.target_table}"
        )
        plan = PhysicalDesignPlanner().plan(
            sink_type=sink_type,
            table=table,
            source_schema=(
                payload.target_projection.target_schema if payload.target_projection is not None else payload.schema
            ),
            schema_contract=SchemaContract.from_config(options.get("schema_contract", {})),
            options=physical,
            type_fidelity=_mapping(options.get("type_fidelity")),
            resolved_columns=_resolved_target_columns(payload, load_config),
        )
        return physical, plan, False

    def _apply(
        self,
        load_config: Any,
        sink: Any,
        options: PhysicalDesignOptions,
        plan: Any,
        _table_exists: bool,
    ) -> Mapping[str, Any]:
        table_exists = bool(sink.target_table_exists(load_config)) if hasattr(sink, "target_table_exists") else False
        executor = _SinkDdlExecutor(sink) if _supports_ddl_execution(sink) else None
        if not options.active:
            return PhysicalDdlApplyService(executor=executor).apply(plan, table_exists=table_exists).to_dict()
        if table_exists and hasattr(sink, "inspect_physical_design"):
            reconciliation, _actual = _reconciliation(load_config, sink, plan, options)
            return PhysicalReconciliationApplyService(executor=executor).apply(reconciliation).to_dict()
        if not table_exists and _is_mssql(plan.sink_type):
            return _delegated_create_report(plan, options)
        if executor is None and table_exists:
            return _blocked_report(plan, "physical_ddl.executor_missing")
        return PhysicalDdlApplyService(executor=executor).apply(plan, table_exists=table_exists).to_dict()

    def _defer_mssql(
        self,
        load_config: Any,
        sink: Any,
        options: PhysicalDesignOptions,
        plan: Any,
        _table_exists: bool,
    ) -> tuple[
        Mapping[str, Any],
        tuple[str, ...],
        MssqlTargetCatalogExpectation | None,
    ]:
        table_exists = bool(sink.target_table_exists(load_config)) if hasattr(sink, "target_table_exists") else False
        if not options.active:
            report = PhysicalDdlApplyService().apply(plan, table_exists=table_exists).to_dict()
            return report, (), None
        if not table_exists:
            return _delegated_create_report(plan, options), (), None
        if not hasattr(sink, "inspect_physical_design"):
            return _blocked_report(plan, "physical_design.existing_target_introspection_required"), (), None
        reconciliation, actual = _reconciliation(load_config, sink, plan, options)
        report = reconciliation.to_dict()
        blockers = list(reconciliation.blockers)
        if options.apply == "plan_only":
            blockers.append("physical_design.plan_only")
        elif options.apply == "manual_approval":
            blockers.append("physical_design.manual_approval_required")
        report.update(
            {
                "applied": False,
                "blockers": blockers,
                "deferred_to_target_transaction": bool(reconciliation.ddl and not blockers),
                "executed": [],
            }
        )
        expectation = (
            physical_catalog_expectation(
                actual,
                expected_after=PhysicalTableState.from_physical_plan(plan),
            )
            if reconciliation.ddl and not blockers
            else None
        )
        return report, tuple(reconciliation.ddl) if not blockers else (), expectation


def _reconciliation(
    load_config: Any,
    sink: Any,
    plan: Any,
    options: PhysicalDesignOptions,
) -> tuple[Any, PhysicalTableState]:
    actual = sink.inspect_physical_design(load_config)
    if not isinstance(actual, PhysicalTableState):
        actual = PhysicalTableState.from_mapping(actual if isinstance(actual, Mapping) else {})
    reconciliation = PhysicalDesignReconciler().reconcile(
        desired=PhysicalTableState.from_physical_plan(plan),
        actual=actual,
        options=options.reconciliation,
        dialect=_migration_dialect(plan.sink_type),
        execution_apply_mode=options.apply,
    )
    return reconciliation, actual


def _delegated_create_report(plan: Any, options: PhysicalDesignOptions) -> Mapping[str, Any]:
    return {
        "applied": False,
        "delegated_to_strategy_target_creation": True,
        "table": plan.table,
        "sink_type": plan.sink_type,
        "apply_mode": options.apply,
        "executed": [],
        "blockers": [],
        "warnings": list(plan.risks),
    }


def _blocked_report(plan: Any, blocker: str) -> Mapping[str, Any]:
    return {
        "applied": False,
        "table": plan.table,
        "sink_type": plan.sink_type,
        "blockers": [blocker],
    }


class _SinkDdlExecutor:
    def __init__(self, sink: Any) -> None:
        self._sink = sink

    def execute(self, request: DdlExecutionRequest) -> None:
        if hasattr(self._sink, "execute_ddl"):
            self._sink.execute_ddl(request)
            return
        self._sink.apply_physical_ddl(request)


def _resolved_target_columns(payload: LoadPayload, load_config: Any) -> Mapping[str, ResolvedTargetColumn] | None:
    projection = payload.target_projection
    if projection is None:
        return None
    resolved = {
        column.name: ResolvedTargetColumn(
            name=column.name,
            logical_type=column.source_type,
            target_type=column.target_type,
            nullable=column.nullable,
            decision_source=column.explicit_contract_source or "source_metadata",
            reason="canonical PostgreSQL→MSSQL target projection",
            decision_category=(
                "explicit_physical_override"
                if column.target_type != column.source_native_mssql_type
                else "explicit_logical_contract"
                if column.explicit_contract_source is not None
                else "auto_inferred"
            ),
        )
        for column in projection.columns
    }
    technical = tuple((name, dtype) for name, dtype in payload.schema if str(name).lower().startswith("__dpone__"))
    if not technical:
        return resolved
    types = resolve_native_column_types(load_config, technical)
    definitions = {item.name.casefold(): item for item in TechnicalColumnCatalog().definitions()}
    for name, logical_type in technical:
        definition = definitions.get(str(name).casefold())
        nullable = definition.nullable if definition is not None else True
        if str(name).casefold() == "__dpone__row_hash":
            nullable = False
        resolved[str(name)] = ResolvedTargetColumn(
            name=str(name),
            logical_type=str(logical_type),
            target_type=types[str(name)],
            nullable=nullable,
            decision_source="technical_column_catalog",
            reason="canonical dpone strategy metadata column",
            decision_category="framework_generated",
        )
    return resolved


def _supports_ddl_execution(sink: Any) -> bool:
    return hasattr(sink, "execute_ddl") or hasattr(sink, "apply_physical_ddl")


def _migration_dialect(sink_type: str) -> TargetPhysicalMigrationDialect:
    normalized = str(sink_type).lower()
    if normalized == "clickhouse":
        return import_module(
            "dpone.runtime.sinks.clickhouse_physical_reconciliation"
        ).ClickHousePhysicalMigrationDialect()
    if _is_mssql(normalized):
        return import_module("dpone.runtime.sinks.mssql_physical_introspection").MssqlPhysicalMigrationDialect()
    raise RuntimeError(f"physical reconciliation is not implemented for sink_type={sink_type}")


def _is_governed_mssql(payload: LoadPayload, sink_type: str) -> bool:
    return _is_mssql(sink_type) and isinstance(payload.mssql_transaction_admission, MssqlTransactionAdmission)


def _is_mssql(value: str) -> bool:
    return str(value).lower() in {"mssql", "sqlserver", "sql_server"}


def _sink_type(load_config: Any) -> str:
    options = _options(load_config)
    return str(options.get("sink_type") or options.get("target_type") or "unknown")


def _options(load_config: Any) -> Mapping[str, Any]:
    return _mapping(getattr(load_config, "options", {}) or {})


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = ["PhysicalDesignLifecycleOutcome", "RuntimePhysicalDesignService"]
