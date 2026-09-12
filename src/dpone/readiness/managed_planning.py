"""Dry-run execution planning service for manifests."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.config.mssql_strategy_contract import normalize_mssql_authoring_strategy
from dpone.config.postgres_mssql_wire_contract import normalize_postgres_mssql_wire
from dpone.contracts.postgres_mssql_type_policy import declared_postgres_mssql_contract_blockers

if TYPE_CHECKING:
    from dpone.manifest.models import ProcessSpec

from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness import managed_native_transfer_plan as native_transfer_plan
from dpone.readiness import managed_planning_r1 as r1_planning
from dpone.readiness import managed_planning_snapshot as snapshot_planning
from dpone.readiness.managed_bulk_path import managed_bulk_path
from dpone.readiness.managed_native_projection import (
    native_transfer_bulk_wire,
    native_transfer_route_decision,
    native_transfer_snapshot_optimization,
    native_transfer_transport,
)
from dpone.readiness.managed_plan_warnings import plan_warnings
from dpone.readiness.managed_source_impact import source_impact
from dpone.readiness.managed_utils import (
    _configured_columns,
    _list_option,
    _qualified_table,
    _redact,
    _schema_contract,
    _source_columns,
    _table,
)
from dpone.readiness.mssql_native_planning import project_mssql_native
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.postgres_mssql_correctness_route import (
    PostgresMssqlCorrectnessRouteResolver,
    postgres_mssql_correctness_plan,
)
from dpone.readiness.resolved_process_route import resolve_process_route
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.partitioning_options import PartitioningOptionsResolver
from dpone.runtime.storage_policy import RuntimeStoragePolicy
from dpone.services.manifest import resolve_single_process
from dpone.services.schema_type_matrix import PairTypeMatrixService
from dpone.strategy_intelligence.native_transfer import NativeTransferPlanBuilder, NativeTransferRequest
from dpone.type_system import TypeInferenceOptions, TypeInferenceService


class ExecutionPlanService:
    """Builds dry-run execution plans from manifests without touching targets."""

    def __init__(
        self,
        *,
        postgres_mssql_correctness: PostgresMssqlCorrectnessRouteResolver | None = None,
    ) -> None:
        self._postgres_mssql_correctness = postgres_mssql_correctness

    def plan_manifest(
        self,
        path: str | Path,
        *,
        selector: str | None = None,
        apply_safe_schema: bool = False,
        explain_strategy: bool = False,
        route_certification_mode_override: str | None = None,
        manifest_dir_override: Path | None = None,
    ) -> dict[str, Any]:
        manifest_path = Path(path)
        manifest = ManifestLoaderRouter().load(manifest_path, metadata_only=True)
        spec = resolve_single_process(manifest, selector=selector)
        return self.plan_process(
            spec,
            apply_safe_schema=apply_safe_schema,
            explain_strategy=explain_strategy,
            route_certification_mode_override=route_certification_mode_override,
            manifest_dir=manifest_dir_override or manifest_path.parent,
        )

    def plan_process(
        self,
        spec: ProcessSpec,
        *,
        apply_safe_schema: bool = False,
        explain_strategy: bool = False,
        route_certification_mode_override: str | None = None,
        manifest_dir: Path,
    ) -> dict[str, Any]:
        """Plan one already-resolved process without recompiling its source."""

        raw = dict(spec.raw_config)
        lc = spec.config.load_config
        route = resolve_process_route(spec)
        source_type = route.source
        sink_type = route.sink
        mssql_strategy_contract = normalize_mssql_authoring_strategy(lc).to_dict() if sink_type == "mssql" else None
        postgres_mssql_wire = (
            normalize_postgres_mssql_wire(lc).to_dict() if source_type == "postgres" and sink_type == "mssql" else None
        )
        postgres_mssql_correctness = postgres_mssql_correctness_plan(
            spec,
            self._postgres_mssql_correctness,
            source_type=source_type,
            sink_type=sink_type,
        )
        typed_snapshot_route = snapshot_planning.is_postgres_xmin_key_snapshot_mssql(
            raw,
            source_type,
            sink_type,
        )
        schema_evolution = self._schema_evolution(raw, apply_safe_schema=apply_safe_schema)
        physical_design = self._physical_design(raw, sink_type)
        if typed_snapshot_route:
            schema_evolution = snapshot_planning.external_schema_evolution_plan(schema_evolution)
            physical_design = snapshot_planning.external_physical_design_plan(
                physical_design,
                unique_key=tuple(_list_option(lc.unique_key)),
            )
        plan = {
            "process": spec.name,
            "selector": spec.selector,
            "source": {
                "type": source_type,
                "connection_id": lc.source_conn_id,
                "table": _table(lc.source_schema, lc.source_table),
                "columns": _configured_columns(raw),
            },
            "sink": {
                "type": sink_type,
                "connection_id": lc.target_conn_id,
                "table": _table(lc.target_schema, lc.target_table),
            },
            "strategy": {
                "mode": route.strategy,
                "unique_key": lc.unique_key,
                "merge_policy": lc.merge_policy,
                "mssql_contract": mssql_strategy_contract,
            },
            "bulk_path": managed_bulk_path(raw, source_type, sink_type, lc.export_format),
            "postgres_mssql_wire": postgres_mssql_wire,
            "postgres_mssql_correctness": postgres_mssql_correctness,
            "columnar_fast_path": native_transfer_plan.columnar_fast_path_plan(
                raw,
                source_type=source_type,
                sink_type=sink_type,
            ),
            "staging": snapshot_planning.staging_plan(lc, sink_type),
            "schema_evolution": schema_evolution,
            "type_fidelity": self._type_fidelity(raw, source_type, sink_type),
            "type_matrix": self._type_matrix(raw, source_type, sink_type),
            "type_inference": self._type_inference(raw),
            "physical_design": physical_design,
            "reconciliation": snapshot_planning.reconciliation_plan(lc),
            "state": snapshot_planning.state_plan(raw, sink_type),
            "partitioning": self._partitioning(lc.options),
            "runtime_storage": self._runtime_storage(raw, lc.options),
            "native_transfer_execution": native_transfer_plan.native_transfer_execution(raw),
            "native_transfer_transport": native_transfer_transport(raw, source_type, sink_type),
            "native_transfer_bulk_wire": native_transfer_bulk_wire(raw, source_type, sink_type),
            "native_transfer_snapshot_optimization": native_transfer_snapshot_optimization(
                raw,
                source_type,
                sink_type,
            ),
            "native_transfer_route_decision": native_transfer_route_decision(
                raw,
                source_type,
                sink_type,
                manifest_dir=manifest_dir,
                certification_mode_override=route_certification_mode_override,
            ),
            "source_impact": self._source_impact(raw, lc.options, source_type),
            "quality": raw.get("quality", {"gates": []}),
            "estimated_rows": lc.options.get("estimated_rows"),
            "dry_run": True,
        }
        plan["warnings"] = plan_warnings(plan)
        if explain_strategy:
            plan["strategy_intelligence"] = lc.options.get("strategy_intelligence", {})
        plan = r1_planning.cohere_selected_r1_plan(
            plan,
            correctness=postgres_mssql_correctness,
            unique_key=tuple(_list_option(lc.unique_key)),
        )
        project_mssql_native(plan, lc)
        return _redact(plan)

    def _schema_evolution(self, raw: Mapping[str, Any], *, apply_safe_schema: bool) -> dict[str, Any]:
        sink_options = raw.get("sink", {}).get("options", {}) if isinstance(raw.get("sink"), Mapping) else {}
        configured = sink_options.get("schema_evolution", {}) if isinstance(sink_options, Mapping) else {}
        return {
            "enabled": bool(configured.get("enabled", True)) if isinstance(configured, Mapping) else True,
            "mode": configured.get("mode", "widening") if isinstance(configured, Mapping) else "widening",
            "apply_safe": bool(apply_safe_schema and configured.get("apply_safe", True))
            if isinstance(configured, Mapping)
            else bool(apply_safe_schema),
            "ddl_preview": [],
            "on_type_change": configured.get("on_type_change", "fail") if isinstance(configured, Mapping) else "fail",
        }

    def _type_inference(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        sink_options = raw.get("sink", {}).get("options", {}) if isinstance(raw.get("sink"), Mapping) else {}
        source_options = raw.get("source", {}).get("options", {}) if isinstance(raw.get("source"), Mapping) else {}
        source_schema = _source_columns(source_options if isinstance(source_options, Mapping) else {})
        contract = SchemaContract.from_config(
            _schema_contract(raw, sink_options if isinstance(sink_options, Mapping) else {})
        )
        options = TypeInferenceOptions.from_config(
            sink_options.get("type_inference", {}) if isinstance(sink_options, Mapping) else {}
        )
        return (
            TypeInferenceService()
            .infer(source_schema=source_schema, schema_contract=contract, options=options)
            .to_dict()
        )

    def _type_matrix(self, raw: Mapping[str, Any], source_type: str, sink_type: str) -> dict[str, Any]:
        service = PairTypeMatrixService()
        available = (source_type, sink_type) in service.available_pairs()
        summary: dict[str, Any] = {
            "available": available,
            "source": source_type,
            "sink": sink_type,
            "explain_command": (f"dpone schema type-matrix --source {source_type} --sink {sink_type} --format md"),
        }
        if not available:
            return summary
        source = raw.get("source", {}) if isinstance(raw.get("source"), Mapping) else {}
        source_options = source.get("options", {}) if isinstance(source, Mapping) else {}
        source_schema = _source_columns(source_options if isinstance(source_options, Mapping) else {})
        source_specs = tuple(f"{name}:{dtype}" for name, dtype in source_schema) or None
        matrix = service.build(source=source_type, sink=sink_type, source_types=source_specs)
        entries = [dict(entry) for entry in matrix.get("entries", [])]
        blockers: tuple[str, ...] = ()
        if source_type == "postgres" and sink_type == "mssql" and source_schema:
            sink = raw.get("sink", {}) if isinstance(raw.get("sink"), Mapping) else {}
            sink_options = sink.get("options", {}) if isinstance(sink, Mapping) else {}
            projection_options = dict(sink_options) if isinstance(sink_options, Mapping) else {}
            projection_options["schema_contract"] = _schema_contract(raw, projection_options)
            blockers = declared_postgres_mssql_contract_blockers(source_schema, projection_options)
            blockers_by_column = {blocker.rsplit(":", 1)[-1]: blocker for blocker in blockers}
            for entry in entries:
                column = str(entry.get("column") or "")
                blocker = blockers_by_column.get(column)
                requires = bool(entry.get("requires_explicit_contract"))
                entry["explicit_contract_satisfied"] = not requires or blocker is None
                entry["explicit_contract_source"] = "schema_contract:string" if requires and blocker is None else None
                entry["blockers"] = [blocker] if blocker else []
        summary.update(
            {
                "profile": matrix.get("profile"),
                "runbook": matrix.get("runbook"),
                "entries_preview": entries[:5] if source_specs else [],
                "blockers": list(blockers),
                "ready": not blockers,
            }
        )
        return summary

    def _type_fidelity(self, raw: Mapping[str, Any], source_type: str, sink_type: str) -> dict[str, Any]:
        source = raw.get("source", {}) if isinstance(raw.get("source"), Mapping) else {}
        sink = raw.get("sink", {}) if isinstance(raw.get("sink"), Mapping) else {}
        source_options = source.get("options", {}) if isinstance(source, Mapping) else {}
        sink_options = sink.get("options", {}) if isinstance(sink, Mapping) else {}
        strategy = sink.get("strategy", {}) if isinstance(sink, Mapping) else {}
        return (
            NativeTransferPlanBuilder()
            .build(
                NativeTransferRequest(
                    source_type=source_type,
                    sink_type=sink_type,
                    source_table=_qualified_table(source.get("table", {}) if isinstance(source, Mapping) else {}),
                    target_table=_qualified_table(sink.get("table", {}) if isinstance(sink, Mapping) else {}),
                    strategy=str(strategy.get("mode", "full_refresh"))
                    if isinstance(strategy, Mapping)
                    else "full_refresh",
                    unique_key=tuple(_list_option(strategy.get("unique_key"))) if isinstance(strategy, Mapping) else (),
                    source_options=dict(source_options) if isinstance(source_options, Mapping) else {},
                    sink_options=dict(sink_options) if isinstance(sink_options, Mapping) else {},
                )
            )
            .type_fidelity
        )

    def _physical_design(self, raw: Mapping[str, Any], sink_type: str) -> dict[str, Any]:
        sink = raw.get("sink", {}) if isinstance(raw.get("sink"), Mapping) else {}
        sink_options = sink.get("options", {}) if isinstance(sink, Mapping) else {}
        source = raw.get("source", {}) if isinstance(raw.get("source"), Mapping) else {}
        source_options = source.get("options", {}) if isinstance(source, Mapping) else {}
        source_schema = _source_columns(source_options if isinstance(source_options, Mapping) else {})
        contract = SchemaContract.from_config(
            _schema_contract(raw, sink_options if isinstance(sink_options, Mapping) else {})
        )
        table = sink.get("table", {}) if isinstance(sink, Mapping) else {}
        qualified = _qualified_table(table if isinstance(table, Mapping) else {})
        configured = sink_options.get("physical_design", {}) if isinstance(sink_options, Mapping) else {}
        plan = (
            PhysicalDesignPlanner()
            .plan(
                sink_type=sink_type,
                table=qualified,
                source_schema=source_schema,
                schema_contract=contract,
                options=PhysicalDesignOptions.from_config(configured),
            )
            .to_dict()
        )
        apply_runtime = bool(configured.get("apply_runtime", False)) if isinstance(configured, Mapping) else False
        plan["apply_runtime"] = apply_runtime
        plan["runtime_ddl"] = "enabled" if apply_runtime else "disabled"
        return plan

    def _partitioning(self, options: Mapping[str, Any]) -> dict[str, Any]:
        resolved = PartitioningOptionsResolver.resolve(options)
        bounds = resolved.bounds
        return {
            "enabled": bool(resolved.column),
            "strategy": resolved.strategy,
            "partition_column": resolved.column,
            "bounds": bounds,
            "lower_bound": (bounds or {}).get("lower") if isinstance(bounds, Mapping) else resolved.lower_bound,
            "upper_bound": (bounds or {}).get("upper") if isinstance(bounds, Mapping) else resolved.upper_bound,
            "num_partitions": resolved.num_partitions,
            "max_partitions": resolved.max_partitions,
            "target_rows_per_partition": resolved.target_rows_per_partition,
            "export_workers": resolved.export_workers,
            "load_workers": resolved.load_workers,
            "warnings": list(resolved.warnings),
        }

    def _runtime_storage(self, raw: Mapping[str, Any], options: Mapping[str, Any]) -> dict[str, Any]:
        runtime = raw.get("runtime", {}) if isinstance(raw.get("runtime"), Mapping) else {}
        policy = RuntimeStoragePolicy.from_sources(
            runtime=runtime,
            source_options=options,
            env={},
        )
        return policy.to_dict()

    def _source_impact(
        self, raw: Mapping[str, Any], options: Mapping[str, Any], source_type: str
    ) -> list[dict[str, Any]]:
        source = raw.get("source", {}) if isinstance(raw.get("source"), Mapping) else {}
        source_options = source.get("options", {}) if isinstance(source.get("options"), Mapping) else {}
        table = source.get("table", {}) if isinstance(source.get("table"), Mapping) else {}
        columns = _configured_columns(raw)
        column_sql = ", ".join(columns) if columns else "*"
        return source_impact(
            source_type=source_type,
            base_query=str(source_options.get("query") or f"SELECT {column_sql} FROM {_qualified_table(table)}"),
            partition_column=PartitioningOptionsResolver.resolve(options).column,
            indexed_columns=tuple(str(item) for item in source_options.get("indexed_columns", ()) or ()),
            source_kind=str(
                source_options.get("source_kind")
                or ("view" if str(table.get("name", "")).startswith("v_") else "table")
            ),
        )


__all__ = ["ExecutionPlanService"]
