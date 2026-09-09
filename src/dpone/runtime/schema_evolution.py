"""Runtime schema evolution orchestration.

This module keeps schema drift handling outside individual load strategies. Sinks
remain responsible for native loading, while this service plans safe DDL and
adapts source payload columns before the staging-first load starts.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.ddl_governance import (
    DdlGovernancePolicy,
    OnlineSchemaPlanner,
    blocker_code,
    decide_missing_table,
    decision_blocks_load,
)
from dpone.readiness.schema_evolution import (
    ColumnDef,
    SchemaComparator,
    SchemaComparisonError,
    SchemaEvolutionPolicy,
)
from dpone.readiness.schema_type_compatibility import MssqlSchemaTypeCompatibility
from dpone.runtime.etl.mssql_schema_preplan import (
    MSSQL_SCHEMA_PREPLAN_OPTION,
    MssqlSchemaPreplan,
    schema_columns_sha256,
)
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.schema_evolution_options import (
    SchemaEvolutionError,
    SchemaEvolutionRuntimeOptions,
    blocked_schema_evolution,
)
from dpone.runtime.schema_evolution_payload import (
    effective_source_columns,
    remap_rows,
    unique_keys,
)
from dpone.runtime.schema_evolution_payload import (
    source_columns as project_source_columns,
)
from dpone.runtime.schema_evolution_preflight import ProtectedColumnPreflight
from dpone.runtime.schema_evolution_runtime_support import (
    apply_schema_plan,
    record_schema_ledger,
    report_governance_plan,
    report_plan,
    target_exists,
    target_row_count,
    target_schema,
)
from dpone.runtime.schema_evolution_target import qualified_target, sink_dialect
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class SchemaEvolutionService:
    """Plans, applies, and projects schema drift for a Source -> Sink load."""

    def preflight_before_extract(
        self,
        *,
        load_config: Any,
        source: Any,
        sink: Any,
    ) -> None:
        """Reject key-to-generated-column routing from catalog metadata only."""

        preflight = ProtectedColumnPreflight()
        try:
            options = SchemaEvolutionRuntimeOptions.from_load_options(getattr(load_config, "options", {}) or {})
            protected_columns = unique_keys(load_config)
            applies = preflight.applies(
                source,
                sink_dialect=sink_dialect(sink),
                enabled=options.enabled,
                on_type_change=options.on_type_change,
                protected_columns=protected_columns,
            )
        except (TypeError, ValueError) as exc:
            raise SchemaEvolutionError(str(exc)) from exc
        if not applies:
            return
        target = target_schema(load_config, sink)
        policy = _schema_policy(options, protected_columns=protected_columns)
        try:
            blocker = preflight.blocker(
                load_config=load_config,
                source=source,
                target_columns=target,
                target_exists=target_exists(load_config, sink, target),
                protected_columns=protected_columns,
                column_factory=ColumnDef,
                compare=lambda source_columns, target_columns: SchemaComparator(
                    policy,
                    type_compatibility=MssqlSchemaTypeCompatibility(),
                ).compare(list(source_columns), list(target_columns)),
            )
        except SchemaComparisonError as exc:
            raise blocked_schema_evolution(exc.blocker) from exc
        if blocker:
            raise blocked_schema_evolution(blocker)

    def prepare_payload(
        self, load_config: Any, sink: Any, payload: LoadPayload, logger: Any | None = None
    ) -> LoadPayload:
        try:
            runtime_options = SchemaEvolutionRuntimeOptions.from_load_options(getattr(load_config, "options", {}) or {})
        except (TypeError, ValueError) as exc:
            raise SchemaEvolutionError(str(exc)) from exc
        frozen = (getattr(load_config, "options", {}) or {}).get(MSSQL_SCHEMA_PREPLAN_OPTION)
        if isinstance(frozen, MssqlSchemaPreplan):
            return self._apply_frozen_preplan(payload, frozen)
        if not runtime_options.enabled:
            return payload
        if sink_dialect(sink) == "mssql" and payload.mssql_transaction_admission is not None:
            raise blocked_schema_evolution("schema_evolution.catalog_preplan_required_before_source")

        source_columns = effective_source_columns(load_config, sink, payload)
        target_columns = target_schema(load_config, sink)
        if not target_exists(load_config, sink, target_columns):
            self._raise_on_reserved_source_columns(source_columns, runtime_options)
            self._raise_if_missing_table_is_blocked(runtime_options, load_config, sink)
            return payload
        if not target_columns:
            raise blocked_schema_evolution(
                "target table exists but its schema could not be verified; refusing to load an unverified payload"
            )

        compatibility = MssqlSchemaTypeCompatibility() if sink_dialect(sink) == "mssql" else None
        try:
            plan = SchemaComparator(
                _schema_policy(
                    runtime_options,
                    protected_columns=unique_keys(load_config),
                ),
                type_compatibility=compatibility,
            ).compare(source_columns, target_columns)
        except SchemaComparisonError as exc:
            raise blocked_schema_evolution(exc.blocker) from exc
        if not plan.changes:
            return payload

        report_plan(logger, plan)
        governed_plan = OnlineSchemaPlanner().plan(
            schema_plan=plan,
            dialect=sink_dialect(sink),
            table=qualified_target(load_config, sink),
            policy=_governance_policy(runtime_options),
            table_row_count=target_row_count(runtime_options, load_config, sink),
        )
        record_schema_ledger(runtime_options, load_config, sink, governed_plan)
        report_governance_plan(logger, governed_plan)
        if plan.has_breaking_changes and runtime_options.on_breaking == "fail":
            raise blocked_schema_evolution(f"breaking schema evolution changes detected: {plan.to_dict()}")
        if governed_plan.blockers:
            context = (
                "online schema evolution blockers"
                if runtime_options.ddl_mode == "online"
                else "schema evolution blockers"
            )
            raise blocked_schema_evolution(f"{context}: {governed_plan.blockers}")

        if plan.safe_changes:
            if not runtime_options.apply_safe or runtime_options.ddl_mode in {"plan_only", "manual_approval"}:
                blocker = (
                    "schema_evolution.apply_safe"
                    if not runtime_options.apply_safe
                    else f"schema_evolution.{runtime_options.ddl_mode}"
                )
                raise blocked_schema_evolution(
                    f"{blocker}: safe DDL was planned but not applied: "
                    f"{plan.ddl_sql(sink_dialect(sink), qualified_target(load_config, sink))}"
                )
            apply_schema_plan(load_config, sink, plan, governed_plan)

        target_projection = payload.target_projection
        if target_projection is not None:
            target_projection = target_projection.project_target_names(
                plan.column_mapping,
                retained_catalog=target_columns,
            )
            if target_projection == payload.target_projection:
                return payload
            return payload.rebind(target_projection=target_projection)
        mapped_schema = plan.mapped_schema(list(payload.schema))
        if mapped_schema == list(payload.schema):
            return payload
        return payload.rebind(
            artifact=self._remap_artifact(payload.artifact, plan.column_mapping),
            schema=mapped_schema,
        )

    def _raise_on_reserved_source_columns(
        self,
        source_columns: list[ColumnDef],
        runtime_options: SchemaEvolutionRuntimeOptions,
    ) -> None:
        try:
            plan = SchemaComparator(_schema_policy(runtime_options)).compare(
                source_columns,
                source_columns,
            )
        except SchemaComparisonError as exc:
            raise blocked_schema_evolution(exc.blocker) from exc
        if plan.has_breaking_changes:
            raise blocked_schema_evolution(f"breaking schema evolution changes detected: {plan.to_dict()}")

    def _apply_frozen_preplan(
        self,
        payload: LoadPayload,
        frozen: MssqlSchemaPreplan,
    ) -> LoadPayload:
        source_columns = project_source_columns(payload)
        if schema_columns_sha256(source_columns) != frozen.source_schema_sha256:
            raise blocked_schema_evolution("schema_evolution.source_catalog_changed_after_preflight")
        mapping = dict(frozen.column_mapping)
        target_projection = payload.target_projection
        if target_projection is not None and mapping:
            target_projection = target_projection.project_target_names(
                mapping,
                retained_catalog=frozen.retained_catalog,
            )
        changes: dict[str, Any] = {
            "mssql_target_mutation_plan": frozen.target_mutation_plan,
        }
        if target_projection is not None:
            changes["target_projection"] = target_projection
        elif mapping:
            changes["artifact"] = self._remap_artifact(payload.artifact, mapping)
            changes["schema"] = [(mapping.get(str(name), str(name)), dtype) for name, dtype in payload.schema]
        return payload.rebind(**changes)

    def _raise_if_missing_table_is_blocked(
        self,
        runtime_options: SchemaEvolutionRuntimeOptions,
        load_config: Any,
        sink: Any,
    ) -> None:
        policy = _governance_policy(runtime_options)
        decision = decide_missing_table(policy)
        if decision == "apply" and runtime_options.apply_safe:
            return
        qualified_table = qualified_target(load_config, sink)
        if decision_blocks_load(decision):
            blocker = blocker_code(
                change_type="create_table",
                column=qualified_table,
                risk_level="metadata_only",
                decision=decision,
                reason="target table is absent",
            )
        else:
            blocker = f"schema_evolution.apply_safe:create_table:{qualified_table}"
        raise blocked_schema_evolution(f"target table creation is not authorized by policy: {blocker}")

    def _remap_artifact(self, artifact: Any, column_mapping: Mapping[str, str]) -> Any:
        if isinstance(artifact, InMemoryRowsArtifact):
            return InMemoryRowsArtifact(remap_rows(artifact._rows, column_mapping))
        if isinstance(artifact, StreamingRowsArtifact):
            return artifact.rebind_iterator(remap_rows(artifact._iterator, column_mapping))
        rebind_columns = getattr(artifact, "rebind_columns", None)
        columns = getattr(artifact, "columns", None)
        if callable(rebind_columns) and isinstance(columns, Sequence):
            return rebind_columns([column_mapping.get(str(column), str(column)) for column in columns])
        return artifact


def _schema_policy(
    options: SchemaEvolutionRuntimeOptions,
    *,
    protected_columns: Sequence[str] = (),
) -> SchemaEvolutionPolicy:
    return SchemaEvolutionPolicy(
        mode=options.mode,
        on_type_change=options.on_type_change,
        new_column_prefix=options.new_column_prefix,
        allow_reserved_dpone_columns=options.allow_reserved_dpone_columns,
        protected_columns=tuple(str(column) for column in protected_columns),
    )


def _governance_policy(
    options: SchemaEvolutionRuntimeOptions,
) -> DdlGovernancePolicy:
    return DdlGovernancePolicy(
        tables=options.tables,
        columns=options.columns,
        data_type=options.data_type,
        ddl_mode=options.ddl_mode,
        lock_timeout_seconds=options.lock_timeout_seconds,
        statement_timeout_seconds=options.statement_timeout_seconds,
        max_table_size_for_inline_ddl=options.max_table_size_for_inline_ddl,
        on_schema_change=options.on_schema_change,
        ledger_path=options.ledger_path,
        allow_blocking_online=options.allow_blocking_online,
    )
