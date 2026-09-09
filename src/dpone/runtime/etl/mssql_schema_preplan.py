"""Catalog-first immutable schema plan for governed PostgreSQL→MSSQL loads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.config.mssql_strategy_contract import normalize_mssql_load_strategy
from dpone.readiness.ddl_governance import OnlineSchemaPlanner
from dpone.readiness.mssql_technical_type_compatibility import (
    bind_mssql_fixed_technical_target_types,
)
from dpone.readiness.schema_evolution import (
    ColumnDef,
    SchemaComparator,
    SchemaComparisonError,
)
from dpone.readiness.schema_type_compatibility import MssqlSchemaTypeCompatibility
from dpone.runtime.etl.mssql_fresh_target_preplan import (
    plan_fresh_mssql_target,
    resolve_mssql_target_columns,
)
from dpone.runtime.etl.mssql_physical_preplan import plan_existing_mssql_physical_design
from dpone.runtime.etl.mssql_schema_preplan_support import (
    exact_target as _exact_target,
)
from dpone.runtime.etl.mssql_schema_preplan_support import (
    governance_policy as _governance_policy,
)
from dpone.runtime.etl.mssql_schema_preplan_support import (
    reject_observed_reserved_columns as _reject_observed_reserved_columns,
)
from dpone.runtime.etl.mssql_schema_preplan_support import (
    require_missing_target_authority as _require_missing_target_authority,
)
from dpone.runtime.etl.mssql_schema_preplan_support import (
    schema_columns_sha256,
)
from dpone.runtime.etl.mssql_schema_preplan_support import (
    schema_policy as _schema_policy,
)
from dpone.runtime.etl.mssql_schema_preplan_support import (
    source_columns as _source_columns,
)
from dpone.runtime.etl.mssql_schema_preplan_support import (
    source_projection as _source_projection,
)
from dpone.runtime.etl.mssql_schema_preplan_support import (
    target_row_count as _target_row_count,
)
from dpone.runtime.etl.portable_scope_preflight import (
    require_current_portable_scope_binding,
)
from dpone.runtime.schema_evolution_options import (
    SchemaEvolutionRuntimeOptions,
    blocked_schema_evolution,
)
from dpone.runtime.schema_evolution_payload import unique_keys
from dpone.runtime.sinks.mssql_existing_target_contract import (
    require_existing_mssql_target_contract,
)
from dpone.runtime.sinks.mssql_preplanned_target_types import (
    MSSQL_SCHEMA_PREPLAN_OPTION,
)
from dpone.runtime.sinks.mssql_target_behavior_policy import (
    require_ordinary_mssql_target_behavior,
)
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import (
    exact_schema_catalog_transition,
)
from dpone.runtime.sinks.mssql_target_catalog_reader import read_schema_catalog_snapshot
from dpone.runtime.sinks.mssql_target_mutation_plan import (
    MssqlTargetMutationPlan,
    append_target_mutations,
)


@dataclass(frozen=True, slots=True)
class MssqlSchemaPreplan:
    """Source/target catalog decision frozen before any source row I/O."""

    source_schema_sha256: bytes
    target_mutation_plan: MssqlTargetMutationPlan
    target_column_types: tuple[tuple[str, str], ...] = ()
    column_mapping: tuple[tuple[str, str], ...] = ()
    retained_catalog: tuple[ColumnDef, ...] = ()
    physical_report: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.source_schema_sha256, bytes) or len(self.source_schema_sha256) != 32:
            raise ValueError("mssql_transaction.schema_preplan_source_digest_invalid")


class MssqlSchemaPreplanner:
    """Compute every schema policy decision from catalogs before COPY/export."""

    def plan(
        self,
        load_config: Any,
        *,
        source: Any,
        sink: Any,
        admission: Any,
    ) -> MssqlSchemaPreplan:
        options = SchemaEvolutionRuntimeOptions.from_load_options(getattr(load_config, "options", {}) or {})
        source_projection = _source_projection(source, load_config)
        source_columns = _source_columns(source_projection)
        _reject_observed_reserved_columns(source_columns)
        source_digest = schema_columns_sha256(source_columns)
        comparison_columns = list(resolve_mssql_target_columns(load_config, source_columns))
        mutation_plan = MssqlTargetMutationPlan.from_admission(admission)
        try:
            target_snapshot = read_schema_catalog_snapshot(sink, load_config)
        except Exception as exc:
            raise blocked_schema_evolution(
                "schema_evolution.target_catalog_unavailable: exact target metadata could not be verified"
            ) from exc
        require_current_portable_scope_binding(
            load_config,
            source_projection=source_projection,
            target_snapshot=target_snapshot,
        )
        target_columns = target_snapshot.column_defs
        target_exists = target_snapshot.exists
        comparison_columns = bind_mssql_fixed_technical_target_types(
            comparison_columns,
            tuple(target_columns),
        )
        require_ordinary_mssql_target_behavior(
            target_snapshot,
            strategy=normalize_mssql_load_strategy(load_config),
        )
        expectation = exact_schema_catalog_transition(target_snapshot, target_snapshot)
        if not target_exists:
            if not options.enabled:
                raise blocked_schema_evolution("schema_evolution.disabled:create_table")
            _require_missing_target_authority(options, load_config, sink)
            fresh = plan_fresh_mssql_target(
                load_config,
                source_columns=source_columns,
                before=target_snapshot,
                admission=admission,
                mutation_plan=mutation_plan,
                qualified_target=_exact_target(load_config),
            )
            return MssqlSchemaPreplan(
                source_digest,
                fresh.mutation_plan,
                tuple((column.name, column.dtype) for column in fresh.columns),
                physical_report={
                    "applied": False,
                    "deferred_to_target_transaction": True,
                    "preplanned": True,
                    "blockers": [],
                    "executed": [],
                },
            )
        if not options.enabled:
            try:
                disabled_plan = SchemaComparator(
                    _schema_policy(
                        options,
                        protected_columns=unique_keys(load_config),
                        allow_framework_columns=True,
                    ),
                    type_compatibility=MssqlSchemaTypeCompatibility(),
                ).compare(comparison_columns, target_columns)
            except SchemaComparisonError as exc:
                raise blocked_schema_evolution(exc.blocker) from exc
            if disabled_plan.changes:
                kinds = ",".join(dict.fromkeys(change.change_type for change in disabled_plan.changes))
                raise blocked_schema_evolution(f"schema_evolution.disabled:{kinds}")
            require_existing_mssql_target_contract(
                load_config,
                comparison_columns,
                target_snapshot,
                qualified_target=_exact_target(load_config),
            )
            mutation_plan = append_target_mutations(
                mutation_plan,
                admission,
                kind="schema_evolution",
                statements=(),
                expectation=expectation,
            )
            return _with_physical_design(
                load_config,
                sink=sink,
                admission=admission,
                source_digest=source_digest,
                mutation_plan=mutation_plan,
                final_columns=tuple(target_columns),
            )
        if not target_columns:
            raise blocked_schema_evolution(
                "schema_evolution.target_catalog_unavailable: exact target metadata could not be verified"
            )
        policy = _schema_policy(
            options,
            protected_columns=unique_keys(load_config),
            allow_framework_columns=True,
        )
        try:
            schema_plan = SchemaComparator(
                policy,
                type_compatibility=MssqlSchemaTypeCompatibility(),
            ).compare(comparison_columns, target_columns)
        except SchemaComparisonError as exc:
            raise blocked_schema_evolution(exc.blocker) from exc
        if not schema_plan.changes:
            require_existing_mssql_target_contract(
                load_config,
                comparison_columns,
                target_snapshot,
                qualified_target=_exact_target(load_config),
            )
            mutation_plan = append_target_mutations(
                mutation_plan,
                admission,
                kind="schema_evolution",
                statements=(),
                expectation=expectation,
            )
            return _with_physical_design(
                load_config,
                sink=sink,
                admission=admission,
                source_digest=source_digest,
                mutation_plan=mutation_plan,
                final_columns=tuple(target_columns),
            )
        governed = OnlineSchemaPlanner().plan(
            schema_plan=schema_plan,
            dialect="mssql",
            table=_exact_target(load_config),
            policy=_governance_policy(options),
            table_row_count=_target_row_count(options, sink, load_config),
        )
        if schema_plan.has_breaking_changes and options.on_breaking == "fail":
            raise blocked_schema_evolution(f"breaking schema evolution changes detected: {schema_plan.to_dict()}")
        if governed.blockers:
            raise blocked_schema_evolution(f"online schema evolution blockers: {governed.blockers}")
        if schema_plan.safe_changes and (
            not options.apply_safe or options.ddl_mode in {"plan_only", "manual_approval"}
        ):
            blocker = (
                "schema_evolution.apply_safe" if not options.apply_safe else f"schema_evolution.{options.ddl_mode}"
            )
            raise blocked_schema_evolution(f"{blocker}: safe DDL is not authorized")
        statements = tuple(
            statement for action in governed.actions if action.decision == "apply" for statement in action.ddl
        )
        expected_after = target_snapshot.apply_schema_plan(schema_plan) if statements else target_snapshot
        expectation = exact_schema_catalog_transition(target_snapshot, expected_after)
        mutation_plan = append_target_mutations(
            mutation_plan,
            admission,
            kind="schema_evolution",
            statements=statements,
            expectation=expectation,
        )
        return _with_physical_design(
            load_config,
            sink=sink,
            admission=admission,
            source_digest=source_digest,
            mutation_plan=mutation_plan,
            final_columns=tuple(expected_after.column_defs),
            column_mapping=tuple(schema_plan.column_mapping.items()),
            retained_catalog=tuple(target_columns),
        )


def _with_physical_design(
    load_config: Any,
    *,
    sink: Any,
    admission: Any,
    source_digest: bytes,
    mutation_plan: MssqlTargetMutationPlan,
    final_columns: tuple[ColumnDef, ...],
    column_mapping: tuple[tuple[str, str], ...] = (),
    retained_catalog: tuple[ColumnDef, ...] = (),
) -> MssqlSchemaPreplan:
    physical = plan_existing_mssql_physical_design(
        load_config,
        sink=sink,
        admission=admission,
        mutation_plan=mutation_plan,
        final_columns=final_columns,
    )
    return MssqlSchemaPreplan(
        source_digest,
        physical.mutation_plan,
        tuple((column.name, column.dtype) for column in final_columns),
        column_mapping,
        retained_catalog,
        dict(physical.report) if physical.report is not None else None,
    )


__all__ = [
    "MSSQL_SCHEMA_PREPLAN_OPTION",
    "MssqlSchemaPreplan",
    "MssqlSchemaPreplanner",
    "schema_columns_sha256",
]
