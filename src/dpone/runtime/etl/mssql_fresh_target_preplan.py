"""Exact absent→created MSSQL target plan resolved before source row I/O."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.contracts.mssql_physical_design import MssqlPhysicalDesignContract
from dpone.readiness.physical_state import PhysicalTableState
from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.sinks.mssql_filegroup_authority import resolve_mssql_physical_filegroups
from dpone.runtime.sinks.mssql_physical_introspection import expected_mssql_created_physical_state
from dpone.runtime.sinks.mssql_table_ddl import (
    MssqlTableDdlRenderer,
    render_load_strategy_create_statements,
)
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import (
    exact_schema_catalog_transition,
    physical_catalog_expectation,
)
from dpone.runtime.sinks.mssql_target_catalog_model import (
    MssqlIndexState,
    MssqlSchemaCatalogSnapshot,
    MssqlTableBehaviorState,
    catalog_column_from_definition,
)
from dpone.runtime.sinks.mssql_target_mutation_plan import (
    MssqlTargetMutationPlan,
    append_target_mutations,
)
from dpone.runtime.sinks.strategies.mssql.mssql_generic_target_contract import (
    render_unique_authority_ddl,
    resolve_unique_authority_contract,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import (
    resolve_mssql_native_lineage_columns,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import (
    mssql_equality_key_collations,
)
from dpone.runtime.sinks.strategies.mssql.mssql_strategy_metadata import (
    resolve_mssql_strategy_metadata_columns,
)
from dpone.runtime.support.mssql_snapshot_projection import (
    resolved_mssql_target_column_type,
)

if TYPE_CHECKING:
    from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission


@dataclass(frozen=True, slots=True)
class MssqlFreshTargetPreplan:
    """One exact target shape and the fenced mutations that create it."""

    columns: tuple[ColumnDef, ...]
    mutation_plan: MssqlTargetMutationPlan
    schema_after: MssqlSchemaCatalogSnapshot
    physical_after: PhysicalTableState


def plan_fresh_mssql_target(
    load_config: Any,
    *,
    source_columns: list[ColumnDef],
    before: MssqlSchemaCatalogSnapshot,
    admission: MssqlTransactionAdmission,
    mutation_plan: MssqlTargetMutationPlan,
    qualified_target: str,
) -> MssqlFreshTargetPreplan:
    """Render target creation and both exact schema/physical after-images."""

    if before.exists:
        raise ValueError("mssql_transaction.fresh_target_preplan_requires_absent_target")
    _require_fresh_physical_authority(load_config)
    columns = resolve_mssql_target_columns(load_config, source_columns)
    column_types = {column.name: column.dtype for column in columns}
    contract = resolve_mssql_physical_filegroups(
        MssqlPhysicalDesignContract.from_options(getattr(load_config, "options", {}) or {}),
        available_row_filegroups=before.available_row_filegroups,
    )
    contract.validate_columns(column_types)
    by_name = {column.name.casefold(): column for column in columns}
    if any(by_name[key.casefold()].nullable for key in contract.primary_key):
        raise RuntimeError("mssql_native_projection.physical_primary_key_nullable")
    columns = _apply_primary_key_nullability(columns, contract)
    nullability = {column.name: column.nullable for column in columns}
    collations = {column.name: column.collation for column in columns if column.collation is not None}
    statements = list(
        render_load_strategy_create_statements(
            options=getattr(load_config, "options", {}) or {},
            qualified_table=qualified_target,
            columns=tuple(column_types.items()),
            quote_identifier=_quote_identifier,
            to_mssql_type=str,
            nullability=nullability,
            collations=collations,
            physical_contract=contract,
        )
    )
    unique = resolve_unique_authority_contract(load_config, column_types)
    if unique is not None:
        nullable = {column.name: column.nullable for column in columns}
        if any(nullable.get(key, True) for key in unique.key_columns):
            raise RuntimeError("mssql_native_projection.unique_key_nullable")
    unique_ddl = render_unique_authority_ddl(load_config, qualified_target, column_types)
    if unique_ddl is not None:
        statements.append(unique_ddl)
    after = _created_schema_snapshot(
        before,
        columns=columns,
        contract=contract,
        unique=unique,
        qualified_target=qualified_target,
    )
    mutation_plan = append_target_mutations(
        mutation_plan,
        admission,
        kind="schema_evolution",
        statements=statements,
        expectation=exact_schema_catalog_transition(before, after),
    )
    physical_before = PhysicalTableState(sink_type="mssql", table=qualified_target)
    physical_after = expected_mssql_created_physical_state(
        table=qualified_target,
        columns=columns,
        contract=contract,
        default_filegroup=before.default_filegroup,
    )
    mutation_plan = append_target_mutations(
        mutation_plan,
        admission,
        kind="physical_design",
        statements=(),
        expectation=physical_catalog_expectation(physical_before, expected_after=physical_after),
    )
    return MssqlFreshTargetPreplan(columns, mutation_plan, after, physical_after)


def resolve_mssql_target_columns(
    load_config: Any,
    source_columns: list[ColumnDef],
) -> tuple[ColumnDef, ...]:
    """Return business plus every strategy/lineage-owned target column."""

    columns = [
        ColumnDef(
            column.name,
            resolved_mssql_target_column_type(load_config, column.name, column.dtype),
            nullable=column.nullable,
            collation=column.collation,
        )
        for column in source_columns
    ]
    columns.extend(
        ColumnDef(name, target_type, nullable=nullable)
        for name, target_type, nullable in resolve_mssql_strategy_metadata_columns(load_config)
    )
    columns.extend(
        ColumnDef(name, target_type, nullable=nullable, collation=collation)
        for name, target_type, nullable, collation, _role in resolve_mssql_native_lineage_columns(load_config)
    )
    names = tuple(column.name for column in columns)
    if len(names) != len(set(names)) or len(names) != len({name.casefold() for name in names}):
        raise RuntimeError("mssql_transaction.fresh_target_column_identity_collision")
    equality_collations = mssql_equality_key_collations(
        load_config,
        {column.name: column.dtype for column in columns},
    )
    return tuple(
        ColumnDef(
            column.name,
            column.dtype,
            nullable=column.nullable,
            collation=equality_collations.get(column.name, column.collation),
        )
        for column in columns
    )


def _apply_primary_key_nullability(
    columns: tuple[ColumnDef, ...],
    contract: MssqlPhysicalDesignContract,
) -> tuple[ColumnDef, ...]:
    primary = {name.casefold() for name in contract.primary_key}
    return tuple(
        ColumnDef(
            column.name,
            column.dtype,
            nullable=False if column.name.casefold() in primary else column.nullable,
            collation=column.collation,
        )
        for column in columns
    )


def _created_schema_snapshot(
    before: MssqlSchemaCatalogSnapshot,
    *,
    columns: tuple[ColumnDef, ...],
    contract: MssqlPhysicalDesignContract,
    unique: Any | None,
    qualified_target: str,
) -> MssqlSchemaCatalogSnapshot:
    effective = contract if contract.active else MssqlPhysicalDesignContract(active=False)
    indexes: list[MssqlIndexState] = []
    data_space = effective.storage.filegroup or before.default_filegroup
    if effective.primary_key:
        indexes.append(
            MssqlIndexState(
                name=MssqlTableDdlRenderer.primary_key_name(qualified_target, effective.primary_key),
                type_desc="CLUSTERED",
                unique=True,
                primary_key=True,
                unique_constraint=False,
                disabled=False,
                hypothetical=False,
                ignore_dup_key=False,
                filter_definition=None,
                key_columns=effective.primary_key,
                included_columns=(),
                descending_keys=(False,) * len(effective.primary_key),
                fill_factor=effective.storage.index_fillfactor or 0,
                data_space_name=data_space,
                data_space_type_desc="ROWS_FILEGROUP",
                partition_compression=(effective.storage.compression,),
            )
        )
    if effective.storage.clustered_columnstore:
        indexes.append(
            MssqlIndexState(
                name=MssqlTableDdlRenderer.clustered_columnstore_name(qualified_target),
                type_desc="CLUSTERED COLUMNSTORE",
                unique=False,
                primary_key=False,
                unique_constraint=False,
                disabled=False,
                hypothetical=False,
                ignore_dup_key=False,
                filter_definition=None,
                key_columns=(),
                included_columns=(),
                descending_keys=(),
                data_space_name=data_space,
                data_space_type_desc="ROWS_FILEGROUP",
                partition_compression=("COLUMNSTORE",),
            )
        )
    if unique is not None:
        indexes.append(
            MssqlIndexState(
                name=unique.name,
                type_desc=unique.type_desc,
                unique=unique.is_unique,
                primary_key=unique.is_primary_key,
                unique_constraint=unique.is_unique_constraint,
                disabled=unique.is_disabled,
                hypothetical=unique.is_hypothetical,
                ignore_dup_key=unique.ignore_dup_key,
                filter_definition=unique.filter_definition,
                key_columns=unique.key_columns,
                included_columns=(),
                descending_keys=(False,) * len(unique.key_columns),
                data_space_name=before.default_filegroup,
                data_space_type_desc="ROWS_FILEGROUP",
                partition_compression=(unique.data_compression,),
            )
        )
    return MssqlSchemaCatalogSnapshot(
        True,
        before.database_collation,
        columns=tuple(
            catalog_column_from_definition(
                column,
                ordinal=index,
                database_collation=before.database_collation,
            )
            for index, column in enumerate(columns, start=1)
        ),
        indexes=tuple(indexes),
        behavior=MssqlTableBehaviorState.ordinary_disk_table(),
        default_filegroup=before.default_filegroup,
        available_row_filegroups=before.available_row_filegroups,
    )


def _require_fresh_physical_authority(load_config: Any) -> None:
    options = getattr(load_config, "options", {}) or {}
    raw = options.get("physical_design") if isinstance(options, Mapping) else None
    if raw is None or raw is False:
        return
    if not isinstance(raw, Mapping):
        raise ValueError("physical_design must be an object")
    enabled = bool(raw.get("enabled", True))
    mode = str(raw.get("mode", "auto")).strip().casefold()
    if not enabled or mode == "off":
        return
    if not bool(raw.get("apply_runtime", True)):
        raise RuntimeError("physical_design.external_provisioning_required_for_missing_target")
    apply_mode = str(raw.get("apply", "online")).strip().casefold()
    if apply_mode in {"plan_only", "manual_approval"}:
        raise RuntimeError(f"physical_design.{apply_mode}:missing_target")


def _quote_identifier(value: str) -> str:
    return "[" + str(value).replace("]", "]]") + "]"


__all__ = [
    "MssqlFreshTargetPreplan",
    "plan_fresh_mssql_target",
    "resolve_mssql_target_columns",
]
