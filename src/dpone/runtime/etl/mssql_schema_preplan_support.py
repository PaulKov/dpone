"""Catalog projection and governance helpers for MSSQL schema preplanning."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.schema_evolution_options import blocked_schema_evolution
from dpone.runtime.schema_evolution_payload import columns_from_schema
from dpone.runtime.support.mssql_object_name import MSSQLObjectName


def schema_columns_sha256(columns: list[ColumnDef]) -> bytes:
    """Hash the ordered source projection used by the immutable preplan."""

    payload = [
        {
            "name": column.name,
            "dtype": column.dtype,
            "nullable": column.nullable,
            "collation": column.collation,
        }
        for column in columns
    ]
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).digest()


def source_projection(source: Any, load_config: Any) -> Any:
    """Read the connector-owned source projection required before row I/O."""

    fetch = getattr(source, "fetch_schema_projection", None)
    if not callable(fetch):
        raise RuntimeError("mssql_transaction.source_schema_preflight_capability_required")
    return fetch(load_config)


def source_columns(fetched: Any) -> list[ColumnDef]:
    """Normalize a connector projection to schema-evolution column definitions."""

    projection = getattr(fetched, "target_projection", None)
    if projection is not None:
        return [
            ColumnDef(
                column.target_name,
                column.target_type,
                nullable=column.nullable,
                collation=column.collation,
            )
            for column in projection.columns
        ]
    projected = getattr(fetched, "projected_schema", None)
    if projected is None:
        raise RuntimeError("mssql_transaction.source_schema_preflight_projection_required")
    return columns_from_schema(projected)


def target_row_count(options: Any, sink: Any, load_config: Any) -> int | None:
    """Read a governed size probe only when inline-DDL policy needs it."""

    if options.max_table_size_for_inline_ddl is None:
        return None
    probe = getattr(sink, "get_target_row_count", None)
    if not callable(probe):
        raise blocked_schema_evolution("schema_evolution.table_size_unavailable")
    value = probe(load_config)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise blocked_schema_evolution("schema_evolution.table_size_invalid")
    return value


def require_missing_target_authority(options: Any, load_config: Any, sink: Any) -> None:
    """Require governance authorization before planning creation of a missing table."""

    from dpone.readiness.ddl_governance import decide_missing_table, decision_blocks_load

    decision = decide_missing_table(governance_policy(options))
    if decision == "apply" and options.apply_safe:
        return
    suffix = "blocked" if decision_blocks_load(decision) else "apply_safe"
    raise blocked_schema_evolution(f"schema_evolution.{suffix}:create_table:{exact_target(load_config)}")


def exact_target(load_config: Any) -> str:
    """Return the strict, database-qualified MSSQL target name."""

    return MSSQLObjectName.from_parts(
        database=getattr(load_config, "target_database", None),
        schema=str(load_config.target_schema),
        table=str(load_config.target_table),
        strict=True,
    ).quoted()


def schema_policy(
    options: Any,
    *,
    protected_columns: tuple[str, ...],
    allow_framework_columns: bool = False,
):
    """Translate runtime options into the pure schema comparison policy."""

    from dpone.readiness.schema_evolution import SchemaEvolutionPolicy

    return SchemaEvolutionPolicy(
        mode=options.mode,
        on_type_change=options.on_type_change,
        accept_existing_nullable_target=options.target_nullability == "accept_existing_nullable",
        new_column_prefix=options.new_column_prefix,
        allow_reserved_dpone_columns=options.allow_reserved_dpone_columns or allow_framework_columns,
        protected_columns=protected_columns,
    )


def reject_observed_reserved_columns(columns: list[ColumnDef]) -> None:
    """Reject source-owned names reserved for dpone technical columns."""

    observed = [column.name for column in columns if column.name.casefold().startswith("__dpone__")]
    if observed:
        raise blocked_schema_evolution("schema_evolution.reserved_source_column:" + ",".join(observed))


def governance_policy(options: Any):
    """Translate runtime options into the online-DDL governance policy."""

    from dpone.readiness.ddl_governance import DdlGovernancePolicy

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


__all__ = [
    "exact_target",
    "governance_policy",
    "reject_observed_reserved_columns",
    "require_missing_target_authority",
    "schema_columns_sha256",
    "schema_policy",
    "source_columns",
    "source_projection",
    "target_row_count",
]
