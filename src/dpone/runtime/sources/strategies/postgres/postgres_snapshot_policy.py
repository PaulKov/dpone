"""Pure authoring policy and query helpers for PostgreSQL snapshots."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from psycopg import sql


def mssql_snapshot_file_config(load_config: Any) -> Any:
    """Project public CSV authoring into the internal safe MSSQL wire."""

    options = getattr(load_config, "options", {}) or {}
    public_format = str(getattr(load_config, "export_format", "csv") or "csv").strip().lower()
    if public_format != "csv":
        raise ValueError("PostgreSQL-to-MSSQL key_snapshot requires public export_format=csv")
    if options.get("allow_unsafe_raw_mssql_bulk_files") is True:
        raise ValueError("PostgreSQL-to-MSSQL key_snapshot forbids unsafe raw MSSQL bulk files")
    if str(options.get("batch_commit_mode", "whole")).lower() != "whole":
        raise ValueError("key_snapshot reconciliation requires source.options.batch_commit_mode=whole")
    partitioning = options.get("partitioning")
    if isinstance(partitioning, dict) and partitioning.get("column"):
        raise ValueError("key_snapshot same-source-snapshot export does not support partition workers in v1")
    if getattr(load_config, "compress_export", False):
        raise ValueError("PostgreSQL-to-MSSQL key_snapshot requires compress_export=false")
    effective_options = dict(options)
    effective_options["__dpone_postgres_mssql_internal_wire"] = True
    return replace(
        load_config,
        export_format="mssql-delimited",
        compress_export=False,
        options=effective_options,
    )


def snapshot_key_query(load_config: Any, keys: tuple[str, ...]) -> Any:
    """Build the complete ordered key query for an incremental envelope."""

    query = sql.SQL("SELECT {keys} FROM {schema}.{table}").format(
        keys=sql.SQL(", ").join(sql.Identifier(key) for key in keys),
        schema=sql.Identifier(load_config.source_schema),
        table=sql.Identifier(load_config.source_table),
    )
    predicate = snapshot_source_predicate(load_config)
    if predicate:
        query += sql.SQL(" WHERE ") + sql.SQL(predicate)
    return query + sql.SQL(" ORDER BY ") + sql.SQL(", ").join(sql.Identifier(key) for key in keys)


def snapshot_source_predicate(load_config: Any) -> str | None:
    """Return the normalized source-only predicate, when authored."""

    options = getattr(load_config, "options", {}) or {}
    value = options.get("source_custom_predicate")
    if value is None:
        return None
    predicate = str(value).strip()
    return predicate or None


def require_bounded_replay_amplification(load_config: Any, safe_checkpoint: int, visible_horizon: int) -> None:
    """Fail closed when a reviewed XMin replay-amplification cap is exceeded."""

    options = getattr(load_config, "options", {}) or {}
    amplification = visible_horizon - safe_checkpoint
    if amplification < 0:
        raise ValueError("postgres_xmin_visible_horizon_precedes_safe_checkpoint")
    raw = options.get("xmin_max_replay_amplification_xids")
    if raw is None:
        return
    try:
        limit = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("postgres_xmin_replay_amplification_limit_invalid") from exc
    if limit < 1:
        raise ValueError("postgres_xmin_replay_amplification_limit_invalid")
    if amplification > limit:
        raise ValueError("postgres_xmin_replay_amplification_limit_exceeded")


__all__ = [
    "mssql_snapshot_file_config",
    "require_bounded_replay_amplification",
    "snapshot_key_query",
    "snapshot_source_predicate",
]
