"""Runtime-issued native typed-staging configuration for MSSQL snapshots."""

from __future__ import annotations

from copy import copy
from dataclasses import is_dataclass, replace
from typing import Any, cast

from dpone.runtime.incremental_snapshot import DELTA_HASH_COLUMN, KEY_HASH_COLUMN
from dpone.runtime.sinks.staging_managers.mssql_typed_values import require_typed_target_type
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import (
    load_id,
    resolved_business_nullability,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_reconciliation import SnapshotReconciliationError
from dpone.runtime.support.mssql_native_projection import resolve_native_column_types
from dpone.runtime.support.mssql_snapshot_projection import (
    MSSQL_TEXT_KEY_COLLATION,
    resolved_text_key_columns,
)


def typed_snapshot_staging_config(
    load_config: Any,
    schema: Any,
    *,
    unique_key: tuple[str, ...],
) -> Any:
    """Bind exact native types/nullability/collation before file materialization."""

    types = resolve_native_column_types(
        load_config,
        schema,
        fixed_types={DELTA_HASH_COLUMN: "char(64)", KEY_HASH_COLUMN: "char(64)"},
    )
    for column, dtype in types.items():
        require_typed_target_type(dtype, column=column)
    nullability = resolved_business_nullability(load_config, schema, unique_key)
    required = {column for column, nullable in nullability.items() if not nullable}
    required.update(column for column in (DELTA_HASH_COLUMN, KEY_HASH_COLUMN) if column in types)
    text_keys = resolved_text_key_columns(schema, load_config, unique_key)
    options = dict(getattr(load_config, "options", {}) or {})
    options.update(
        {
            "__dpone_snapshot_native_staging": True,
            "__dpone_snapshot_native_column_types": types,
            "__dpone_snapshot_native_not_null_columns": sorted(required),
            "__dpone_snapshot_native_collations": {column: MSSQL_TEXT_KEY_COLLATION for column in text_keys},
            "__dpone_mssql_typed_file_staging_v1": True,
        }
    )
    if is_dataclass(load_config):
        return replace(cast(Any, load_config), options=options)
    resolved = copy(load_config)
    resolved.options = options
    return resolved


def typed_snapshot_staging_configs(connector: Any, load_config: Any, envelope: Any) -> tuple[Any, Any]:
    """Issue exact delta/key staging configs for one typed snapshot envelope."""

    if not callable(getattr(connector, "bcp_import_format", None)):
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.typed_bcp_required")
    unique_key = tuple(envelope.key_receipt.key_columns)
    key_coordinates = {
        "staging_database": load_config.staging_database or load_config.target_database,
        "staging_table": f"stg_{load_config.target_table}_keys_{load_id(load_config)[-8:]}",
    }
    if is_dataclass(load_config):
        key_load_config = replace(cast(Any, load_config), **key_coordinates)
    else:
        key_load_config = copy(load_config)
        for name, value in key_coordinates.items():
            setattr(key_load_config, name, value)
    return (
        typed_snapshot_staging_config(load_config, envelope.delta_schema, unique_key=unique_key),
        typed_snapshot_staging_config(key_load_config, envelope.key_schema, unique_key=unique_key),
    )


__all__ = ["typed_snapshot_staging_config", "typed_snapshot_staging_configs"]
