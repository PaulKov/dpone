"""Exact existing-target admission before PostgreSQL row export.

Schema evolution answers whether a change is allowed.  It is not the final
native-load contract: framework columns, defaults, computed/identity behavior,
and the unique-key authority are also part of the target surface.  This pure
validator consumes the catalog snapshot already read by admission so a
structural error is reported before COPY and before parallel chunk lanes are
started.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.backfill.shadow_append_authority import require_shadow_append_authority
from dpone.readiness.mssql_technical_type_compatibility import (
    mssql_fixed_technical_types_compatible,
)
from dpone.readiness.schema_evolution import ColumnDef
from dpone.readiness.schema_type_compatibility import MssqlSchemaTypeCompatibility
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.schema_evolution_options import accepts_existing_nullable_target
from dpone.runtime.sinks.mssql_target_catalog_model import (
    MssqlIndexState,
    MssqlSchemaCatalogSnapshot,
)
from dpone.runtime.sinks.strategies.mssql.mssql_unique_authority import (
    MssqlUniqueAuthorityContract,
    ObservedUniqueAuthority,
    external_physical_primary_key_authority,
    resolve_unique_authority_contract,
    unique_authority_matches,
)


def require_existing_mssql_target_contract(
    load_config: Any,
    expected_columns: Sequence[ColumnDef],
    snapshot: MssqlSchemaCatalogSnapshot,
    *,
    qualified_target: str,
) -> None:
    """Require the exact native target projection and key authority."""

    if not snapshot.exists:
        return
    expected = {column.name: column for column in expected_columns}
    actual = {column.name: column for column in snapshot.columns}
    if set(expected) != set(actual):
        missing = ",".join(sorted(set(expected).difference(actual))) or "none"
        unexpected = ",".join(sorted(set(actual).difference(expected))) or "none"
        _raise(f"target_shape_invalid:columns:missing={missing}:unexpected={unexpected}")
    compatibility = MssqlSchemaTypeCompatibility()
    for name, desired in expected.items():
        observed = actual[name]
        actual_type = observed.to_column_def().dtype
        if not (
            compatibility.types_equal(desired.dtype, actual_type)
            or mssql_fixed_technical_types_compatible(name, desired.dtype, actual_type)
        ):
            _raise(f"target_shape_invalid:type:{name}")
        accepts_nullable_legacy_target = (
            accepts_existing_nullable_target(load_config) and not desired.nullable and observed.nullable
        )
        if observed.nullable is not desired.nullable and not accepts_nullable_legacy_target:
            _raise(f"target_shape_invalid:nullability:{name}")
        if desired.collation and str(observed.collation or "").casefold() != desired.collation.casefold():
            _raise(f"target_collation_invalid:{name}")
        if any(
            (
                observed.identity,
                observed.computed,
                observed.sparse,
                observed.rowguidcol,
                observed.generated_always_type,
                observed.default_definition,
                observed.computed_definition,
            )
        ):
            _raise(f"target_shape_invalid:behavior:{name}")
    if require_shadow_append_authority(load_config) is not None:
        # The campaign publisher has already validated the unique authority on
        # the registered live target.  The owned shadow deliberately defers
        # secondary indexes until all disjoint, receipted chunks are durable.
        return
    expected_unique = external_physical_primary_key_authority(
        load_config,
        qualified_target=qualified_target,
    ) or resolve_unique_authority_contract(
        load_config,
        {column.name: column.dtype for column in expected_columns},
    )
    if expected_unique is not None and not any(
        _catalog_unique_matches(index, expected_unique) for index in snapshot.indexes
    ):
        _raise("target_unique_authority_missing")


def _catalog_unique_matches(index: MssqlIndexState, expected: MssqlUniqueAuthorityContract) -> bool:
    compression = tuple(str(value).upper() for value in index.partition_compression)
    if not index.unique or len(index.descending_keys) != len(index.key_columns):
        return False
    observed = ObservedUniqueAuthority(
        name=index.name,
        type_desc=index.type_desc.upper(),
        primary_key=index.primary_key,
        unique_constraint=index.unique_constraint,
        disabled=index.disabled,
        hypothetical=index.hypothetical,
        ignore_dup_key=index.ignore_dup_key,
        filter_definition=index.filter_definition,
        data_space_type_desc=(index.data_space_type_desc.upper() if index.data_space_type_desc is not None else None),
        key_columns=index.key_columns,
        descending_keys=index.descending_keys,
        partition_count=len(compression),
        minimum_compression=min(compression, default=None),
        maximum_compression=max(compression, default=None),
    )
    return unique_authority_matches(observed, expected)


def _raise(suffix: str) -> None:
    raise SnapshotReconciliationError(f"mssql_native_projection.{suffix}")


__all__ = ["require_existing_mssql_target_contract"]
