"""Catalog-first portable-scope binding before MSSQL state admission."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, NoReturn

from dpone.backfill.portable_scope_runtime import is_postgres_mssql_backfill_route
from dpone.config.mssql_strategy_contract import normalize_mssql_load_strategy
from dpone.contracts.mssql_lossless_type_contract import (
    MssqlLosslessProjectionError,
    validate_mssql_lossless_projection,
)
from dpone.contracts.portable_relation_scope import resolve_portable_relation_scope
from dpone.contracts.portable_scope_binding import (
    PORTABLE_SCOPE_BINDING_OPTION,
    PortableScopeBindingError,
    PortableScopeColumnContract,
    bind_portable_scope,
    require_exact_portable_identifier,
    require_portable_scope_binding,
)
from dpone.runtime.sinks.mssql_target_catalog_reader import read_schema_catalog_snapshot


def prepare_portable_scope_binding(load_config: Any, *, source: Any, sink: Any) -> Any:
    """Freeze PG/MSSQL scope semantics before state or source-row access."""

    scope = resolve_portable_relation_scope(load_config)
    if scope is None:
        return load_config
    options = getattr(load_config, "options", {}) or {}
    _require_postgres_mssql_route(load_config)
    if isinstance(options, Mapping) and PORTABLE_SCOPE_BINDING_OPTION in options:
        require_portable_scope_binding(load_config, scope)
        return replace(load_config, portable_scope=scope)
    normalize_mssql_load_strategy(load_config)
    columns = resolve_portable_scope_column_contract(
        load_config,
        column=scope.column,
        source=source,
        sink=sink,
    )
    binding = bind_portable_scope(scope, columns)
    prepared_options = dict(options)
    prepared_options[PORTABLE_SCOPE_BINDING_OPTION] = binding
    return replace(load_config, portable_scope=scope, options=prepared_options)


def resolve_portable_scope_column_contract(
    load_config: Any,
    *,
    column: str,
    source: Any,
    sink: Any,
) -> PortableScopeColumnContract:
    """Read one reusable catalog contract before campaign state is created.

    Backfill planning calls this service with the unscoped campaign config:
    the planner, not the author, owns each chunk's portable scope.  Strategy
    normalization therefore belongs to the scoped runtime admission paths,
    while this resolver is deliberately limited to route and catalog proof.
    """

    _require_postgres_mssql_route(load_config)
    fetch = getattr(source, "fetch_schema_projection", None)
    if not callable(fetch):
        _blocked("portable_scope.binding.source_projection_required", "source catalog projection is unavailable")
    fetched = fetch(load_config)
    _portable_scope_source_column(column, fetched)
    target_snapshot = read_schema_catalog_snapshot(sink, load_config)
    return portable_scope_column_contract_from_catalogs(
        load_config,
        column=column,
        source_projection=fetched,
        target_snapshot=target_snapshot,
    )


def portable_scope_column_contract_from_catalogs(
    load_config: Any,
    *,
    column: str,
    source_projection: Any,
    target_snapshot: Any,
) -> PortableScopeColumnContract:
    """Resolve one route-bound column contract from caller-owned catalogs."""

    _require_postgres_mssql_route(load_config)
    projected = _portable_scope_source_column(column, source_projection)
    expected_target_type = projected.target_type
    expected_target_collation = projected.collation
    if expected_target_collation is None and _is_mssql_text(expected_target_type):
        expected_target_collation = target_snapshot.database_collation
    if target_snapshot.exists:
        target_names = tuple(column.name for column in target_snapshot.columns)
        require_exact_portable_identifier(column, target_names, side="target")
        target_column = next(item for item in target_snapshot.columns if item.name == column)
        target_definition = target_column.to_column_def()
        _prove_expected_target_transition(
            column,
            before_type=target_definition.dtype,
            before_collation=target_definition.collation,
            expected_type=expected_target_type,
            expected_collation=expected_target_collation,
        )

    return PortableScopeColumnContract(
        source_name=projected.source_name,
        source_type=projected.source_type,
        source_collation=projected.source_collation,
        target_name=projected.target_name,
        target_type=expected_target_type,
        target_collation=expected_target_collation,
    )


def _portable_scope_source_column(column: str, fetched: Any) -> Any:
    """Validate the authoritative source projection before target catalog I/O."""

    relation_schema = getattr(fetched, "relation_schema", None)
    projection = getattr(fetched, "target_projection", None)
    if relation_schema is None or projection is None:
        _blocked(
            "portable_scope.binding.postgres_mssql_projection_required",
            "authoritative PostgreSQL and MSSQL projections are required",
        )
    source_names = tuple(str(name) for name, _dtype in relation_schema)
    require_exact_portable_identifier(column, source_names, side="source")
    projected_columns = tuple(getattr(projection, "columns", ()) or ())
    projected_source_names = tuple(str(column.source_name) for column in projected_columns)
    require_exact_portable_identifier(column, projected_source_names, side="source_projection")
    projected = next(item for item in projected_columns if item.source_name == column)
    if projected.target_name != column:
        _blocked(
            "portable_scope.binding.renamed_target",
            "portable scopes do not follow schema-evolution target renames",
        )

    return projected


def require_current_portable_scope_binding(
    load_config: Any,
    *,
    source_projection: Any,
    target_snapshot: Any,
) -> None:
    """Compare a frozen binding with the schema preplan's current catalogs."""

    scope = resolve_portable_relation_scope(load_config)
    if scope is None:
        return
    # This is a chunk-runtime admission path: unlike the parent catalog
    # resolver above, the planner-issued scope must now satisfy the complete
    # strategy contract before its frozen binding is compared.
    normalize_mssql_load_strategy(load_config)
    frozen = require_portable_scope_binding(load_config, scope)
    columns = portable_scope_column_contract_from_catalogs(
        load_config,
        column=scope.column,
        source_projection=source_projection,
        target_snapshot=target_snapshot,
    )
    current = bind_portable_scope(scope, columns)
    if current != frozen:
        _blocked(
            "portable_scope.binding.catalog_changed",
            "current source/target column semantics differ from the frozen campaign binding",
        )


@dataclass(frozen=True, slots=True)
class PortableScopeColumnResolver:
    """Parent-only resolver composed with hydrated source and sink adapters."""

    source: Any
    sink: Any

    def __call__(self, load_config: Any, column: str) -> PortableScopeColumnContract:
        return resolve_portable_scope_column_contract(
            load_config,
            column=column,
            source=self.source,
            sink=self.sink,
        )


def portable_scope_column(load_config: Any) -> str | None:
    """Return the normalized portable-scope column without exposing its AST type."""

    scope = resolve_portable_relation_scope(load_config)
    return None if scope is None else scope.column


def portable_scope_column_contract(load_config: Any) -> Any:
    """Project one cacheable column contract from a fully prepared binding."""

    scope = resolve_portable_relation_scope(load_config)
    if scope is None:
        raise PortableScopeBindingError(
            "portable_scope.binding.scope_required",
            "a portable scope is required before projecting its column contract",
        )
    binding = require_portable_scope_binding(load_config, scope)
    return PortableScopeColumnContract(
        source_name=binding.column,
        source_type=binding.source_type,
        source_collation=binding.source_collation,
        target_name=binding.column,
        target_type=binding.target_type,
        target_collation=binding.target_collation,
    )


def bind_cached_portable_scope_contract(load_config: Any, contract: Any) -> Any:
    """Bind a new chunk AST to a previously parent-observed column contract."""

    scope = resolve_portable_relation_scope(load_config)
    if scope is None:
        return load_config
    if not isinstance(contract, PortableScopeColumnContract):
        raise PortableScopeBindingError(
            "portable_scope.binding.column_contract_invalid",
            "the cached portable-scope column contract is invalid",
        )
    options = dict(getattr(load_config, "options", {}) or {})
    options[PORTABLE_SCOPE_BINDING_OPTION] = bind_portable_scope(scope, contract)
    return replace(load_config, portable_scope=scope, options=options)


def _require_postgres_mssql_route(load_config: Any) -> None:
    if not is_postgres_mssql_backfill_route(load_config):
        _blocked(
            "portable_scope.binding.route_unsupported",
            "this portable-scope binding requires a PostgreSQL to MSSQL route",
        )


def _is_mssql_text(value: str) -> bool:
    return str(value).strip().casefold().startswith(("nvarchar(", "varchar(", "nchar(", "char("))


def _prove_expected_target_transition(
    column: str,
    *,
    before_type: str,
    before_collation: str | None,
    expected_type: str,
    expected_collation: str | None,
) -> None:
    """Prove the observed target can reach the deterministic projected shape."""

    try:
        validate_mssql_lossless_projection(
            before_type,
            expected_type,
            column=column,
            allow_value_guarded=False,
        )
    except (MssqlLosslessProjectionError, ValueError) as exc:
        raise PortableScopeBindingError(
            "portable_scope.binding.target_transition_type",
            "existing target type cannot evolve losslessly to the projected comparison type",
        ) from exc
    if _is_mssql_text(expected_type) and (before_collation or "").casefold() != (expected_collation or "").casefold():
        _blocked(
            "portable_scope.binding.target_transition_collation",
            "existing target collation differs from the deterministic projected comparison collation",
        )


def _blocked(blocker: str, detail: str) -> NoReturn:
    raise PortableScopeBindingError(blocker, detail)


__all__ = [
    "PortableScopeBindingError",
    "PortableScopeColumnResolver",
    "bind_cached_portable_scope_contract",
    "portable_scope_column",
    "portable_scope_column_contract",
    "portable_scope_column_contract_from_catalogs",
    "prepare_portable_scope_binding",
    "require_current_portable_scope_binding",
    "resolve_portable_scope_column_contract",
]
