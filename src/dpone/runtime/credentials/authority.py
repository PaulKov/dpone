"""Select and resolve one runtime connection authority before connector I/O."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.runtime.credentials.authority_resolution import (
    canonical_ref,
    canonical_runtime_endpoint_type,
    error,
    mapping,
    nested_optional_ref,
    optional_ref,
    reject_canonical_without_context,
    reject_strict_legacy_authority,
    require_explicit_legacy_authority,
    require_type,
    resolve,
    source_ref,
    state_ref,
    warn_legacy_runtime_connections,
)

from .governed_database_authority import (
    require_governed_mssql_database_authority,
    require_governed_postgres_source_authority,
)
from .runtime_context import RuntimeConnectionContext

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

_STATELESS_STRATEGIES = frozenset({LoadStrategy.FULL_REFRESH, LoadStrategy.REPLACE, LoadStrategy.BACKFILL})


@dataclass(frozen=True, slots=True)
class RuntimeResolvedConnections:
    """All canonical connections resolved once before runtime objects are built."""

    strict: bool
    source: ResolvedBindingConnection | None = None
    sink: ResolvedBindingConnection | None = None
    state: ResolvedBindingConnection | None = None
    proxy: ResolvedBindingConnection | None = None
    object_storage_runtime: ResolvedBindingConnection | None = None
    object_storage_clickhouse: ResolvedBindingConnection | None = None
    source_materialization: ResolvedBindingConnection | None = None
    by_ref: Mapping[str, ResolvedBindingConnection] = field(default_factory=lambda: MappingProxyType({}))
    receipts: tuple[Mapping[str, Any], ...] = ()


def resolve_runtime_connections(
    *,
    config: Mapping[str, Any],
    load_config: LoadConfig,
    context: RuntimeConnectionContext | None,
) -> RuntimeResolvedConnections:
    """Resolve all selected capabilities before any connector can perform I/O."""

    source = mapping(config.get("source"))
    sink = mapping(config.get("sink"))
    state = mapping(config.get("state"))
    proxy = mapping(config.get("bigquery_proxy"))
    object_storage = mapping(config.get("object_storage"))
    source_materialization_ref = _source_materialization_connection_ref(load_config)
    if context is None:
        if source_materialization_ref is not None:
            raise error(
                "DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED",
                "Source materialization work_connection_ref requires a verified runtime connection context.",
            )
        reject_canonical_without_context(source, sink, state, proxy, object_storage)
        require_explicit_legacy_authority(
            config,
            source=source,
            sink=sink,
            state=state,
            proxy=proxy,
            object_storage=object_storage,
        )
        warn_legacy_runtime_connections()
        return RuntimeResolvedConnections(strict=False)
    reject_strict_legacy_authority(
        source=source,
        sink=sink,
        state=state,
        proxy=proxy,
        object_storage=object_storage,
    )

    refs: dict[str, str] = {}
    selected_source_ref = source_ref(source)
    if selected_source_ref is not None:
        refs["source"] = selected_source_ref
    if source_materialization_ref is not None:
        refs["source_materialization"] = source_materialization_ref
    refs["sink"] = canonical_ref(sink, capability="sink", required=True)
    selected_state_ref = state_ref(
        state,
        sink_ref=refs["sink"],
        state_required=load_config.load_strategy not in _STATELESS_STRATEGIES,
    )
    if selected_state_ref is not None:
        refs["state"] = selected_state_ref
    optional_refs = {
        "proxy": optional_ref(proxy, capability="bigquery_proxy"),
        "object_storage_runtime": nested_optional_ref(
            object_storage,
            "runtime_access",
            capability="object_storage.runtime_access",
        ),
        "object_storage_clickhouse": nested_optional_ref(
            object_storage,
            "clickhouse_write_access",
            capability="object_storage.clickhouse_write_access",
        ),
    }
    refs.update({key: value for key, value in optional_refs.items() if value is not None})

    resolved_by_ref = {
        connection_ref: resolve(context, connection_ref) for connection_ref in sorted(set(refs.values()))
    }
    if "source" in refs:
        require_type(resolved_by_ref[refs["source"]], source, capability="source")
    if "source_materialization" in refs:
        require_type(
            resolved_by_ref[refs["source_materialization"]],
            {"type": "mssql"},
            capability="source_materialization",
        )
    require_type(resolved_by_ref[refs["sink"]], sink, capability="sink")
    if "state" in refs and state and str(state.get("reuse") or "") != "sink":
        require_type(resolved_by_ref[refs["state"]], state, capability="state")
    require_governed_postgres_source_authority(
        source=(resolved_by_ref[refs["source"]] if "source" in refs else None),
        sink=resolved_by_ref[refs["sink"]],
        state_config=state,
        load_config=load_config,
    )
    resolved_sink = resolved_by_ref[refs["sink"]]
    require_governed_mssql_database_authority(
        sink=resolved_sink,
        state=(resolved_by_ref[refs["state"]] if "state" in refs else None),
        state_config=state,
        target_database=str(load_config.target_database or resolved_sink.credentials.database or ""),
        staging_database=str(
            load_config.staging_database or load_config.target_database or resolved_sink.credentials.database or ""
        ),
    )
    return RuntimeResolvedConnections(
        strict=True,
        source=resolved_by_ref[refs["source"]] if "source" in refs else None,
        sink=resolved_sink,
        state=resolved_by_ref[refs["state"]] if "state" in refs else None,
        proxy=resolved_by_ref[refs["proxy"]] if "proxy" in refs else None,
        object_storage_runtime=(
            resolved_by_ref[refs["object_storage_runtime"]] if "object_storage_runtime" in refs else None
        ),
        object_storage_clickhouse=(
            resolved_by_ref[refs["object_storage_clickhouse"]] if "object_storage_clickhouse" in refs else None
        ),
        source_materialization=(
            resolved_by_ref[refs["source_materialization"]] if "source_materialization" in refs else None
        ),
        by_ref=MappingProxyType(dict(resolved_by_ref)),
        receipts=tuple(
            MappingProxyType(dict(resolved_by_ref[connection_ref].safe_metadata))
            for connection_ref in sorted(resolved_by_ref)
            if resolved_by_ref[connection_ref].safe_metadata
        ),
    )


def _source_materialization_connection_ref(load_config: LoadConfig) -> str | None:
    native = mapping(load_config.options.get("native_transfer"))
    snapshot = mapping(native.get("snapshot"))
    materialization = mapping(snapshot.get("materialization"))
    value = materialization.get("work_connection_ref")
    if value in (None, ""):
        return None
    if not isinstance(value, str) or not value.strip():
        raise error(
            "DPONE_RUNTIME_CONNECTION_REF_INVALID",
            "source materialization work_connection_ref must be a non-empty string.",
        )
    return value.strip()


__all__ = [
    "ResolvedBindingConnection",
    "RuntimeResolvedConnections",
    "canonical_runtime_endpoint_type",
    "resolve_runtime_connections",
    "warn_legacy_runtime_connections",
]
