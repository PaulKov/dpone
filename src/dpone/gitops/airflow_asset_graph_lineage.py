"""Lineage URI resolution helpers for the Airflow asset graph."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.gitops.airflow_asset_uri import (
    MSSQL_ASSET_URI_INVALID,
    AssetUriIssue,
    AssetUriResolution,
    MssqlAssetAuthority,
    ResolvedMssqlAssetRegistry,
    canonicalize_table_uri,
    connection_ref_from_block,
    is_templated_value,
    resolve_explicit_mssql_uri,
    resolve_mssql_asset_authority,
)

if TYPE_CHECKING:
    from dpone.manifest.loader import ProcessSpec

_MSSQL_ENGINE = "mssql"


def lineage_uri(
    block: Mapping[str, Any],
    process: ProcessSpec,
    *,
    role: str,
    authority_index: Mapping[str, MssqlAssetAuthority],
    path: str,
    database_index: Mapping[str, str] | None = None,
) -> AssetUriResolution | None:
    engine = canonical_endpoint_type(str(block.get("type") or ""))
    if not engine:
        return None
    table = _mapping(block.get("table"))
    schema = str(table.get("schema") or "").strip()
    name = str(table.get("name") or "").strip()
    database = str(table.get("database") or "").strip() or None
    load_config = getattr(process.config, "load_config", None)
    if load_config is not None:
        if role == "source" and not database:
            database = str(getattr(load_config, "source_database", None) or "").strip() or None
        if role == "sink" and not database:
            database = str(getattr(load_config, "target_database", None) or "").strip() or None
    if not database and engine == _MSSQL_ENGINE:
        ref = connection_ref_from_block(block)
        if ref and database_index:
            database = database_index.get(ref)
    if schema and name and not is_templated_value(schema) and not is_templated_value(name):
        return resolve_engine_uri(
            engine,
            schema,
            name,
            database=database,
            block=block,
            authority_index=authority_index,
            path=path,
        )
    if load_config is None:
        if engine == _MSSQL_ENGINE and (connection_ref_from_block(block) or database):
            return AssetUriResolution(
                uri=None,
                issues=(
                    AssetUriIssue(
                        code=MSSQL_ASSET_URI_INVALID,
                        message="mssql lineage is incomplete; need schema/table/database and asset_authority",
                        path=path,
                    ),
                ),
            )
        return None
    if role == "source" and load_config.source_schema and load_config.source_table:
        return resolve_engine_uri(
            engine,
            str(load_config.source_schema),
            str(load_config.source_table),
            database=database or (str(getattr(load_config, "source_database", None) or "").strip() or None),
            block=block,
            authority_index=authority_index,
            path=path,
        )
    if role == "sink" and load_config.target_schema and load_config.target_table:
        return resolve_engine_uri(
            engine,
            str(load_config.target_schema),
            str(load_config.target_table),
            database=database or (str(getattr(load_config, "target_database", None) or "").strip() or None),
            block=block,
            authority_index=authority_index,
            path=path,
        )
    if engine == _MSSQL_ENGINE:
        return AssetUriResolution(
            uri=None,
            issues=(
                AssetUriIssue(
                    code=MSSQL_ASSET_URI_INVALID,
                    message="mssql lineage is incomplete; need schema/table/database and asset_authority",
                    path=path,
                ),
            ),
        )
    return None


def resolve_engine_uri(
    engine: str,
    schema: str,
    name: str,
    *,
    database: str | None,
    block: Mapping[str, Any],
    authority_index: Mapping[str, MssqlAssetAuthority],
    path: str,
) -> AssetUriResolution:
    if engine != _MSSQL_ENGINE:
        return canonicalize_table_uri(engine, schema, name, database=database, path=path)
    authority, auth_issues = resolve_mssql_asset_authority(
        block,
        authority_index=authority_index,
        path=path,
    )
    if auth_issues:
        return AssetUriResolution(uri=None, issues=auth_issues)
    assert authority is not None
    return canonicalize_table_uri(
        engine,
        schema,
        name,
        database=database,
        authority=authority,
        path=path,
    )


def canonicalize_declared_uri(
    uri: str,
    *,
    path: str,
    mssql_registry: ResolvedMssqlAssetRegistry | None = None,
    authority_index: Mapping[str, MssqlAssetAuthority] | None = None,
) -> AssetUriResolution:
    if not uri.lower().startswith("mssql://"):
        return AssetUriResolution(uri=uri, issues=())
    registry = mssql_registry
    if registry is None and authority_index is not None:
        registry = ResolvedMssqlAssetRegistry(
            env="inline",
            path=None,
            authorities=dict(authority_index),
            databases={},
        )
    if registry is None:
        return AssetUriResolution(
            uri=None,
            issues=(
                AssetUriIssue(
                    code=MSSQL_ASSET_URI_INVALID,
                    message="mssql Asset URI requires an environment connection registry snapshot",
                    path=path,
                ),
            ),
        )
    return resolve_explicit_mssql_uri(uri, registry=registry, path=path)


def requires_canonical_assets(execution: Mapping[str, Any]) -> bool:
    if declared_uris(execution.get("outlets")) or declared_uris(execution.get("inlets")):
        return True
    for field in ("outlets", "inlets"):
        raw = execution.get(field)
        if isinstance(raw, list) and any(isinstance(item, Mapping) and item.get("partition") for item in raw):
            return True
    schedule = execution.get("schedule")
    return isinstance(schedule, Mapping) and bool(schedule.get("assets"))


def declared_uris(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    uris: list[str] = []
    for item in value:
        uri = str(item.get("uri") or "").strip() if isinstance(item, Mapping) else str(item).strip()
        if uri:
            uris.append(uri)
    return tuple(uris)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "canonicalize_declared_uri",
    "declared_uris",
    "lineage_uri",
    "requires_canonical_assets",
    "resolve_engine_uri",
]
