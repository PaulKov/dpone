"""Canonical Airflow Asset URI helpers (MSSQL AIP-60 and other engines).

MSSQL Asset identity is deployment-owned. The URI host is never a
``connection_ref`` alias. See ``docs/airflow-mssql-asset-uri.md``.

Canonical MSSQL shapes (port always explicit; path segments percent-encoded):

- ``mssql://{host}:{port}/{database}/{schema}/{table}``
- ``mssql://{host}:{port}/{INSTANCE}/{database}/{schema}/{table}``

Explicit URIs may omit the port; with a registry snapshot the sole approved
authority for that host supplies port/instance, otherwise ``:1433``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.gitops.airflow_mssql_asset_authority import (
    MSSQL_ASSET_URI_INVALID,
    AssetUriIssue,
    AssetUriResolution,
    MssqlAssetAuthority,
    authority_from_connection_metadata,
    connection_ref_from_block,
    connection_registry_path_for_env,
    discover_connection_registry_path,
    is_templated_value,
    issue,
    load_connection_registry_document,
    load_mssql_asset_authority_index,
    load_mssql_default_database_index,
    normalize_hostname,
    normalize_instance,
    parse_mssql_asset_authority,
    resolve_mssql_asset_authority,
    validate_authority_host,
)
from dpone.gitops.airflow_mssql_explicit_authority import (
    MssqlAssetAuthorityKey,
    resolve_explicit_mssql_uri,
)
from dpone.gitops.airflow_mssql_registry_snapshot import (
    ResolvedMssqlAssetRegistry,
    load_mssql_registry_indexes,
    resolve_mssql_asset_registry,
)

_MSSQL_ENGINE = "mssql"
_DEFAULT_MSSQL_PORT = 1433


def canonicalize_mssql_asset_uri(
    *,
    authority: MssqlAssetAuthority,
    database: str,
    schema: str,
    table: str,
    path: str | None = None,
) -> AssetUriResolution:
    """Build one canonical MSSQL AIP-60 Asset URI via the shared pack codec."""

    from dpone_airflow_pack.mssql_asset_uri_codec import canonicalize_mssql_asset_uri_parts

    issues: list[AssetUriIssue] = []
    host_error = validate_authority_host(authority.host)
    if host_error:
        issues.append(issue(host_error, path=path))
    if not 1 <= int(authority.port) <= 65535:
        issues.append(issue("mssql asset_authority.port must be an integer 1..65535", path=path))
    segments = {
        "database": database,
        "schema": schema,
        "table": table,
    }
    normalized_instance = normalize_instance(authority.instance)
    if normalized_instance is not None:
        segments = {"instance": normalized_instance, **segments}
    for name, value in segments.items():
        text = (value or "").strip()
        if not text:
            issues.append(issue(f"mssql Asset URI requires {name}", path=path))
        elif is_templated_value(text):
            issues.append(issue(f"mssql Asset URI {name} must not be templated", path=path))
    if issues:
        return AssetUriResolution(uri=None, issues=tuple(issues))
    try:
        uri = canonicalize_mssql_asset_uri_parts(
            host=normalize_hostname(authority.host),
            port=int(authority.port),
            instance=normalized_instance,
            database=database.strip(),
            schema=schema.strip(),
            table=table.strip(),
        )
    except ValueError as exc:
        return AssetUriResolution(uri=None, issues=(issue(str(exc), path=path),))
    return AssetUriResolution(uri=uri, issues=())


def canonicalize_declared_mssql_uri(uri: str, *, path: str | None = None) -> AssetUriResolution:
    """Validate/normalize an explicit ``mssql://`` inlet/outlet URI.

    Missing port is allowed and normalized to ``:1433``. Malformed ports,
    credentials, query strings, and fragments are fail-closed blockers.
    Parsing is delegated to the shared pack codec so GitOps builder, projection
    validator, and Airflow provider cannot drift on named-instance shapes.
    """

    from dpone_airflow_pack.mssql_asset_uri_codec import (
        canonicalize_mssql_asset_uri as shared_canonicalize,
    )
    from dpone_airflow_pack.mssql_asset_uri_codec import (
        parse_mssql_asset_uri,
    )

    text = uri.strip()
    try:
        parsed = parse_mssql_asset_uri(text, require_explicit_port=False)
        canonical = shared_canonicalize(text, require_explicit_port=False)
    except ValueError as exc:
        detail = str(exc)
        if "scheme" in detail or "not an mssql" in detail:
            return AssetUriResolution(uri=None, issues=(issue("not an mssql:// URI", path=path),))
        if "credentials" in detail:
            return AssetUriResolution(
                uri=None,
                issues=(issue("mssql Asset URI must not embed credentials", path=path),),
            )
        if "query" in detail or "fragment" in detail:
            return AssetUriResolution(
                uri=None,
                issues=(issue("mssql Asset URI must not include query or fragment", path=path),),
            )
        if "port" in detail:
            return AssetUriResolution(
                uri=None,
                issues=(issue("mssql Asset URI port must be an integer 1..65535", path=path),),
            )
        if "host" in detail or "path" in detail or "segment" in detail:
            return AssetUriResolution(
                uri=None,
                issues=(
                    issue(
                        "compact or host-less mssql:// URIs are invalid; expected "
                        "mssql://{host}:{port}/{database}/{schema}/{table} "
                        "(optional /{INSTANCE}/ before database; port defaults to 1433)",
                        path=path,
                    ),
                ),
            )
        return AssetUriResolution(uri=None, issues=(issue(detail, path=path),))
    _ = parsed  # parsed validates coordinates; wire form comes from shared canonicalize
    return AssetUriResolution(uri=canonical, issues=())


def canonicalize_table_uri(
    engine: str,
    schema: str,
    table: str,
    *,
    database: str | None = None,
    authority: MssqlAssetAuthority | None = None,
    host: str | None = None,
    port: int | None = None,
    instance: str | None = None,
    path: str | None = None,
) -> AssetUriResolution:
    """Build a stable dataset URI from connector type and table coordinates."""

    engine_norm = engine.strip().lower()
    schema_norm = schema.strip()
    table_norm = table.strip()
    if not engine_norm or not schema_norm or not table_norm:
        return AssetUriResolution(
            uri=None,
            issues=(issue("engine, schema, and table are required for a canonical URI", path=path),),
        )
    if is_templated_value(schema_norm) or is_templated_value(table_norm) or is_templated_value(database):
        return AssetUriResolution(
            uri=None,
            issues=(issue("templated schema/table/database cannot be inferred into Asset URIs", path=path),),
        )
    if engine_norm != _MSSQL_ENGINE:
        if database and database.strip():
            return AssetUriResolution(uri=f"{engine_norm}://{database.strip()}/{schema_norm}/{table_norm}")
        return AssetUriResolution(uri=f"{engine_norm}://{schema_norm}/{table_norm}")

    resolved = authority
    if resolved is None:
        if host:
            resolved, auth_issues = parse_mssql_asset_authority(
                {"host": host, "port": port or _DEFAULT_MSSQL_PORT, "instance": instance},
                path=path,
            )
            if auth_issues:
                return AssetUriResolution(uri=None, issues=auth_issues)
        else:
            return AssetUriResolution(
                uri=None,
                issues=(
                    issue(
                        "mssql Asset URIs require deployment-owned asset_authority "
                        f"(host/port); expected mssql://{{host}}:{_DEFAULT_MSSQL_PORT}/"
                        "{database}/{schema}/{table}",
                        path=path,
                    ),
                ),
            )
    assert resolved is not None
    return canonicalize_mssql_asset_uri(
        authority=resolved,
        database=(database or "").strip(),
        schema=schema_norm,
        table=table_norm,
        path=path,
    )


def canonical_table_uri(
    engine: str,
    schema: str,
    table: str,
    *,
    database: str | None = None,
    authority: MssqlAssetAuthority | None = None,
    host: str | None = None,
    port: int | None = None,
    instance: str | None = None,
) -> str:
    """Compatibility wrapper that raises ``ValueError`` on resolution failure."""

    resolution = canonicalize_table_uri(
        engine,
        schema,
        table,
        database=database,
        authority=authority,
        host=host,
        port=port,
        instance=instance,
    )
    if resolution.uri is None:
        detail = resolution.issues[0].message if resolution.issues else "invalid asset URI"
        raise ValueError(detail)
    return resolution.uri


def is_aip60_mssql_asset_uri(uri: str) -> bool:
    """Return True when ``uri`` is already in canonical AIP-60 shape."""

    resolution = canonicalize_declared_mssql_uri(uri)
    return resolution.ok and resolution.uri == uri.strip()


def lineage_host_from_block(block: Mapping[str, Any]) -> str | None:
    """Deprecated: connection_ref is not Asset URI host authority."""

    return None


__all__ = [
    "MSSQL_ASSET_URI_INVALID",
    "AssetUriIssue",
    "AssetUriResolution",
    "MssqlAssetAuthority",
    "MssqlAssetAuthorityKey",
    "ResolvedMssqlAssetRegistry",
    "authority_from_connection_metadata",
    "canonical_table_uri",
    "canonicalize_declared_mssql_uri",
    "canonicalize_mssql_asset_uri",
    "canonicalize_table_uri",
    "connection_ref_from_block",
    "connection_registry_path_for_env",
    "discover_connection_registry_path",
    "is_aip60_mssql_asset_uri",
    "is_templated_value",
    "lineage_host_from_block",
    "load_connection_registry_document",
    "load_mssql_asset_authority_index",
    "load_mssql_default_database_index",
    "load_mssql_registry_indexes",
    "normalize_hostname",
    "normalize_instance",
    "parse_mssql_asset_authority",
    "resolve_explicit_mssql_uri",
    "resolve_mssql_asset_authority",
    "resolve_mssql_asset_registry",
]
