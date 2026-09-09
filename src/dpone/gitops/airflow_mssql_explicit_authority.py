"""Exact physical-authority validation for explicit ``mssql://`` Asset URIs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

from dpone.gitops.airflow_mssql_asset_authority import (
    AssetUriResolution,
    MssqlAssetAuthority,
    issue,
    normalize_hostname,
    normalize_instance,
    validate_authority_host,
)
from dpone.gitops.airflow_mssql_registry_snapshot import ResolvedMssqlAssetRegistry

_MSSQL_ENGINE = "mssql"
_DEFAULT_MSSQL_PORT = 1433


@dataclass(frozen=True, slots=True)
class MssqlAssetAuthorityKey:
    """Canonical physical authority identity: host + port + instance."""

    host: str
    port: int
    instance: str | None = None

    @classmethod
    def from_authority(cls, authority: MssqlAssetAuthority) -> MssqlAssetAuthorityKey:
        return cls(
            host=normalize_hostname(authority.host),
            port=int(authority.port),
            instance=normalize_instance(authority.instance),
        )


def resolve_explicit_mssql_uri(
    uri: str,
    *,
    registry: ResolvedMssqlAssetRegistry,
    path: str | None = None,
) -> AssetUriResolution:
    """Canonicalize an explicit MSSQL URI against full approved authorities.

    Portless URIs may adopt the sole registry authority for the host (port and
    instance). When the port is present, ``host + port + instance`` must match
    exactly (instance compared case-insensitively).
    """

    from dpone.gitops.airflow_asset_uri import canonicalize_mssql_asset_uri

    parsed = _parse_explicit_mssql_uri(uri, path=path)
    if isinstance(parsed, AssetUriResolution):
        return parsed
    host, port, instance, database, schema, table = parsed
    physical = _unique_physical_authorities(registry.authorities)
    if port is None:
        candidates = [auth for auth in physical if auth.host == host]
        if not candidates:
            return AssetUriResolution(
                uri=None,
                issues=(
                    issue(
                        f"mssql Asset URI host {host!r} is not an approved physical "
                        "authority from the environment connection registry",
                        path=path,
                    ),
                ),
            )
        if len(candidates) > 1:
            return AssetUriResolution(
                uri=None,
                issues=(
                    issue(
                        f"mssql Asset URI host {host!r} is ambiguous across multiple "
                        "approved authorities; specify port/instance explicitly",
                        path=path,
                    ),
                ),
            )
        authority = candidates[0]
        if instance is not None and normalize_instance(instance) != normalize_instance(authority.instance):
            return AssetUriResolution(
                uri=None,
                issues=(
                    issue(
                        f"mssql Asset URI instance does not match the approved authority for host {host!r}",
                        path=path,
                    ),
                ),
            )
        return canonicalize_mssql_asset_uri(
            authority=authority,
            database=database,
            schema=schema,
            table=table,
            path=path,
        )

    key = MssqlAssetAuthorityKey(host=host, port=port, instance=normalize_instance(instance))
    match = next((auth for auth in physical if MssqlAssetAuthorityKey.from_authority(auth) == key), None)
    if match is None:
        return AssetUriResolution(
            uri=None,
            issues=(
                issue(
                    "mssql Asset URI authority "
                    f"(host={host!r}, port={port}, instance={instance!r}) is not an "
                    "approved physical authority from the environment connection registry",
                    path=path,
                ),
            ),
        )
    return canonicalize_mssql_asset_uri(
        authority=match,
        database=database,
        schema=schema,
        table=table,
        path=path,
    )


def _unique_physical_authorities(
    authorities: Mapping[str, MssqlAssetAuthority],
) -> tuple[MssqlAssetAuthority, ...]:
    unique: dict[MssqlAssetAuthorityKey, MssqlAssetAuthority] = {}
    for authority in authorities.values():
        unique.setdefault(MssqlAssetAuthorityKey.from_authority(authority), authority)
    return tuple(unique.values())


def _parse_explicit_mssql_uri(
    uri: str,
    *,
    path: str | None,
) -> tuple[str, int | None, str | None, str, str, str] | AssetUriResolution:
    text = uri.strip()
    parsed = urlsplit(text)
    if parsed.scheme.lower() != _MSSQL_ENGINE:
        return AssetUriResolution(uri=None, issues=(issue("not an mssql:// URI", path=path),))
    if parsed.username or parsed.password:
        return AssetUriResolution(
            uri=None,
            issues=(issue("mssql Asset URI must not embed credentials", path=path),),
        )
    if parsed.query or parsed.fragment:
        return AssetUriResolution(
            uri=None,
            issues=(issue("mssql Asset URI must not include query or fragment", path=path),),
        )
    host = (parsed.hostname or "").strip()
    host_error = validate_authority_host(host)
    if host_error:
        return AssetUriResolution(
            uri=None,
            issues=(
                issue(
                    "compact or host-less mssql:// URIs are invalid; expected "
                    "mssql://{host}:{port}/{database}/{schema}/{table} "
                    "(optional /{INSTANCE}/ before database; port defaults from registry "
                    f"or {_DEFAULT_MSSQL_PORT})",
                    path=path,
                ),
            ),
        )
    try:
        port = parsed.port
    except ValueError:
        return AssetUriResolution(
            uri=None,
            issues=(issue("mssql Asset URI port must be an integer 1..65535", path=path),),
        )
    if port is not None and not 1 <= port <= 65535:
        return AssetUriResolution(
            uri=None,
            issues=(issue("mssql Asset URI port must be an integer 1..65535", path=path),),
        )
    parts = [unquote(part) for part in parsed.path.split("/") if part]
    if len(parts) == 4:
        instance, database, schema, table = parts[0], parts[1], parts[2], parts[3]
    elif len(parts) == 3:
        instance, database, schema, table = None, parts[0], parts[1], parts[2]
    else:
        return AssetUriResolution(
            uri=None,
            issues=(
                issue(
                    "invalid mssql Asset URI path; expected "
                    "mssql://{host}:{port}/{database}/{schema}/{table} or "
                    "mssql://{host}:{port}/{INSTANCE}/{database}/{schema}/{table}",
                    path=path,
                ),
            ),
        )
    return normalize_hostname(host), port, instance, database, schema, table


__all__ = [
    "MssqlAssetAuthorityKey",
    "resolve_explicit_mssql_uri",
]
